//! Paced writes for large background rewrites.
//!
//! A compaction streams tens of GB to disk. Left alone the kernel
//! buffers it as dirty pages and writes them back in bursts, and the
//! final `fsync` then waits for whatever is left. On a disk shared with
//! the write-ahead log those bursts queue the WAL's own `fsync` behind
//! them, so every client write stalls for as long as the burst lasts.
//!
//! [`PacedFile`] bounds that in two ways: every `sync_bytes` it starts
//! writeback of the range it just wrote and waits for it, so only a
//! small window is ever dirty; and the shared [`Pacer`] sleeps so the
//! average rate stays under `max_bytes_per_sec`. The written pages stay
//! in the page cache (nothing is evicted), so the new generation is
//! warm when it replaces the old one.

use std::fs::File;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Writeback window: how much of a file may be dirty at a time.
pub const DEFAULT_SYNC_BYTES: u64 = 64 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WritePacing {
    /// Start and wait for writeback after this many bytes; 0 disables
    /// both the windowed writeback and the rate cap.
    pub sync_bytes: u64,
    /// Average bytes per second across every file sharing the
    /// [`Pacer`]; 0 means unlimited.
    pub max_bytes_per_sec: u64,
}

impl Default for WritePacing {
    /// Windowed writeback, no rate cap.
    fn default() -> WritePacing {
        WritePacing {
            sync_bytes: DEFAULT_SYNC_BYTES,
            max_bytes_per_sec: 0,
        }
    }
}

/// The rate budget shared by the files of one rewrite.
#[derive(Debug)]
pub struct Pacer {
    pacing: WritePacing,
    /// When the bytes accounted so far have been "paid for".
    next: Mutex<Instant>,
}

impl Pacer {
    pub fn new(pacing: WritePacing) -> Arc<Pacer> {
        Arc::new(Pacer {
            pacing,
            next: Mutex::new(Instant::now()),
        })
    }

    pub fn pacing(&self) -> WritePacing {
        self.pacing
    }

    /// Account for *n* bytes just written and sleep until the average
    /// rate is back under the cap. Idle time earns no credit, so a
    /// stall is not followed by a catch-up burst.
    pub fn throttle(&self, n: u64) {
        let rate: u64 = self.pacing.max_bytes_per_sec;
        if rate == 0 || n == 0 {
            return;
        }
        let cost: Duration = Duration::from_secs_f64(n as f64 / rate as f64);
        let now: Instant = Instant::now();
        let wake: Instant = {
            let mut next = self.next.lock().expect("pacer lock");
            *next = (*next).max(now) + cost;
            *next
        };
        if wake > now {
            std::thread::sleep(wake - now);
        }
    }
}

/// A sequentially written file that bounds its dirty pages and rate.
pub struct PacedFile {
    file: File,
    pacer: Arc<Pacer>,
    written: u64,
    synced: u64,
}

impl PacedFile {
    pub fn new(file: File, pacer: Arc<Pacer>) -> PacedFile {
        PacedFile {
            file,
            pacer,
            written: 0,
            synced: 0,
        }
    }

    /// The underlying file, e.g. to patch the header and `sync_all`.
    pub fn into_file(self) -> File {
        self.file
    }

    fn write_window(&mut self) {
        let len: u64 = self.written - self.synced;
        // Best effort: an unsupported filesystem just loses the
        // windowing. Durability comes from the caller's `sync_all`.
        let _ = start_writeback(&self.file, self.synced, len);
        self.synced = self.written;
        self.pacer.throttle(len);
    }
}

impl Write for PacedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n: usize = self.file.write(buf)?;
        self.written += n as u64;
        let window: u64 = self.pacer.pacing().sync_bytes;
        if window > 0 && self.written - self.synced >= window {
            self.write_window();
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Start writeback of `[offset, offset + len)` and wait for it.
#[cfg(target_os = "linux")]
fn start_writeback(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: the fd is open for the duration of the call and no
    // memory is passed to the kernel.
    let rc: i32 = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            offset as libc::off64_t,
            len as libc::off64_t,
            libc::SYNC_FILE_RANGE_WRITE | libc::SYNC_FILE_RANGE_WAIT_AFTER,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn start_writeback(file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    file.sync_data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn paced(
        dir: &std::path::Path,
        pacing: WritePacing,
    ) -> (PacedFile, std::path::PathBuf) {
        let path = dir.join("f");
        let file: File = File::create(&path).unwrap();
        (PacedFile::new(file, Pacer::new(pacing)), path)
    }

    #[test]
    fn writes_every_byte_in_order() {
        let d = tempfile::tempdir().unwrap();
        let (mut f, path) = paced(
            d.path(),
            WritePacing {
                sync_bytes: 100,
                max_bytes_per_sec: 0,
            },
        );
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        for part in data.chunks(37) {
            f.write_all(part).unwrap();
        }
        f.flush().unwrap();
        drop(f);
        let mut got: Vec<u8> = Vec::new();
        File::open(path).unwrap().read_to_end(&mut got).unwrap();
        assert_eq!(got, data);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn writeback_syscall_succeeds() {
        // PacedFile ignores a failing hint, so check it directly.
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("f");
        let mut file: File = File::create(&path).unwrap();
        file.write_all(&[1u8; 4096 * 8]).unwrap();
        start_writeback(&file, 0, 4096 * 8).unwrap();
    }

    #[test]
    fn rate_cap_slows_the_writer() {
        let d = tempfile::tempdir().unwrap();
        let (mut f, _) = paced(
            d.path(),
            WritePacing {
                sync_bytes: 10_000,
                max_bytes_per_sec: 100_000,
            },
        );
        let t0: Instant = Instant::now();
        // 30 KB at 100 KB/s is at least 0.3 s.
        f.write_all(&[7u8; 30_000]).unwrap();
        assert!(
            t0.elapsed() >= Duration::from_millis(250),
            "{:?}",
            t0.elapsed()
        );
    }

    #[test]
    fn unlimited_does_not_sleep() {
        let d = tempfile::tempdir().unwrap();
        let (mut f, _) = paced(
            d.path(),
            WritePacing {
                sync_bytes: 10_000,
                max_bytes_per_sec: 0,
            },
        );
        let t0: Instant = Instant::now();
        f.write_all(&[7u8; 300_000]).unwrap();
        assert!(t0.elapsed() < Duration::from_millis(250));
    }

    #[test]
    fn idle_time_earns_no_credit() {
        let pacer: Arc<Pacer> = Pacer::new(WritePacing {
            sync_bytes: 1,
            max_bytes_per_sec: 100_000,
        });
        std::thread::sleep(Duration::from_millis(300));
        let t0: Instant = Instant::now();
        // The 300 ms idle must not pay for this 20 KB (0.2 s).
        pacer.throttle(20_000);
        assert!(t0.elapsed() >= Duration::from_millis(150));
    }
}
