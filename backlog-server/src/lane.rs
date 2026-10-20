//! An append-only FIFO of u64 IDs in segment files
//! `<first_pos:020>.seg`, each a [`Header`] followed by little-endian
//! u64 entries. Positions are absolute entry indexes since the lane
//! was created; the entry count comes from the file size.

pub use crate::wal::LaneId;

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::fileformat::{FileKind, HEADER_LEN, Header};
use crate::fsutil::fsync_dir;

pub const LANE_SEGMENT_ENTRIES: u64 = 8_388_608;

/// Entries read from disk per refill of the read-ahead buffer.
const READ_AHEAD: u64 = 4096;

pub fn segment_path(dir: &Path, first: u64) -> PathBuf {
    dir.join(format!("{first:020}.seg"))
}

pub fn segment_firsts(dir: &Path) -> io::Result<Vec<u64>> {
    let mut out: Vec<u64> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name: String = entry?.file_name().to_string_lossy().into_owned();
        if let Some(n) = name.strip_suffix(".seg")
            && let Ok(v) = n.parse::<u64>()
        {
            out.push(v);
        }
    }
    out.sort_unstable();
    Ok(out)
}

fn entries_in(path: &Path) -> io::Result<u64> {
    let len: u64 = fs::metadata(path)?.len();
    Ok(len.saturating_sub(HEADER_LEN as u64) / 8)
}

fn corrupt(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

pub struct Lane {
    dir: PathBuf,
    per_segment: u64,
    /// First positions of the segments on disk, ascending.
    segments: Vec<u64>,
    head: u64,
    tail: u64,
    writer: Option<File>,
    buf: Vec<u64>,
    buf_start: u64,
}

impl Lane {
    pub fn open(dir: &Path, tail: Option<u64>, head: u64) -> io::Result<Lane> {
        Lane::open_with(dir, tail, head, LANE_SEGMENT_ENTRIES)
    }

    /// Open the lane in *dir*. `tail = Some(n)` truncates it to *n*
    /// entries (recovery); *head* sets the read cursor.
    pub fn open_with(
        dir: &Path,
        tail: Option<u64>,
        head: u64,
        per_segment: u64,
    ) -> io::Result<Lane> {
        fs::create_dir_all(dir)?;
        let mut segments: Vec<u64> = segment_firsts(dir)?;
        for w in segments.windows(2) {
            let n: u64 = entries_in(&segment_path(dir, w[0]))?;
            if w[0] + n != w[1] {
                return Err(corrupt(format!("lane gap after {}", w[0])));
            }
        }
        let actual: u64 = match segments.last() {
            Some(last) => last + entries_in(&segment_path(dir, *last))?,
            None => 0,
        };
        let want: u64 = tail.unwrap_or(actual);
        if want > actual {
            return Err(corrupt(format!(
                "lane has {actual} entries, checkpoint expects {want}"
            )));
        }
        while segments.len() > 1 && *segments.last().unwrap() >= want {
            let last: u64 = segments.pop().unwrap();
            fs::remove_file(segment_path(dir, last))?;
        }
        let mut writer: Option<File> = None;
        if let Some(last) = segments.last().copied() {
            if last > want {
                return Err(corrupt(format!(
                    "lane starts at {last}, checkpoint expects {want}"
                )));
            }
            let mut f: File = OpenOptions::new()
                .read(true)
                .write(true)
                .open(segment_path(dir, last))?;
            // Always set the length: drops a torn trailing entry too.
            f.set_len(HEADER_LEN as u64 + (want - last) * 8)?;
            f.seek(SeekFrom::End(0))?;
            writer = Some(f);
        }
        let first: u64 = segments.first().copied().unwrap_or(0);
        if head < first || head > want {
            return Err(corrupt(format!(
                "head {head} outside lane [{first}, {want}]"
            )));
        }
        Ok(Lane {
            dir: dir.to_path_buf(),
            per_segment,
            segments,
            head,
            tail: want,
            writer,
            buf: Vec::new(),
            buf_start: head,
        })
    }

    pub fn head(&self) -> u64 {
        self.head
    }

    pub fn tail(&self) -> u64 {
        self.tail
    }

    pub fn len(&self) -> u64 {
        self.tail - self.head
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append *id* and return its position.
    pub fn append(&mut self, id: u64) -> io::Result<u64> {
        let full: bool = match self.segments.last() {
            Some(last) => self.tail - last >= self.per_segment,
            None => true,
        };
        if full {
            if let Some(w) = self.writer.as_mut() {
                w.sync_data()?;
            }
            self.writer = Some(self.create_segment(self.tail)?);
            self.segments.push(self.tail);
        }
        let w: &mut File = self.writer.as_mut().expect("writer exists");
        w.write_all(&id.to_le_bytes())?;
        let pos: u64 = self.tail;
        self.tail += 1;
        Ok(pos)
    }

    fn create_segment(&self, first: u64) -> io::Result<File> {
        let mut f: File = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(segment_path(&self.dir, first))?;
        f.write_all(&Header::new(FileKind::LaneSegment).encode())?;
        fsync_dir(&self.dir)?;
        Ok(f)
    }

    /// Return the entry at the head and advance it.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> io::Result<Option<(u64, u64)>> {
        if self.head >= self.tail {
            return Ok(None);
        }
        let idx: u64 = self.head.wrapping_sub(self.buf_start);
        if self.head < self.buf_start || idx >= self.buf.len() as u64 {
            self.fill_buffer()?;
        }
        let id: u64 = self.buf[(self.head - self.buf_start) as usize];
        let pos: u64 = self.head;
        self.head += 1;
        Ok(Some((pos, id)))
    }

    fn fill_buffer(&mut self) -> io::Result<()> {
        let i: usize = self.segments.partition_point(|f| *f <= self.head) - 1;
        let first: u64 = self.segments[i];
        let seg_end: u64 = self
            .segments
            .get(i + 1)
            .copied()
            .unwrap_or(self.tail)
            .min(self.tail);
        let n: u64 = (seg_end - self.head).min(READ_AHEAD);
        let mut raw: Vec<u8> = vec![0; (n * 8) as usize];
        let f: File = File::open(segment_path(&self.dir, first))?;
        let off: u64 = HEADER_LEN as u64 + (self.head - first) * 8;
        f.read_exact_at(&mut raw, off)?;
        self.buf = raw
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().expect("8 bytes")))
            .collect();
        self.buf_start = self.head;
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        match self.writer.as_mut() {
            Some(w) => w.sync_data(),
            None => Ok(()),
        }
    }

    /// Delete segments that lie wholly below *pos*. The tail segment
    /// is always kept.
    pub fn release_before(&mut self, pos: u64) -> io::Result<()> {
        let mut removed: bool = false;
        while self.segments.len() > 1 && self.segments[1] <= pos {
            let first: u64 = self.segments.remove(0);
            fs::remove_file(segment_path(&self.dir, first))?;
            removed = true;
        }
        if removed {
            fsync_dir(&self.dir)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(l: &mut Lane, ids: std::ops::Range<u64>) {
        for id in ids {
            l.append(id).unwrap();
        }
    }

    fn drain(l: &mut Lane) -> Vec<(u64, u64)> {
        std::iter::from_fn(|| l.next().unwrap()).collect()
    }

    #[test]
    fn fifo_across_segments() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut l: Lane = Lane::open_with(d.path(), None, 0, 4).unwrap();
        fill(&mut l, 100..110);
        assert_eq!((l.head(), l.tail(), l.len()), (0, 10, 10));
        let got: Vec<(u64, u64)> = drain(&mut l);
        let want: Vec<(u64, u64)> = (0..10).map(|i| (i, 100 + i)).collect();
        assert_eq!(got, want);
        assert_eq!(l.len(), 0);
    }

    #[test]
    fn reopen_with_tail_truncates() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut l: Lane = Lane::open_with(d.path(), None, 0, 4).unwrap();
            fill(&mut l, 0..10);
            l.sync().unwrap();
        }
        let mut l: Lane = Lane::open_with(d.path(), Some(6), 0, 4).unwrap();
        assert_eq!(l.tail(), 6);
        l.append(99).unwrap();
        let ids: Vec<u64> = drain(&mut l).into_iter().map(|p| p.1).collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5, 99]);
        drop(l);
        let l2: Lane = Lane::open_with(d.path(), Some(4), 0, 4).unwrap();
        assert_eq!(l2.tail(), 4);
        assert!(Lane::open_with(d.path(), Some(9), 0, 4).is_err());
    }

    #[test]
    fn reopen_with_head_resumes() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut l: Lane = Lane::open_with(d.path(), None, 0, 4).unwrap();
            fill(&mut l, 0..10);
        }
        let mut l: Lane = Lane::open_with(d.path(), None, 7, 4).unwrap();
        assert_eq!(drain(&mut l), vec![(7, 7), (8, 8), (9, 9)]);
    }

    #[test]
    fn release_before_deletes_only_whole_segments() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut l: Lane = Lane::open_with(d.path(), None, 0, 4).unwrap();
        fill(&mut l, 0..10);
        drain(&mut l);
        l.release_before(5).unwrap();
        assert_eq!(segment_firsts(d.path()).unwrap(), vec![4, 8]);
        l.release_before(10).unwrap();
        // The tail segment always stays.
        assert_eq!(segment_firsts(d.path()).unwrap(), vec![8]);
        drop(l);
        let mut l: Lane = Lane::open_with(d.path(), None, 10, 4).unwrap();
        assert_eq!(l.tail(), 10);
        l.append(42).unwrap();
        assert_eq!(drain(&mut l), vec![(10, 42)]);
        assert!(Lane::open_with(d.path(), None, 3, 4).is_err());
    }

    #[test]
    fn next_on_empty_is_none() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut l: Lane = Lane::open(d.path(), None, 0).unwrap();
        assert_eq!(l.next().unwrap(), None);
        assert_eq!(l.len(), 0);
    }

    #[test]
    fn append_after_drain_readable() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut l: Lane = Lane::open_with(d.path(), None, 0, 4).unwrap();
        fill(&mut l, 0..3);
        assert_eq!(drain(&mut l).len(), 3);
        fill(&mut l, 3..6);
        assert_eq!(drain(&mut l), vec![(3, 3), (4, 4), (5, 5)]);
    }

    #[test]
    fn torn_entry_dropped_on_open() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut l: Lane = Lane::open_with(d.path(), None, 0, 4).unwrap();
            fill(&mut l, 0..2);
        }
        let p: PathBuf = segment_path(d.path(), 0);
        let len: u64 = std::fs::metadata(&p).unwrap().len();
        let f: File = OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(len - 3).unwrap();
        let l: Lane = Lane::open_with(d.path(), None, 0, 4).unwrap();
        assert_eq!(l.tail(), 1);
    }
}
