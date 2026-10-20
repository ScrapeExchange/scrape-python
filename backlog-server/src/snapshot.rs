//! Hard-link snapshots: after a checkpoint, link the files it refers
//! to into a new directory that can be copied off and restored as a
//! queue directory.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::base::{ids_path, state_path, terminal_path};
use crate::fsutil::fsync_dir;

/// `YYYYmmddTHHMMSSZ` for a unix time (UTC).
pub fn utc_stamp(secs: u64) -> String {
    let days: i64 = (secs / 86_400) as i64;
    let rem: u64 = secs % 86_400;
    // Civil-from-days (Howard Hinnant).
    let z: i64 = days + 719_468;
    let era: i64 = z.div_euclid(146_097);
    let doe: i64 = z - era * 146_097;
    let yoe: i64 = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy: i64 = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp: i64 = (5 * doy + 2) / 153;
    let d: i64 = doy - (153 * mp + 2) / 5 + 1;
    let m: i64 = if mp < 10 { mp + 3 } else { mp - 9 };
    let y: i64 = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

fn link_dir_files(src: &Path, dst: &Path, ext: &str) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    if src.exists() {
        for e in fs::read_dir(src)?.flatten() {
            let p: PathBuf = e.path();
            if p.extension().is_some_and(|x| x == ext) {
                fs::hard_link(&p, dst.join(e.file_name()))?;
            }
        }
    }
    fsync_dir(dst)
}

/// Link `CHECKPOINT`, generation *generation*'s base files, all lane
/// segments and all WAL segments of *queue_dir* into a fresh
/// directory under `<root>/<queue dir name>/`.
pub fn link_snapshot(
    queue_dir: &Path,
    generation: u64,
    root: &Path,
) -> io::Result<PathBuf> {
    let name = queue_dir.file_name().unwrap_or_default();
    let parent: PathBuf = root.join(name);
    fs::create_dir_all(&parent)?;
    let secs: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let stamp: String = utc_stamp(secs);
    let mut dest: PathBuf = parent.join(&stamp);
    let mut n: u32 = 1;
    while dest.exists() {
        dest = parent.join(format!("{stamp}-{n}"));
        n += 1;
    }
    fs::create_dir(&dest)?;
    // The checkpoint file is replaced, never rewritten, so a link
    // pins exactly the version just written.
    fs::hard_link(queue_dir.join("CHECKPOINT"), dest.join("CHECKPOINT"))?;
    let src_base: PathBuf = queue_dir.join("base");
    let dst_base: PathBuf = dest.join("base");
    fs::create_dir_all(&dst_base)?;
    if generation != 0 {
        for f in [
            ids_path(&src_base, generation),
            state_path(&src_base, generation),
            terminal_path(&src_base, generation),
        ] {
            let file = f.file_name().unwrap_or_default();
            fs::hard_link(&f, dst_base.join(file))?;
        }
    }
    fsync_dir(&dst_base)?;
    for lane in ["main", "priority"] {
        let rel: PathBuf = Path::new("lanes").join(lane);
        link_dir_files(&queue_dir.join(&rel), &dest.join(&rel), "seg")?;
    }
    link_dir_files(&queue_dir.join("wal"), &dest.join("wal"), "log")?;
    fsync_dir(&dest)?;
    fsync_dir(&parent)?;
    Ok(dest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_format() {
        assert_eq!(utc_stamp(0), "19700101T000000Z");
        assert_eq!(utc_stamp(1_792_496_096), "20261020T113456Z");
        assert_eq!(utc_stamp(951_782_400), "20000229T000000Z");
    }
}
