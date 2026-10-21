//! Filesystem helpers: atomic replace, directory fsync, cleanup and
//! data-directory checks.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// The temp-file name used while a file is being replaced atomically.
pub fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Write *bytes* to `<path>.tmp`, fsync it, rename it over *path*
/// and fsync the parent directory.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp: PathBuf = tmp_path(path);
    {
        let mut f: File = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(path.parent().unwrap_or(Path::new(".")))
}

pub fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Recursively delete `*.tmp` files under *dir*.
pub fn remove_tmp_files(dir: &Path) -> io::Result<usize> {
    let mut removed: usize = 0;
    if !dir.exists() {
        return Ok(0);
    }
    for entry in fs::read_dir(dir)? {
        let entry: fs::DirEntry = entry?;
        let p: PathBuf = entry.path();
        if entry.file_type()?.is_dir() {
            removed += remove_tmp_files(&p)?;
        } else if p.extension().is_some_and(|e| e == "tmp") {
            fs::remove_file(&p)?;
            removed += 1;
        }
    }
    Ok(removed)
}

#[derive(Debug, thiserror::Error)]
pub enum DataDirError {
    #[error("refusing to run as root")]
    RunningAsRoot,
    #[error("data directory does not exist: {0}")]
    Missing(PathBuf),
    #[error("data directory not writable: {0}: {1}")]
    NotWritable(PathBuf, io::Error),
}

/// Fail unless the process runs as a non-root user that can create
/// files in *dir*.
pub fn check_data_dir(dir: &Path) -> Result<(), DataDirError> {
    // SAFETY: geteuid has no preconditions and cannot fail.
    if unsafe { libc::geteuid() } == 0 {
        return Err(DataDirError::RunningAsRoot);
    }
    if !dir.is_dir() {
        return Err(DataDirError::Missing(dir.to_path_buf()));
    }
    let probe: PathBuf = dir.join(".write-probe");
    let r: io::Result<()> = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&probe)
        .and_then(|_| fs::remove_file(&probe));
    r.map_err(|e| DataDirError::NotWritable(dir.to_path_buf(), e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn write_atomic_replaces() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let p: PathBuf = d.path().join("CHECKPOINT");
        write_atomic(&p, b"one").unwrap();
        write_atomic(&p, b"two").unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"two");
        assert!(!d.path().join("CHECKPOINT.tmp").exists());
    }

    #[test]
    fn remove_tmp_files_counts() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        fs::create_dir(d.path().join("sub")).unwrap();
        fs::write(d.path().join("a.tmp"), b"").unwrap();
        fs::write(d.path().join("sub/b.tmp"), b"").unwrap();
        fs::write(d.path().join("c.seg"), b"").unwrap();
        assert_eq!(remove_tmp_files(d.path()).unwrap(), 2);
        assert!(d.path().join("c.seg").exists());
        assert!(!d.path().join("sub/b.tmp").exists());
    }

    #[test]
    fn check_data_dir_missing() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let r = check_data_dir(&d.path().join("nope"));
        if unsafe { libc::geteuid() } == 0 {
            assert!(matches!(r, Err(DataDirError::RunningAsRoot)));
        } else {
            assert!(matches!(r, Err(DataDirError::Missing(_))));
        }
    }

    #[test]
    fn check_data_dir_ok() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        if unsafe { libc::geteuid() } != 0 {
            check_data_dir(d.path()).unwrap();
        }
    }
}
