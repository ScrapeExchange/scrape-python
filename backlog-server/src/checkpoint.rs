//! The `CHECKPOINT` file: which generation to load, where to replay
//! the change log from, and each lane's tail and resume cursor.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::fsutil::write_atomic;

pub const CHECKPOINT_VERSION: u32 = 1;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
pub struct LaneCkpt {
    pub tail: u64,
    pub resume: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub version: u32,
    pub generation: u64,
    /// Puts are replayed from here (the base generation's WAL cut).
    pub replay_from_lsn: u64,
    /// Lane appends at or after this LSN are not yet in lane files.
    pub lanes_as_of_lsn: u64,
    pub main: LaneCkpt,
    pub priority: LaneCkpt,
}

impl Default for Checkpoint {
    fn default() -> Checkpoint {
        Checkpoint {
            version: CHECKPOINT_VERSION,
            generation: 0,
            replay_from_lsn: 0,
            lanes_as_of_lsn: 0,
            main: LaneCkpt::default(),
            priority: LaneCkpt::default(),
        }
    }
}

pub fn path(dir: &Path) -> PathBuf {
    dir.join("CHECKPOINT")
}

pub fn write(dir: &Path, c: &Checkpoint) -> io::Result<()> {
    let body: Vec<u8> = serde_json::to_vec_pretty(c)?;
    write_atomic(&path(dir), &body)
}

pub fn read(dir: &Path) -> io::Result<Option<Checkpoint>> {
    match std::fs::read(path(dir)) {
        Ok(b) => {
            let c: Checkpoint = serde_json::from_slice(&b)?;
            if c.version != CHECKPOINT_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported checkpoint version {}", c.version),
                ));
            }
            Ok(Some(c))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_missing() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        assert_eq!(read(d.path()).unwrap(), None);
        let c: Checkpoint = Checkpoint {
            generation: 3,
            main: LaneCkpt { tail: 9, resume: 2 },
            ..Checkpoint::default()
        };
        write(d.path(), &c).unwrap();
        assert_eq!(read(d.path()).unwrap(), Some(c));
    }
}
