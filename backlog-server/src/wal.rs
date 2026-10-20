//! The change log: fixed 24-byte records in segment files
//! `wal/<first_lsn:020>.log`, each after a [`Header`] whose `wal_cut`
//! field holds the segment's first LSN. An LSN is a record's global
//! index.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::fileformat::{FileKind, HEADER_LEN, Header, u32_at, u64_at};
use crate::fsutil::fsync_dir;
use crate::meta::Meta;

pub const RECORD_LEN: usize = 24;
pub const WAL_SEGMENT_RECORDS: u64 = 4_194_304;

/// The two FIFO lanes of a queue.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LaneId {
    Main = 0,
    Priority = 1,
}

impl LaneId {
    pub fn name(&self) -> &'static str {
        match self {
            LaneId::Main => "main",
            LaneId::Priority => "priority",
        }
    }

    fn from_u8(v: u8) -> Option<LaneId> {
        match v {
            0 => Some(LaneId::Main),
            1 => Some(LaneId::Priority),
            _ => None,
        }
    }
}

/// A physical change: set an ID's meta, or append it to a lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Record {
    Put { id: u64, meta: Meta, ts: u32 },
    Append { lane: LaneId, id: u64 },
}

const TAG_PUT: u8 = 1;
const TAG_APPEND: u8 = 2;

impl Record {
    pub fn encode(&self) -> [u8; RECORD_LEN] {
        let mut b: [u8; RECORD_LEN] = [0; RECORD_LEN];
        let (tag, small, ts, id) = match *self {
            Record::Put { id, meta, ts } => (TAG_PUT, meta.0, ts, id),
            Record::Append { lane, id } => (TAG_APPEND, lane as u8, 0, id),
        };
        b[0] = tag;
        b[1] = small;
        b[4..8].copy_from_slice(&ts.to_le_bytes());
        b[8..16].copy_from_slice(&id.to_le_bytes());
        let crc: u32 = crc32fast::hash(&b[..16]);
        b[16..20].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// None for a torn or corrupt record.
    pub fn decode(b: &[u8]) -> Option<Record> {
        if b.len() < RECORD_LEN || crc32fast::hash(&b[..16]) != u32_at(b, 16) {
            return None;
        }
        let id: u64 = u64_at(b, 8);
        match b[0] {
            TAG_PUT => Some(Record::Put {
                id,
                meta: Meta(b[1]),
                ts: u32_at(b, 4),
            }),
            TAG_APPEND => Some(Record::Append {
                lane: LaneId::from_u8(b[1])?,
                id,
            }),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("change log corrupt: {0}")]
    Corrupt(String),
    #[error("change log poisoned by an earlier unrecoverable write error")]
    Poisoned,
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub fn segment_path(dir: &Path, first_lsn: u64) -> PathBuf {
    dir.join(format!("{first_lsn:020}.log"))
}

/// First LSNs of all segments, ascending.
pub fn segment_firsts(dir: &Path) -> io::Result<Vec<u64>> {
    let mut out: Vec<u64> = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let name: String = entry?.file_name().to_string_lossy().into_owned();
        if let Some(n) = name.strip_suffix(".log")
            && let Ok(v) = n.parse::<u64>()
        {
            out.push(v);
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Valid records of one segment, and whether the file had anything
/// after them (a torn or corrupt tail).
fn read_segment(
    dir: &Path,
    first: u64,
) -> Result<(Vec<Record>, bool), WalError> {
    let mut b: Vec<u8> = Vec::new();
    File::open(segment_path(dir, first))?.read_to_end(&mut b)?;
    let h: Header = Header::decode(&b)
        .map_err(|e| WalError::Corrupt(format!("segment {first}: {e}")))?;
    if h.kind != FileKind::Wal || h.wal_cut != first {
        return Err(WalError::Corrupt(format!("segment {first}: header")));
    }
    let body: &[u8] = &b[HEADER_LEN..];
    let mut out: Vec<Record> = Vec::with_capacity(body.len() / RECORD_LEN);
    for chunk in body.chunks(RECORD_LEN) {
        match Record::decode(chunk) {
            Some(r) => out.push(r),
            None => return Ok((out, true)),
        }
    }
    Ok((out, false))
}

/// All records with LSN >= *from_lsn*, in order.
pub fn replay(
    dir: &Path,
    from_lsn: u64,
) -> Result<Vec<(u64, Record)>, WalError> {
    let firsts: Vec<u64> = segment_firsts(dir)?;
    let mut out: Vec<(u64, Record)> = Vec::new();
    for (i, first) in firsts.iter().enumerate() {
        if firsts.get(i + 1).is_some_and(|next| *next <= from_lsn) {
            continue;
        }
        let (recs, _) = read_segment(dir, *first)?;
        for (j, r) in recs.into_iter().enumerate() {
            let lsn: u64 = first + j as u64;
            if lsn >= from_lsn {
                out.push((lsn, r));
            }
        }
    }
    Ok(out)
}

pub struct Wal {
    dir: PathBuf,
    per_segment: u64,
    file: File,
    first: u64,
    in_segment: u64,
    poisoned: bool,
    #[cfg(test)]
    fail_next: bool,
}

impl Wal {
    pub fn open(dir: &Path) -> Result<Wal, WalError> {
        Wal::open_with(dir, WAL_SEGMENT_RECORDS)
    }

    /// Open with a custom segment size (tests).
    pub fn open_with(dir: &Path, per_segment: u64) -> Result<Wal, WalError> {
        fs::create_dir_all(dir)?;
        let firsts: Vec<u64> = segment_firsts(dir)?;
        let Some(&last) = firsts.last() else {
            let file: File = create_segment(dir, 0)?;
            return Ok(Wal::from_parts(dir, per_segment, file, 0, 0));
        };
        for (i, first) in firsts[..firsts.len() - 1].iter().enumerate() {
            let (recs, torn) = read_segment(dir, *first)?;
            if torn || first + recs.len() as u64 != firsts[i + 1] {
                return Err(WalError::Corrupt(format!(
                    "segment {first} is not a complete prefix"
                )));
            }
        }
        let (recs, torn) = read_segment(dir, last)?;
        let n: u64 = recs.len() as u64;
        let mut file: File = OpenOptions::new()
            .write(true)
            .open(segment_path(dir, last))?;
        if torn {
            file.set_len(HEADER_LEN as u64 + n * RECORD_LEN as u64)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Wal::from_parts(dir, per_segment, file, last, n))
    }

    fn from_parts(
        dir: &Path,
        per_segment: u64,
        file: File,
        first: u64,
        in_segment: u64,
    ) -> Wal {
        Wal {
            dir: dir.to_path_buf(),
            per_segment,
            file,
            first,
            in_segment,
            poisoned: false,
            #[cfg(test)]
            fail_next: false,
        }
    }

    pub fn next_lsn(&self) -> u64 {
        self.first + self.in_segment
    }

    /// Append one record and return its LSN. On a write error the
    /// segment is cut back to its last whole record.
    pub fn append(&mut self, r: &Record) -> Result<u64, WalError> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }
        if self.in_segment >= self.per_segment {
            self.rotate()?;
        }
        let good: u64 = HEADER_LEN as u64 + self.in_segment * RECORD_LEN as u64;
        let res: io::Result<()> = self.write_record(r);
        if let Err(e) = res {
            let rollback: io::Result<u64> = self
                .file
                .set_len(good)
                .and_then(|_| self.file.seek(SeekFrom::Start(good)));
            if rollback.is_err() {
                self.poisoned = true;
            }
            return Err(WalError::Io(e));
        }
        let lsn: u64 = self.next_lsn();
        self.in_segment += 1;
        Ok(lsn)
    }

    fn write_record(&mut self, r: &Record) -> io::Result<()> {
        let b: [u8; RECORD_LEN] = r.encode();
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next) {
            // Simulate a short write followed by an error.
            self.file.write_all(&b[..10])?;
            return Err(io::Error::other("injected write failure"));
        }
        self.file.write_all(&b)
    }

    fn rotate(&mut self) -> Result<(), WalError> {
        self.file.sync_data()?;
        let next: u64 = self.next_lsn();
        self.file = create_segment(&self.dir, next)?;
        self.first = next;
        self.in_segment = 0;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Delete segments whose records all lie below *lsn*. The current
    /// segment is never deleted.
    pub fn truncate_before(&mut self, lsn: u64) -> Result<(), WalError> {
        let firsts: Vec<u64> = segment_firsts(&self.dir)?;
        for (i, first) in firsts.iter().enumerate() {
            let Some(next) = firsts.get(i + 1) else { break };
            if *next <= lsn {
                fs::remove_file(segment_path(&self.dir, *first))?;
            }
        }
        fsync_dir(&self.dir)?;
        Ok(())
    }

    #[cfg(test)]
    pub fn fail_next_write(&mut self) {
        self.fail_next = true;
    }
}

fn create_segment(dir: &Path, first: u64) -> io::Result<File> {
    let mut f: File = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(segment_path(dir, first))?;
    let h: Header = Header {
        wal_cut: first,
        ..Header::new(FileKind::Wal)
    };
    f.write_all(&h.encode())?;
    f.sync_all()?;
    fsync_dir(dir)?;
    Ok(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{Reason, State};

    fn put(id: u64) -> Record {
        Record::Put {
            id,
            meta: Meta::new(State::Queued, false, Reason::None),
            ts: id as u32,
        }
    }

    fn sample(n: u64) -> Vec<Record> {
        (0..n)
            .map(|i| {
                if i % 2 == 0 {
                    put(i)
                } else {
                    Record::Append {
                        lane: LaneId::Priority,
                        id: i,
                    }
                }
            })
            .collect()
    }

    #[test]
    fn append_replay_roundtrip() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let recs: Vec<Record> = sample(10);
        {
            let mut w: Wal = Wal::open(d.path()).unwrap();
            for (i, r) in recs.iter().enumerate() {
                assert_eq!(w.append(r).unwrap(), i as u64);
            }
            w.sync().unwrap();
        }
        let all: Vec<(u64, Record)> = replay(d.path(), 0).unwrap();
        assert_eq!(all.len(), 10);
        assert_eq!(all[3], (3, recs[3]));
        let tail: Vec<(u64, Record)> = replay(d.path(), 4).unwrap();
        assert_eq!(tail.first(), Some(&(4, recs[4])));
        assert_eq!(tail.len(), 6);
        assert_eq!(Wal::open(d.path()).unwrap().next_lsn(), 10);
    }

    #[test]
    fn rotation_and_truncate_before() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut w: Wal = Wal::open_with(d.path(), 4).unwrap();
        for r in sample(10) {
            w.append(&r).unwrap();
        }
        assert_eq!(segment_firsts(d.path()).unwrap(), vec![0, 4, 8]);
        w.truncate_before(5).unwrap();
        assert_eq!(segment_firsts(d.path()).unwrap(), vec![4, 8]);
        w.truncate_before(100).unwrap();
        assert_eq!(segment_firsts(d.path()).unwrap(), vec![8]);
        let left: Vec<(u64, Record)> = replay(d.path(), 0).unwrap();
        assert_eq!(left.iter().map(|(l, _)| *l).collect::<Vec<_>>(), [8, 9]);
        drop(w);
        let w2: Wal = Wal::open_with(d.path(), 4).unwrap();
        assert_eq!(w2.next_lsn(), 10);
    }

    #[test]
    fn torn_tail_truncated() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut w: Wal = Wal::open(d.path()).unwrap();
            for r in sample(3) {
                w.append(&r).unwrap();
            }
        }
        let p: PathBuf = segment_path(d.path(), 0);
        let len: u64 = std::fs::metadata(&p).unwrap().len();
        let f: File = OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(len - 7).unwrap();
        let mut w: Wal = Wal::open(d.path()).unwrap();
        assert_eq!(w.next_lsn(), 2);
        assert_eq!(replay(d.path(), 0).unwrap().len(), 2);
        assert_eq!(w.append(&put(77)).unwrap(), 2);
        assert_eq!(replay(d.path(), 2).unwrap(), vec![(2, put(77))]);
    }

    #[test]
    fn corrupt_middle_segment_errors() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut w: Wal = Wal::open_with(d.path(), 4).unwrap();
            for r in sample(10) {
                w.append(&r).unwrap();
            }
        }
        let p: PathBuf = segment_path(d.path(), 4);
        let mut b: Vec<u8> = std::fs::read(&p).unwrap();
        b[HEADER_LEN + RECORD_LEN + 9] ^= 1;
        std::fs::write(&p, b).unwrap();
        assert!(matches!(
            Wal::open_with(d.path(), 4),
            Err(WalError::Corrupt(_))
        ));
    }

    #[test]
    fn append_failure_rolls_back() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut w: Wal = Wal::open(d.path()).unwrap();
        w.append(&put(1)).unwrap();
        w.fail_next_write();
        assert!(w.append(&put(2)).is_err());
        assert_eq!(w.next_lsn(), 1);
        assert_eq!(w.append(&put(3)).unwrap(), 1);
        w.sync().unwrap();
        assert_eq!(
            replay(d.path(), 0).unwrap(),
            vec![(0, put(1)), (1, put(3))]
        );
        drop(w);
        assert_eq!(Wal::open(d.path()).unwrap().next_lsn(), 2);
    }
}
