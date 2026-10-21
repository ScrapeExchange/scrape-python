//! A base generation: the sorted, mmap'd snapshot of every known ID.
//!
//! `ids.<gen>` holds sorted u64 IDs and `state.<gen>` the parallel
//! meta bytes, each after a [`Header`]. `terminal.<gen>` holds the
//! sorted `(id, ts)` pairs for terminal IDs. Generation 0 means
//! "empty" and has no files.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use crate::fileformat::{
    FileKind, FormatError, HEADER_LEN, Header, u32_at, u64_at,
};
use crate::fsutil::{fsync_dir, tmp_path};
use crate::meta::Meta;

pub fn ids_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("ids.{generation:08}"))
}

pub fn state_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("state.{generation:08}"))
}

pub fn terminal_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("terminal.{generation:08}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BaseInfo {
    pub generation: u64,
    pub count: u64,
    pub wal_cut: u64,
    pub state_counts: [u64; 5],
}

/// Streams a new generation to temp files; `finish` publishes it.
pub struct BaseWriter {
    dir: PathBuf,
    info: BaseInfo,
    ids: BufWriter<File>,
    states: BufWriter<File>,
    ids_crc: crc32fast::Hasher,
    states_crc: crc32fast::Hasher,
    last: Option<u64>,
}

impl BaseWriter {
    pub fn create(
        dir: &Path,
        generation: u64,
        wal_cut: u64,
    ) -> io::Result<BaseWriter> {
        fs::create_dir_all(dir)?;
        let mut ids: BufWriter<File> =
            BufWriter::new(File::create(tmp_path(&ids_path(dir, generation)))?);
        let mut states: BufWriter<File> = BufWriter::new(File::create(
            tmp_path(&state_path(dir, generation)),
        )?);
        ids.write_all(&[0; HEADER_LEN])?;
        states.write_all(&[0; HEADER_LEN])?;
        Ok(BaseWriter {
            dir: dir.to_path_buf(),
            info: BaseInfo {
                generation,
                wal_cut,
                ..BaseInfo::default()
            },
            ids,
            states,
            ids_crc: crc32fast::Hasher::new(),
            states_crc: crc32fast::Hasher::new(),
            last: None,
        })
    }

    /// Append one entry; IDs must be strictly increasing.
    pub fn push(&mut self, id: u64, meta: Meta) -> io::Result<()> {
        if self.last.is_some_and(|l| id <= l) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("id {id} not strictly increasing"),
            ));
        }
        self.last = Some(id);
        let b: [u8; 8] = id.to_le_bytes();
        self.ids.write_all(&b)?;
        self.ids_crc.update(&b);
        self.states.write_all(&[meta.0])?;
        self.states_crc.update(&[meta.0]);
        self.info.count += 1;
        self.info.state_counts[meta.state() as usize] += 1;
        Ok(())
    }

    pub fn finish(self) -> io::Result<BaseInfo> {
        let info: BaseInfo = self.info;
        let g: u64 = info.generation;
        let pairs = [
            (
                self.ids,
                self.ids_crc,
                FileKind::BaseIds,
                ids_path(&self.dir, g),
            ),
            (
                self.states,
                self.states_crc,
                FileKind::BaseState,
                state_path(&self.dir, g),
            ),
        ];
        for (w, crc, kind, final_path) in pairs {
            let mut f: File = w.into_inner().map_err(|e| e.into_error())?;
            let h: Header = Header {
                count: info.count,
                generation: g,
                wal_cut: info.wal_cut,
                state_counts: info.state_counts,
                payload_crc: crc.finalize(),
                ..Header::new(kind)
            };
            f.seek(SeekFrom::Start(0))?;
            f.write_all(&h.encode())?;
            f.sync_all()?;
            fs::rename(tmp_path(&final_path), &final_path)?;
        }
        fsync_dir(&self.dir)?;
        Ok(info)
    }
}

/// A read-only generation, mmap'd.
pub struct Base {
    info: BaseInfo,
    ids: Option<Mmap>,
    states: Option<Mmap>,
}

fn map(path: &Path, kind: FileKind) -> Result<(Header, Mmap), FormatError> {
    let f: File = File::open(path)?;
    // SAFETY: base files are immutable once renamed into place; nothing
    // writes to them while they are mapped.
    let m: Mmap = unsafe { Mmap::map(&f)? };
    let h: Header = Header::decode(&m)?;
    if h.kind != kind {
        return Err(FormatError::Kind);
    }
    Ok((h, m))
}

impl Base {
    pub fn empty() -> Base {
        Base {
            info: BaseInfo::default(),
            ids: None,
            states: None,
        }
    }

    pub fn open(dir: &Path, generation: u64) -> Result<Base, FormatError> {
        if generation == 0 {
            return Ok(Base::empty());
        }
        let (hi, ids) = map(&ids_path(dir, generation), FileKind::BaseIds)?;
        let (hs, states) =
            map(&state_path(dir, generation), FileKind::BaseState)?;
        let n: u64 = hi.count;
        if hs.count != n
            || hi.generation != generation
            || hs.generation != generation
            || ids.len() as u64 != HEADER_LEN as u64 + n * 8
            || states.len() as u64 != HEADER_LEN as u64 + n
        {
            return Err(FormatError::Inconsistent(format!(
                "generation {generation}"
            )));
        }
        ids.advise(memmap2::Advice::Random)?;
        Ok(Base {
            info: BaseInfo {
                generation,
                count: n,
                wal_cut: hi.wal_cut,
                state_counts: hi.state_counts,
            },
            ids: Some(ids),
            states: Some(states),
        })
    }

    pub fn len(&self) -> u64 {
        self.info.count
    }

    pub fn is_empty(&self) -> bool {
        self.info.count == 0
    }

    pub fn info(&self) -> &BaseInfo {
        &self.info
    }

    fn id_at(&self, i: usize) -> u64 {
        let m: &Mmap = self.ids.as_ref().expect("non-empty base");
        u64_at(m, HEADER_LEN + i * 8)
    }

    fn meta_at(&self, i: usize) -> Meta {
        let m: &Mmap = self.states.as_ref().expect("non-empty base");
        Meta(m[HEADER_LEN + i])
    }

    /// Interpolation search (IDs are uniform over u64), falling back
    /// to binary search if it stops converging.
    pub fn get(&self, id: u64) -> Option<Meta> {
        if self.is_empty() {
            return None;
        }
        let mut lo: usize = 0;
        let mut hi: usize = self.info.count as usize - 1;
        for _ in 0..8 {
            let lv: u64 = self.id_at(lo);
            let hv: u64 = self.id_at(hi);
            if id < lv || id > hv {
                return None;
            }
            if lv == hv {
                return (lv == id).then(|| self.meta_at(lo));
            }
            let span: u128 = u128::from(hv - lv);
            let off: u128 = u128::from(id - lv) * (hi - lo) as u128 / span;
            let pos: usize = lo + off as usize;
            let v: u64 = self.id_at(pos);
            if v == id {
                return Some(self.meta_at(pos));
            }
            if v < id {
                lo = pos + 1;
            } else if pos == 0 {
                return None;
            } else {
                hi = pos - 1;
            }
            if lo > hi {
                return None;
            }
        }
        while lo <= hi {
            let mid: usize = lo + (hi - lo) / 2;
            let v: u64 = self.id_at(mid);
            if v == id {
                return Some(self.meta_at(mid));
            }
            if v < id {
                lo = mid + 1;
            } else if mid == 0 {
                return None;
            } else {
                hi = mid - 1;
            }
        }
        None
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, Meta)> + '_ {
        (0..self.info.count as usize).map(|i| (self.id_at(i), self.meta_at(i)))
    }

    /// Check both payload checksums (reads the whole generation).
    pub fn verify(&self) -> Result<(), FormatError> {
        for m in [&self.ids, &self.states].into_iter().flatten() {
            let h: Header = Header::decode(m)?;
            if crc32fast::hash(&m[HEADER_LEN..]) != h.payload_crc {
                return Err(FormatError::PayloadChecksum);
            }
        }
        Ok(())
    }
}

/// Write the terminal-timestamp sidecar for *generation*.
pub fn write_terminal(
    dir: &Path,
    generation: u64,
    terminal: &HashMap<u64, u32>,
) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let mut pairs: Vec<(u64, u32)> =
        terminal.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_unstable();
    let mut payload: Vec<u8> = Vec::with_capacity(pairs.len() * 12);
    for (id, ts) in &pairs {
        payload.extend_from_slice(&id.to_le_bytes());
        payload.extend_from_slice(&ts.to_le_bytes());
    }
    let h: Header = Header {
        count: pairs.len() as u64,
        generation,
        payload_crc: crc32fast::hash(&payload),
        ..Header::new(FileKind::Terminal)
    };
    let path: PathBuf = terminal_path(dir, generation);
    let tmp: PathBuf = tmp_path(&path);
    {
        let mut f: BufWriter<File> = BufWriter::new(File::create(&tmp)?);
        f.write_all(&h.encode())?;
        f.write_all(&payload)?;
        f.into_inner().map_err(|e| e.into_error())?.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    fsync_dir(dir)
}

pub fn read_terminal(
    dir: &Path,
    generation: u64,
) -> Result<HashMap<u64, u32>, FormatError> {
    if generation == 0 {
        return Ok(HashMap::new());
    }
    let b: Vec<u8> = fs::read(terminal_path(dir, generation))?;
    let h: Header = Header::decode(&b)?;
    let payload: &[u8] = &b[HEADER_LEN..];
    if h.kind != FileKind::Terminal
        || payload.len() as u64 != h.count * 12
        || crc32fast::hash(payload) != h.payload_crc
    {
        return Err(FormatError::PayloadChecksum);
    }
    Ok(payload
        .chunks_exact(12)
        .map(|c| (u64_at(c, 0), u32_at(c, 8)))
        .collect())
}

/// Delete a generation's files, including temp files a failed write
/// left; missing files are fine.
pub fn remove_generation(dir: &Path, generation: u64) -> io::Result<()> {
    let finals: [PathBuf; 3] = [
        ids_path(dir, generation),
        state_path(dir, generation),
        terminal_path(dir, generation),
    ];
    let tmps: Vec<PathBuf> = finals.iter().map(|p| tmp_path(p)).collect();
    for p in finals.into_iter().chain(tmps) {
        match fs::remove_file(&p) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
    }
    Ok(())
}

/// Generations with a published `ids.<gen>` file, ascending.
pub fn list_generations(dir: &Path) -> io::Result<Vec<u64>> {
    let mut out: Vec<u64> = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let name: String = entry?.file_name().to_string_lossy().into_owned();
        if let Some(n) = name.strip_prefix("ids.")
            && let Ok(g) = n.parse::<u64>()
        {
            out.push(g);
        }
    }
    out.sort_unstable();
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::meta::{Reason, State};
    use std::collections::{BTreeMap, HashMap};

    /// Deterministic pseudo-random u64s (splitmix64).
    pub(crate) fn rng(seed: u64) -> impl FnMut() -> u64 {
        let mut s: u64 = seed;
        move || {
            s = s.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z: u64 = s;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }
    }

    fn meta_for(id: u64) -> Meta {
        let s: State = crate::meta::ALL_STATES[(id % 5) as usize];
        Meta::new(s, id.is_multiple_of(7), Reason::None)
    }

    fn build(dir: &Path, g: u64, ids: &BTreeMap<u64, Meta>) -> BaseInfo {
        let mut w: BaseWriter = BaseWriter::create(dir, g, 11).unwrap();
        for (id, m) in ids {
            w.push(*id, *m).unwrap();
        }
        w.finish().unwrap()
    }

    #[test]
    fn lookup_hits_and_misses() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut next = rng(1);
        let ids: BTreeMap<u64, Meta> = (0..100_000)
            .map(|_| next())
            .map(|id| (id, meta_for(id)))
            .collect();
        build(d.path(), 1, &ids);
        let b: Base = Base::open(d.path(), 1).unwrap();
        assert_eq!(b.len(), ids.len() as u64);
        for (id, m) in &ids {
            assert_eq!(b.get(*id), Some(*m));
        }
        let mut other = rng(2);
        for _ in 0..100_000 {
            let id: u64 = other();
            assert_eq!(b.get(id), ids.get(&id).copied());
        }
    }

    #[test]
    fn counts_in_info() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let ids: BTreeMap<u64, Meta> =
            (0..50u64).map(|id| (id, meta_for(id))).collect();
        let info: BaseInfo = build(d.path(), 3, &ids);
        assert_eq!(info.state_counts, [10, 10, 10, 10, 10]);
        assert_eq!((info.generation, info.count, info.wal_cut), (3, 50, 11));
        let b: Base = Base::open(d.path(), 3).unwrap();
        assert_eq!(*b.info(), info);
    }

    #[test]
    fn push_out_of_order_errors() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut w: BaseWriter = BaseWriter::create(d.path(), 1, 0).unwrap();
        let m: Meta = meta_for(1);
        w.push(5, m).unwrap();
        assert!(w.push(5, m).is_err());
        assert!(w.push(4, m).is_err());
    }

    #[test]
    fn empty_base_get_none() {
        let b: Base = Base::empty();
        assert_eq!(b.get(0), None);
        assert_eq!(b.len(), 0);
        assert_eq!(b.iter().count(), 0);
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        assert_eq!(Base::open(d.path(), 0).unwrap().len(), 0);
    }

    #[test]
    fn iter_in_order() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let ids: BTreeMap<u64, Meta> = [3u64, 9, 27]
            .iter()
            .map(|id| (*id, meta_for(*id)))
            .collect();
        build(d.path(), 1, &ids);
        let b: Base = Base::open(d.path(), 1).unwrap();
        let got: Vec<(u64, Meta)> = b.iter().collect();
        let want: Vec<(u64, Meta)> = ids.into_iter().collect();
        assert_eq!(got, want);
    }

    #[test]
    fn verify_detects_payload_flip() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let ids: BTreeMap<u64, Meta> =
            (0..100u64).map(|id| (id * 3, meta_for(id))).collect();
        build(d.path(), 1, &ids);
        Base::open(d.path(), 1).unwrap().verify().unwrap();
        let p: PathBuf = ids_path(d.path(), 1);
        let mut bytes: Vec<u8> = std::fs::read(&p).unwrap();
        bytes[HEADER_LEN + 9] ^= 0x10;
        std::fs::write(&p, bytes).unwrap();
        let b: Base = Base::open(d.path(), 1).unwrap();
        assert!(matches!(b.verify(), Err(FormatError::PayloadChecksum)));
    }

    #[test]
    fn terminal_roundtrip() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let t: HashMap<u64, u32> = [(5u64, 100u32), (1, 7), (u64::MAX, 9)]
            .into_iter()
            .collect();
        write_terminal(d.path(), 2, &t).unwrap();
        assert_eq!(read_terminal(d.path(), 2).unwrap(), t);
        assert!(read_terminal(d.path(), 0).unwrap().is_empty());
    }

    #[test]
    fn edge_ids() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let ids: BTreeMap<u64, Meta> = [0u64, 1, u64::MAX - 1, u64::MAX]
            .iter()
            .map(|id| (*id, meta_for(*id)))
            .collect();
        build(d.path(), 1, &ids);
        let b: Base = Base::open(d.path(), 1).unwrap();
        for id in ids.keys() {
            assert!(b.get(*id).is_some(), "{id}");
        }
        assert_eq!(b.get(2), None);
    }

    #[test]
    fn list_and_remove_generations() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let ids: BTreeMap<u64, Meta> = [(1u64, meta_for(1))].into();
        build(d.path(), 4, &ids);
        build(d.path(), 9, &ids);
        write_terminal(d.path(), 9, &HashMap::new()).unwrap();
        std::fs::write(d.path().join("ids.00000011.tmp"), b"").unwrap();
        assert_eq!(list_generations(d.path()).unwrap(), vec![4, 9]);
        remove_generation(d.path(), 9).unwrap();
        assert_eq!(list_generations(d.path()).unwrap(), vec![4]);
        assert!(!terminal_path(d.path(), 9).exists());
    }
}
