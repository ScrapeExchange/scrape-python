//! One named queue: known set, lanes, leases and change log.
//!
//! Every mutation is written to the change log first, then to the
//! lane file, then applied in memory, so a failed write leaves memory
//! unchanged.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::base::{
    Base, BaseInfo, list_generations, read_terminal, remove_generation,
};
use crate::checkpoint::{self, Checkpoint, LaneCkpt};
use crate::compaction::CompactionJob;
use crate::fileformat::FormatError;
use crate::fsutil::remove_tmp_files;
use crate::known::KnownSet;
use crate::lane::{Lane, LaneId};
use crate::leases::Leases;
use crate::meta::{Meta, Reason, State};
use crate::wal::{Record, Wal, WalError, replay};

#[derive(Debug, Clone, Copy)]
pub struct QueueParams {
    pub lease_default: Duration,
    pub pop_scan_limit: usize,
    pub compaction_interval: Duration,
    pub compaction_delta_entries: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForceOutcome {
    Added,
    Revived,
    ForcedPending,
}

impl ForceOutcome {
    pub fn name(&self) -> &'static str {
        match self {
            ForceOutcome::Added => "added",
            ForceOutcome::Revived => "revived",
            ForceOutcome::ForcedPending => "forced_pending",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PopItem {
    pub id: u64,
    pub force: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueInfo {
    pub counts: [u64; 5],
    pub leased: u64,
    pub lane_main: u64,
    pub lane_priority: u64,
    pub base_entries: u64,
    pub delta_entries: u64,
    pub generation: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("bad state")]
    BadState,
    #[error(transparent)]
    Wal(#[from] WalError),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Take an exclusive, non-blocking lock on `<dir>/LOCK`; held until
/// the returned file is dropped.
pub fn lock_dir(dir: &Path) -> io::Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    let f: std::fs::File = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("LOCK"))?;
    // SAFETY: flock on a valid open descriptor has no other effects.
    let rc: i32 =
        unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("queue directory {dir:?} is in use by another process"),
        ));
    }
    Ok(f)
}

pub struct Queue {
    _lock: std::fs::File,
    dir: PathBuf,
    params: QueueParams,
    known: KnownSet,
    wal: Wal,
    main: Lane,
    priority: Lane,
    leases: Leases,
    replayed: usize,
    /// (generation, wal_cut) of the compaction in flight.
    compacting: Option<(u64, u64)>,
    last_compaction: Instant,
}

fn queued(force: bool) -> Meta {
    Meta::new(State::Queued, force, Reason::None)
}

impl Queue {
    /// Open (or create) the queue in *dir* and recover it: load the
    /// checkpointed generation, cut lanes back to the checkpoint and
    /// replay the change log.
    pub fn open(dir: &Path, params: QueueParams) -> Result<Queue, QueueError> {
        std::fs::create_dir_all(dir)?;
        let lock: std::fs::File = lock_dir(dir)?;
        remove_tmp_files(dir)?;
        let c: Checkpoint = checkpoint::read(dir)?.unwrap_or_default();
        let base_dir: PathBuf = dir.join("base");
        for g in list_generations(&base_dir)? {
            if g != c.generation {
                remove_generation(&base_dir, g)?;
            }
        }
        let base: Arc<Base> = Arc::new(Base::open(&base_dir, c.generation)?);
        let terminal: HashMap<u64, u32> =
            read_terminal(&base_dir, c.generation)?;
        let main: Lane = Lane::open(
            &dir.join("lanes/main"),
            Some(c.main.tail),
            c.main.resume,
        )?;
        let priority: Lane = Lane::open(
            &dir.join("lanes/priority"),
            Some(c.priority.tail),
            c.priority.resume,
        )?;
        let wal_dir: PathBuf = dir.join("wal");
        let wal: Wal = Wal::open(&wal_dir)?;
        let mut q: Queue = Queue {
            _lock: lock,
            dir: dir.to_path_buf(),
            params,
            known: KnownSet::new(base, terminal),
            wal,
            main,
            priority,
            leases: Leases::default(),
            replayed: 0,
            compacting: None,
            last_compaction: Instant::now(),
        };
        let records: Vec<(u64, Record)> = replay(&wal_dir, c.replay_from_lsn)?;
        q.replayed = records.len();
        for (lsn, r) in records {
            let in_lane: bool = lsn < c.lanes_as_of_lsn;
            match r {
                Record::Put { id, meta, ts } => q.known.put(id, meta, ts),
                Record::Append { lane, id } => {
                    if !in_lane {
                        q.lane_mut(lane).append(id)?;
                    }
                }
                Record::PutAppend { id, meta, ts, lane } => {
                    q.known.put(id, meta, ts);
                    if !in_lane {
                        q.lane_mut(lane).append(id)?;
                    }
                }
            }
        }
        Ok(q)
    }

    /// Records replayed by the last [`Queue::open`].
    pub fn replayed(&self) -> usize {
        self.replayed
    }

    /// Make everything durable and record where recovery starts, then
    /// drop lane segments that are wholly consumed.
    pub fn write_checkpoint(&mut self) -> Result<(), QueueError> {
        self.sync()?;
        let main_resume: u64 = self.resume_cursor(LaneId::Main);
        let prio_resume: u64 = self.resume_cursor(LaneId::Priority);
        let c: Checkpoint = Checkpoint {
            generation: self.known.base().info().generation,
            replay_from_lsn: self.known.base().info().wal_cut,
            lanes_as_of_lsn: self.wal.next_lsn(),
            main: LaneCkpt {
                tail: self.main.tail(),
                resume: main_resume,
            },
            priority: LaneCkpt {
                tail: self.priority.tail(),
                resume: prio_resume,
            },
            ..Checkpoint::default()
        };
        checkpoint::write(&self.dir, &c)?;
        self.main.release_before(main_resume)?;
        self.priority.release_before(prio_resume)?;
        Ok(())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn params(&self) -> &QueueParams {
        &self.params
    }

    fn lane_mut(&mut self, lane: LaneId) -> &mut Lane {
        match lane {
            LaneId::Main => &mut self.main,
            LaneId::Priority => &mut self.priority,
        }
    }

    fn lane(&self, lane: LaneId) -> &Lane {
        match lane {
            LaneId::Main => &self.main,
            LaneId::Priority => &self.priority,
        }
    }

    /// Log and apply a meta change.
    fn put(&mut self, id: u64, meta: Meta, ts: u32) -> Result<(), QueueError> {
        self.wal.append(&Record::Put { id, meta, ts })?;
        self.known.put(id, meta, ts);
        Ok(())
    }

    /// Log and apply a meta change plus a lane append, atomically.
    fn put_append(
        &mut self,
        id: u64,
        meta: Meta,
        ts: u32,
        lane: LaneId,
    ) -> Result<(), QueueError> {
        self.wal.append(&Record::PutAppend { id, meta, ts, lane })?;
        self.lane_mut(lane).append(id)?;
        self.known.put(id, meta, ts);
        Ok(())
    }

    pub fn meta(&self, id: u64) -> Option<Meta> {
        self.known.get(id)
    }

    pub fn state(&self, id: u64) -> Option<State> {
        self.known.get(id).map(|m| m.state())
    }

    /// Queue a new ID; false if it is already known in any state.
    pub fn add(&mut self, id: u64, ts: u32) -> Result<bool, QueueError> {
        if self.known.get(id).is_some() {
            return Ok(false);
        }
        self.put_append(id, queued(false), ts, LaneId::Main)?;
        Ok(true)
    }

    pub fn force(
        &mut self,
        id: u64,
        ts: u32,
    ) -> Result<ForceOutcome, QueueError> {
        let before: Option<State> = self.state(id);
        if self.leases.contains(id) {
            self.put(id, queued(true), ts)?;
        } else {
            self.put_append(id, queued(true), ts, LaneId::Priority)?;
        }
        Ok(match before {
            Some(State::Queued) => ForceOutcome::ForcedPending,
            Some(_) => ForceOutcome::Revived,
            None => ForceOutcome::Added,
        })
    }

    pub fn pop(
        &mut self,
        n: usize,
        lease: Duration,
        now: Instant,
    ) -> Result<Vec<PopItem>, QueueError> {
        let mut out: Vec<PopItem> = Vec::new();
        let mut scanned: usize = 0;
        let deadline: Instant = now + lease;
        for lane in [LaneId::Priority, LaneId::Main] {
            while out.len() < n && scanned < self.params.pop_scan_limit {
                let Some((pos, id)) = self.lane_mut(lane).next()? else {
                    break;
                };
                scanned += 1;
                if self.leases.contains(id) {
                    continue;
                }
                match self.known.get(id) {
                    Some(m) if m.state() == State::Queued => {
                        self.leases.insert(id, lane, pos, deadline);
                        out.push(PopItem {
                            id,
                            force: m.force(),
                        });
                    }
                    _ => {}
                }
            }
        }
        Ok(out)
    }

    pub fn touch(&mut self, id: u64, lease: Duration, now: Instant) -> bool {
        self.leases.touch(id, now + lease)
    }

    pub fn complete(&mut self, id: u64, ts: u32) -> Result<(), QueueError> {
        let m: Meta = Meta::new(State::Scraped, false, Reason::None);
        self.put(id, m, ts)?;
        self.leases.remove(id);
        Ok(())
    }

    pub fn mark(
        &mut self,
        id: u64,
        state: State,
        reason: Reason,
        ts: u32,
    ) -> Result<(), QueueError> {
        if !state.is_terminal() {
            return Err(QueueError::BadState);
        }
        self.put(id, Meta::new(state, false, reason), ts)?;
        self.leases.remove(id);
        Ok(())
    }

    /// Queue a scraped, terminal or unknown ID again. False if it is
    /// already queued (a queued ID is always leased or in a lane).
    pub fn requeue(
        &mut self,
        id: u64,
        lane: LaneId,
        ts: u32,
    ) -> Result<bool, QueueError> {
        if self.state(id) == Some(State::Queued) {
            return Ok(false);
        }
        self.put_append(id, queued(false), ts, lane)?;
        Ok(true)
    }

    pub fn info(&self) -> QueueInfo {
        let b: &Base = self.known.base();
        QueueInfo {
            counts: self.known.counts(),
            leased: self.leases.len() as u64,
            lane_main: self.main.len(),
            lane_priority: self.priority.len(),
            base_entries: b.len(),
            delta_entries: self.known.delta_len() as u64,
            generation: b.info().generation,
        }
    }

    /// Re-queue expired leases at the back of the priority lane. Each
    /// lease is dropped only once its re-queue is in the change log.
    pub fn expire_leases(&mut self, now: Instant) -> Result<usize, QueueError> {
        let expired: Vec<(u64, LaneId)> = self.leases.expired(now);
        for (id, _) in &expired {
            let r: Record = Record::Append {
                lane: LaneId::Priority,
                id: *id,
            };
            self.wal.append(&r)?;
            self.priority.append(*id)?;
            self.leases.remove(*id);
        }
        Ok(expired.len())
    }

    pub fn resume_cursor(&self, lane: LaneId) -> u64 {
        self.leases
            .min_pos(lane)
            .unwrap_or_else(|| self.lane(lane).head())
    }

    pub fn sync(&mut self) -> Result<(), QueueError> {
        self.wal.sync()?;
        self.main.sync()?;
        self.priority.sync()?;
        Ok(())
    }

    pub fn should_compact(&self, now: Instant) -> bool {
        let n: usize = self.known.delta_len();
        self.compacting.is_none()
            && n > 0
            && (n >= self.params.compaction_delta_entries
                || now.duration_since(self.last_compaction)
                    >= self.params.compaction_interval)
    }

    /// Freeze the delta and describe the next generation to build.
    /// None if a compaction is running or there is nothing to merge.
    pub fn start_compaction(&mut self) -> Option<CompactionJob> {
        if self.compacting.is_some() || self.known.delta_len() == 0 {
            return None;
        }
        let wal_cut: u64 = self.wal.next_lsn();
        let generation: u64 = self.known.base().info().generation + 1;
        let frozen: Arc<HashMap<u64, Meta>> = self.known.freeze();
        self.compacting = Some((generation, wal_cut));
        Some(CompactionJob {
            dir: self.dir.join("base"),
            base: self.known.base().clone(),
            frozen,
            terminal: self.known.terminal_ts().clone(),
            generation,
            wal_cut,
        })
    }

    /// Install a generation built by [`crate::compaction::run`],
    /// checkpoint it and drop what it superseded.
    pub fn finish_compaction(
        &mut self,
        info: BaseInfo,
    ) -> Result<(), QueueError> {
        let Some((generation, wal_cut)) = self.compacting else {
            return Ok(());
        };
        let base_dir: PathBuf = self.dir.join("base");
        let old: u64 = self.known.base().info().generation;
        let base: Arc<Base> = Arc::new(Base::open(&base_dir, info.generation)?);
        debug_assert_eq!(info.generation, generation);
        self.known.install_base(base);
        self.compacting = None;
        self.last_compaction = Instant::now();
        self.write_checkpoint()?;
        self.wal.truncate_before(wal_cut)?;
        if old != 0 {
            remove_generation(&base_dir, old)?;
        }
        Ok(())
    }

    /// Give the frozen delta back after a failed compaction.
    pub fn abort_compaction(&mut self) {
        if let Some((generation, _)) = self.compacting.take() {
            self.known.unfreeze();
            let _ = remove_generation(&self.dir.join("base"), generation);
        }
    }

    pub fn is_compacting(&self) -> bool {
        self.compacting.is_some()
    }

    /// Hard-link a consistent copy of the queue under *root*.
    pub fn snapshot(&mut self, root: &Path) -> Result<PathBuf, QueueError> {
        self.write_checkpoint()?;
        let g: u64 = self.known.base().info().generation;
        Ok(crate::snapshot::link_snapshot(&self.dir, g, root)?)
    }

    /// Check the base generation's payload checksums.
    pub fn verify_base(&self) -> Result<(), QueueError> {
        Ok(self.known.base().verify()?)
    }

    #[cfg(test)]
    pub fn wal_mut(&mut self) -> &mut Wal {
        &mut self.wal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::BaseWriter;

    const L: Duration = Duration::from_secs(60);

    fn params() -> QueueParams {
        QueueParams {
            lease_default: L,
            pop_scan_limit: 100_000,
            compaction_interval: Duration::from_secs(3600),
            compaction_delta_entries: 10_000_000,
        }
    }

    fn q(d: &tempfile::TempDir) -> Queue {
        Queue::open(d.path(), params()).unwrap()
    }

    fn ids(items: &[PopItem]) -> Vec<(u64, bool)> {
        items.iter().map(|p| (p.id, p.force)).collect()
    }

    #[test]
    fn add_dedupes_across_states() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        assert!(q.add(1, 0).unwrap());
        assert!(!q.add(1, 0).unwrap());
        q.complete(1, 0).unwrap();
        assert!(!q.add(1, 0).unwrap());
        q.mark(1, State::Failed, Reason::Other, 0).unwrap();
        assert!(!q.add(1, 0).unwrap());
        assert_eq!(q.info().counts, [0, 0, 1, 0, 0]);
    }

    #[test]
    fn pop_is_fifo_and_priority_first() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        for id in [1, 2, 3] {
            q.add(id, 0).unwrap();
        }
        assert_eq!(q.force(4, 0).unwrap(), ForceOutcome::Added);
        let now: Instant = Instant::now();
        let got: Vec<PopItem> = q.pop(10, L, now).unwrap();
        assert_eq!(
            ids(&got),
            vec![(4, true), (1, false), (2, false), (3, false)]
        );
        assert_eq!(q.info().leased, 4);
    }

    #[test]
    fn popped_ids_not_reissued_until_expiry() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        q.add(1, 0).unwrap();
        let now: Instant = Instant::now();
        assert_eq!(q.pop(10, L, now).unwrap().len(), 1);
        assert!(q.pop(10, L, now).unwrap().is_empty());
        assert_eq!(q.expire_leases(now).unwrap(), 0);
        let later: Instant = now + L + Duration::from_secs(1);
        assert_eq!(q.expire_leases(later).unwrap(), 1);
        assert_eq!(ids(&q.pop(10, L, later).unwrap()), vec![(1, false)]);
    }

    #[test]
    fn touch_extends_lease() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        q.add(1, 0).unwrap();
        let now: Instant = Instant::now();
        q.pop(1, L, now).unwrap();
        assert!(q.touch(1, L * 3, now));
        assert!(!q.touch(2, L, now));
        assert_eq!(q.expire_leases(now + L * 2).unwrap(), 0);
    }

    #[test]
    fn complete_and_mark_clear_force() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        let now: Instant = Instant::now();
        q.force(1, 0).unwrap();
        q.pop(10, L, now).unwrap();
        q.mark(1, State::Failed, Reason::BotDetection, 5).unwrap();
        assert_eq!(q.state(1), Some(State::Failed));
        assert_eq!(q.force(1, 0).unwrap(), ForceOutcome::Revived);
        assert_eq!(ids(&q.pop(10, L, now).unwrap()), vec![(1, true)]);
        q.complete(1, 0).unwrap();
        assert_eq!(q.state(1), Some(State::Scraped));
        assert!(!q.meta(1).unwrap().force());
        assert_eq!(q.info().leased, 0);
    }

    #[test]
    fn force_on_leased_is_pending_and_not_duplicated() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        let now: Instant = Instant::now();
        q.add(1, 0).unwrap();
        q.pop(1, L, now).unwrap();
        assert_eq!(q.force(1, 0).unwrap(), ForceOutcome::ForcedPending);
        assert_eq!(q.info().lane_priority, 0);
        assert!(q.meta(1).unwrap().force());
    }

    #[test]
    fn force_on_waiting_jumps_ahead() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        q.add(1, 0).unwrap();
        q.add(2, 0).unwrap();
        assert_eq!(q.force(2, 0).unwrap(), ForceOutcome::ForcedPending);
        let got: Vec<PopItem> = q.pop(10, L, Instant::now()).unwrap();
        assert_eq!(ids(&got), vec![(2, true), (1, false)]);
    }

    #[test]
    fn stale_lane_entries_skipped() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        let now: Instant = Instant::now();
        q.add(1, 0).unwrap();
        q.force(1, 0).unwrap();
        assert_eq!(ids(&q.pop(1, L, now).unwrap()), vec![(1, true)]);
        q.complete(1, 0).unwrap();
        assert!(q.pop(10, L, now).unwrap().is_empty());
        assert_eq!(q.info().lane_main, 0);
    }

    #[test]
    fn resume_cursor_tracks_oldest_lease() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        let now: Instant = Instant::now();
        for id in [1, 2, 3] {
            q.add(id, 0).unwrap();
        }
        q.pop(3, L, now).unwrap();
        q.complete(2, 0).unwrap();
        q.complete(3, 0).unwrap();
        assert_eq!(q.resume_cursor(LaneId::Main), 0);
        q.complete(1, 0).unwrap();
        assert_eq!(q.resume_cursor(LaneId::Main), 3);
    }

    #[test]
    fn requeue_rules() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        q.add(1, 0).unwrap();
        assert!(!q.requeue(1, LaneId::Main, 0).unwrap());
        q.complete(1, 0).unwrap();
        assert!(q.requeue(1, LaneId::Priority, 0).unwrap());
        assert_eq!(q.state(1), Some(State::Queued));
        assert_eq!(q.info().lane_priority, 1);
    }

    #[test]
    fn pop_bounded_scan_returns_partial() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = Queue::open(
            d.path(),
            QueueParams {
                pop_scan_limit: 1000,
                ..params()
            },
        )
        .unwrap();
        let now: Instant = Instant::now();
        assert!(q.pop(10, L, now).unwrap().is_empty());
        for id in 0..5000 {
            q.add(id, 0).unwrap();
            q.complete(id, 0).unwrap();
        }
        q.add(99_999, 0).unwrap();
        assert!(q.pop(10, L, now).unwrap().is_empty());
        let mut calls: usize = 1;
        let got: Vec<PopItem> = loop {
            calls += 1;
            let got: Vec<PopItem> = q.pop(10, L, now).unwrap();
            if !got.is_empty() || calls > 10 {
                break got;
            }
        };
        assert_eq!(ids(&got), vec![(99_999, false)]);
        assert!(calls <= 6, "took {calls} calls");
    }

    #[test]
    fn mark_rejects_non_terminal() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        assert!(matches!(
            q.mark(1, State::Scraped, Reason::None, 0),
            Err(QueueError::BadState)
        ));
    }

    #[test]
    fn wal_failure_leaves_memory_unchanged() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        q.add(1, 0).unwrap();
        q.wal_mut().fail_next_write();
        assert!(q.add(2, 0).is_err());
        assert_eq!(q.state(2), None);
        assert_eq!(q.info().counts, [1, 0, 0, 0, 0]);
        assert_eq!(q.info().lane_main, 1);
        assert!(q.add(2, 0).unwrap());
    }

    fn drain(q: &mut Queue) -> Vec<u64> {
        let got: Vec<PopItem> = q.pop(1000, L, Instant::now()).unwrap();
        got.into_iter().map(|p| p.id).collect()
    }

    #[test]
    fn reopen_without_checkpoint_replays_everything() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut q: Queue = q(&d);
            for id in [5, 6, 7] {
                q.add(id, 0).unwrap();
            }
            q.mark(6, State::Failed, Reason::Premiere, 42).unwrap();
            q.force(8, 0).unwrap();
            q.sync().unwrap();
        }
        let mut q: Queue = q(&d);
        assert_eq!(q.state(6), Some(State::Failed));
        assert_eq!(q.meta(6).unwrap().reason(), Reason::Premiere);
        assert_eq!(drain(&mut q), vec![8, 5, 7]);
        assert_eq!(q.replayed(), 5);
    }

    #[test]
    fn reopen_after_checkpoint_no_duplicate_lane_entries() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut q: Queue = q(&d);
            for id in 1..=3 {
                q.add(id, 0).unwrap();
            }
            q.write_checkpoint().unwrap();
            for id in 4..=5 {
                q.add(id, 0).unwrap();
            }
            q.sync().unwrap();
        }
        let mut q: Queue = q(&d);
        assert_eq!(q.info().lane_main, 5);
        assert_eq!(drain(&mut q), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn leased_ids_reissued_after_restart() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut q: Queue = q(&d);
            q.add(1, 0).unwrap();
            q.add(2, 0).unwrap();
            assert_eq!(drain(&mut q), vec![1, 2]);
            q.complete(2, 0).unwrap();
            q.write_checkpoint().unwrap();
        }
        let mut q: Queue = q(&d);
        assert_eq!(drain(&mut q), vec![1]);
    }

    #[test]
    fn checkpoint_releases_consumed_segments() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = q(&d);
        q.add(1, 0).unwrap();
        drain(&mut q);
        q.complete(1, 0).unwrap();
        q.write_checkpoint().unwrap();
        let c: Checkpoint = checkpoint::read(d.path()).unwrap().unwrap();
        assert_eq!((c.main.tail, c.main.resume), (1, 1));
        assert_eq!(c.lanes_as_of_lsn, 2);
    }

    #[test]
    fn tmp_and_orphan_generation_removed() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let bdir: PathBuf = d.path().join("base");
        let mut w: BaseWriter = BaseWriter::create(&bdir, 9, 0).unwrap();
        w.push(1, queued(false)).unwrap();
        w.finish().unwrap();
        std::fs::write(bdir.join("ids.00000010.tmp"), b"").unwrap();
        let q: Queue = q(&d);
        assert!(crate::base::list_generations(&bdir).unwrap().is_empty());
        assert!(!bdir.join("ids.00000010.tmp").exists());
        assert_eq!(q.state(1), None);
    }

    #[test]
    fn counts_exact_after_replay() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let before: QueueInfo;
        {
            let mut q: Queue = q(&d);
            for id in 0..100 {
                q.add(id, 0).unwrap();
            }
            for id in 0..30 {
                q.complete(id, 0).unwrap();
            }
            for id in 30..40 {
                q.mark(id, State::Unavailable, Reason::Unavailable, 1)
                    .unwrap();
            }
            q.write_checkpoint().unwrap();
            before = q.info();
        }
        let q: Queue = q(&d);
        assert_eq!(q.info().counts, before.counts);
        assert_eq!(q.info().counts, [60, 30, 0, 10, 0]);
    }

    fn copy_tree(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for e in std::fs::read_dir(src).unwrap().flatten() {
            let to: PathBuf = dst.join(e.file_name());
            if e.file_type().unwrap().is_dir() {
                copy_tree(&e.path(), &to);
            } else {
                std::fs::copy(e.path(), &to).unwrap();
            }
        }
    }

    #[test]
    fn snapshot_restores_exact_state() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let root: tempfile::TempDir = tempfile::tempdir().unwrap();
        let qdir: PathBuf = d.path().join("youtube_video");
        let mut q: Queue = Queue::open(&qdir, params()).unwrap();
        for id in 1..=5 {
            q.add(id, 0).unwrap();
        }
        drain(&mut q);
        q.complete(1, 0).unwrap();
        q.mark(2, State::Failed, Reason::Transient, 9).unwrap();
        let snap: PathBuf = q.snapshot(root.path()).unwrap();
        assert!(snap.starts_with(root.path().join("youtube_video")));
        for id in 6..=8 {
            q.add(id, 0).unwrap();
        }
        q.sync().unwrap();
        let restored: tempfile::TempDir = tempfile::tempdir().unwrap();
        copy_tree(&snap, restored.path());
        let mut r: Queue = Queue::open(restored.path(), params()).unwrap();
        assert_eq!(r.state(1), Some(State::Scraped));
        assert_eq!(r.meta(2).unwrap().reason(), Reason::Transient);
        // Leased-but-unfinished 3, 4, 5 come back; later adds may too.
        let got: Vec<u64> = drain(&mut r);
        assert_eq!(&got[..3], &[3, 4, 5]);
        let mut sorted: Vec<u64> = got.clone();
        sorted.dedup();
        assert_eq!(sorted.len(), got.len(), "no duplicates: {got:?}");
        let again: PathBuf = q.snapshot(root.path()).unwrap();
        assert_ne!(again, snap);
    }
}
