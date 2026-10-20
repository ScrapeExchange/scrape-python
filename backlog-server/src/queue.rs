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

use crate::base::Base;
use crate::fileformat::FormatError;
use crate::known::KnownSet;
use crate::lane::{Lane, LaneId};
use crate::leases::Leases;
use crate::meta::{Meta, Reason, State};
use crate::wal::{Record, Wal, WalError};

#[derive(Debug, Clone, Copy)]
pub struct QueueParams {
    pub lease_default: Duration,
    pub pop_scan_limit: usize,
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

pub struct Queue {
    dir: PathBuf,
    params: QueueParams,
    known: KnownSet,
    wal: Wal,
    main: Lane,
    priority: Lane,
    leases: Leases,
}

fn queued(force: bool) -> Meta {
    Meta::new(State::Queued, force, Reason::None)
}

impl Queue {
    pub fn new_in_memory_for_tests(dir: &Path, params: QueueParams) -> Queue {
        Queue {
            dir: dir.to_path_buf(),
            params,
            known: KnownSet::new(Arc::new(Base::empty()), HashMap::new()),
            wal: Wal::open(&dir.join("wal")).expect("wal"),
            main: Lane::open(&dir.join("lanes/main"), None, 0).expect("lane"),
            priority: Lane::open(&dir.join("lanes/priority"), None, 0)
                .expect("lane"),
            leases: Leases::default(),
        }
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

    #[cfg(test)]
    pub fn wal_mut(&mut self) -> &mut Wal {
        &mut self.wal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Duration = Duration::from_secs(60);

    fn params() -> QueueParams {
        QueueParams {
            lease_default: L,
            pop_scan_limit: 100_000,
        }
    }

    fn q(d: &tempfile::TempDir) -> Queue {
        Queue::new_in_memory_for_tests(d.path(), params())
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
        let mut q: Queue = Queue::new_in_memory_for_tests(
            d.path(),
            QueueParams {
                pop_scan_limit: 1000,
                ..params()
            },
        );
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
}
