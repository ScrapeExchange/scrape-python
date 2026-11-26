//! Every known ID: the immutable base generation plus in-memory
//! deltas, with per-state counters kept up to date incrementally.

use std::collections::HashMap;
use std::sync::Arc;

use crate::base::Base;
use crate::meta::{Meta, State};

pub struct KnownSet {
    base: Arc<Base>,
    frozen: Option<Arc<HashMap<u64, Meta>>>,
    active: HashMap<u64, Meta>,
    counts: [u64; 6],
    terminal_ts: HashMap<u64, u32>,
}

impl KnownSet {
    pub fn new(base: Arc<Base>, terminal_ts: HashMap<u64, u32>) -> KnownSet {
        let counts: [u64; 6] = base.info().state_counts;
        KnownSet {
            base,
            frozen: None,
            active: HashMap::new(),
            counts,
            terminal_ts,
        }
    }

    pub fn get(&self, id: u64) -> Option<Meta> {
        if let Some(m) = self.active.get(&id) {
            return Some(*m);
        }
        if let Some(f) = &self.frozen
            && let Some(m) = f.get(&id)
        {
            return Some(*m);
        }
        self.base.get(id)
    }

    /// [`KnownSet::get`] for many IDs: the in-memory layers answer
    /// first, and the IDs only the base can answer go to
    /// [`Base::get_many`] as one batch.
    pub fn get_many(&self, ids: &[u64]) -> Vec<Option<Meta>> {
        let mut out: Vec<Option<Meta>> = Vec::with_capacity(ids.len());
        let mut from_base: Vec<usize> = Vec::new();
        for (k, id) in ids.iter().enumerate() {
            let hit: Option<Meta> =
                self.active.get(id).copied().or_else(|| {
                    self.frozen.as_ref().and_then(|f| f.get(id).copied())
                });
            if hit.is_none() {
                from_base.push(k);
            }
            out.push(hit);
        }
        if from_base.is_empty() {
            return out;
        }
        let query: Vec<u64> = from_base.iter().map(|&k| ids[k]).collect();
        for (k, meta) in from_base.into_iter().zip(self.base.get_many(&query)) {
            out[k] = meta;
        }
        out
    }

    pub fn put(&mut self, id: u64, meta: Meta, ts: u32) {
        if let Some(old) = self.get(id) {
            self.counts[old.state() as usize] -= 1;
        }
        self.counts[meta.state() as usize] += 1;
        self.active.insert(id, meta);
        if meta.state().is_terminal() {
            self.terminal_ts.insert(id, ts);
        } else {
            self.terminal_ts.remove(&id);
        }
    }

    pub fn counts(&self) -> [u64; 6] {
        self.counts
    }

    pub fn total(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Entries in the active delta only.
    pub fn delta_len(&self) -> usize {
        self.active.len()
    }

    pub fn terminal_ts(&self) -> &HashMap<u64, u32> {
        &self.terminal_ts
    }

    pub fn base(&self) -> &Arc<Base> {
        &self.base
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen.is_some()
    }

    /// Move the active delta aside for compaction.
    pub fn freeze(&mut self) -> Arc<HashMap<u64, Meta>> {
        assert!(self.frozen.is_none(), "a frozen delta already exists");
        let f: Arc<HashMap<u64, Meta>> =
            Arc::new(std::mem::take(&mut self.active));
        self.frozen = Some(f.clone());
        f
    }

    /// Give a frozen delta back after a failed compaction; newer
    /// active entries win.
    pub fn unfreeze(&mut self) {
        if let Some(f) = self.frozen.take() {
            for (id, m) in f.iter() {
                self.active.entry(*id).or_insert(*m);
            }
        }
    }

    /// Swap in a generation that already contains the frozen delta.
    ///
    /// Every ID reads the same before and after (the new base is the
    /// old one with the frozen delta merged in, and the active delta
    /// still wins over it), so the counts [`KnownSet::put`] keeps are
    /// already right and are kept as they are. Recounting here would
    /// look every active ID up in the new base: on the owner task,
    /// against a multi-GB generation that compaction wrote around the
    /// page cache, that is one cold random read per ID and stalled the
    /// server for minutes.
    pub fn install_base(&mut self, base: Arc<Base>) {
        self.base = base;
        self.frozen = None;
    }

    /// Swap in a generation rewritten from outside input (the offline
    /// `mark-uploaded` tool), whose states may differ from what the
    /// counts say, and recount from its header plus the active delta.
    pub fn install_rewritten_base(&mut self, base: Arc<Base>) {
        let mut counts: [u64; 6] = base.info().state_counts;
        for (id, m) in &self.active {
            if let Some(old) = base.get(*id) {
                counts[old.state() as usize] -= 1;
            }
            counts[m.state() as usize] += 1;
        }
        self.counts = counts;
        self.base = base;
        self.frozen = None;
    }

    /// Count of IDs in *state*.
    pub fn count(&self, state: State) -> u64 {
        self.counts[state as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::BaseWriter;
    use crate::meta::{ALL_STATES, Reason};
    use std::collections::BTreeMap;

    fn m(s: State) -> Meta {
        Meta::new(s, false, Reason::None)
    }

    fn base_of(
        dir: &std::path::Path,
        g: u64,
        ids: &BTreeMap<u64, Meta>,
    ) -> Arc<Base> {
        let mut w: BaseWriter = BaseWriter::create(dir, g, 0).unwrap();
        for (id, meta) in ids {
            w.push(*id, *meta).unwrap();
        }
        w.finish().unwrap();
        Arc::new(Base::open(dir, g).unwrap())
    }

    fn recount(k: &KnownSet, ids: &[u64]) -> [u64; 6] {
        let mut c: [u64; 6] = [0; 6];
        for id in ids {
            if let Some(meta) = k.get(*id) {
                c[meta.state() as usize] += 1;
            }
        }
        c
    }

    #[test]
    fn get_prefers_active_over_frozen_over_base() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let b: Arc<Base> = base_of(
            d.path(),
            1,
            &[(1, m(State::Queued)), (2, m(State::Queued))].into(),
        );
        let mut k: KnownSet = KnownSet::new(b, HashMap::new());
        k.put(1, m(State::Scraped), 0);
        let _frozen: Arc<HashMap<u64, Meta>> = k.freeze();
        k.put(1, m(State::Failed), 5);
        assert_eq!(k.get(1), Some(m(State::Failed)));
        assert_eq!(k.get(2), Some(m(State::Queued)));
        assert_eq!(k.get(3), None);
        assert_eq!(k.delta_len(), 1);
    }

    #[test]
    fn get_many_matches_get_across_layers() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let b: Arc<Base> = base_of(
            d.path(),
            1,
            &[
                (1, m(State::Queued)),
                (2, m(State::Queued)),
                (3, m(State::Queued)),
            ]
            .into(),
        );
        let mut k: KnownSet = KnownSet::new(b, HashMap::new());
        k.put(1, m(State::Scraped), 0);
        k.put(4, m(State::Queued), 0);
        let _frozen: Arc<HashMap<u64, Meta>> = k.freeze();
        k.put(1, m(State::Failed), 5);
        k.put(2, m(State::Scraped), 5);
        let query: [u64; 6] = [1, 2, 3, 4, 5, 1];
        let want: Vec<Option<Meta>> =
            query.iter().map(|id| k.get(*id)).collect();
        assert_eq!(k.get_many(&query), want);
        assert_eq!(want[0], Some(m(State::Failed)));
        assert_eq!(want[3], Some(m(State::Queued)));
        assert_eq!(want[4], None);
    }

    #[test]
    fn uploaded_is_counted() {
        let mut k: KnownSet =
            KnownSet::new(Arc::new(Base::empty()), HashMap::new());
        k.put(1, m(State::Uploaded), 0);
        assert_eq!(k.counts()[5], 1);
        assert_eq!(k.total(), 1);
    }

    #[test]
    fn put_updates_counts() {
        let mut k: KnownSet =
            KnownSet::new(Arc::new(Base::empty()), HashMap::new());
        k.put(1, m(State::Queued), 0);
        assert_eq!(k.counts(), [1, 0, 0, 0, 0, 0]);
        k.put(1, m(State::Scraped), 0);
        assert_eq!(k.counts(), [0, 1, 0, 0, 0, 0]);
        k.put(1, m(State::Scraped).with_force(true), 0);
        assert_eq!(k.counts(), [0, 1, 0, 0, 0, 0]);
        assert_eq!(k.total(), 1);
    }

    #[test]
    fn freeze_then_install_keeps_counts_exact() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let ids: Vec<u64> = (0..200).collect();
        let initial: BTreeMap<u64, Meta> = ids[..100]
            .iter()
            .map(|id| (*id, m(ALL_STATES[(*id % 5) as usize])))
            .collect();
        let b1: Arc<Base> = base_of(d.path(), 1, &initial);
        let mut k: KnownSet = KnownSet::new(b1.clone(), HashMap::new());
        for id in 50..150 {
            k.put(id, m(State::Scraped), 0);
        }
        let frozen: Arc<HashMap<u64, Meta>> = k.freeze();
        for id in 120..200 {
            k.put(id, m(State::Failed), 1);
        }
        let mut merged: BTreeMap<u64, Meta> = b1.iter().collect();
        merged.extend(frozen.iter().map(|(a, b)| (*a, *b)));
        let b2: Arc<Base> = base_of(d.path(), 2, &merged);
        k.install_base(b2);
        assert_eq!(k.counts(), recount(&k, &ids));
        assert_eq!(k.get(130), Some(m(State::Failed)));
        assert_eq!(k.get(60), Some(m(State::Scraped)));
        k.freeze();
    }

    #[test]
    fn install_base_reads_nothing_from_the_new_base() {
        // The counts put() kept are carried over unchanged, and they
        // match a recount over the installed state.
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let ids: Vec<u64> = (0..50).collect();
        let initial: BTreeMap<u64, Meta> =
            ids.iter().map(|id| (*id, m(State::Queued))).collect();
        let b1: Arc<Base> = base_of(d.path(), 1, &initial);
        let mut k: KnownSet = KnownSet::new(b1.clone(), HashMap::new());
        for id in 0..10 {
            k.put(id, m(State::Scraped), 0);
        }
        let frozen: Arc<HashMap<u64, Meta>> = k.freeze();
        for id in 40..60 {
            k.put(id, m(State::Failed), 1);
        }
        let before: [u64; 6] = k.counts();
        let mut merged: BTreeMap<u64, Meta> = b1.iter().collect();
        merged.extend(frozen.iter().map(|(a, b)| (*a, *b)));
        let b2: Arc<Base> = base_of(d.path(), 2, &merged);
        k.install_base(b2);
        assert_eq!(k.counts(), before);
        assert_eq!(k.counts(), recount(&k, &(0..60).collect::<Vec<u64>>()));
    }

    #[test]
    fn install_rewritten_base_recounts_from_the_new_base() {
        // mark-uploaded rewrites states the counts never saw.
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let ids: Vec<u64> = (0..40).collect();
        let initial: BTreeMap<u64, Meta> =
            ids.iter().map(|id| (*id, m(State::Scraped))).collect();
        let b1: Arc<Base> = base_of(d.path(), 1, &initial);
        let mut k: KnownSet = KnownSet::new(b1, HashMap::new());
        k.freeze();
        let rewritten: BTreeMap<u64, Meta> = ids
            .iter()
            .map(|id| {
                let s: State = if id % 2 == 0 {
                    State::Uploaded
                } else {
                    State::Scraped
                };
                (*id, m(s))
            })
            .collect();
        let b2: Arc<Base> = base_of(d.path(), 2, &rewritten);
        k.put(41, m(State::Queued), 2);
        k.install_rewritten_base(b2);
        assert_eq!(k.counts(), recount(&k, &(0..42).collect::<Vec<u64>>()));
        assert_eq!(k.count(State::Uploaded), 20);
    }

    #[test]
    fn terminal_ts_follows_state() {
        let mut k: KnownSet =
            KnownSet::new(Arc::new(Base::empty()), HashMap::new());
        k.put(7, m(State::Failed), 123);
        assert_eq!(k.terminal_ts().get(&7), Some(&123));
        k.put(7, m(State::Queued), 200);
        assert_eq!(k.terminal_ts().get(&7), None);
    }
}
