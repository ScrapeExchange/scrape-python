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
    counts: [u64; 5],
    terminal_ts: HashMap<u64, u32>,
}

impl KnownSet {
    pub fn new(base: Arc<Base>, terminal_ts: HashMap<u64, u32>) -> KnownSet {
        let counts: [u64; 5] = base.info().state_counts;
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

    pub fn counts(&self) -> [u64; 5] {
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

    /// Swap in a generation that already contains the frozen delta,
    /// and recount from its header plus the active delta.
    pub fn install_base(&mut self, base: Arc<Base>) {
        let mut counts: [u64; 5] = base.info().state_counts;
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

    fn recount(k: &KnownSet, ids: &[u64]) -> [u64; 5] {
        let mut c: [u64; 5] = [0; 5];
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
    fn put_updates_counts() {
        let mut k: KnownSet =
            KnownSet::new(Arc::new(Base::empty()), HashMap::new());
        k.put(1, m(State::Queued), 0);
        assert_eq!(k.counts(), [1, 0, 0, 0, 0]);
        k.put(1, m(State::Scraped), 0);
        assert_eq!(k.counts(), [0, 1, 0, 0, 0]);
        k.put(1, m(State::Scraped).with_force(true), 0);
        assert_eq!(k.counts(), [0, 1, 0, 0, 0]);
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
    fn terminal_ts_follows_state() {
        let mut k: KnownSet =
            KnownSet::new(Arc::new(Base::empty()), HashMap::new());
        k.put(7, m(State::Failed), 123);
        assert_eq!(k.terminal_ts().get(&7), Some(&123));
        k.put(7, m(State::Queued), 200);
        assert_eq!(k.terminal_ts().get(&7), None);
    }
}
