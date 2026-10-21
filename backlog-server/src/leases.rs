//! In-memory leases on popped IDs, indexed by lane position so the
//! oldest unresolved one bounds the persisted resume cursor.

use std::collections::{BTreeSet, HashMap};
use std::time::Instant;

use crate::wal::LaneId;

#[derive(Debug, Clone, Copy)]
struct Lease {
    lane: LaneId,
    pos: u64,
    deadline: Instant,
}

#[derive(Default)]
pub struct Leases {
    by_id: HashMap<u64, Lease>,
    positions: [BTreeSet<u64>; 2],
}

impl Leases {
    pub fn insert(
        &mut self,
        id: u64,
        lane: LaneId,
        pos: u64,
        deadline: Instant,
    ) {
        if let Some(old) = self.by_id.insert(
            id,
            Lease {
                lane,
                pos,
                deadline,
            },
        ) {
            self.positions[old.lane as usize].remove(&old.pos);
        }
        self.positions[lane as usize].insert(pos);
    }

    pub fn remove(&mut self, id: u64) -> bool {
        match self.by_id.remove(&id) {
            Some(l) => {
                self.positions[l.lane as usize].remove(&l.pos);
                true
            }
            None => false,
        }
    }

    pub fn contains(&self, id: u64) -> bool {
        self.by_id.contains_key(&id)
    }

    pub fn touch(&mut self, id: u64, deadline: Instant) -> bool {
        match self.by_id.get_mut(&id) {
            Some(l) => {
                l.deadline = deadline;
                true
            }
            None => false,
        }
    }

    /// Leases past their deadline. They stay in place until removed.
    pub fn expired(&self, now: Instant) -> Vec<(u64, LaneId)> {
        let mut out: Vec<(u64, LaneId, u64)> = self
            .by_id
            .iter()
            .filter(|(_, l)| l.deadline <= now)
            .map(|(id, l)| (*id, l.lane, l.pos))
            .collect();
        out.sort_unstable_by_key(|(_, lane, pos)| (*lane, *pos));
        out.into_iter().map(|(id, lane, _)| (id, lane)).collect()
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn min_pos(&self, lane: LaneId) -> Option<u64> {
        self.positions[lane as usize].first().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn lifecycle() {
        let now: Instant = Instant::now();
        let mut l: Leases = Leases::default();
        l.insert(1, LaneId::Main, 10, now + Duration::from_secs(5));
        l.insert(2, LaneId::Main, 11, now + Duration::from_secs(1));
        l.insert(3, LaneId::Priority, 0, now + Duration::from_secs(9));
        assert_eq!(l.len(), 3);
        assert!(l.contains(2));
        assert_eq!(l.min_pos(LaneId::Main), Some(10));
        let soon: Instant = now + Duration::from_secs(2);
        assert_eq!(l.expired(soon), vec![(2, LaneId::Main)]);
        assert!(l.contains(2), "expired() only reports");
        assert!(l.touch(2, now + Duration::from_secs(20)));
        assert!(l.expired(soon).is_empty());
        assert!(l.remove(1));
        assert!(!l.remove(1));
        assert_eq!(l.min_pos(LaneId::Main), Some(11));
        assert!(!l.touch(99, now));
        assert!(l.remove(2));
        assert_eq!(l.min_pos(LaneId::Main), None);
    }
}
