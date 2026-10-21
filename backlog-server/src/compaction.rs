//! Merge a base generation with a frozen delta into the next
//! generation. Runs off the owner task; reads only immutable inputs.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use crate::base::{Base, BaseInfo, BaseWriter, write_terminal};
use crate::meta::Meta;

pub struct CompactionJob {
    /// The queue's `base/` directory.
    pub dir: PathBuf,
    pub base: Arc<Base>,
    pub frozen: Arc<HashMap<u64, Meta>>,
    pub terminal: HashMap<u64, u32>,
    pub generation: u64,
    pub wal_cut: u64,
}

/// Write generation `job.generation`: a streaming merge of the base
/// with the frozen entries (frozen wins), plus the terminal sidecar.
pub fn run(job: &CompactionJob) -> io::Result<BaseInfo> {
    let mut delta: Vec<(u64, Meta)> =
        job.frozen.iter().map(|(k, v)| (*k, *v)).collect();
    delta.sort_unstable_by_key(|(id, _)| *id);
    let mut w: BaseWriter =
        BaseWriter::create(&job.dir, job.generation, job.wal_cut)?;
    let mut base = job.base.iter().peekable();
    let mut delta = delta.into_iter().peekable();
    loop {
        let next: Option<(u64, Meta)> = match (base.peek(), delta.peek()) {
            (None, None) => break,
            (Some(_), None) => base.next(),
            (None, Some(_)) => delta.next(),
            (Some((b, _)), Some((d, _))) => {
                if b < d {
                    base.next()
                } else {
                    if b == d {
                        base.next();
                    }
                    delta.next()
                }
            }
        };
        let (id, meta) = next.expect("peeked");
        w.push(id, meta)?;
    }
    let info: BaseInfo = w.finish()?;
    write_terminal(&job.dir, job.generation, &job.terminal)?;
    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::BaseWriter;
    use crate::base::tests::rng;
    use crate::meta::{ALL_STATES, Reason, State};
    use crate::queue::{Queue, QueueParams};
    use std::time::{Duration, Instant};

    fn params() -> QueueParams {
        QueueParams {
            lease_default: Duration::from_secs(60),
            pop_scan_limit: 100_000,
            compaction_interval: Duration::from_secs(3600),
            compaction_delta_entries: 10_000_000,
        }
    }

    fn compact(q: &mut Queue) {
        let job: CompactionJob = q.start_compaction().unwrap();
        let info: BaseInfo = run(&job).unwrap();
        q.finish_compaction(info).unwrap();
    }

    #[test]
    fn compaction_preserves_state() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = Queue::open(d.path(), params()).unwrap();
        let mut model: HashMap<u64, State> = HashMap::new();
        let mut next = rng(7);
        for round in 0..3 {
            for _ in 0..3000 {
                let id: u64 = next() % 5000;
                match next() % 3 {
                    0 => {
                        if q.add(id, 0).unwrap() {
                            model.insert(id, State::Queued);
                        }
                    }
                    1 => {
                        q.complete(id, 0).unwrap();
                        model.insert(id, State::Scraped);
                    }
                    _ => {
                        let s: State = ALL_STATES[2 + (id % 3) as usize];
                        q.mark(id, s, Reason::Other, round).unwrap();
                        model.insert(id, s);
                    }
                }
            }
            compact(&mut q);
            assert_eq!(q.info().generation, round as u64 + 1);
            assert_eq!(q.info().delta_entries, 0);
        }
        let mut want: [u64; 5] = [0; 5];
        for (id, s) in &model {
            assert_eq!(q.state(*id), Some(*s), "{id}");
            want[*s as usize] += 1;
        }
        assert_eq!(q.info().counts, want);
        drop(q);
        let q: Queue = Queue::open(d.path(), params()).unwrap();
        assert_eq!(q.info().counts, want);
        assert_eq!(q.replayed(), 0);
    }

    #[test]
    fn writes_during_compaction_survive() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut q: Queue = Queue::open(d.path(), params()).unwrap();
            for id in 0..100 {
                q.add(id, 0).unwrap();
            }
            let job: CompactionJob = q.start_compaction().unwrap();
            for id in 100..150 {
                q.add(id, 0).unwrap();
            }
            q.complete(5, 0).unwrap();
            let info: BaseInfo = run(&job).unwrap();
            q.finish_compaction(info).unwrap();
            assert_eq!(q.state(149), Some(State::Queued));
            q.sync().unwrap();
        }
        let mut q: Queue = Queue::open(d.path(), params()).unwrap();
        assert_eq!(q.info().counts, [149, 1, 0, 0, 0]);
        let got = q.pop(1000, Duration::from_secs(9), Instant::now()).unwrap();
        assert_eq!(got.len(), 149);
    }

    #[test]
    fn crash_before_finish_recovers() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut q: Queue = Queue::open(d.path(), params()).unwrap();
            for id in 0..10 {
                q.add(id, 0).unwrap();
            }
            q.write_checkpoint().unwrap();
            let job: CompactionJob = q.start_compaction().unwrap();
            run(&job).unwrap();
        }
        let q: Queue = Queue::open(d.path(), params()).unwrap();
        assert_eq!(q.info().generation, 0);
        assert_eq!(q.info().counts, [10, 0, 0, 0, 0]);
        let bdir = d.path().join("base");
        assert!(crate::base::list_generations(&bdir).unwrap().is_empty());
    }

    #[test]
    fn wal_truncated_after_finish() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        {
            let mut q: Queue = Queue::open(d.path(), params()).unwrap();
            for id in 0..10 {
                q.add(id, 0).unwrap();
            }
            compact(&mut q);
            q.add(10, 0).unwrap();
            q.sync().unwrap();
        }
        let c = crate::checkpoint::read(d.path()).unwrap().unwrap();
        assert_eq!((c.generation, c.replay_from_lsn), (1, 10));
        let q: Queue = Queue::open(d.path(), params()).unwrap();
        assert_eq!(q.replayed(), 1);
        assert_eq!(q.info().counts[0], 11);
    }

    #[test]
    fn abort_restores_delta() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = Queue::open(d.path(), params()).unwrap();
        q.add(1, 0).unwrap();
        let _job: CompactionJob = q.start_compaction().unwrap();
        assert!(q.start_compaction().is_none());
        q.complete(2, 0).unwrap();
        q.abort_compaction(Instant::now());
        assert_eq!(q.info().delta_entries, 2);
        assert_eq!(q.state(1), Some(State::Queued));
        compact(&mut q);
        assert_eq!(q.info().counts, [1, 1, 0, 0, 0]);
    }

    #[test]
    fn should_compact_triggers() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let p: QueueParams = QueueParams {
            compaction_delta_entries: 3,
            ..params()
        };
        let mut q: Queue = Queue::open(d.path(), p).unwrap();
        let now: Instant = Instant::now();
        assert!(!q.should_compact(now));
        q.add(1, 0).unwrap();
        assert!(!q.should_compact(now));
        assert!(q.should_compact(now + Duration::from_secs(3601)));
        q.add(2, 0).unwrap();
        q.add(3, 0).unwrap();
        assert!(q.should_compact(now));
    }

    #[test]
    fn failed_compaction_backs_off_and_cleans_up() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut q: Queue = Queue::open(d.path(), params()).unwrap();
        q.add(1, 0).unwrap();
        let job: CompactionJob = q.start_compaction().unwrap();
        // A merge that died part-way leaves temp files behind.
        drop(BaseWriter::create(&job.dir, job.generation, 0).unwrap());
        let tmp = crate::fsutil::tmp_path(&crate::base::ids_path(
            &job.dir,
            job.generation,
        ));
        assert!(tmp.exists());
        let t0: Instant = Instant::now();
        q.abort_compaction(t0);
        assert!(!tmp.exists(), "temp files removed on abort");
        let past_interval: Instant = t0 + Duration::from_secs(3601);
        assert!(!q.should_compact(t0 + Duration::from_secs(30)));
        assert!(q.should_compact(t0 + Duration::from_secs(61)));
        // A second failure doubles the wait.
        let t1: Instant = t0 + Duration::from_secs(61);
        q.start_compaction().unwrap();
        q.abort_compaction(t1);
        assert!(!q.should_compact(t1 + Duration::from_secs(100)));
        assert!(q.should_compact(t1 + Duration::from_secs(121)));
        // Success resets the backoff.
        compact(&mut q);
        q.add(2, 0).unwrap();
        assert!(q.should_compact(past_interval + Duration::from_secs(3601)));
    }
}
