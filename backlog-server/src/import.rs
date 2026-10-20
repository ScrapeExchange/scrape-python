//! Offline import: build a ready queue directory (base generation,
//! main lane, checkpoint) from flat export files, with an external
//! merge sort so memory stays under a limit.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use tracing::{info, warn};

use crate::base::{BaseInfo, BaseWriter, write_terminal};
use crate::checkpoint::{self, Checkpoint, LaneCkpt};
use crate::codec::Codec;
use crate::config::queue_dir_name;
use crate::fsutil::fsync_dir;
use crate::lane::Lane;
use crate::meta::{Meta, Reason, State};

#[derive(clap::Args, Debug, Clone)]
pub struct ImportArgs {
    /// Server data directory; the queue directory is created inside.
    #[arg(long, env = "BACKLOG_DATA_DIR")]
    pub data_dir: PathBuf,
    #[arg(long, default_value = "youtube:video")]
    pub queue: String,
    #[arg(long, default_value = "youtube_video")]
    pub codec: String,
    /// One video_id per line: already on scrape.exchange.
    #[arg(long)]
    pub scraped: Option<PathBuf>,
    /// `video_id<TAB>enqueued_at` per line.
    #[arg(long)]
    pub queued: Option<PathBuf>,
    /// `video_id<TAB>state<TAB>reason<TAB>ts` per line.
    #[arg(long)]
    pub terminal: Option<PathBuf>,
    /// RAM budget for sorting.
    #[arg(long, default_value_t = 16.0)]
    pub memory_limit_gb: f64,
    /// Records per sort run (overrides the memory limit; tests).
    #[arg(long, hide = true)]
    pub run_records: Option<usize>,
    /// Write the JSON report here as well as to stdout.
    #[arg(long)]
    pub report: Option<PathBuf>,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct InputStats {
    pub lines: u64,
    pub invalid: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct Conflicts {
    pub scraped_over_terminal: u64,
    pub scraped_over_queued: u64,
    pub terminal_over_queued: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct FinalCounts {
    pub queued: u64,
    pub scraped: u64,
    pub failed: u64,
    pub unavailable: u64,
    pub removed: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct ImportReport {
    pub inputs: BTreeMap<&'static str, InputStats>,
    pub duplicates: u64,
    pub conflicts: Conflicts,
    #[serde(rename = "final")]
    pub final_counts: FinalCounts,
    pub lane_main: u64,
    pub runs: usize,
}

/// A fixed-size record that sorts by its natural order.
trait FixedRecord: Ord + Copy {
    const LEN: usize;
    fn put(&self, out: &mut [u8]);
    fn get(b: &[u8]) -> Self;
}

const RANK_SCRAPED: u8 = 0;
const RANK_TERMINAL: u8 = 1;
const RANK_QUEUED: u8 = 2;

/// Pass 1: sorted by (id, rank, ts).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Known {
    id: u64,
    rank: u8,
    ts: u32,
    meta: u8,
}

impl FixedRecord for Known {
    const LEN: usize = 14;
    fn put(&self, o: &mut [u8]) {
        o[0..8].copy_from_slice(&self.id.to_le_bytes());
        o[8] = self.rank;
        o[9..13].copy_from_slice(&self.ts.to_le_bytes());
        o[13] = self.meta;
    }
    fn get(b: &[u8]) -> Known {
        Known {
            id: u64::from_le_bytes(b[0..8].try_into().expect("8")),
            rank: b[8],
            ts: u32::from_le_bytes(b[9..13].try_into().expect("4")),
            meta: b[13],
        }
    }
}

/// Pass 2: queued IDs sorted by (enqueued_at, id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Order {
    at: u32,
    id: u64,
}

impl FixedRecord for Order {
    const LEN: usize = 12;
    fn put(&self, o: &mut [u8]) {
        o[0..4].copy_from_slice(&self.at.to_le_bytes());
        o[4..12].copy_from_slice(&self.id.to_le_bytes());
    }
    fn get(b: &[u8]) -> Order {
        Order {
            at: u32::from_le_bytes(b[0..4].try_into().expect("4")),
            id: u64::from_le_bytes(b[4..12].try_into().expect("8")),
        }
    }
}

/// External merge sort: sorted runs spilled to *dir*, merged on read.
struct Sorter<T: FixedRecord> {
    dir: PathBuf,
    tag: &'static str,
    run_len: usize,
    buf: Vec<T>,
    runs: Vec<PathBuf>,
}

impl<T: FixedRecord> Sorter<T> {
    fn new(dir: &Path, tag: &'static str, run_len: usize) -> Sorter<T> {
        Sorter {
            dir: dir.to_path_buf(),
            tag,
            run_len: run_len.max(1),
            buf: Vec::new(),
            runs: Vec::new(),
        }
    }

    fn push(&mut self, r: T) -> io::Result<()> {
        self.buf.push(r);
        if self.buf.len() >= self.run_len {
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.buf.sort_unstable();
        let p: PathBuf =
            self.dir
                .join(format!("{}.{:06}.run", self.tag, self.runs.len()));
        let mut w: BufWriter<File> = BufWriter::new(File::create(&p)?);
        let mut b: Vec<u8> = vec![0; T::LEN];
        for r in self.buf.drain(..) {
            r.put(&mut b);
            w.write_all(&b)?;
        }
        w.flush()?;
        self.runs.push(p);
        Ok(())
    }

    /// Spill the remainder and return a merged iterator.
    fn finish(mut self) -> io::Result<(Merged<T>, usize)> {
        self.spill()?;
        let n: usize = self.runs.len();
        Ok((Merged::open(self.runs)?, n))
    }
}

struct Merged<T: FixedRecord> {
    readers: Vec<BufReader<File>>,
    heap: BinaryHeap<Reverse<(T, usize)>>,
    paths: Vec<PathBuf>,
}

impl<T: FixedRecord> Merged<T> {
    fn open(paths: Vec<PathBuf>) -> io::Result<Merged<T>> {
        let mut m: Merged<T> = Merged {
            readers: Vec::new(),
            heap: BinaryHeap::new(),
            paths,
        };
        for (i, p) in m.paths.iter().enumerate() {
            m.readers
                .push(BufReader::with_capacity(1 << 20, File::open(p)?));
            if let Some(r) = read_one::<T>(&mut m.readers[i])? {
                m.heap.push(Reverse((r, i)));
            }
        }
        Ok(m)
    }

    fn next(&mut self) -> io::Result<Option<T>> {
        let Some(Reverse((r, i))) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(n) = read_one::<T>(&mut self.readers[i])? {
            self.heap.push(Reverse((n, i)));
        }
        Ok(Some(r))
    }
}

impl<T: FixedRecord> Drop for Merged<T> {
    fn drop(&mut self) {
        for p in &self.paths {
            let _ = fs::remove_file(p);
        }
    }
}

fn read_one<T: FixedRecord>(r: &mut BufReader<File>) -> io::Result<Option<T>> {
    let mut b: Vec<u8> = vec![0; T::LEN];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(T::get(&b))),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

const MAX_LOGGED_INVALID: u64 = 20;

/// Read one input file, turning each valid line into a record.
fn read_input(
    path: &Path,
    name: &'static str,
    stats: &mut InputStats,
    mut parse: impl FnMut(&str) -> Option<Known>,
    sorter: &mut Sorter<Known>,
) -> Result<()> {
    let f: File =
        File::open(path).with_context(|| format!("opening {path:?}"))?;
    for line in BufReader::with_capacity(1 << 20, f).lines() {
        let line: String = line?;
        let t: &str = line.trim_end_matches(['\r', '\n']);
        if t.trim().is_empty() {
            continue;
        }
        stats.lines += 1;
        match parse(t) {
            Some(k) => sorter.push(k)?,
            None => {
                stats.invalid += 1;
                stats.lines -= 1;
                if stats.invalid <= MAX_LOGGED_INVALID {
                    warn!(input = name, line = t, "invalid line skipped");
                }
            }
        }
    }
    Ok(())
}

fn parse_ts(s: &str) -> Option<u32> {
    let v: f64 = s.trim().parse::<f64>().ok()?;
    if !v.is_finite() {
        return None;
    }
    Some(v.clamp(0.0, f64::from(u32::MAX)) as u32)
}

pub fn run(args: &ImportArgs) -> Result<ImportReport> {
    let Some(codec) = Codec::parse(&args.codec) else {
        bail!("unknown codec {}", args.codec);
    };
    let qdir: PathBuf = args.data_dir.join(queue_dir_name(&args.queue));
    if checkpoint::path(&qdir).exists() {
        bail!("{qdir:?} already holds a queue; refusing to import");
    }
    fs::create_dir_all(&qdir)?;
    let lock: File = crate::queue::lock_dir(&qdir)?;
    let tmp: PathBuf = qdir.join("import-tmp");
    fs::create_dir_all(&tmp)?;
    let run_len: usize = args.run_records.unwrap_or_else(|| {
        let bytes: f64 = args.memory_limit_gb * 1024.0 * 1024.0 * 1024.0;
        // A Vec<Known> element is 24 bytes; keep half the budget spare.
        (bytes / 24.0 / 2.0).max(1024.0) as usize
    });
    let mut report: ImportReport = ImportReport::default();
    let mut sorter: Sorter<Known> = Sorter::new(&tmp, "known", run_len);

    let mut s: InputStats = InputStats::default();
    if let Some(p) = &args.scraped {
        read_input(
            p,
            "scraped",
            &mut s,
            |t| {
                let id: u64 = codec.encode(t.trim()).ok()?;
                let m: Meta = Meta::new(State::Scraped, false, Reason::None);
                Some(Known {
                    id,
                    rank: RANK_SCRAPED,
                    ts: 0,
                    meta: m.0,
                })
            },
            &mut sorter,
        )?;
    }
    report.inputs.insert("scraped", s);

    let mut s: InputStats = InputStats::default();
    if let Some(p) = &args.terminal {
        read_input(
            p,
            "terminal",
            &mut s,
            |t| {
                let f: Vec<&str> = t.split('\t').collect();
                let [id, state, reason, ts] = f[..] else {
                    return None;
                };
                let id: u64 = codec.encode(id.trim()).ok()?;
                let state: State =
                    State::parse(state.trim()).filter(|s| s.is_terminal())?;
                let m: Meta =
                    Meta::new(state, false, Reason::from_name(reason.trim()));
                Some(Known {
                    id,
                    rank: RANK_TERMINAL,
                    ts: parse_ts(ts)?,
                    meta: m.0,
                })
            },
            &mut sorter,
        )?;
    }
    report.inputs.insert("terminal", s);

    let mut s: InputStats = InputStats::default();
    if let Some(p) = &args.queued {
        read_input(
            p,
            "queued",
            &mut s,
            |t| {
                let (id, at) = t.split_once('\t')?;
                let id: u64 = codec.encode(id.trim()).ok()?;
                let m: Meta = Meta::new(State::Queued, false, Reason::None);
                Some(Known {
                    id,
                    rank: RANK_QUEUED,
                    ts: parse_ts(at)?,
                    meta: m.0,
                })
            },
            &mut sorter,
        )?;
    }
    report.inputs.insert("queued", s);

    let (mut merged, runs) = sorter.finish()?;
    report.runs = runs;
    info!(runs, "pass 1: merging known IDs");
    let base_dir: PathBuf = qdir.join("base");
    let mut w: BaseWriter = BaseWriter::create(&base_dir, 1, 0)?;
    let mut terminal: HashMap<u64, u32> = HashMap::new();
    let mut order: Sorter<Order> = Sorter::new(&tmp, "order", run_len);
    let mut group: Vec<Known> = Vec::new();
    let mut flush =
        |group: &mut Vec<Known>, report: &mut ImportReport| -> Result<()> {
            let Some(win) = group.first().copied() else {
                return Ok(());
            };
            report.duplicates += group.len() as u64 - 1;
            let has = |r: u8| group.iter().any(|k| k.rank == r);
            let (s, t, q) =
                (has(RANK_SCRAPED), has(RANK_TERMINAL), has(RANK_QUEUED));
            let c: &mut Conflicts = &mut report.conflicts;
            c.scraped_over_terminal += u64::from(s && t);
            c.scraped_over_queued += u64::from(s && q);
            c.terminal_over_queued += u64::from(!s && t && q);
            let meta: Meta = Meta(win.meta);
            w.push(win.id, meta)?;
            let f: &mut FinalCounts = &mut report.final_counts;
            match meta.state() {
                State::Queued => {
                    f.queued += 1;
                    // Rank ties sort by ts, so this is the earliest.
                    order.push(Order {
                        at: win.ts,
                        id: win.id,
                    })?;
                }
                State::Scraped => f.scraped += 1,
                State::Failed => f.failed += 1,
                State::Unavailable => f.unavailable += 1,
                State::Removed => f.removed += 1,
            }
            if meta.state().is_terminal() {
                terminal.insert(win.id, win.ts);
            }
            group.clear();
            Ok(())
        };
    while let Some(k) = merged.next()? {
        if group.first().is_some_and(|g| g.id != k.id) {
            flush(&mut group, &mut report)?;
        }
        group.push(k);
    }
    flush(&mut group, &mut report)?;
    drop(merged);
    let info: BaseInfo = w.finish()?;
    write_terminal(&base_dir, 1, &terminal)?;
    drop(terminal);

    info!("pass 2: writing the main lane in enqueue order");
    let (mut ordered, _) = order.finish()?;
    let mut lane: Lane = Lane::open(&qdir.join("lanes/main"), None, 0)?;
    while let Some(o) = ordered.next()? {
        lane.append(o.id)?;
    }
    lane.sync()?;
    drop(ordered);
    Lane::open(&qdir.join("lanes/priority"), None, 0)?;
    report.lane_main = lane.tail();

    let c: Checkpoint = Checkpoint {
        generation: info.generation,
        replay_from_lsn: 0,
        lanes_as_of_lsn: 0,
        main: LaneCkpt {
            tail: lane.tail(),
            resume: 0,
        },
        priority: LaneCkpt::default(),
        ..Checkpoint::default()
    };
    fs::remove_dir_all(&tmp)?;
    checkpoint::write(&qdir, &c)?;
    fsync_dir(&qdir)?;
    drop(lock);
    let json: String = serde_json::to_string_pretty(&report)?;
    if let Some(p) = &args.report {
        fs::write(p, &json)?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{Queue, QueueParams};
    use std::time::{Duration, Instant};

    const A: &str = "AAAAAAAAAAA"; // 0
    const B: &str = "AAAAAAAAAAE"; // 1
    const C: &str = "AAAAAAAAAAI"; // 2
    const D: &str = "AAAAAAAAAAM"; // 3

    fn params() -> QueueParams {
        QueueParams {
            lease_default: Duration::from_secs(60),
            pop_scan_limit: 100_000,
            compaction_interval: Duration::from_secs(3600),
            compaction_delta_entries: 1_000_000,
        }
    }

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p: PathBuf = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    fn args(data: &Path) -> ImportArgs {
        ImportArgs {
            data_dir: data.to_path_buf(),
            queue: "youtube:video".into(),
            codec: "youtube_video".into(),
            scraped: None,
            queued: None,
            terminal: None,
            memory_limit_gb: 1.0,
            run_records: None,
            report: None,
        }
    }

    fn pop_all(q: &mut Queue) -> Vec<u64> {
        q.pop(1000, Duration::from_secs(60), Instant::now())
            .unwrap()
            .into_iter()
            .map(|p| p.id)
            .collect()
    }

    #[test]
    fn import_resolves_conflicts() {
        let t: tempfile::TempDir = tempfile::tempdir().unwrap();
        let data: PathBuf = t.path().join("data");
        let mut a: ImportArgs = args(&data);
        a.scraped = Some(write(t.path(), "s.txt", &format!("{A}\n\n")));
        a.queued = Some(write(
            t.path(),
            "q.tsv",
            &format!("{A}\t5\n{B}\t6\n{C}\t7\nnot-an-id\t1\n"),
        ));
        a.terminal = Some(write(
            t.path(),
            "t.tsv",
            &format!("{A}\tfailed\tother\t1\n{B}\tunavailable\tpremiere\t42\n"),
        ));
        let r: ImportReport = run(&a).unwrap();
        assert_eq!(r.inputs["scraped"].lines, 1);
        assert_eq!(r.inputs["queued"].lines, 3);
        assert_eq!(r.inputs["queued"].invalid, 1);
        assert_eq!(r.inputs["terminal"].lines, 2);
        assert_eq!(r.conflicts.scraped_over_terminal, 1);
        assert_eq!(r.conflicts.scraped_over_queued, 1);
        assert_eq!(r.conflicts.terminal_over_queued, 1);
        assert_eq!(r.duplicates, 3);
        assert_eq!(r.final_counts.queued, 1);
        assert_eq!(r.final_counts.scraped, 1);
        assert_eq!(r.final_counts.unavailable, 1);
        assert_eq!(r.lane_main, 1);
        let mut q: Queue =
            Queue::open(&data.join("youtube_video"), params()).unwrap();
        assert_eq!(q.state(0), Some(State::Scraped));
        assert_eq!(q.state(1), Some(State::Unavailable));
        assert_eq!(q.meta(1).unwrap().reason(), Reason::Premiere);
        assert_eq!(pop_all(&mut q), vec![2]);
    }

    #[test]
    fn import_orders_lane_by_enqueued_at() {
        let t: tempfile::TempDir = tempfile::tempdir().unwrap();
        let data: PathBuf = t.path().join("data");
        let mut a: ImportArgs = args(&data);
        a.queued = Some(write(
            t.path(),
            "q.tsv",
            &format!("{C}\t30\n{A}\t20\n{D}\t10.7\n{B}\t40\n{A}\t50\n"),
        ));
        let r: ImportReport = run(&a).unwrap();
        assert_eq!(r.duplicates, 1);
        let mut q: Queue =
            Queue::open(&data.join("youtube_video"), params()).unwrap();
        assert_eq!(pop_all(&mut q), vec![3, 0, 2, 1]);
    }

    #[test]
    fn import_small_memory_limit_spills_runs() {
        let t: tempfile::TempDir = tempfile::tempdir().unwrap();
        let mut body: String = String::new();
        let mut next = crate::base::tests::rng(3);
        for i in 0..10_000u64 {
            let id: String = crate::codec::decode_yt_video_id(next() % 7000);
            body.push_str(&format!("{id}\t{}\n", 10_000 - i));
        }
        let q_path: PathBuf = write(t.path(), "q.tsv", &body);
        let mut reports: Vec<(ImportReport, Vec<u64>)> = Vec::new();
        for (sub, runs) in [("small", Some(4000)), ("big", None)] {
            let data: PathBuf = t.path().join(sub);
            let mut a: ImportArgs = args(&data);
            a.queued = Some(q_path.clone());
            a.run_records = runs;
            let r: ImportReport = run(&a).unwrap();
            if sub == "small" {
                assert!(r.runs >= 3, "runs {}", r.runs);
            }
            let mut q: Queue =
                Queue::open(&data.join("youtube_video"), params()).unwrap();
            let mut all: Vec<u64> = Vec::new();
            loop {
                let got: Vec<u64> = pop_all(&mut q);
                if got.is_empty() {
                    break;
                }
                all.extend(got);
            }
            reports.push((r, all));
        }
        assert_eq!(reports[0].1, reports[1].1);
        assert_eq!(
            reports[0].0.final_counts.queued,
            reports[1].0.final_counts.queued
        );
    }

    #[test]
    fn import_refuses_existing_queue() {
        let t: tempfile::TempDir = tempfile::tempdir().unwrap();
        let data: PathBuf = t.path().join("data");
        let mut a: ImportArgs = args(&data);
        a.queued = Some(write(t.path(), "q.tsv", &format!("{A}\t1\n")));
        run(&a).unwrap();
        assert!(run(&a).is_err());
    }

    #[test]
    fn imported_queue_serves() {
        let t: tempfile::TempDir = tempfile::tempdir().unwrap();
        let data: PathBuf = t.path().join("data");
        let mut a: ImportArgs = args(&data);
        a.queued = Some(write(t.path(), "q.tsv", &format!("{A}\t1\n{B}\t2\n")));
        run(&a).unwrap();
        let qdir: PathBuf = data.join("youtube_video");
        let mut q: Queue = Queue::open(&qdir, params()).unwrap();
        assert_eq!(pop_all(&mut q), vec![0, 1]);
        assert!(q.add(5, 0).unwrap());
        q.complete(0, 0).unwrap();
        q.write_checkpoint().unwrap();
        drop(q);
        let q: Queue = Queue::open(&qdir, params()).unwrap();
        assert_eq!(q.info().counts, [2, 1, 0, 0, 0]);
    }

    #[test]
    fn second_open_of_queue_dir_fails() {
        let t: tempfile::TempDir = tempfile::tempdir().unwrap();
        let _q: Queue = Queue::open(t.path(), params()).unwrap();
        assert!(Queue::open(t.path(), params()).is_err());
    }
}
