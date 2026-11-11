//! Offline backfill: mark every ID in an export as `uploaded`.
//!
//! Runs on a stopped queue directory, like `import` and `verify`: the
//! change cannot go through the running server, whose in-memory delta
//! would hold about a billion entries (20-30 GB) before the first
//! compaction.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::base::{BaseInfo, BaseScan, BaseWriter, write_terminal_paced};
use crate::codec::Codec;
use crate::compaction::CompactionJob;
use crate::config::queue_dir_name;
use crate::import::{FixedRecord, Merged, Sorter};
use crate::meta::{Meta, Reason, State};
use crate::queue::{Queue, QueueParams};

#[derive(clap::Args, Debug, Clone)]
pub struct MarkUploadedArgs {
    /// Server data directory; the queue directory lives inside it.
    #[arg(long, env = "BACKLOG_DATA_DIR")]
    pub data_dir: PathBuf,
    #[arg(long, default_value = "youtube:video")]
    pub queue: String,
    #[arg(long, default_value = "youtube_video")]
    pub codec: String,
    /// One video_id per line: on scrape.exchange.
    #[arg(long)]
    pub ids: PathBuf,
    /// RAM budget for sorting, in MiB.
    #[arg(long, default_value_t = 16384)]
    pub memory_limit_mb: u64,
    /// Records per sort run (overrides the memory limit; tests).
    #[arg(long, hide = true)]
    pub run_records: Option<usize>,
}

#[derive(Debug, Default, Serialize)]
pub struct MarkUploadedReport {
    /// Non-empty input lines read.
    pub input: u64,
    /// Input lines the codec rejected.
    pub invalid: u64,
    /// Unknown IDs inserted as `uploaded`.
    pub inserted: u64,
    /// Per previous state name: how many became `uploaded`.
    pub became_uploaded: BTreeMap<String, u64>,
    /// Per reason: how many kept their state.
    pub kept: BTreeMap<String, u64>,
}

/// One ID from the export; the sort key is the ID itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ExportId {
    id: u64,
}

impl FixedRecord for ExportId {
    const LEN: usize = 8;

    fn put(&self, o: &mut [u8]) {
        o[0..8].copy_from_slice(&self.id.to_le_bytes());
    }

    fn get(b: &[u8]) -> ExportId {
        ExportId {
            id: u64::from_le_bytes(b[0..8].try_into().expect("8")),
        }
    }
}

fn params() -> QueueParams {
    QueueParams {
        lease_default: Duration::from_secs(1800),
        pop_scan_limit: 100_000,
        compaction_interval: Duration::from_secs(3600),
        compaction_delta_entries: 10_000_000,
        compaction_pacing: Default::default(),
    }
}

/// Rewrite the queue's base generation with every exported ID marked
/// `uploaded`, leaving lanes and terminal states alone.
pub fn run(a: &MarkUploadedArgs) -> Result<MarkUploadedReport> {
    let Some(codec) = Codec::parse(&a.codec) else {
        bail!("unknown codec {}", a.codec);
    };
    let qdir: PathBuf = a.data_dir.join(queue_dir_name(&a.queue));
    if !crate::checkpoint::path(&qdir).exists() {
        bail!("no queue at {qdir:?}");
    }
    // Queue::open takes the directory lock, so a running server makes
    // this fail instead of rewriting files underneath it.
    let mut q: Queue = Queue::open(&qdir, params())
        .with_context(|| format!("opening {qdir:?}: is the server stopped?"))?;
    let tmp: PathBuf = qdir.join("mark-uploaded-tmp");
    fs::create_dir_all(&tmp)?;
    let mut report: MarkUploadedReport = MarkUploadedReport::default();
    let mut input: Merged<ExportId> = sort_ids(a, codec, &tmp, &mut report)?;
    let job: CompactionJob = q
        .start_base_rewrite()
        .context("a compaction is already in progress")?;
    let mut w: BaseWriter = BaseWriter::create_paced(
        &job.dir,
        job.generation,
        job.wal_cut,
        job.pacing,
    )?;
    merge(&job, &mut input, &mut w, &mut report)?;
    let info: BaseInfo = w.finish()?;
    // Terminal states are kept, so the sidecar is carried over.
    write_terminal_paced(&job.dir, job.generation, &job.terminal, job.pacing)?;
    if let Some(cleanup) = q.finish_compaction(info)? {
        cleanup.run()?;
    }
    q.sync()?;
    fs::remove_dir_all(&tmp)?;
    Ok(report)
}

fn sort_ids(
    a: &MarkUploadedArgs,
    codec: Codec,
    tmp: &Path,
    report: &mut MarkUploadedReport,
) -> Result<Merged<ExportId>> {
    let run_len: usize = a.run_records.unwrap_or_else(|| {
        let bytes: u64 = a.memory_limit_mb * 1024 * 1024;
        // An ExportId is 8 bytes; keep half the budget spare.
        (bytes / 16).max(1024) as usize
    });
    let mut sorter: Sorter<ExportId> = Sorter::new(tmp, "ids", run_len);
    let f: File =
        File::open(&a.ids).with_context(|| format!("opening {:?}", a.ids))?;
    for line in BufReader::new(f).lines() {
        let line: String = line?;
        let text: &str = line.trim();
        if text.is_empty() {
            continue;
        }
        report.input += 1;
        match codec.encode(text) {
            Ok(id) => sorter.push(ExportId { id })?,
            Err(_) => report.invalid += 1,
        }
    }
    let (merged, _runs) = sorter.finish()?;
    Ok(merged)
}

/// Stream the base, the delta and the export together, applying the
/// backfill rules to the IDs the export names.
fn merge(
    job: &CompactionJob,
    input: &mut Merged<ExportId>,
    w: &mut BaseWriter,
    report: &mut MarkUploadedReport,
) -> Result<()> {
    let mut delta: Vec<(u64, Meta)> =
        job.frozen.iter().map(|(id, m)| (*id, *m)).collect();
    delta.sort_unstable_by_key(|(id, _)| *id);
    let mut delta = delta.into_iter().peekable();
    let mut scan: BaseScan = job.base.scan(&job.dir)?;
    let mut base: Option<(u64, Meta)> = scan.next_entry()?;
    let mut last: Option<u64> = None;
    let mut export: Option<u64> = next_export(input, &mut last)?;
    loop {
        let b: Option<u64> = base.map(|(id, _)| id);
        let d: Option<u64> = delta.peek().map(|(id, _)| *id);
        let Some(id) = [b, d, export].into_iter().flatten().min() else {
            break;
        };
        let mut meta: Option<Meta> = None;
        if b == Some(id) {
            meta = base.map(|(_, m)| m);
            base = scan.next_entry()?;
        }
        if d == Some(id) {
            meta = delta.next().map(|(_, m)| m);
        }
        let out: Meta = if export == Some(id) {
            export = next_export(input, &mut last)?;
            apply(meta, report)
        } else {
            meta.expect("a base or delta entry for a known id")
        };
        w.push(id, out)?;
    }
    Ok(())
}

/// The next export ID, skipping the repeats an unsorted-looking export
/// can hold; the stream is sorted, so repeats are adjacent.
fn next_export(
    input: &mut Merged<ExportId>,
    last: &mut Option<u64>,
) -> io::Result<Option<u64>> {
    while let Some(r) = input.next()? {
        if *last == Some(r.id) {
            continue;
        }
        *last = Some(r.id);
        return Ok(Some(r.id));
    }
    Ok(None)
}

/// The §4.4 rules: unknown, `scraped` and non-forced `queued` become
/// `uploaded`; forced `queued`, terminal and `uploaded` IDs keep their
/// state.
fn apply(prev: Option<Meta>, report: &mut MarkUploadedReport) -> Meta {
    let uploaded: Meta = Meta::new(State::Uploaded, false, Reason::None);
    let Some(m) = prev else {
        report.inserted += 1;
        return uploaded;
    };
    let state: State = m.state();
    if state.is_terminal() {
        bump(&mut report.kept, state.name());
        return m;
    }
    if state == State::Queued && m.force() {
        bump(&mut report.kept, "queued_forced");
        return m;
    }
    if state == State::Uploaded {
        bump(&mut report.kept, "uploaded");
        return m;
    }
    bump(&mut report.became_uploaded, state.name());
    uploaded
}

fn bump(counts: &mut BTreeMap<String, u64>, key: &str) {
    *counts.entry(key.to_string()).or_default() += 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::queue_dir_name;
    use crate::meta::{Reason, State as MetaState};
    use crate::queue::{Queue, QueueParams};
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    fn params() -> QueueParams {
        QueueParams {
            lease_default: Duration::from_secs(60),
            pop_scan_limit: 100_000,
            compaction_interval: Duration::from_secs(3600),
            compaction_delta_entries: 10_000_000,
            compaction_pacing: Default::default(),
        }
    }

    /// Every file under `lanes/`, by relative path.
    fn lane_bytes(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let p: PathBuf = entry.unwrap().path();
                if p.is_dir() {
                    walk(root, &p, out);
                } else {
                    let rel: PathBuf =
                        p.strip_prefix(root).unwrap().to_path_buf();
                    out.insert(rel, std::fs::read(&p).unwrap());
                }
            }
        }
        let mut out: BTreeMap<PathBuf, Vec<u8>> = BTreeMap::new();
        walk(&dir.join("lanes"), &dir.join("lanes"), &mut out);
        out
    }

    fn write_ids(path: &Path, codec: Codec, ids: &[u64], extra: &[&str]) {
        let mut s: String = String::new();
        for id in ids {
            s.push_str(&codec.decode(*id));
            s.push('\n');
        }
        for e in extra {
            s.push_str(e);
            s.push('\n');
        }
        std::fs::write(path, s).unwrap();
    }

    #[test]
    fn mark_uploaded_applies_backfill_rules() {
        let d: tempfile::TempDir = tempfile::tempdir().unwrap();
        let qdir: PathBuf = d.path().join(queue_dir_name("youtube:video"));
        std::fs::create_dir_all(&qdir).unwrap();
        let codec: Codec = Codec::YoutubeVideo;
        // 1 unknown, 2 scraped, 3 queued, 4 forced queued, 5 failed,
        // 6 unavailable, 7 removed, 8 uploaded.
        {
            let mut q: Queue = Queue::open(&qdir, params()).unwrap();
            q.complete(2, 0).unwrap();
            q.add(3, 0).unwrap();
            q.force(4, 0).unwrap();
            q.add(5, 0).unwrap();
            q.mark(5, MetaState::Failed, Reason::Other, 5).unwrap();
            q.add(6, 0).unwrap();
            q.mark(6, MetaState::Unavailable, Reason::Unavailable, 5)
                .unwrap();
            q.add(7, 0).unwrap();
            q.mark(7, MetaState::Removed, Reason::Operator, 5).unwrap();
            q.uploaded(8, 0).unwrap();
            q.write_checkpoint().unwrap();
        }
        let lanes_before: BTreeMap<PathBuf, Vec<u8>> = lane_bytes(&qdir);
        let ids_path: PathBuf = d.path().join("ids.txt");
        let ids: [u64; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
        write_ids(&ids_path, codec, &ids, &["not-a-video-id"]);
        let a: MarkUploadedArgs = MarkUploadedArgs {
            data_dir: d.path().to_path_buf(),
            queue: "youtube:video".into(),
            codec: "youtube_video".into(),
            ids: ids_path,
            memory_limit_mb: 16,
            run_records: Some(4),
        };
        let r: MarkUploadedReport = run(&a).unwrap();
        assert_eq!(r.input, 9);
        assert_eq!(r.invalid, 1);
        assert_eq!(r.inserted, 1);
        assert_eq!(
            r.became_uploaded,
            BTreeMap::from([
                ("queued".to_string(), 1),
                ("scraped".to_string(), 1),
            ])
        );
        assert_eq!(
            r.kept,
            BTreeMap::from([
                ("failed".to_string(), 1),
                ("queued_forced".to_string(), 1),
                ("removed".to_string(), 1),
                ("unavailable".to_string(), 1),
                ("uploaded".to_string(), 1),
            ])
        );
        let mut q: Queue = Queue::open(&qdir, params()).unwrap();
        assert_eq!(q.state(1), Some(MetaState::Uploaded));
        assert_eq!(q.state(2), Some(MetaState::Uploaded));
        assert_eq!(q.state(3), Some(MetaState::Uploaded));
        assert_eq!(q.state(4), Some(MetaState::Queued));
        assert_eq!(q.state(5), Some(MetaState::Failed));
        assert_eq!(q.state(6), Some(MetaState::Unavailable));
        assert_eq!(q.state(7), Some(MetaState::Removed));
        assert_eq!(q.state(8), Some(MetaState::Uploaded));
        assert_eq!(q.info().counts[5], 4);
        assert_eq!(lane_bytes(&qdir), lanes_before);
        let now: std::time::Instant = std::time::Instant::now();
        let popped: Vec<u64> = q
            .pop(10, Duration::from_secs(60), now)
            .unwrap()
            .into_iter()
            .map(|p| p.id)
            .collect();
        assert_eq!(popped, vec![4]);
    }
}
