//! Prometheus metrics, rendered in the text exposition format.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::meta::ALL_STATES;
use crate::queue::QueueInfo;

const OP_BUCKETS: &[f64] =
    &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];
const FSYNC_BUCKETS: &[f64] =
    &[0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];
const COMPACTION_BUCKETS: &[f64] = &[
    1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
];

#[derive(Debug, Clone)]
struct Histogram {
    bounds: &'static [f64],
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    fn new(bounds: &'static [f64]) -> Histogram {
        Histogram {
            bounds,
            counts: vec![0; bounds.len()],
            sum: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, v: f64) {
        for (i, b) in self.bounds.iter().enumerate() {
            if v <= *b {
                self.counts[i] += 1;
            }
        }
        self.sum += v;
        self.count += 1;
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        for (b, c) in self.bounds.iter().zip(&self.counts) {
            let _ = writeln!(out, "{name}_bucket{{{labels},le=\"{b}\"}} {c}");
        }
        let n: u64 = self.count;
        let _ = writeln!(out, "{name}_bucket{{{labels},le=\"+Inf\"}} {n}");
        let _ = writeln!(out, "{name}_sum{{{labels}}} {}", self.sum);
        let _ = writeln!(out, "{name}_count{{{labels}}} {n}");
    }
}

struct Inner {
    info: QueueInfo,
    ops: BTreeMap<(String, &'static str), u64>,
    op_duration: BTreeMap<String, Histogram>,
    leases_expired: u64,
    fsync: Histogram,
    compaction: Histogram,
    compaction_last_success: f64,
    disk: BTreeMap<&'static str, u64>,
}

pub struct QueueMetrics {
    pub queue: String,
    pub invalid_ids: Arc<AtomicU64>,
    inner: Mutex<Inner>,
}

impl QueueMetrics {
    pub fn new(queue: &str) -> QueueMetrics {
        QueueMetrics {
            queue: queue.to_string(),
            invalid_ids: Arc::new(AtomicU64::new(0)),
            inner: Mutex::new(Inner {
                info: QueueInfo::default(),
                ops: BTreeMap::new(),
                op_duration: BTreeMap::new(),
                leases_expired: 0,
                fsync: Histogram::new(FSYNC_BUCKETS),
                compaction: Histogram::new(COMPACTION_BUCKETS),
                compaction_last_success: 0.0,
                disk: BTreeMap::new(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set_info(&self, info: QueueInfo) {
        self.lock().info = info;
    }

    pub fn op(&self, op: &str, ok: bool, secs: f64) {
        let mut g = self.lock();
        let result: &'static str = if ok { "ok" } else { "error" };
        *g.ops.entry((op.to_string(), result)).or_insert(0) += 1;
        g.op_duration
            .entry(op.to_string())
            .or_insert_with(|| Histogram::new(OP_BUCKETS))
            .observe(secs);
    }

    pub fn leases_expired(&self, n: u64) {
        self.lock().leases_expired += n;
    }

    pub fn fsync(&self, secs: f64) {
        self.lock().fsync.observe(secs);
    }

    pub fn compaction(&self, secs: f64, finished_at_unix: f64) {
        let mut g = self.lock();
        g.compaction.observe(secs);
        g.compaction_last_success = finished_at_unix;
    }

    /// Refresh `backlog_disk_bytes{kind}` from the queue directory.
    pub fn refresh_disk(&self, queue_dir: &Path) {
        let sizes: Vec<(&'static str, u64)> = ["base", "lanes", "wal"]
            .into_iter()
            .map(|k| (k, dir_bytes(&queue_dir.join(k))))
            .collect();
        let mut g = self.lock();
        for (k, v) in sizes {
            g.disk.insert(k, v);
        }
    }
}

fn dir_bytes(p: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(p) else {
        return 0;
    };
    rd.flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&e.path()),
            _ => e.metadata().map(|m| m.len()).unwrap_or(0),
        })
        .sum()
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// Render every queue's metrics.
pub fn render(queues: &[Arc<QueueMetrics>]) -> String {
    let mut out: String = String::new();
    let snaps: Vec<(String, u64, std::sync::MutexGuard<'_, Inner>)> = queues
        .iter()
        .map(|q| {
            let inv: u64 = q.invalid_ids.load(Ordering::Relaxed);
            (format!("queue=\"{}\"", q.queue), inv, q.lock())
        })
        .collect();
    family(&mut out, "backlog_items", "gauge", "Known IDs per state");
    for (l, _, g) in &snaps {
        for s in ALL_STATES {
            let v: u64 = g.info.counts[s as usize];
            let _ = writeln!(
                out,
                "backlog_items{{{l},state=\"{}\"}} {v}",
                s.name()
            );
        }
        let _ = writeln!(
            out,
            "backlog_items{{{l},state=\"leased\"}} {}",
            g.info.leased
        );
    }
    family(
        &mut out,
        "backlog_lane_length",
        "gauge",
        "Unpopped lane entries, including stale ones",
    );
    for (l, _, g) in &snaps {
        for (lane, v) in [
            ("main", g.info.lane_main),
            ("priority", g.info.lane_priority),
        ] {
            let _ =
                writeln!(out, "backlog_lane_length{{{l},lane=\"{lane}\"}} {v}");
        }
    }
    family(&mut out, "backlog_ops_total", "counter", "Commands handled");
    for (l, _, g) in &snaps {
        for ((op, result), v) in &g.ops {
            let _ = writeln!(
                out,
                "backlog_ops_total{{{l},op=\"{op}\",result=\"{result}\"}} {v}"
            );
        }
    }
    family(
        &mut out,
        "backlog_op_duration_seconds",
        "histogram",
        "Time to handle a command in the queue owner",
    );
    for (l, _, g) in &snaps {
        for (op, h) in &g.op_duration {
            h.render(
                &mut out,
                "backlog_op_duration_seconds",
                &format!("{l},op=\"{op}\""),
            );
        }
    }
    let simple: [(&str, &str, &str); 6] = [
        (
            "backlog_invalid_ids_total",
            "counter",
            "Malformed IDs rejected",
        ),
        (
            "backlog_leases_expired_total",
            "counter",
            "Leases that expired and were re-queued",
        ),
        ("backlog_wal_bytes", "gauge", "Change log size on disk"),
        (
            "backlog_compaction_last_success_timestamp",
            "gauge",
            "Unix time of the last successful compaction",
        ),
        (
            "backlog_delta_entries",
            "gauge",
            "Entries in the active delta",
        ),
        (
            "backlog_base_entries",
            "gauge",
            "Entries in the base generation",
        ),
    ];
    for (name, kind, help) in simple {
        family(&mut out, name, kind, help);
        for (l, inv, g) in &snaps {
            let v: f64 = match name {
                "backlog_invalid_ids_total" => *inv as f64,
                "backlog_leases_expired_total" => g.leases_expired as f64,
                "backlog_wal_bytes" => {
                    g.disk.get("wal").copied().unwrap_or(0) as f64
                }
                "backlog_compaction_last_success_timestamp" => {
                    g.compaction_last_success
                }
                "backlog_delta_entries" => g.info.delta_entries as f64,
                _ => g.info.base_entries as f64,
            };
            let _ = writeln!(out, "{name}{{{l}}} {v}");
        }
    }
    family(
        &mut out,
        "backlog_wal_fsync_seconds",
        "histogram",
        "Duration of change log and lane fsyncs",
    );
    for (l, _, g) in &snaps {
        g.fsync.render(&mut out, "backlog_wal_fsync_seconds", l);
    }
    family(
        &mut out,
        "backlog_compaction_seconds",
        "histogram",
        "Duration of compactions",
    );
    for (l, _, g) in &snaps {
        g.compaction
            .render(&mut out, "backlog_compaction_seconds", l);
    }
    family(
        &mut out,
        "backlog_disk_bytes",
        "gauge",
        "Bytes on disk per file kind",
    );
    for (l, _, g) in &snaps {
        for (k, v) in &g.disk {
            let _ = writeln!(out, "backlog_disk_bytes{{{l},kind=\"{k}\"}} {v}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_families_and_labels() {
        let m: Arc<QueueMetrics> = Arc::new(QueueMetrics::new("youtube:video"));
        m.set_info(QueueInfo {
            counts: [3, 0, 0, 0, 0],
            leased: 1,
            ..QueueInfo::default()
        });
        m.op("ADD", true, 0.0002);
        m.invalid_ids.fetch_add(2, Ordering::Relaxed);
        let s: String = render(&[m]);
        assert!(s.contains(
            "backlog_items{queue=\"youtube:video\",state=\"queued\"} 3"
        ));
        assert!(s.contains(
            "backlog_ops_total{queue=\"youtube:video\",\
                            op=\"ADD\",result=\"ok\"} 1"
        ));
        assert!(
            s.contains("backlog_invalid_ids_total{queue=\"youtube:video\"} 2")
        );
        assert!(s.contains(
            "backlog_op_duration_seconds_bucket{queue=\
                            \"youtube:video\",op=\"ADD\",le=\"0.0005\"} 1"
        ));
        assert_eq!(s.matches("# TYPE backlog_items gauge").count(), 1);
    }
}
