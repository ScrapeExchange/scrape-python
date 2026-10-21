//! `serve` settings, from the command line or `BACKLOG_*` env vars.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};

use crate::codec::Codec;
use crate::queue::QueueParams;

pub const MAX_FSYNC_INTERVAL_MS: u64 = 60_000;
pub const MAX_CHECKPOINT_INTERVAL_SECS: u64 = 60;

#[derive(clap::Args, Debug, Clone)]
pub struct ServeConfig {
    /// Address for RESP clients.
    #[arg(long, env = "BACKLOG_LISTEN", default_value = "127.0.0.1:6390")]
    pub listen: SocketAddr,
    /// Password clients must AUTH with. Required unless listening on
    /// loopback only.
    #[arg(long, env = "BACKLOG_PASSWORD", hide_env_values = true)]
    pub password: Option<String>,
    /// Address for the Prometheus /metrics endpoint.
    #[arg(
        long,
        env = "BACKLOG_METRICS_LISTEN",
        default_value = "127.0.0.1:9396"
    )]
    pub metrics_listen: SocketAddr,
    /// Directory holding one subdirectory per queue.
    #[arg(long, env = "BACKLOG_DATA_DIR")]
    pub data_dir: PathBuf,
    /// Comma-separated `name=codec` queue declarations.
    #[arg(
        long,
        env = "BACKLOG_QUEUES",
        default_value = "youtube:video=youtube_video"
    )]
    pub queues: String,
    #[arg(long, env = "BACKLOG_FSYNC_INTERVAL_MS", default_value_t = 1000)]
    pub fsync_interval_ms: u64,
    #[arg(long, env = "BACKLOG_CHECKPOINT_INTERVAL_SECS", default_value_t = 5)]
    pub checkpoint_interval_secs: u64,
    #[arg(
        long,
        env = "BACKLOG_COMPACTION_INTERVAL_SECS",
        default_value_t = 3600
    )]
    pub compaction_interval_secs: u64,
    #[arg(
        long,
        env = "BACKLOG_COMPACTION_DELTA_ENTRIES",
        default_value_t = 10_000_000
    )]
    pub compaction_delta_entries: usize,
    /// Default lease length for popped IDs.
    #[arg(long, env = "BACKLOG_LEASE_SECS", default_value_t = 1800)]
    pub lease_secs: u64,
    /// Most lane entries one POP may scan.
    #[arg(long, env = "BACKLOG_POP_SCAN_LIMIT", default_value_t = 100_000)]
    pub pop_scan_limit: usize,
    /// Most concurrent client connections; extra ones are refused.
    #[arg(long, env = "BACKLOG_MAX_CONNECTIONS", default_value_t = 1024)]
    pub max_connections: usize,
    #[arg(long, env = "BACKLOG_LOG_LEVEL", default_value = "info")]
    pub log_level: String,
}

/// `youtube:video` -> `youtube_video`.
pub fn queue_dir_name(name: &str) -> String {
    name.replace(':', "_")
}

/// Parse `name=codec[,name=codec...]`.
pub fn parse_queues(spec: &str) -> Result<Vec<(String, Codec)>> {
    let mut out: Vec<(String, Codec)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let Some((name, codec)) = part.split_once('=') else {
            bail!("queue declaration {part:?} is not name=codec");
        };
        let ok_name: bool = !name.is_empty()
            && name.bytes().all(|c| {
                c.is_ascii_lowercase()
                    || c.is_ascii_digit()
                    || b":_".contains(&c)
            });
        if !ok_name {
            bail!("invalid queue name {name:?}");
        }
        let Some(codec) = Codec::parse(codec) else {
            bail!("unknown codec {codec:?} for queue {name}");
        };
        if !seen.insert(queue_dir_name(name)) {
            bail!("duplicate queue {name}");
        }
        out.push((name.to_string(), codec));
    }
    if out.is_empty() {
        bail!("no queues declared");
    }
    Ok(out)
}

impl ServeConfig {
    /// Check the settings and return the declared queues.
    pub fn validate(&self) -> Result<Vec<(String, Codec)>> {
        if self.fsync_interval_ms == 0
            || self.fsync_interval_ms > MAX_FSYNC_INTERVAL_MS
        {
            bail!(
                "BACKLOG_FSYNC_INTERVAL_MS must be 1..={MAX_FSYNC_INTERVAL_MS}"
            );
        }
        if self.checkpoint_interval_secs == 0
            || self.checkpoint_interval_secs > MAX_CHECKPOINT_INTERVAL_SECS
        {
            bail!(
                "BACKLOG_CHECKPOINT_INTERVAL_SECS must be \
                 1..={MAX_CHECKPOINT_INTERVAL_SECS}"
            );
        }
        if !self.listen.ip().is_loopback() && self.password.is_none() {
            bail!(
                "BACKLOG_PASSWORD is required when listening on {}",
                self.listen
            );
        }
        if self.max_connections == 0 {
            bail!("BACKLOG_MAX_CONNECTIONS must be positive");
        }
        if self.pop_scan_limit == 0 {
            bail!("BACKLOG_POP_SCAN_LIMIT must be positive");
        }
        let max_lease: u64 = crate::commands::MAX_LEASE_SECS;
        if !(1..=max_lease).contains(&self.lease_secs) {
            bail!("BACKLOG_LEASE_SECS must be 1..={max_lease}");
        }
        parse_queues(&self.queues)
    }

    pub fn queue_params(&self) -> QueueParams {
        QueueParams {
            lease_default: Duration::from_secs(self.lease_secs),
            pop_scan_limit: self.pop_scan_limit,
            compaction_interval: Duration::from_secs(
                self.compaction_interval_secs,
            ),
            compaction_delta_entries: self.compaction_delta_entries,
        }
    }

    pub fn queue_dir(&self, name: &str) -> PathBuf {
        self.data_dir.join(queue_dir_name(name))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrap {
        #[command(flatten)]
        cfg: ServeConfig,
    }

    pub(crate) fn cfg(extra: &[&str]) -> ServeConfig {
        let mut argv: Vec<&str> = vec!["x", "--data-dir", "/nonexistent"];
        argv.extend_from_slice(extra);
        Wrap::try_parse_from(argv).unwrap().cfg
    }

    #[test]
    fn defaults_validate() {
        let c: ServeConfig = cfg(&[]);
        assert_eq!(c.fsync_interval_ms, 1000);
        assert_eq!(c.listen.port(), 6390);
        assert_eq!(c.metrics_listen.port(), 9396);
        assert_eq!(c.lease_secs, 1800);
        let q: Vec<(String, Codec)> = c.validate().unwrap();
        assert_eq!(q, vec![("youtube:video".into(), Codec::YoutubeVideo)]);
        assert_eq!(
            c.queue_dir("youtube:video"),
            PathBuf::from("/nonexistent/youtube_video")
        );
    }

    #[test]
    fn config_rejects_slow_fsync() {
        assert!(cfg(&["--fsync-interval-ms", "60001"]).validate().is_err());
        assert!(cfg(&["--fsync-interval-ms", "60000"]).validate().is_ok());
        assert!(
            cfg(&["--checkpoint-interval-secs", "61"])
                .validate()
                .is_err()
        );
    }

    #[test]
    fn config_rejects_public_listen_without_password() {
        let c: ServeConfig = cfg(&["--listen", "0.0.0.0:6390"]);
        assert!(c.validate().is_err());
        let c: ServeConfig =
            cfg(&["--listen", "0.0.0.0:6390", "--password", "pw"]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn config_parses_queues() {
        let q = parse_queues("youtube:video=youtube_video").unwrap();
        assert_eq!(q.len(), 1);
        assert!(parse_queues("a=youtube_video,a=youtube_video").is_err());
        assert!(parse_queues("tiktok:video=tiktok_video").is_err());
        assert!(parse_queues("Bad/Name=youtube_video").is_err());
        assert!(parse_queues("").is_err());
    }
}
