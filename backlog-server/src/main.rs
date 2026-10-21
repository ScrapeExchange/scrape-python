use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use backlog_server::config::{ServeConfig, queue_dir_name};
use backlog_server::import::ImportArgs;
use backlog_server::meta::ALL_STATES;
use backlog_server::queue::{Queue, QueueInfo};

#[derive(Parser)]
#[command(name = "backlog-server", version, about = "Scrape backlog server")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve the RESP protocol and /metrics.
    Serve(ServeConfig),
    /// Build a queue directory offline from export files.
    Import(ImportArgs),
    /// Check a stopped queue directory and print its counts as JSON.
    Verify(VerifyArgs),
}

#[derive(clap::Args)]
struct VerifyArgs {
    #[arg(long, env = "BACKLOG_DATA_DIR")]
    data_dir: std::path::PathBuf,
    #[arg(long, default_value = "youtube:video")]
    queue: String,
}

fn verify(a: &VerifyArgs) -> anyhow::Result<()> {
    let dir = a.data_dir.join(queue_dir_name(&a.queue));
    if !dir.join("CHECKPOINT").exists() {
        anyhow::bail!("no queue at {dir:?}");
    }
    let defaults = backlog_server::queue::QueueParams {
        lease_default: std::time::Duration::from_secs(1800),
        pop_scan_limit: 100_000,
        compaction_interval: std::time::Duration::from_secs(3600),
        compaction_delta_entries: 10_000_000,
    };
    let q: Queue = Queue::open(&dir, defaults)?;
    q.verify_base()?;
    let i: QueueInfo = q.info();
    let mut out = serde_json::Map::new();
    for s in ALL_STATES {
        out.insert(s.name().into(), i.counts[s as usize].into());
    }
    for (k, v) in [
        ("lane_main", i.lane_main),
        ("lane_priority", i.lane_priority),
        ("base_entries", i.base_entries),
        ("delta_entries", i.delta_entries),
        ("generation", i.generation),
        ("replayed", q.replayed() as u64),
    ] {
        out.insert(k.into(), v.into());
    }
    println!("{}", serde_json::Value::Object(out));
    Ok(())
}

fn exit(r: anyhow::Result<()>) -> ExitCode {
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = format!("{e:#}"), "fatal");
            ExitCode::FAILURE
        }
    }
}

/// JSON logs; to stdout for `serve` (container logs), to stderr for
/// the CLI commands whose stdout carries their JSON result.
fn init_logging(level: &str, to_stderr: bool) {
    let b = tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::new(level))
        .with_current_span(false);
    if to_stderr {
        b.with_writer(std::io::stderr).init();
    } else {
        b.init();
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli: Cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve(cfg) => {
            init_logging(&cfg.log_level, false);
            exit(backlog_server::server::run(cfg).await)
        }
        Cmd::Import(a) => {
            init_logging("info", true);
            exit(backlog_server::import::run(&a).and_then(|r| {
                println!("{}", serde_json::to_string_pretty(&r)?);
                Ok(())
            }))
        }
        Cmd::Verify(a) => {
            init_logging("info", true);
            exit(verify(&a))
        }
    }
}
