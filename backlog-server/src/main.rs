use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use backlog_server::config::ServeConfig;

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
}

fn init_logging(level: &str) {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::new(level))
        .with_current_span(false)
        .init();
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli: Cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve(cfg) => {
            init_logging(&cfg.log_level);
            match backlog_server::server::run(cfg).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"), "fatal");
                    ExitCode::FAILURE
                }
            }
        }
    }
}
