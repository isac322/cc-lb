use std::path::PathBuf;

use clap::{Args, CommandFactory, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "cc-lb",
    version,
    about = "Anthropic-compatible cc-lb reverse proxy"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Cli {
    pub fn command() -> clap::Command {
        let version: &'static str = Box::leak(crate::version::format_version().into_boxed_str());
        <Self as CommandFactory>::command().version(version)
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve {
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        strict_preflight: bool,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Doctor {
        #[command(subcommand)]
        command: DoctorCommand,
    },
    /// Operator maintenance over stored request events.
    RequestEvents {
        #[command(subcommand)]
        command: RequestEventsCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Validate {
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub enum DoctorCommand {
    ListAbandonedChainEntries,
}

#[derive(Debug, Subcommand)]
pub enum RequestEventsCommand {
    /// Write the stored diagnostic fields of request events to stdout as
    /// `cc_lb.request_diagnostics.v1` NDJSON lines (backfill of the live diagnostics log).
    ExportDiagnostics(ExportDiagnosticsArgs),
    /// Remove the diagnostic fields from stored request-event payloads in `(after_seq, until_seq]`.
    StripDiagnostics(StripDiagnosticsArgs),
}

#[derive(Debug, Args)]
pub struct ExportDiagnosticsArgs {
    #[arg(long, value_name = "PATH")]
    pub config: PathBuf,
    /// Inclusive lower bound on the event timestamp (RFC 3339).
    #[arg(long, value_name = "RFC3339", value_parser = parse_rfc3339_ms)]
    pub from_ts: Option<u64>,
    /// Exclusive upper bound on the event timestamp (RFC 3339).
    #[arg(long, value_name = "RFC3339", value_parser = parse_rfc3339_ms)]
    pub to_ts: Option<u64>,
    /// Export rows with a cursor greater than this value (resume point).
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub after_seq: u64,
    /// Export rows with a cursor up to and including this value. Always clamped to the stable
    /// cursor ceiling (the highest cursor below which no row can still commit), which is also the
    /// default; `export_summary.last_seq` therefore never passes `seq_ceiling`, and resuming with
    /// `--after-seq <last_seq>` cannot skip a row that committed late.
    #[arg(long, value_name = "N")]
    pub until_seq: Option<u64>,
    /// Seconds to wait for in-flight inserts to settle while computing the stable cursor ceiling
    /// (1 to 3600).
    #[arg(
        long,
        value_name = "N",
        default_value_t = 60,
        value_parser = clap::value_parser!(u64).range(1..=3600)
    )]
    pub ceiling_wait_secs: u64,
    #[arg(
        long,
        value_name = "N",
        default_value_t = 500,
        value_parser = clap::value_parser!(u64).range(1..=500)
    )]
    pub page_size: u64,
    #[arg(
        long,
        value_name = "N",
        default_value_t = 500,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub max_rows_per_sec: u64,
    /// Print per-UTC-day stored row counts (timestamp >= 1 ms) instead of exporting lines.
    #[arg(long)]
    pub count_only: bool,
}

#[derive(Debug, Args)]
pub struct StripDiagnosticsArgs {
    #[arg(long, value_name = "PATH")]
    pub config: PathBuf,
    /// Last cursor to strip (inclusive); pass the cursor recorded after strip-on-write was live
    /// on every pod.
    #[arg(long, value_name = "N")]
    pub until_seq: u64,
    /// Strip rows with a cursor greater than this value (resume point).
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub after_seq: u64,
    #[arg(
        long,
        value_name = "N",
        default_value_t = 500,
        value_parser = clap::value_parser!(u64).range(1..=1000)
    )]
    pub batch_size: u64,
    /// Pause between batches, in milliseconds.
    #[arg(long, value_name = "N", default_value_t = 200)]
    pub sleep_ms: u64,
    #[arg(
        long,
        value_name = "N",
        default_value_t = 1000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub max_rows_per_sec: u64,
    /// Postgres only: pause while the WAL directory holds more than N bytes (needs `pg_monitor`).
    #[arg(long, value_name = "N")]
    pub max_wal_bytes: Option<u64>,
    /// Report the counters without updating any row.
    #[arg(long)]
    pub dry_run: bool,
}

fn parse_rfc3339_ms(raw: &str) -> Result<u64, String> {
    let parsed = chrono::DateTime::parse_from_rfc3339(raw)
        .map_err(|error| format!("invalid RFC 3339 timestamp '{raw}': {error}"))?;
    u64::try_from(parsed.timestamp_millis())
        .map_err(|_| format!("timestamp '{raw}' is before 1970-01-01T00:00:00Z"))
}
