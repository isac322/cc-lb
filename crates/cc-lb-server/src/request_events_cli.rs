//! `cc-lb request-events …` operator commands.
//!
//! These commands open storage through
//! [`storage_factory::open_storage_for_maintenance`]: they never migrate, never create a missing
//! SQLite file, and refuse a schema that does not match this binary. The process builds no app,
//! binds no listener and handles no requests, and it does not initialize tracing.
//!
//! stdout carries only data lines (`cc_lb.request_diagnostics.v1` NDJSON or the `day_counts`
//! line) — unlike the live diagnostics sink, which writes to stderr. Progress, summaries and
//! errors go to stderr as JSON lines.

use std::collections::BTreeMap;
use std::io::{BufWriter, Write};
use std::time::Duration;

use cc_lb_config::{Config, ConfigError, StorageConfig};
use cc_lb_request_log::{
    RequestDiagnostics, RequestDiagnosticsRecord, RequestDiagnosticsSource,
    render_request_diagnostics_line, strip_payload_diagnostics,
};
use cc_lb_storage_api::{RequestEventExportPage, RequestEventPayloadScan, StorageError};
use serde::Serialize;
use thiserror::Error;
use tokio::time::Instant;

use crate::cli::{ExportDiagnosticsArgs, RequestEventsCommand, StripDiagnosticsArgs};
use crate::storage_factory::{self, StorageFactoryError, WalSizeProbe};

const MS_PER_DAY: u64 = 86_400_000;
const SECS_PER_DAY: i64 = 86_400;
/// Upper bound used when no `--to-ts` is given; storage binds timestamps as signed 64-bit.
const NO_UPPER_TS_MS: u64 = i64::MAX.unsigned_abs();
const WAL_PAUSE_STEP: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum RequestEventsCliError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),
    #[error(transparent)]
    StorageOpen(#[from] StorageFactoryError),
    #[error("storage query failed: {0}")]
    Storage(#[from] StorageError),
    #[error("request event at seq {seq} has no event_id")]
    MissingEventId { seq: u64 },
    #[error("failed to render diagnostics for seq {seq}: {source}")]
    Render {
        seq: u64,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to decode stored payload at seq {seq}: {source}")]
    Decode {
        seq: u64,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to encode JSON report: {0}")]
    Json(#[source] serde_json::Error),
    #[error("failed to write output: {0}")]
    Io(#[from] std::io::Error),
    #[error("UTC day index {day} is outside the supported date range")]
    DayOutOfRange { day: u64 },
}

impl RequestEventsCliError {
    /// 2 for configuration, argument and schema problems and a missing SQLite file; 1 otherwise.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Config(_)
            | Self::InvalidArguments(_)
            | Self::StorageOpen(
                StorageFactoryError::StorageNotFound { .. }
                | StorageFactoryError::SchemaMismatch { .. },
            ) => 2,
            _ => 1,
        }
    }

    /// The `{"error":…}` object printed to stderr.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::StorageOpen(StorageFactoryError::StorageNotFound { path }) => {
                serde_json::json!({ "error": "storage not found", "path": path })
            }
            Self::MissingEventId { seq } | Self::Render { seq, .. } | Self::Decode { seq, .. } => {
                serde_json::json!({ "error": self.to_string(), "seq": seq })
            }
            _ => serde_json::json!({ "error": self.to_string() }),
        }
    }
}

pub async fn run(
    command: RequestEventsCommand,
    clock: cc_lb_clock::ClockHandle,
) -> Result<(), RequestEventsCliError> {
    match command {
        RequestEventsCommand::ExportDiagnostics(args) => run_export(args, clock).await,
        RequestEventsCommand::StripDiagnostics(args) => run_strip(args, clock).await,
    }
}

#[derive(Serialize)]
struct ExportProgress {
    kind: &'static str,
    last_seq: u64,
    rows: u64,
    seq_ceiling: u64,
}

#[derive(Serialize)]
struct ExportSummary {
    kind: &'static str,
    rows: u64,
    last_seq: u64,
    /// Highest seq below which no row can still commit; the export never reads past it.
    seq_ceiling: u64,
    /// `last_seq <= seq_ceiling`: resuming with `--after-seq <last_seq>` cannot skip a row that
    /// commits late. False only when the operator passed an `--after-seq` above the ceiling.
    resume_safe: bool,
    days: BTreeMap<String, u64>,
    days_excluded_ts0: u64,
}

#[derive(Serialize)]
struct DayCounts {
    kind: &'static str,
    days: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct StripProgress {
    kind: &'static str,
    last_seq: u64,
    scanned: u64,
    rewritten: u64,
    bytes_before: u64,
    bytes_after: u64,
}

#[derive(Serialize)]
struct WalPause {
    kind: &'static str,
    wal_bytes: u64,
    max_wal_bytes: u64,
}

async fn run_export(
    args: ExportDiagnosticsArgs,
    clock: cc_lb_clock::ClockHandle,
) -> Result<(), RequestEventsCliError> {
    if let (Some(from), Some(to)) = (args.from_ts, args.to_ts)
        && from >= to
    {
        return Err(RequestEventsCliError::InvalidArguments(
            "--from-ts must be earlier than --to-ts".to_owned(),
        ));
    }
    let page_size = batch_limit(args.page_size, "--page-size")?;
    let config = Config::load(&args.config)?;
    let storage = storage_factory::open_storage_for_maintenance(&config.storage, clock).await?;

    if args.count_only {
        let from = args.from_ts.unwrap_or(0).max(1);
        let to = args.to_ts.unwrap_or(NO_UPPER_TS_MS);
        let counts = storage.request_event_day_counts(from, to).await?;
        let days = day_labels(counts)?;
        let report = DayCounts {
            kind: "day_counts",
            days,
        };
        let mut line = serde_json::to_vec(&report).map_err(RequestEventsCliError::Json)?;
        line.push(b'\n');
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(&line)?;
        stdout.flush()?;
        return Ok(());
    }

    // Postgres assigns `seq` before commit, so a lower seq can become visible after a higher one.
    // Clamping every read to the stable ceiling keeps the reported resume point safe.
    let seq_ceiling = storage
        .request_event_stable_seq_ceiling(Duration::from_secs(args.ceiling_wait_secs))
        .await?;
    let until = args.until_seq.unwrap_or(seq_ceiling).min(seq_ceiling);
    let mut rows = 0_u64;
    let mut days_by_index: BTreeMap<u64, u64> = BTreeMap::new();
    let mut days_excluded_ts0 = 0_u64;

    let ts_bounds = if args.from_ts.is_some() || args.to_ts.is_some() {
        let from = args.from_ts.unwrap_or(0);
        let to = args.to_ts.unwrap_or(NO_UPPER_TS_MS);
        Some(storage.request_event_seq_bounds(from, to).await?)
    } else {
        None
    };
    let window = narrow_seq_window(args.after_seq, until, ts_bounds);
    let mut after = window.after;
    let until = window.until;

    if window.has_rows {
        let mut pacer = RowPacer::new(args.max_rows_per_sec);
        let mut out = BufWriter::new(std::io::stdout().lock());
        while after < until {
            let page = storage
                .export_request_events(&RequestEventExportPage {
                    after_seq: after,
                    until_seq: until,
                    from_ts_ms: args.from_ts,
                    to_ts_ms: args.to_ts,
                    limit: page_size,
                })
                .await?;
            let Some(page_last_seq) = page.last().map(|(seq, _)| *seq) else {
                break;
            };
            pacer.acquire(page.len()).await;
            for (seq, mut event) in page {
                let event_id = event
                    .event_id
                    .take()
                    .ok_or(RequestEventsCliError::MissingEventId { seq })?;
                let diagnostics = RequestDiagnostics::take_from(&mut event);
                let record = RequestDiagnosticsRecord::new(
                    RequestDiagnosticsSource::Backfill,
                    seq,
                    event_id,
                    &event,
                    diagnostics,
                );
                let line = render_request_diagnostics_line(&record)
                    .map_err(|source| RequestEventsCliError::Render { seq, source })?;
                out.write_all(&line)?;
                rows = rows.saturating_add(1);
                let ts_ms = record.ts_ms;
                if ts_ms == 0 {
                    days_excluded_ts0 = days_excluded_ts0.saturating_add(1);
                } else {
                    let count = days_by_index.entry(ts_ms / MS_PER_DAY).or_insert(0);
                    *count = count.saturating_add(1);
                }
            }
            out.flush()?;
            after = page_last_seq;
            write_stderr_json(&ExportProgress {
                kind: "export_progress",
                last_seq: after,
                rows,
                seq_ceiling,
            })?;
        }
        out.flush()?;
    }

    write_stderr_json(&ExportSummary {
        kind: "export_summary",
        rows,
        last_seq: after,
        seq_ceiling,
        resume_safe: after <= seq_ceiling,
        days: day_labels(days_by_index)?,
        days_excluded_ts0,
    })
}

async fn run_strip(
    args: StripDiagnosticsArgs,
    clock: cc_lb_clock::ClockHandle,
) -> Result<(), RequestEventsCliError> {
    let batch_size = batch_limit(args.batch_size, "--batch-size")?;
    let config = Config::load(&args.config)?;
    if args.max_wal_bytes.is_some() && matches!(config.storage, StorageConfig::Sqlite { .. }) {
        return Err(RequestEventsCliError::InvalidArguments(
            "--max-wal-bytes requires postgres storage".to_owned(),
        ));
    }
    let storage = storage_factory::open_storage_for_maintenance(&config.storage, clock).await?;
    let wal_guard = match args.max_wal_bytes {
        Some(limit) => Some((
            storage_factory::open_wal_size_probe(&config.storage).await?,
            limit,
        )),
        None => None,
    };

    let mut pacer = RowPacer::new(args.max_rows_per_sec);
    let mut progress = StripProgress {
        kind: "strip_progress",
        last_seq: args.after_seq,
        scanned: 0,
        rewritten: 0,
        bytes_before: 0,
        bytes_after: 0,
    };
    let mut after = args.after_seq;
    while after < args.until_seq {
        if let Some((probe, limit)) = &wal_guard {
            wait_for_wal_below(probe, *limit).await?;
        }
        let rows = storage
            .scan_request_event_payloads(&RequestEventPayloadScan {
                after_seq: after,
                until_seq: args.until_seq,
                limit: batch_size,
            })
            .await?;
        let Some(batch_last_seq) = rows.last().map(|(seq, _)| *seq) else {
            break;
        };
        let batch_len = rows.len();
        pacer.acquire(batch_len).await;

        let mut rewrites = Vec::new();
        let mut bytes_before = 0_u64;
        let mut bytes_after = 0_u64;
        for (seq, payload) in rows {
            let stripped = strip_payload_diagnostics(&payload)
                .map_err(|source| RequestEventsCliError::Decode { seq, source })?;
            bytes_before = bytes_before.saturating_add(byte_len(&payload));
            match stripped {
                Some(stripped) => {
                    bytes_after = bytes_after.saturating_add(byte_len(&stripped));
                    rewrites.push((seq, stripped));
                }
                None => bytes_after = bytes_after.saturating_add(byte_len(&payload)),
            }
        }
        let rewritten = if args.dry_run || rewrites.is_empty() {
            u64::try_from(rewrites.len()).unwrap_or(u64::MAX)
        } else {
            storage.rewrite_request_event_payloads(&rewrites).await?
        };

        after = batch_last_seq;
        progress.last_seq = after;
        progress.scanned = progress
            .scanned
            .saturating_add(u64::try_from(batch_len).unwrap_or(u64::MAX));
        progress.rewritten = progress.rewritten.saturating_add(rewritten);
        progress.bytes_before = progress.bytes_before.saturating_add(bytes_before);
        progress.bytes_after = progress.bytes_after.saturating_add(bytes_after);
        write_stderr_json(&progress)?;

        // The scan is an exact primary-key range, so a short batch means the range is done.
        if batch_len < batch_size || after >= args.until_seq {
            break;
        }
        tokio::time::sleep(Duration::from_millis(args.sleep_ms)).await;
    }
    Ok(())
}

async fn wait_for_wal_below(probe: &WalSizeProbe, limit: u64) -> Result<(), RequestEventsCliError> {
    loop {
        let wal_bytes = probe.wal_bytes().await?;
        if wal_bytes <= limit {
            return Ok(());
        }
        write_stderr_json(&WalPause {
            kind: "wal_pause",
            wal_bytes,
            max_wal_bytes: limit,
        })?;
        tokio::time::sleep(WAL_PAUSE_STEP).await;
    }
}

/// Paces rows to an average of `rows_per_sec`: a token bucket whose burst is one batch.
///
/// Each batch is admitted at `next`, and admitting `n` rows moves `next` forward by
/// `n / rows_per_sec` seconds.
struct RowPacer {
    rows_per_sec: u64,
    next: Option<Instant>,
}

impl RowPacer {
    fn new(rows_per_sec: u64) -> Self {
        Self {
            rows_per_sec: rows_per_sec.max(1),
            next: None,
        }
    }

    async fn acquire(&mut self, rows: usize) {
        let now = Instant::now();
        let start = self.next.map_or(now, |next| next.max(now));
        let rows = u128::try_from(rows).unwrap_or(u128::MAX);
        let nanos = rows.saturating_mul(1_000_000_000) / u128::from(self.rows_per_sec);
        let cost = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
        self.next = Some(start.checked_add(cost).unwrap_or(start));
        tokio::time::sleep_until(start).await;
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SeqWindow {
    after: u64,
    until: u64,
    has_rows: bool,
}

/// Narrows `(after, until]` (with `until` already clamped to the stable ceiling) by the seq
/// bounds of the timestamp filter. `ts_bounds` is `None` without a timestamp filter and
/// `Some(None)` when no row matches it.
///
/// The bounds query runs after the ceiling, so it can see rows above the ceiling. The narrowed
/// lower bound is therefore capped at `until`. When nothing remains, `after` stays the
/// operator's value, so the reported resume point never moves past the ceiling on its own.
fn narrow_seq_window(after: u64, until: u64, ts_bounds: Option<Option<(u64, u64)>>) -> SeqWindow {
    let empty = SeqWindow {
        after,
        until,
        has_rows: false,
    };
    let Some(bounds) = ts_bounds else {
        return SeqWindow {
            after,
            until,
            has_rows: after < until,
        };
    };
    let Some((min_seq, max_seq)) = bounds else {
        return empty;
    };
    let narrowed_until = until.min(max_seq);
    let narrowed_after = after.max(min_seq.saturating_sub(1));
    if narrowed_after >= narrowed_until {
        return empty;
    }
    SeqWindow {
        after: narrowed_after,
        until: narrowed_until,
        has_rows: true,
    }
}

fn batch_limit(value: u64, flag: &str) -> Result<usize, RequestEventsCliError> {
    usize::try_from(value)
        .map_err(|_| RequestEventsCliError::InvalidArguments(format!("{flag} is too large")))
}

fn byte_len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

fn day_labels(
    counts: impl IntoIterator<Item = (u64, u64)>,
) -> Result<BTreeMap<String, u64>, RequestEventsCliError> {
    let mut days = BTreeMap::new();
    for (day, count) in counts {
        days.insert(day_label(day)?, count);
    }
    Ok(days)
}

fn day_label(day: u64) -> Result<String, RequestEventsCliError> {
    i64::try_from(day)
        .ok()
        .and_then(|day| day.checked_mul(SECS_PER_DAY))
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .ok_or(RequestEventsCliError::DayOutOfRange { day })
}

fn write_stderr_json<T: Serialize>(value: &T) -> Result<(), RequestEventsCliError> {
    let mut line = serde_json::to_vec(value).map_err(RequestEventsCliError::Json)?;
    line.push(b'\n');
    std::io::stderr().lock().write_all(&line)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_label_formats_utc_dates() {
        assert_eq!(day_label(0).ok().as_deref(), Some("1970-01-01"));
        // 2026-10-13T00:00:00Z = 1_791_849_600 s.
        assert_eq!(
            day_label(1_791_849_600 / 86_400).ok().as_deref(),
            Some("2026-10-13")
        );
        assert!(matches!(
            day_label(u64::MAX),
            Err(RequestEventsCliError::DayOutOfRange { .. })
        ));
    }

    #[test]
    fn ts_bounds_above_the_ceiling_never_push_the_resume_point_past_it() {
        // Ceiling 10; rows matching the window appear at seq 11 (open) and 12 (committed).
        let window = narrow_seq_window(0, 10, Some(Some((12, 12))));
        assert_eq!(
            window,
            SeqWindow {
                after: 0,
                until: 10,
                has_rows: false
            }
        );
        // Bounds straddling the ceiling: read only up to it.
        let window = narrow_seq_window(0, 10, Some(Some((5, 40))));
        assert_eq!(
            window,
            SeqWindow {
                after: 4,
                until: 10,
                has_rows: true
            }
        );
        // min_seq exactly one above the ceiling.
        assert!(!narrow_seq_window(3, 10, Some(Some((11, 11)))).has_rows);
        assert_eq!(narrow_seq_window(3, 10, Some(Some((11, 11)))).after, 3);
    }

    #[test]
    fn seq_window_without_ts_filter_or_matches() {
        assert!(narrow_seq_window(0, 10, None).has_rows);
        assert!(!narrow_seq_window(10, 10, None).has_rows);
        // Operator --after-seq above the ceiling stays as given.
        assert_eq!(narrow_seq_window(15, 10, None).after, 15);
        assert_eq!(
            narrow_seq_window(2, 10, Some(None)),
            SeqWindow {
                after: 2,
                until: 10,
                has_rows: false
            }
        );
        // Operator --after-seq above the narrowed lower bound wins.
        assert_eq!(narrow_seq_window(6, 10, Some(Some((3, 8)))).after, 6);
    }

    #[test]
    fn exit_codes_follow_the_operator_contract() {
        let not_found = RequestEventsCliError::StorageOpen(StorageFactoryError::StorageNotFound {
            path: "/missing.sqlite".to_owned(),
        });
        assert_eq!(not_found.exit_code(), 2);
        assert_eq!(
            not_found.to_json(),
            serde_json::json!({ "error": "storage not found", "path": "/missing.sqlite" })
        );
        let schema = RequestEventsCliError::StorageOpen(StorageFactoryError::SchemaMismatch {
            message: "missing 133".to_owned(),
        });
        assert_eq!(schema.exit_code(), 2);
        assert_eq!(
            RequestEventsCliError::InvalidArguments("x".to_owned()).exit_code(),
            2
        );
        assert_eq!(
            RequestEventsCliError::MissingEventId { seq: 7 }.exit_code(),
            1
        );
        assert_eq!(
            RequestEventsCliError::MissingEventId { seq: 7 }.to_json()["seq"],
            serde_json::json!(7)
        );
        let connection =
            RequestEventsCliError::StorageOpen(StorageFactoryError::ConnectionFailed {
                message: "refused".to_owned(),
            });
        assert_eq!(connection.exit_code(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn pacer_enforces_the_average_row_rate() {
        let started = Instant::now();
        let mut pacer = RowPacer::new(100);
        pacer.acquire(100).await;
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "first batch is admitted at once"
        );
        pacer.acquire(50).await;
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        pacer.acquire(1).await;
        assert_eq!(started.elapsed(), Duration::from_millis(1500));
    }
}
