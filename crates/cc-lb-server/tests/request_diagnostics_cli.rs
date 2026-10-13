//! `cc-lb request-events export-diagnostics` / `strip-diagnostics` and the serve-side
//! diagnostics log, exercised through the real binary.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_domain::InternalError;
use cc_lb_request_log::{
    REQUEST_DIAGNOSTIC_PAYLOAD_KEYS, RequestCacheBreakpoint, RequestCacheBreakpointSource,
    RequestCacheLookbackPrefix, RequestDiagnostics, RequestDiagnosticsRecord,
    RequestDiagnosticsSource, RequestEvent, render_request_diagnostics_line,
};
use cc_lb_storage_api::{MetaStore, RequestEventStore};
use serde_json::{Map, Value};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;

use super::common;

const LINE_PREFIX: &[u8] = br#"{"schema":"cc_lb.request_diagnostics.v1","#;
/// 2026-07-23T00:00:00Z, 2026-07-24T00:00:00Z, 2026-07-25T00:00:00Z.
const DAY_A_MS: u64 = 1_784_764_800_000;
const DAY_B_MS: u64 = 1_784_851_200_000;
const DAY_C_MS: u64 = 1_784_937_600_000;
const DAY_A: &str = "2026-07-23";
const DAY_B: &str = "2026-07-24";
const DAY_C: &str = "2026-07-25";
const MESSAGES_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#;
const MESSAGES_MODEL: &str = "claude-3-5-sonnet-20241022";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct CliOutput {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: String,
}

impl CliOutput {
    fn stderr_json(&self) -> Vec<Value> {
        self.stderr
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .unwrap_or_else(|error| panic!("stderr line is not JSON ({error}): {line}"))
            })
            .collect()
    }

    fn last_of_kind(&self, kind: &str) -> Value {
        self.stderr_json()
            .into_iter()
            .rev()
            .find(|line| line["kind"] == kind)
            .unwrap_or_else(|| panic!("no {kind} line on stderr: {}", self.stderr))
    }

    fn count_of_kind(&self, kind: &str) -> usize {
        self.stderr_json()
            .iter()
            .filter(|line| line["kind"] == kind)
            .count()
    }

    /// stdout split into lines; every line must be a diagnostics data line.
    fn diagnostic_lines(&self) -> Vec<Vec<u8>> {
        let lines = split_lines(&self.stdout);
        for line in &lines {
            assert!(
                line.starts_with(LINE_PREFIX),
                "stdout must carry only diagnostics lines, got: {}",
                String::from_utf8_lossy(line)
            );
        }
        lines
    }

    fn assert_success(&self) {
        assert_eq!(self.code, Some(0), "stderr={}", self.stderr);
    }
}

fn run_cli(args: &[&str]) -> CliOutput {
    let output = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .arg("request-events")
        .args(args)
        .env(
            "CC_LB_MASTER_KEY",
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .env("CC_LB_ADMIN_TOKEN", "admin-token")
        .output()
        .expect("run cc-lb request-events");
    CliOutput {
        code: output.status.code(),
        stdout: output.stdout,
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn path_arg(path: &Path) -> String {
    path.to_str().expect("utf-8 temp path").to_owned()
}

/// Lines including their trailing `\n`.
fn split_lines(bytes: &[u8]) -> Vec<Vec<u8>> {
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

fn line_json(line: &[u8]) -> Value {
    serde_json::from_slice(line).expect("diagnostics line is JSON")
}

fn line_event_id(line: &[u8]) -> String {
    line_json(line)["event_id"]
        .as_str()
        .expect("event_id string")
        .to_owned()
}

fn line_seq(line: &[u8]) -> u64 {
    line_json(line)["seq"].as_u64().expect("seq number")
}

fn normalize_source(line: &[u8]) -> Vec<u8> {
    let text = std::str::from_utf8(line).expect("utf-8 line");
    text.replacen(r#""source":"live""#, r#""source":"backfill""#, 1)
        .into_bytes()
}

fn write_sqlite_config(dir: &Path, db_path: &Path) -> PathBuf {
    let config_path = dir.join("cli.toml");
    std::fs::write(
        &config_path,
        format!(
            "[listener]\n\n[storage]\nkind = \"sqlite\"\npath = \"{}\"\n",
            db_path.display()
        ),
    )
    .expect("write CLI config");
    config_path
}

async fn open_seeded_sqlite(db_path: &Path) -> cc_lb_storage_sqlite::SqliteStorage {
    let storage = cc_lb_storage_sqlite::open_sqlite(
        &format!("sqlite://{}", db_path.display()),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    storage
}

async fn sqlite_pool(db_path: &Path) -> sqlx::SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("open sqlite pool")
}

async fn sqlite_payloads(db_path: &Path) -> BTreeMap<u64, String> {
    let pool = sqlite_pool(db_path).await;
    let rows = sqlx::query("SELECT id, payload FROM request_events_v1 ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read payloads");
    pool.close().await;
    rows.into_iter()
        .map(|row| {
            let id: i64 = row.get("id");
            let payload: String = row.get("payload");
            (u64::try_from(id).expect("positive id"), payload)
        })
        .collect()
}

async fn sqlite_migrations(db_path: &Path) -> Vec<(i64, Vec<u8>)> {
    let pool = sqlite_pool(db_path).await;
    let rows = sqlx::query_as::<_, (i64, Vec<u8>)>(
        "SELECT version, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .expect("read _sqlx_migrations");
    pool.close().await;
    rows
}

/// A request event carrying all five diagnostic fields.
fn diagnostic_event(label: &str, ts_ms: u64) -> RequestEvent {
    RequestEvent {
        ts: ts_ms / 1000,
        ts_ms: Some(ts_ms),
        request_id: format!("req-{label}"),
        event_id: Some(format!("evt-{label}-{}", uuid::Uuid::now_v7())),
        principal_id: Some("principal-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 10,
        cache_control_message_indices: vec![1, 3],
        cache_breakpoints: vec![RequestCacheBreakpoint {
            block_index: 0,
            source: RequestCacheBreakpointSource::System,
            path: "system[0]".to_owned(),
            message_index: None,
            ttl: Some("5m".to_owned()),
            prefix_hash: format!("prefix-{label}"),
            prefix_token_count: 12,
            lookback_prefixes: vec![RequestCacheLookbackPrefix {
                prefix_hash: format!("lookback-{label}"),
                content_block_index: 0,
                lookback_distance: 1,
            }],
            token_estimate_source: Some("bytes".to_owned()),
        }],
        iterations: Some(serde_json::json!([{ "type": "message", "input_tokens": 3 }])),
        internal_errors: vec![InternalError {
            message: Some("token=[REDACTED] tail".to_owned()),
            ..InternalError::default()
        }],
        ..RequestEvent::default()
    }
}

/// A request event without any of the five diagnostic fields.
fn plain_event(label: &str, ts_ms: u64) -> RequestEvent {
    RequestEvent {
        ts: ts_ms / 1000,
        ts_ms: Some(ts_ms),
        request_id: format!("req-{label}"),
        event_id: Some(format!("evt-{label}-{}", uuid::Uuid::now_v7())),
        principal_id: Some("principal-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 10,
        ..RequestEvent::default()
    }
}

/// The line the live sink would emit for `event` at `seq`, with `source` normalized to backfill.
fn expected_backfill_line(seq: u64, event: &RequestEvent) -> Vec<u8> {
    let mut row = event.clone();
    let event_id = row.event_id.clone().expect("seeded event has event_id");
    let diagnostics = RequestDiagnostics::cloned_from(&row);
    let record = RequestDiagnosticsRecord::new(
        RequestDiagnosticsSource::Live,
        seq,
        event_id,
        &row,
        diagnostics,
    );
    let live = render_request_diagnostics_line(&record).expect("render live line");
    // Strip-on-write renders from the stripped row; both must agree.
    let taken = RequestDiagnostics::take_from(&mut row);
    let stripped_record = RequestDiagnosticsRecord::new(
        RequestDiagnosticsSource::Live,
        seq,
        event.event_id.clone().expect("event_id"),
        &row,
        taken,
    );
    assert_eq!(
        render_request_diagnostics_line(&stripped_record).expect("render stripped line"),
        live
    );
    normalize_source(&live)
}

fn payload_map(payload: &str) -> Map<String, Value> {
    match serde_json::from_str::<Value>(payload).expect("payload is JSON") {
        Value::Object(map) => map,
        other => panic!("payload is not an object: {other}"),
    }
}

fn without_diagnostic_keys(mut map: Map<String, Value>) -> Map<String, Value> {
    for key in REQUEST_DIAGNOSTIC_PAYLOAD_KEYS {
        map.remove(key);
    }
    map
}

fn has_any_diagnostic_key(payload: &str) -> bool {
    let map = payload_map(payload);
    REQUEST_DIAGNOSTIC_PAYLOAD_KEYS
        .iter()
        .any(|key| map.contains_key(*key))
}

/// Seeds rows over three UTC days plus one `ts_ms = 0` row; returns `(seq, event)` in order.
async fn seed_day_spread<S: RequestEventStore + ?Sized>(storage: &S) -> Vec<(u64, RequestEvent)> {
    let events = vec![
        diagnostic_event("a1", DAY_A_MS + 1_000),
        diagnostic_event("a2", DAY_A_MS + 86_399_999),
        diagnostic_event("zero", 0),
        diagnostic_event("b1", DAY_B_MS + 5),
        diagnostic_event("c1", DAY_C_MS),
        plain_event("c2", DAY_C_MS + 60_000),
    ];
    let mut seeded = Vec::with_capacity(events.len());
    for event in events {
        let seq = storage
            .append_request_event(&event)
            .await
            .expect("append request event");
        seeded.push((seq, event));
    }
    seeded
}

// ---------------------------------------------------------------------------
// QA 8: backfill parity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn export_lines_equal_live_rendering_of_the_same_rows_sqlite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    let seeded = seed_day_spread(&storage).await;
    let config = write_sqlite_config(dir.path(), &db_path);

    let output = run_cli(&["export-diagnostics", "--config", &path_arg(&config)]);

    output.assert_success();
    let lines = output.diagnostic_lines();
    let expected: Vec<Vec<u8>> = seeded
        .iter()
        .map(|(seq, event)| expected_backfill_line(*seq, event))
        .collect();
    assert_eq!(
        lines
            .iter()
            .map(|line| String::from_utf8_lossy(line).into_owned())
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|line| String::from_utf8_lossy(line).into_owned())
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_live_lines_match_export_byte_for_byte() {
    let envs = vec![(
        "CC_LB_OBSERVABILITY__REQUEST_DIAGNOSTICS_LOG".to_owned(),
        "true".to_owned(),
    )];
    let server = common::spawn_test_server_with_env("", &envs).await;
    const REQUESTS: usize = 3;
    for _ in 0..REQUESTS {
        let response = common::http_post(
            server.proxy_addr,
            "/v1/messages",
            &server.managed_key.plaintext,
            MESSAGES_BODY,
            &[],
        )
        .await
        .expect("post messages");
        assert_eq!(response.status, 200, "body={}", response.body);
    }

    // Wait until every stored row has its live line.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let (db_event_ids, live_lines) = loop {
        let db_event_ids = sqlite_event_ids(&server.sqlite_path).await;
        let message_rows = sqlite_message_rows(&server.sqlite_path).await;
        let live_lines = first_live_lines(&server._process.stdout_snapshot());
        if message_rows >= REQUESTS && db_event_ids.iter().all(|id| live_lines.contains_key(id)) {
            break (db_event_ids, live_lines);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "live diagnostics lines did not cover stored rows: db={db_event_ids:?} live={:?}",
            live_lines.keys().collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let config = path_arg(&server.config_path());
    let output = run_cli(&["export-diagnostics", "--config", &config]);

    output.assert_success();
    let exported = output.diagnostic_lines();
    let exported_ids: BTreeSet<String> = exported.iter().map(|line| line_event_id(line)).collect();
    assert_eq!(
        exported_ids, db_event_ids,
        "export covers exactly the stored rows"
    );
    for line in &exported {
        let event_id = line_event_id(line);
        let live = live_lines
            .get(&event_id)
            .unwrap_or_else(|| panic!("no live line for {event_id}"));
        assert_eq!(
            String::from_utf8_lossy(&normalize_source(live)),
            String::from_utf8_lossy(line),
            "live and backfill lines differ for {event_id}"
        );
    }
    let routed = exported
        .iter()
        .map(|line| line_json(line))
        .filter(|line| line["model"] == MESSAGES_MODEL)
        .count();
    assert!(routed >= REQUESTS, "exported {routed} /v1/messages rows");
}

/// First stdout line per `event_id` among diagnostics lines (first line wins).
fn first_live_lines(stdout: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut lines = BTreeMap::new();
    for line in split_lines(stdout) {
        if !line.starts_with(LINE_PREFIX) || !line.ends_with(b"\n") {
            continue;
        }
        let json = line_json(&line);
        assert_eq!(json["source"], "live");
        lines.entry(line_event_id(&line)).or_insert(line);
    }
    lines
}

async fn sqlite_event_ids(db_path: &Path) -> BTreeSet<String> {
    let pool = sqlite_pool(db_path).await;
    let ids = sqlx::query_scalar::<_, String>("SELECT event_id FROM request_events_v1")
        .fetch_all(&pool)
        .await
        .expect("read event ids");
    pool.close().await;
    ids.into_iter().collect()
}

async fn sqlite_message_rows(db_path: &Path) -> usize {
    let pool = sqlite_pool(db_path).await;
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM request_events_v1 WHERE model = ? AND upstream_id IS NOT NULL",
    )
    .bind(MESSAGES_MODEL)
    .fetch_one(&pool)
    .await
    .expect("count message rows");
    pool.close().await;
    usize::try_from(count).expect("non-negative count")
}

// ---------------------------------------------------------------------------
// QA 18 (server part): default-off parity
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_defaults_writes_no_diagnostics_lines() {
    let server = common::spawn_test_server_with_env(
        "",
        &[
            (
                "CC_LB_OBSERVABILITY__REQUEST_DIAGNOSTICS_LOG".to_owned(),
                "false".to_owned(),
            ),
            (
                "CC_LB_REQUEST_EVENT_STRIP_DIAGNOSTICS".to_owned(),
                "false".to_owned(),
            ),
        ],
    )
    .await;
    for _ in 0..2 {
        let response = common::http_post(
            server.proxy_addr,
            "/v1/messages",
            &server.managed_key.plaintext,
            MESSAGES_BODY,
            &[],
        )
        .await
        .expect("post messages");
        assert_eq!(response.status, 200, "body={}", response.body);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while sqlite_message_rows(&server.sqlite_path).await < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "rows were not stored"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Give a (wrongly) enabled sink time to flush before asserting absence.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let stdout = server._process.stdout_snapshot();
    let schema_lines = split_lines(&stdout)
        .into_iter()
        .filter(|line| {
            line.windows(b"cc_lb.request_diagnostics.v1".len())
                .any(|window| window == b"cc_lb.request_diagnostics.v1")
        })
        .count();
    assert_eq!(
        schema_lines,
        0,
        "stdout={}",
        String::from_utf8_lossy(&stdout)
    );

    // Strip is off by default, so stored payloads keep their diagnostic keys.
    let payloads = sqlite_payloads(&server.sqlite_path).await;
    assert!(
        payloads
            .values()
            .any(|payload| has_any_diagnostic_key(payload)),
        "default-off rows must keep the diagnostic payload keys"
    );
}

// ---------------------------------------------------------------------------
// QA 9: ranges, resume, --count-only, stdout purity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn export_summary_counts_days_and_excludes_ts0() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    let seeded = seed_day_spread(&storage).await;
    let config = write_sqlite_config(dir.path(), &db_path);
    let config = path_arg(&config);

    let output = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--page-size",
        "2",
    ]);

    output.assert_success();
    assert_eq!(output.diagnostic_lines().len(), seeded.len());
    let summary = output.last_of_kind("export_summary");
    assert_eq!(summary["rows"], 6);
    assert_eq!(
        summary["last_seq"],
        seeded.last().map(|(seq, _)| *seq).unwrap_or(0)
    );
    assert_eq!(
        summary["days"],
        serde_json::json!({ DAY_A: 2, DAY_B: 1, DAY_C: 2 })
    );
    assert_eq!(summary["days_excluded_ts0"], 1);
    assert_eq!(
        output.count_of_kind("export_progress"),
        3,
        "one progress line per page"
    );
}

#[tokio::test]
async fn count_only_matches_database_day_counts_without_ts0() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    seed_day_spread(&storage).await;
    let config = write_sqlite_config(dir.path(), &db_path);
    let config = path_arg(&config);

    let output = run_cli(&["export-diagnostics", "--config", &config, "--count-only"]);

    output.assert_success();
    let stdout_lines = split_lines(&output.stdout);
    assert_eq!(stdout_lines.len(), 1, "count-only prints exactly one line");
    let counts = line_json(&stdout_lines[0]);
    assert_eq!(counts["kind"], "day_counts");

    let pool = sqlite_pool(&db_path).await;
    let db_counts = sqlx::query_as::<_, (i64, i64)>(
        "SELECT list_ts_ms / 86400000, COUNT(*) FROM request_events_v1 \
         WHERE list_ts_ms >= 1 GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(&pool)
    .await
    .expect("db day counts");
    pool.close().await;
    let total: i64 = db_counts.iter().map(|(_, count)| count).sum();
    assert_eq!(total, 5, "the ts_ms = 0 row is excluded");
    assert_eq!(
        counts["days"],
        serde_json::json!({ DAY_A: 2, DAY_B: 1, DAY_C: 2 })
    );

    let ranged = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--count-only",
        "--from-ts",
        "2026-07-24T00:00:00Z",
        "--to-ts",
        "2026-07-25T00:00:00.001Z",
    ]);
    ranged.assert_success();
    let ranged_counts = line_json(&split_lines(&ranged.stdout)[0]);
    assert_eq!(
        ranged_counts["days"],
        serde_json::json!({ DAY_B: 1, DAY_C: 1 })
    );
}

#[tokio::test]
async fn export_ts_range_is_half_open_on_list_ts_ms() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    let seeded = seed_day_spread(&storage).await;
    let config = write_sqlite_config(dir.path(), &db_path);
    let config = path_arg(&config);

    let output = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--from-ts",
        "2026-07-23T23:59:59.999Z",
        "--to-ts",
        "2026-07-25T00:00:00Z",
    ]);

    output.assert_success();
    let ids: Vec<String> = output
        .diagnostic_lines()
        .iter()
        .map(|line| line_event_id(line))
        .collect();
    let expected: Vec<String> = seeded
        .iter()
        .filter(|(_, event)| {
            let ts_ms = event.ts_ms.unwrap_or(0);
            (DAY_A_MS + 86_399_999..DAY_C_MS).contains(&ts_ms)
        })
        .map(|(_, event)| event.event_id.clone().expect("event_id"))
        .collect();
    assert_eq!(
        expected.len(),
        2,
        "a2 (inclusive start) and b1; c1 sits on the open end"
    );
    assert_eq!(ids, expected);

    let empty = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--from-ts",
        "2030-01-01T00:00:00Z",
    ]);
    empty.assert_success();
    assert!(empty.stdout.is_empty());
    let summary = empty.last_of_kind("export_summary");
    assert_eq!(summary["rows"], 0);
    assert_eq!(summary["days"], serde_json::json!({}));
}

#[tokio::test]
async fn export_seq_window_and_resume_reassemble_the_full_export() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    let seeded = seed_day_spread(&storage).await;
    let config = write_sqlite_config(dir.path(), &db_path);
    let config = path_arg(&config);
    let cut = seeded[1].0.to_string();

    let full = run_cli(&["export-diagnostics", "--config", &config]);
    let head = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--until-seq",
        &cut,
    ]);
    let head_summary = head.last_of_kind("export_summary");
    let resume_from = head_summary["last_seq"]
        .as_u64()
        .expect("last_seq")
        .to_string();
    let tail = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--after-seq",
        &resume_from,
    ]);

    full.assert_success();
    head.assert_success();
    tail.assert_success();
    assert_eq!(head.diagnostic_lines().len(), 2);
    assert_eq!(resume_from, cut);
    assert!(
        tail.diagnostic_lines()
            .iter()
            .all(|line| line_seq(line) > seeded[1].0)
    );
    let mut reassembled = head.stdout.clone();
    reassembled.extend_from_slice(&tail.stdout);
    assert_eq!(reassembled, full.stdout);
}

#[tokio::test]
async fn export_clamps_to_the_stable_seq_ceiling_and_reports_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    let seeded = seed_day_spread(&storage).await;
    let config = write_sqlite_config(dir.path(), &db_path);
    let config = path_arg(&config);
    let last = seeded.last().map(|(seq, _)| *seq).expect("seeded rows");
    let beyond = (last + 1_000).to_string();

    let output = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--until-seq",
        &beyond,
        "--page-size",
        "4",
    ]);

    output.assert_success();
    assert_eq!(output.diagnostic_lines().len(), seeded.len());
    let summary = output.last_of_kind("export_summary");
    // SQLite commits serially, so every written row is below the ceiling.
    assert_eq!(summary["seq_ceiling"], last);
    let ceiling = summary["seq_ceiling"].as_u64().expect("seq_ceiling");
    let last_seq = summary["last_seq"].as_u64().expect("last_seq");
    assert!(
        last_seq <= ceiling,
        "resume point {last_seq} passes ceiling {ceiling}"
    );
    assert_eq!(summary["resume_safe"], true);
    for progress in output
        .stderr_json()
        .into_iter()
        .filter(|line| line["kind"] == "export_progress")
    {
        assert_eq!(progress["seq_ceiling"], ceiling);
        assert!(progress["last_seq"].as_u64().expect("last_seq") <= ceiling);
    }

    // A row appended after the run lands above the reported resume point and is picked up.
    let late = diagnostic_event("late", DAY_C_MS + 1);
    let late_seq = storage
        .append_request_event(&late)
        .await
        .expect("append late row");
    let resumed = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--after-seq",
        &last_seq.to_string(),
    ]);
    resumed.assert_success();
    let resumed_seqs: Vec<u64> = resumed
        .diagnostic_lines()
        .iter()
        .map(|line| line_seq(line))
        .collect();
    assert_eq!(resumed_seqs, vec![late_seq]);
}

#[tokio::test]
async fn export_rejects_an_inverted_ts_range() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    open_seeded_sqlite(&db_path).await;
    let config = write_sqlite_config(dir.path(), &db_path);

    let output = run_cli(&[
        "export-diagnostics",
        "--config",
        &path_arg(&config),
        "--from-ts",
        "2026-07-25T00:00:00Z",
        "--to-ts",
        "2026-07-24T00:00:00Z",
    ]);

    assert_eq!(output.code, Some(2), "stderr={}", output.stderr);
    assert!(output.stdout.is_empty());
    assert!(output.stderr_json()[0]["error"].is_string());
}

// ---------------------------------------------------------------------------
// QA 10: strip CLI
// ---------------------------------------------------------------------------

#[tokio::test]
async fn strip_dry_run_then_real_run_then_rerun_then_resume() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    // seq order: a1, a2, zero, b1, c1, c2(plain).
    let seeded = seed_day_spread(&storage).await;
    let config = write_sqlite_config(dir.path(), &db_path);
    let config = path_arg(&config);
    let until = seeded[3].0; // a1..b1
    let until_arg = until.to_string();
    let before = sqlite_payloads(&db_path).await;

    // Dry run: counters, no writes.
    let dry = run_cli(&[
        "strip-diagnostics",
        "--config",
        &config,
        "--until-seq",
        &until_arg,
        "--dry-run",
        "--sleep-ms",
        "0",
    ]);
    dry.assert_success();
    assert!(dry.stdout.is_empty(), "strip writes nothing to stdout");
    let dry_progress = dry.last_of_kind("strip_progress");
    assert_eq!(dry_progress["scanned"], 4);
    assert_eq!(dry_progress["rewritten"], 4);
    assert_eq!(dry_progress["last_seq"], until);
    let expected_before: u64 = before
        .range(..=until)
        .map(|(_, payload)| payload.len() as u64)
        .sum();
    assert_eq!(dry_progress["bytes_before"], expected_before);
    assert!(
        dry_progress["bytes_after"].as_u64().expect("bytes_after") < expected_before,
        "stripping shrinks payloads"
    );
    assert_eq!(
        sqlite_payloads(&db_path).await,
        before,
        "--dry-run wrote rows"
    );

    // Real run in batches of 3.
    let real = run_cli(&[
        "strip-diagnostics",
        "--config",
        &config,
        "--until-seq",
        &until_arg,
        "--batch-size",
        "3",
        "--sleep-ms",
        "0",
    ]);
    real.assert_success();
    assert_eq!(
        real.count_of_kind("strip_progress"),
        2,
        "4 rows in batches of 3"
    );
    let real_progress = real.last_of_kind("strip_progress");
    assert_eq!(real_progress["rewritten"], 4);
    assert_eq!(real_progress["bytes_before"], dry_progress["bytes_before"]);
    assert_eq!(real_progress["bytes_after"], dry_progress["bytes_after"]);

    let after_real = sqlite_payloads(&db_path).await;
    for (seq, payload) in &after_real {
        let original = &before[seq];
        if *seq <= until {
            assert!(
                !has_any_diagnostic_key(payload),
                "seq {seq} still has diagnostics"
            );
            assert_eq!(
                payload_map(payload),
                without_diagnostic_keys(payload_map(original)),
                "seq {seq}: only the five keys may change"
            );
        } else {
            assert_eq!(payload, original, "seq {seq} above --until-seq was touched");
        }
    }
    // Stripped rows still decode and export with empty diagnostics.
    let exported = run_cli(&[
        "export-diagnostics",
        "--config",
        &config,
        "--until-seq",
        &until_arg,
    ]);
    exported.assert_success();
    for line in exported.diagnostic_lines() {
        let json = line_json(&line);
        assert_eq!(json["cache_breakpoints"], serde_json::json!([]));
        assert_eq!(json["cache_control_message_indices"], serde_json::json!([]));
        assert_eq!(json["routing_trace"], Value::Null);
        assert_eq!(json["iterations"], Value::Null);
        assert_eq!(json["internal_errors"], serde_json::json!([]));
    }

    // Rerun: fixed point, nothing rewritten.
    let rerun = run_cli(&[
        "strip-diagnostics",
        "--config",
        &config,
        "--until-seq",
        &until_arg,
        "--sleep-ms",
        "0",
    ]);
    rerun.assert_success();
    let rerun_progress = rerun.last_of_kind("strip_progress");
    assert_eq!(rerun_progress["scanned"], 4);
    assert_eq!(rerun_progress["rewritten"], 0);
    assert_eq!(sqlite_payloads(&db_path).await, after_real);

    // Resume past the first window: c1 is stripped, the plain c2 row is a no-op.
    let last = seeded.last().map(|(seq, _)| *seq).expect("seeded rows");
    let resumed = run_cli(&[
        "strip-diagnostics",
        "--config",
        &config,
        "--after-seq",
        &until_arg,
        "--until-seq",
        &last.to_string(),
        "--sleep-ms",
        "0",
    ]);
    resumed.assert_success();
    let resumed_progress = resumed.last_of_kind("strip_progress");
    assert_eq!(resumed_progress["scanned"], 2);
    assert_eq!(resumed_progress["rewritten"], 1);
    assert_eq!(resumed_progress["last_seq"], last);
    let after_resume = sqlite_payloads(&db_path).await;
    assert!(
        after_resume
            .values()
            .all(|payload| !has_any_diagnostic_key(payload))
    );
    assert_eq!(
        after_resume[&last], before[&last],
        "a row without the keys is not rewritten"
    );
}

#[tokio::test]
async fn strip_stops_with_the_seq_of_an_undecodable_payload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    let seeded = seed_day_spread(&storage).await;
    let broken_seq = seeded[2].0;
    let pool = sqlite_pool(&db_path).await;
    sqlx::query("UPDATE request_events_v1 SET payload = 'not json' WHERE id = ?")
        .bind(i64::try_from(broken_seq).expect("seq fits i64"))
        .execute(&pool)
        .await
        .expect("corrupt payload");
    pool.close().await;
    let before = sqlite_payloads(&db_path).await;
    let config = write_sqlite_config(dir.path(), &db_path);

    let output = run_cli(&[
        "strip-diagnostics",
        "--config",
        &path_arg(&config),
        "--until-seq",
        &seeded.last().map(|(seq, _)| *seq).unwrap_or(0).to_string(),
        "--batch-size",
        "1",
        "--sleep-ms",
        "0",
    ]);

    assert_eq!(output.code, Some(1), "stderr={}", output.stderr);
    let error = output
        .stderr_json()
        .into_iter()
        .find(|line| line.get("error").is_some())
        .expect("error line");
    assert_eq!(error["seq"], broken_seq);
    let progress = output.last_of_kind("strip_progress");
    assert_eq!(
        progress["last_seq"], seeded[1].0,
        "rows before the broken one were processed"
    );
    let after = sqlite_payloads(&db_path).await;
    for (seq, payload) in &after {
        if *seq >= broken_seq {
            assert_eq!(
                payload, &before[seq],
                "seq {seq} past the failure was touched"
            );
        }
    }
}

#[tokio::test]
async fn strip_rejects_max_wal_bytes_on_sqlite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    let storage = open_seeded_sqlite(&db_path).await;
    seed_day_spread(&storage).await;
    let before = sqlite_payloads(&db_path).await;
    let config = write_sqlite_config(dir.path(), &db_path);

    let output = run_cli(&[
        "strip-diagnostics",
        "--config",
        &path_arg(&config),
        "--until-seq",
        "100",
        "--max-wal-bytes",
        "1",
    ]);

    assert_eq!(output.code, Some(2), "stderr={}", output.stderr);
    assert_eq!(sqlite_payloads(&db_path).await, before);
}

// ---------------------------------------------------------------------------
// QA 12 / 12b: schema verification and no-create open
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cli_refuses_a_database_missing_a_migration_and_never_migrates() {
    for subcommand in ["export-diagnostics", "strip-diagnostics"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("cc-lb.sqlite");
        let storage = open_seeded_sqlite(&db_path).await;
        seed_day_spread(&storage).await;
        drop(storage);
        let pool = sqlite_pool(&db_path).await;
        sqlx::query(
            "DELETE FROM _sqlx_migrations WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)",
        )
        .execute(&pool)
        .await
        .expect("drop newest migration row");
        pool.close().await;
        let migrations_before = sqlite_migrations(&db_path).await;
        let payloads_before = sqlite_payloads(&db_path).await;
        let config = write_sqlite_config(dir.path(), &db_path);

        let output = run_cli(&[
            subcommand,
            "--config",
            &path_arg(&config),
            "--until-seq",
            "100",
        ]);

        assert_eq!(
            output.code,
            Some(2),
            "{subcommand}: stderr={}",
            output.stderr
        );
        assert!(
            output.stdout.is_empty(),
            "{subcommand}: no data on schema mismatch"
        );
        assert!(output.stderr_json()[0]["error"].is_string());
        assert_eq!(
            sqlite_migrations(&db_path).await,
            migrations_before,
            "{subcommand} must not apply migrations"
        );
        assert_eq!(sqlite_payloads(&db_path).await, payloads_before);
    }
}

#[tokio::test]
async fn cli_refuses_a_migration_checksum_mismatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    drop(open_seeded_sqlite(&db_path).await);
    let pool = sqlite_pool(&db_path).await;
    sqlx::query(
        "UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = (SELECT MIN(version) FROM _sqlx_migrations)",
    )
    .execute(&pool)
    .await
    .expect("tamper checksum");
    pool.close().await;
    let migrations_before = sqlite_migrations(&db_path).await;
    let config = write_sqlite_config(dir.path(), &db_path);

    let output = run_cli(&[
        "export-diagnostics",
        "--config",
        &path_arg(&config),
        "--count-only",
    ]);

    assert_eq!(output.code, Some(2), "stderr={}", output.stderr);
    assert!(output.stdout.is_empty());
    assert_eq!(sqlite_migrations(&db_path).await, migrations_before);
}

#[tokio::test]
async fn cli_accepts_a_current_schema_without_touching_migrations() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("cc-lb.sqlite");
    drop(open_seeded_sqlite(&db_path).await);
    let migrations_before = sqlite_migrations(&db_path).await;
    let config = write_sqlite_config(dir.path(), &db_path);

    let output = run_cli(&["export-diagnostics", "--config", &path_arg(&config)]);

    output.assert_success();
    assert!(output.stdout.is_empty(), "empty database exports no lines");
    assert_eq!(sqlite_migrations(&db_path).await, migrations_before);
}

#[test]
fn cli_with_a_missing_sqlite_path_exits_2_and_creates_nothing() {
    for args in [
        vec!["export-diagnostics"],
        vec!["export-diagnostics", "--count-only"],
        vec!["strip-diagnostics", "--until-seq", "10"],
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("missing.sqlite");
        let config = write_sqlite_config(dir.path(), &db_path);
        let config = path_arg(&config);
        let mut full_args = args.clone();
        full_args.extend(["--config", config.as_str()]);

        let output = run_cli(&full_args);

        assert_eq!(output.code, Some(2), "{args:?}: stderr={}", output.stderr);
        let errors = output.stderr_json();
        assert_eq!(errors.len(), 1, "{args:?}: stderr={}", output.stderr);
        assert_eq!(errors[0]["error"], "storage not found");
        assert_eq!(errors[0]["path"], db_path.display().to_string());
        assert!(output.stdout.is_empty());
        assert!(!db_path.exists(), "{args:?} created {}", db_path.display());
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read tempdir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .filter(|name| name.to_string_lossy().starts_with("missing.sqlite"))
            .collect();
        assert!(leftovers.is_empty(), "{args:?} left {leftovers:?}");
    }
}

#[test]
fn cli_rejects_an_invalid_config_with_exit_2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("cli.toml");
    std::fs::write(
        &config_path,
        "definitely_unknown_root_key = 1\n\n[listener]\n",
    )
    .expect("write config");

    let output = run_cli(&["export-diagnostics", "--config", &path_arg(&config_path)]);

    assert_eq!(output.code, Some(2), "stderr={}", output.stderr);
    assert!(output.stdout.is_empty());
    assert!(output.stderr_json()[0]["error"].is_string());
}

// ---------------------------------------------------------------------------
// Postgres (CI_POSTGRES_URL)
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    use cc_lb_storage_postgres::PostgresStorage;
    use sqlx::postgres::PgPoolOptions;

    fn ci_postgres_url() -> Option<String> {
        std::env::var("CI_POSTGRES_URL").ok()
    }

    fn write_postgres_config(dir: &Path, url: &str) -> PathBuf {
        let config_path = dir.join("cli.toml");
        std::fs::write(
            &config_path,
            format!(
                "[listener]\n\n[storage]\nkind = \"postgres\"\nurl = \"{}\"\n\n\
                 [cluster]\ninstance_url = \"http://127.0.0.1:9090\"\n\
                 token_env = \"CI_POSTGRES_URL\"\n",
                url.replace('\\', "\\\\").replace('"', "\\\"")
            ),
        )
        .expect("write CLI config");
        config_path
    }

    async fn reset_and_open(url: &str) -> (sqlx::PgPool, PostgresStorage) {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(url)
            .await
            .expect("connect postgres");
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        storage.initialize().await.expect("initialize postgres");
        sqlx::query("TRUNCATE request_events_v1 RESTART IDENTITY")
            .execute(&pool)
            .await
            .expect("truncate request events");
        (pool, storage)
    }

    async fn pg_rows(pool: &sqlx::PgPool) -> BTreeMap<u64, (Option<String>, Vec<u8>)> {
        let rows = sqlx::query_as::<_, (i64, Option<String>, Vec<u8>)>(
            "SELECT seq, tx_id::text, payload FROM request_events_v1 ORDER BY seq",
        )
        .fetch_all(pool)
        .await
        .expect("read rows");
        rows.into_iter()
            .map(|(seq, tx_id, payload)| {
                (u64::try_from(seq).expect("positive seq"), (tx_id, payload))
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_export_parity_ranges_and_counts() {
        let Some(url) = ci_postgres_url() else {
            eprintln!("skipped: CI_POSTGRES_URL unset");
            return;
        };
        let _serial = common::postgres_test_lock(&url)
            .await
            .expect("postgres test lock");
        let (pool, storage) = reset_and_open(&url).await;
        let seeded = seed_day_spread(&storage).await;
        let dir = tempfile::tempdir().expect("tempdir");
        let config = path_arg(&write_postgres_config(dir.path(), &url));

        let full = run_cli(&["export-diagnostics", "--config", &config]);
        full.assert_success();
        let expected: Vec<String> = seeded
            .iter()
            .map(|(seq, event)| {
                String::from_utf8_lossy(&expected_backfill_line(*seq, event)).into_owned()
            })
            .collect();
        let actual: Vec<String> = full
            .diagnostic_lines()
            .iter()
            .map(|line| String::from_utf8_lossy(line).into_owned())
            .collect();
        assert_eq!(actual, expected);
        let summary = full.last_of_kind("export_summary");
        assert_eq!(
            summary["days"],
            serde_json::json!({ DAY_A: 2, DAY_B: 1, DAY_C: 2 })
        );
        assert_eq!(summary["days_excluded_ts0"], 1);

        let counts = run_cli(&["export-diagnostics", "--config", &config, "--count-only"]);
        counts.assert_success();
        assert_eq!(
            line_json(&split_lines(&counts.stdout)[0])["days"],
            serde_json::json!({ DAY_A: 2, DAY_B: 1, DAY_C: 2 })
        );

        let cut = seeded[1].0.to_string();
        let head = run_cli(&[
            "export-diagnostics",
            "--config",
            &config,
            "--until-seq",
            &cut,
        ]);
        let tail = run_cli(&[
            "export-diagnostics",
            "--config",
            &config,
            "--after-seq",
            &cut,
        ]);
        head.assert_success();
        tail.assert_success();
        let mut reassembled = head.stdout.clone();
        reassembled.extend_from_slice(&tail.stdout);
        assert_eq!(reassembled, full.stdout);

        let ranged = run_cli(&[
            "export-diagnostics",
            "--config",
            &config,
            "--from-ts",
            "2026-07-24T00:00:00Z",
            "--to-ts",
            "2026-07-25T00:00:00Z",
        ]);
        ranged.assert_success();
        let ranged_ids: Vec<String> = ranged
            .diagnostic_lines()
            .iter()
            .map(|line| line_event_id(line))
            .collect();
        assert_eq!(
            ranged_ids,
            vec![seeded[3].1.event_id.clone().expect("event_id")]
        );
        pool.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_strip_keeps_seq_and_tx_id() {
        let Some(url) = ci_postgres_url() else {
            eprintln!("skipped: CI_POSTGRES_URL unset");
            return;
        };
        let _serial = common::postgres_test_lock(&url)
            .await
            .expect("postgres test lock");
        let (pool, storage) = reset_and_open(&url).await;
        let seeded = seed_day_spread(&storage).await;
        let dir = tempfile::tempdir().expect("tempdir");
        let config = path_arg(&write_postgres_config(dir.path(), &url));
        let until = seeded[3].0;
        let until_arg = until.to_string();
        let before = pg_rows(&pool).await;

        let dry = run_cli(&[
            "strip-diagnostics",
            "--config",
            &config,
            "--until-seq",
            &until_arg,
            "--dry-run",
            "--sleep-ms",
            "0",
        ]);
        dry.assert_success();
        assert_eq!(dry.last_of_kind("strip_progress")["rewritten"], 4);
        assert_eq!(pg_rows(&pool).await, before, "--dry-run wrote rows");

        let real = run_cli(&[
            "strip-diagnostics",
            "--config",
            &config,
            "--until-seq",
            &until_arg,
            "--batch-size",
            "3",
            "--sleep-ms",
            "0",
        ]);
        real.assert_success();
        assert_eq!(real.last_of_kind("strip_progress")["rewritten"], 4);

        let after = pg_rows(&pool).await;
        assert_eq!(
            after.keys().collect::<Vec<_>>(),
            before.keys().collect::<Vec<_>>(),
            "seq values unchanged"
        );
        for (seq, (tx_id, payload)) in &after {
            let (original_tx_id, original_payload) = &before[seq];
            assert_eq!(tx_id, original_tx_id, "seq {seq}: tx_id changed");
            let payload = std::str::from_utf8(payload).expect("utf-8 payload");
            let original_payload = std::str::from_utf8(original_payload).expect("utf-8 payload");
            if *seq <= until {
                assert!(
                    !has_any_diagnostic_key(payload),
                    "seq {seq} still has diagnostics"
                );
                assert_eq!(
                    payload_map(payload),
                    without_diagnostic_keys(payload_map(original_payload))
                );
            } else {
                assert_eq!(
                    payload, original_payload,
                    "seq {seq} above --until-seq was touched"
                );
            }
        }

        let rerun = run_cli(&[
            "strip-diagnostics",
            "--config",
            &config,
            "--until-seq",
            &until_arg,
            "--sleep-ms",
            "0",
        ]);
        rerun.assert_success();
        assert_eq!(rerun.last_of_kind("strip_progress")["rewritten"], 0);
        pool.close().await;
    }
}
