//! Operator maintenance methods behind `cc-lb request-events`: seq bounds,
//! per-day counts, the diagnostics export page, the raw payload scan, the
//! payload rewrite, and `verify_schema_current`. Both backends must agree.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_domain::{InternalError, RoutingTrace};
use cc_lb_storage_api::{
    MetaStore, RequestEvent, RequestEventExportPage, RequestEventListQuery,
    RequestEventPayloadScan, RequestEventStore, RequestEventStreamFilters, StorageError,
    UsageRollup, UsageRollupResolution, UsageRollupStore,
};
use serde_json::json;

use crate::harness::{ConformanceBackend, ConformanceFixture};

const DAY_MS: u64 = 86_400_000;
const BASE_DAY: u64 = 20_000;
const DIAGNOSTIC_KEYS: [&str; 5] = [
    "cache_breakpoints",
    "cache_control_message_indices",
    "routing_trace",
    "iterations",
    "internal_errors",
];

fn day_start_ms(offset: u64) -> u64 {
    (BASE_DAY + offset) * DAY_MS
}

fn operator_event(event_id: &str, ts_ms: u64, with_diagnostics: bool) -> RequestEvent {
    let mut event = RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id: format!("{event_id}-request"),
        event_id: Some(event_id.to_owned()),
        principal_id: Some("operator-principal".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 120,
        input_tokens: Some(11),
        output_tokens: Some(7),
        cost_usd_micros: Some(42),
        ..Default::default()
    };
    if with_diagnostics {
        event.cache_control_message_indices = vec![1, 3];
        event.routing_trace = Some(RoutingTrace::default());
        event.internal_errors = vec![InternalError::default()];
        event.iterations = Some(json!([{"input_tokens": 3, "output_tokens": 4}]));
    }
    event
}

fn without_diagnostics(event: &RequestEvent) -> RequestEvent {
    let mut event = event.clone();
    event.cache_breakpoints.clear();
    event.cache_control_message_indices.clear();
    event.routing_trace = None;
    event.iterations = None;
    event.internal_errors.clear();
    event
}

fn payload_diagnostic_keys(payload: &[u8]) -> Result<Vec<&'static str>> {
    let value = serde_json::from_slice::<serde_json::Value>(payload)?;
    let object = value
        .as_object()
        .context("stored request-event payload must be a JSON object")?;
    Ok(DIAGNOSTIC_KEYS
        .into_iter()
        .filter(|key| object.contains_key(*key))
        .collect())
}

/// Removes the five diagnostic keys and leaves every other key untouched;
/// `None` when the payload carries none of them.
fn strip_diagnostic_keys(payload: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut map = serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(payload)?;
    let mut removed = false;
    for key in DIAGNOSTIC_KEYS {
        removed |= map.remove(key).is_some();
    }
    if !removed {
        return Ok(None);
    }
    Ok(Some(serde_json::to_vec(&map)?))
}

fn all_rows_list_query() -> RequestEventListQuery {
    RequestEventListQuery {
        since_unix_secs: 0,
        until_unix_secs: u64::MAX,
        until_ts_ms: None,
        until_event_id: None,
        limit: 50,
        filters: RequestEventStreamFilters::default(),
        source_kind: Some("all".to_owned()),
    }
}

async fn all_usage_rollups<S>(storage: &S) -> Result<Vec<UsageRollup>>
where
    S: UsageRollupStore + ?Sized,
{
    let mut rollups = Vec::new();
    for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
        rollups.extend(
            storage
                .query_usage_rollups_in_range(resolution, 0, i64::MAX as u64)
                .await?,
        );
    }
    Ok(rollups)
}

fn export_page(
    after_seq: u64,
    until_seq: u64,
    from_ts_ms: Option<u64>,
    to_ts_ms: Option<u64>,
    limit: usize,
) -> RequestEventExportPage {
    RequestEventExportPage {
        after_seq,
        until_seq,
        from_ts_ms,
        to_ts_ms,
        limit,
    }
}

fn seqs<T>(rows: &[(u64, T)]) -> Vec<u64> {
    rows.iter().map(|(seq, _)| *seq).collect()
}

/// QA 11 (seq bounds, day counts, export, scan, rewrite), QA 10b (an empty
/// rewrite batch writes nothing), QA 15 (a rewrite moves no cursor: list
/// view and usage rollups stay identical, no double count) and QA 22 (a
/// rewrite of a concurrently pruned seq affects 0 rows without error).
pub async fn request_event_operator_methods<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result = operator_methods_scenario(backend.as_ref(), &fixture).await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn operator_methods_scenario<B: ConformanceBackend>(
    backend: &B,
    fixture: &ConformanceFixture<B>,
) -> Result<()> {
    let storage = fixture.storage();

    let events = [
        operator_event("operator-a", day_start_ms(0) + 1_000, true),
        operator_event("operator-b", day_start_ms(0) + 5 * 3_600_000, true),
        operator_event("operator-c", day_start_ms(1) + 10, false),
        // Exactly on a day boundary: belongs to day 2, never to day 1.
        operator_event("operator-d", day_start_ms(2), false),
    ];
    let mut cursors = Vec::with_capacity(events.len());
    for event in &events {
        cursors.push(storage.append_request_event(event).await?);
    }
    let [c1, c2, c3, c4] = <[u64; 4]>::try_from(cursors.clone())
        .map_err(|_| anyhow::anyhow!("expected four append cursors"))?;
    ensure!(
        c1 < c2 && c2 < c3 && c3 < c4,
        "append cursors must ascend: {cursors:?}"
    );

    // Seq bounds over the half-open list timestamp window.
    let bounds = |from: u64, to: u64| {
        let storage = Arc::clone(&storage);
        async move { storage.request_event_seq_bounds(from, to).await }
    };
    ensure!(bounds(day_start_ms(0), day_start_ms(1)).await? == Some((c1, c2)));
    ensure!(
        bounds(day_start_ms(0), day_start_ms(2)).await? == Some((c1, c3)),
        "the upper bound is exclusive: a row at exactly to_ts_ms is outside"
    );
    ensure!(bounds(day_start_ms(2), day_start_ms(3)).await? == Some((c4, c4)));
    ensure!(bounds(day_start_ms(3), day_start_ms(4)).await?.is_none());
    ensure!(bounds(day_start_ms(1), day_start_ms(1)).await?.is_none());

    // Per-UTC-day counts keyed by list_ts_ms / 86_400_000.
    ensure!(
        storage.request_event_day_counts(1, day_start_ms(3)).await?
            == [(BASE_DAY, 2), (BASE_DAY + 1, 1), (BASE_DAY + 2, 1)]
    );
    ensure!(
        storage
            .request_event_day_counts(day_start_ms(0), day_start_ms(2))
            .await?
            == [(BASE_DAY, 2), (BASE_DAY + 1, 1)]
    );
    ensure!(
        storage
            .request_event_day_counts(day_start_ms(3), day_start_ms(4))
            .await?
            .is_empty()
    );

    // Export pages in seq order and decodes exactly like the detail view.
    // The stable ceiling waits out in-flight writers, so every row at or
    // below it is past the Postgres export xmin guard.
    let until = storage
        .request_event_stable_seq_ceiling(std::time::Duration::from_secs(30))
        .await?;
    ensure!(
        until == c4,
        "stable ceiling {until} must equal the last append {c4}"
    );
    let page_one = storage
        .export_request_events(&export_page(0, until, None, None, 2))
        .await?;
    ensure!(seqs(&page_one) == [c1, c2], "first export page");
    let page_two = storage
        .export_request_events(&export_page(c2, until, None, None, 2))
        .await?;
    ensure!(seqs(&page_two) == [c3, c4], "resumed export page");
    ensure!(
        storage
            .export_request_events(&export_page(c4, until, None, None, 2))
            .await?
            .is_empty(),
        "export past the last row is empty"
    );
    for (seq, exported) in page_one.iter().chain(page_two.iter()) {
        let event_id = exported
            .event_id
            .as_deref()
            .with_context(|| format!("exported row {seq} lacks event_id"))?;
        let detail = storage
            .get_request_event(event_id)
            .await?
            .with_context(|| format!("detail for {event_id}"))?;
        ensure!(
            exported == &detail,
            "export of {event_id} must decode exactly like the detail view"
        );
    }
    let exported_a = &page_one.first().context("first export row")?.1;
    ensure!(
        exported_a.cache_control_message_indices == [1, 3]
            && exported_a.iterations.is_some()
            && exported_a.routing_trace.is_some()
            && exported_a.internal_errors.len() == 1,
        "export must carry the stored diagnostic fields"
    );
    ensure!(
        seqs(
            &storage
                .export_request_events(&export_page(0, c3, None, None, 10))
                .await?
        ) == [c1, c2, c3],
        "until_seq is inclusive"
    );
    ensure!(
        seqs(
            &storage
                .export_request_events(&export_page(
                    0,
                    until,
                    Some(day_start_ms(1)),
                    Some(day_start_ms(3)),
                    10
                ))
                .await?
        ) == [c3, c4],
        "from_ts_ms is inclusive"
    );
    ensure!(
        seqs(
            &storage
                .export_request_events(&export_page(0, until, None, Some(day_start_ms(1)), 10))
                .await?
        ) == [c1, c2],
        "to_ts_ms is exclusive"
    );
    ensure!(
        storage
            .export_request_events(&export_page(0, until, None, None, 0))
            .await?
            .is_empty()
    );

    // Raw payload scan: seq order, inclusive until, limit.
    let scanned = storage
        .scan_request_event_payloads(&RequestEventPayloadScan {
            after_seq: 0,
            until_seq: c4,
            limit: 10,
        })
        .await?;
    ensure!(seqs(&scanned) == [c1, c2, c3, c4], "full payload scan");
    let [(_, payload_a), (_, payload_b), (_, payload_c), _] = scanned.as_slice() else {
        anyhow::bail!("payload scan returned {} rows", scanned.len());
    };
    ensure!(
        payload_diagnostic_keys(payload_a)?
            == [
                "cache_control_message_indices",
                "routing_trace",
                "iterations",
                "internal_errors"
            ],
        "scan returns the raw stored payload with its diagnostic keys"
    );
    ensure!(payload_diagnostic_keys(payload_c)?.is_empty());
    ensure!(
        seqs(
            &storage
                .scan_request_event_payloads(&RequestEventPayloadScan {
                    after_seq: c1,
                    until_seq: c3,
                    limit: 1,
                })
                .await?
        ) == [c2],
        "scan honors after_seq and limit"
    );

    // Baselines before the rewrite.
    backend.wait_for_events_visible_for_rollup(&storage).await?;
    let first_rollup = storage.rollup_usage_once().await?;
    ensure!(
        first_rollup.processed_events == 4,
        "baseline rollup must process the four events, got {}",
        first_rollup.processed_events
    );
    let rollups_before = all_usage_rollups(storage.as_ref()).await?;
    let list_before = storage.list_request_events(&all_rows_list_query()).await?;
    let cursor_before = storage.current_request_event_cursor().await?;
    let detail_a_before = storage
        .get_request_event("operator-a")
        .await?
        .context("detail operator-a")?;

    ensure!(
        storage.rewrite_request_event_payloads(&[]).await? == 0,
        "an empty rewrite batch writes nothing"
    );
    ensure!(
        strip_diagnostic_keys(payload_c)?.is_none(),
        "a payload without diagnostic keys needs no rewrite"
    );
    let stripped_a = strip_diagnostic_keys(payload_a)?.context("strip operator-a")?;
    let stripped_b = strip_diagnostic_keys(payload_b)?.context("strip operator-b")?;
    let rewritten = storage
        .rewrite_request_event_payloads(&[(c1, stripped_a.clone()), (c2, stripped_b.clone())])
        .await?;
    ensure!(
        rewritten == 2,
        "rewrite must update both rows, got {rewritten}"
    );

    let rescanned = storage
        .scan_request_event_payloads(&RequestEventPayloadScan {
            after_seq: 0,
            until_seq: c4,
            limit: 10,
        })
        .await?;
    ensure!(
        seqs(&rescanned) == [c1, c2, c3, c4],
        "rewrite keeps every seq"
    );
    ensure!(
        rescanned.first().map(|(_, payload)| payload) == Some(&stripped_a)
            && rescanned.get(1).map(|(_, payload)| payload) == Some(&stripped_b),
        "rewrite stores the given payload bytes verbatim"
    );
    for (seq, payload) in &rescanned {
        ensure!(
            payload_diagnostic_keys(payload)?.is_empty(),
            "row {seq} still carries diagnostic keys after the rewrite"
        );
    }
    let detail_a_after = storage
        .get_request_event("operator-a")
        .await?
        .context("detail operator-a after rewrite")?;
    ensure!(
        detail_a_after == without_diagnostics(&detail_a_before),
        "rewrite must remove exactly the diagnostic fields"
    );

    ensure!(
        storage.current_request_event_cursor().await? == cursor_before,
        "rewrite must not move the request-event cursor"
    );
    ensure!(
        storage.list_request_events(&all_rows_list_query()).await? == list_before,
        "rewrite must not change the list view"
    );
    backend.wait_for_events_visible_for_rollup(&storage).await?;
    let second_rollup = storage.rollup_usage_once().await?;
    ensure!(
        second_rollup.processed_events == 0,
        "rewritten rows must not be rolled up again, got {}",
        second_rollup.processed_events
    );
    ensure!(
        all_usage_rollups(storage.as_ref()).await? == rollups_before,
        "rewrite must not change usage rollups"
    );

    // A seq pruned between scan and rewrite affects nothing and is no error.
    let cutoff_ms = ((day_start_ms(0) + 1_000) / 1_000 + 1) * 1_000;
    let pruned = storage
        .prune_request_events_before(cutoff_ms * 1_000_000, 10)
        .await?;
    ensure!(
        pruned == 1,
        "prune must delete only operator-a, got {pruned}"
    );
    let rewritten = storage
        .rewrite_request_event_payloads(&[(c1, stripped_a), (c2, stripped_b)])
        .await?;
    ensure!(
        rewritten == 1,
        "a pruned seq must affect 0 rows, got {rewritten} total"
    );
    ensure!(
        seqs(
            &storage
                .scan_request_event_payloads(&RequestEventPayloadScan {
                    after_seq: 0,
                    until_seq: c4,
                    limit: 10,
                })
                .await?
        ) == [c2, c3, c4],
        "rewrite must not resurrect a pruned row"
    );

    Ok(())
}

/// QA 11: `verify_schema_current` accepts the freshly migrated schema and
/// versions from a newer binary, and rejects a checksum mismatch and a
/// missing version.
pub async fn verify_schema_current_detects_drift<B: ConformanceBackend>(
    backend: Arc<B>,
) -> Result<()> {
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result = verify_schema_scenario(backend.as_ref(), &fixture).await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn verify_schema_scenario<B: ConformanceBackend>(
    backend: &B,
    fixture: &ConformanceFixture<B>,
) -> Result<()> {
    let storage = fixture.storage();
    storage
        .verify_schema_current()
        .await
        .context("freshly migrated schema must verify")?;

    let inserted = backend
        .execute_raw_sql(
            &storage,
            "INSERT INTO _sqlx_migrations \
             (version, description, success, checksum, execution_time) \
             SELECT 999999999, 'applied by a newer binary', success, checksum, execution_time \
             FROM _sqlx_migrations \
             WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)",
        )
        .await?;
    ensure!(inserted == 1, "seed newer-binary migration row");
    storage
        .verify_schema_current()
        .await
        .context("versions unknown to this binary are allowed")?;

    let tampered = backend
        .execute_raw_sql(
            &storage,
            "UPDATE _sqlx_migrations SET checksum = \
             (SELECT checksum FROM _sqlx_migrations WHERE version = 999999999) \
             WHERE version = (SELECT MIN(version) FROM _sqlx_migrations)",
        )
        .await?;
    ensure!(tampered == 1, "tamper oldest migration checksum");
    match storage.verify_schema_current().await {
        Err(StorageError::Fatal { message }) => ensure!(
            message.contains("checksum mismatch versions [")
                && !message.contains("checksum mismatch versions []"),
            "checksum mismatch must be listed: {message}"
        ),
        other => anyhow::bail!("checksum mismatch must fail as Fatal, got {other:?}"),
    }

    let deleted = backend
        .execute_raw_sql(
            &storage,
            "DELETE FROM _sqlx_migrations \
             WHERE version = (SELECT MAX(version) FROM _sqlx_migrations WHERE version < 999999999)",
        )
        .await?;
    ensure!(deleted == 1, "delete newest embedded migration row");
    match storage.verify_schema_current().await {
        Err(StorageError::Fatal { message }) => ensure!(
            message.contains("missing migration versions [")
                && !message.contains("missing migration versions []"),
            "missing version must be listed: {message}"
        ),
        other => anyhow::bail!("missing migration must fail as Fatal, got {other:?}"),
    }

    Ok(())
}
