use std::sync::Arc;

use cc_lb_storage_api::{
    CacheKeepaliveSessionStore, MetaStore, PoolQuotaHistoryStore, PoolQuotaSnapshotRecord,
    RequestEvent, RequestEventExportPage, RequestEventKind, RequestEventStore,
    RequestEventStreamFilters, SubscriptionQuotaWindow, UsageRollupResolution, UsageRollupStore,
};
use sqlx::Row;
use uuid::Uuid;

async fn storage(name: &str) -> (tempfile::TempDir, cc_lb_storage_sqlite::SqliteStorage) {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!("sqlite://{}", dir.path().join(name).display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .expect("open sqlite");
    storage.initialize().await.expect("migrate sqlite");
    (dir, storage)
}

#[tokio::test]
async fn every_relational_scalar_roundtrips_without_persisting_diagnostics() {
    let (_dir, storage) = storage("all-relational-scalars.sqlite").await;
    let event = RequestEvent {
        ts: 1_800_000_000,
        request_id: "request_id-value".to_owned(),
        source_kind: Some("source_kind-value".to_owned()),
        source_ref_id: Some("source_ref_id-value".to_owned()),
        event_kind: Some(RequestEventKind::Messages),
        ts_ms: Some(1_800_000_000_000),
        principal_id: Some("principal_id-value".to_owned()),
        key_id: Some("key_id-value".to_owned()),
        principal_kind: Some("principal_kind-value".to_owned()),
        upstream_id: Some(Uuid::from_u128(7)),
        upstream_name: Some("upstream_name-value".to_owned()),
        model: Some("model-value".to_owned()),
        status: 403,
        input_tokens: Some(14),
        output_tokens: Some(15),
        cache_creation_input_tokens: Some(16),
        cache_creation_input_tokens_5m: Some(17),
        cache_creation_input_tokens_1h: Some(18),
        cache_read_input_tokens: Some(19),
        thread_id: Some("thread_id-value".to_owned()),
        observed_session_id: Some("observed_session_id-value".to_owned()),
        request_kind: Some("request_kind-value".to_owned()),
        claude_agent_id: Some("claude_agent_id-value".to_owned()),
        claude_parent_agent_id: Some("claude_parent_agent_id-value".to_owned()),
        parent_session_id: Some("parent_session_id-value".to_owned()),
        client_app: Some("client_app-value".to_owned()),
        session_id_source: Some("session_id_source-value".to_owned()),
        thinking_budget_tokens: Some(28),
        reasoning_effort: Some("reasoning_effort-value".to_owned()),
        token_estimate_source: Some("token_estimate_source-value".to_owned()),
        cost_usd_micros: Some(31),
        cost_input_micros: Some(32),
        cost_output_micros: Some(33),
        cost_cache_creation_5m_micros: Some(34),
        cost_cache_creation_1h_micros: Some(35),
        cost_cache_read_micros: Some(36),
        auth_ms: Some(37),
        route_ms: Some(38),
        limit_reserve_ms: Some(39),
        json_parse_ms: Some(40.125),
        cache_structure_ms: Some(41.125),
        cache_token_key_ms: Some(42.125),
        cache_count_lookup_ms: Some(43.125),
        cache_tokenizer_queue_ms: Some(44.125),
        cache_serialize_ms: Some(45.125),
        cache_tokenize_ms: Some(46.125),
        prepare_signer_ms: Some(47.125),
        bulkhead_wait_ms: Some(48),
        dns_ms: Some(49),
        connect_ms: Some(50),
        connection_reused: Some(false),
        request_body_read_ms: Some(52),
        request_body_first_chunk_ms: Some(53.125),
        request_body_receive_ms: Some(54.125),
        request_body_wait_ms: Some(55.125),
        request_body_process_ms: Some(56.125),
        request_body_chunk_count: Some(57),
        request_body_bytes: Some(58),
        duration_ms: 59,
        proxy_setup_ms: Some(60),
        shape_ms: Some(61),
        sign_ms: Some(62),
        upstream_ttfb_ms: Some(63),
        upstream_body_ms: Some(64),
        response_body_wait_ms: Some(65.125),
        response_body_process_ms: Some(66.125),
        response_body_downstream_poll_gap_ms: Some(67.125),
        retry_overhead_ms: Some(68.125),
        finalize_ms: Some(69),
        body_chunk_count: Some(70),
        body_bytes: Some(71),
        stream_message_start_ms: Some(72),
        stream_content_block_start_ms: Some(73),
        stream_first_content_delta_ms: Some(74),
        stream_last_content_delta_ms: Some(75),
        stream_message_stop_ms: Some(76),
        stream_last_chunk_ms: Some(77),
        stream_total_ms: Some(78),
        sse_event_count: Some(79),
        content_delta_count: Some(80),
        ping_count: Some(81),
        inter_token_avg_ms: Some(82),
        error_code: Some("error_code-value".to_owned()),
        event_id: Some("event_id-value".to_owned()),
        thinking_tokens: Some(85),
        service_tier: Some("service_tier-value".to_owned()),
        upstream_error_type: Some("upstream_error_type-value".to_owned()),
        upstream_error_message: Some("upstream_error_message-value".to_owned()),
        upstream_http_version: Some("upstream_http_version-value".to_owned()),
        upstream_request_id: Some("upstream_request_id-value".to_owned()),
        upstream_content_encoding: Some("upstream_content_encoding-value".to_owned()),
        upstream_content_length: Some(92),
        upstream_body_bytes: Some(93),
        upstream_body_end: Some("upstream_body_end-value".to_owned()),
        upstream_body_error_cause: Some("upstream_body_error_cause-value".to_owned()),
        upstream_body_error_io_kind: Some("upstream_body_error_io_kind-value".to_owned()),
        upstream_body_error_h2_reason: Some("upstream_body_error_h2_reason-value".to_owned()),
        upstream_stream_warning_type: Some("upstream_stream_warning_type-value".to_owned()),
        upstream_stream_warning_message: Some("upstream_stream_warning_message-value".to_owned()),
        cache_control_message_indices: vec![2, 4],
        cache_breakpoints: vec![cc_lb_storage_api::RequestCacheBreakpoint {
            block_index: 1,
            source: cc_lb_storage_api::RequestCacheBreakpointSource::System,
            path: "system[0]".to_owned(),
            message_index: None,
            ttl: Some("5m".to_owned()),
            prefix_hash: "diagnostic-only".to_owned(),
            prefix_token_count: 4,
            lookback_prefixes: Vec::new(),
            token_estimate_source: None,
        }],
        iterations: Some(serde_json::json!({"diagnostic": true})),
        ..RequestEvent::default()
    };
    let cursor = storage
        .append_request_event(&event)
        .await
        .expect("append all scalar values");
    let mut expected = event.clone();
    expected.cache_control_message_indices.clear();
    expected.cache_breakpoints.clear();
    expected.iterations = None;
    let detail = storage
        .get_request_event(event.event_id.as_deref().expect("event id"))
        .await
        .expect("read detail")
        .expect("detail exists");
    assert_eq!(detail, expected);
    let page = storage
        .query_request_events_between_cursors(0, cursor, 10, &RequestEventStreamFilters::default())
        .await
        .expect("read cursor page");
    assert_eq!(page, vec![(cursor, expected.clone())]);
    let exported = storage
        .export_request_events(&RequestEventExportPage {
            after_seq: 0,
            until_seq: cursor,
            limit: 10,
            from_ts_ms: None,
            to_ts_ms: None,
        })
        .await
        .expect("export relational values");
    assert_eq!(exported, vec![(cursor, expected)]);
    let columns = sqlx::query("PRAGMA table_info(request_events_v1)")
        .fetch_all(storage.pool())
        .await
        .expect("schema");
    assert!(
        columns
            .iter()
            .all(|row| row.get::<String, _>("name") != "payload")
    );
    let run = storage
        .rollup_usage_once()
        .await
        .expect("relational rollup");
    assert_eq!(run.processed_events, 1);
    let rollups = storage
        .query_usage_rollups_in_range(
            UsageRollupResolution::Minute,
            0,
            u64::try_from(i64::MAX).expect("max"),
        )
        .await
        .expect("rollups");
    assert_eq!(rollups.len(), 1);
    let rollup = &rollups[0];
    assert_eq!(
        rollup.input_tokens,
        event.input_tokens.expect("input tokens")
    );
    assert_eq!(
        rollup.output_tokens,
        event.output_tokens.expect("output tokens")
    );
    assert_eq!(rollup.latency_ms_sum, event.duration_ms);
    assert_eq!(
        rollup.upstream_ttfb_ms_sum,
        event.upstream_ttfb_ms.expect("ttfb")
    );
    assert_eq!(rollup.error_count, 1);
    let excluded = storage
        .query_overview_excluded_error_buckets_in_range(
            UsageRollupResolution::Minute,
            0,
            u64::try_from(i64::MAX).expect("max"),
        )
        .await
        .expect("relational excluded-error rollup");
    assert_eq!(excluded.len(), 1);
    assert_eq!(excluded[0].error_count, 1);
    assert_eq!(
        storage
            .rollup_usage_once()
            .await
            .expect("idempotent rollup")
            .processed_events,
        0
    );
}

fn pool_record(
    at: i64,
    window: SubscriptionQuotaWindow,
    utilization: f64,
) -> PoolQuotaSnapshotRecord {
    PoolQuotaSnapshotRecord {
        snapshot_at_unix_secs: at,
        window,
        utilization: Some(utilization),
        weighted_utilization_sum: utilization,
        capacity_ratio_sum: 1.0,
        eligible_upstreams: 2,
        contributing_upstreams: 1,
        stale_upstreams: 0,
        missing_observation_upstreams: 1,
        missing_metadata_upstreams: 0,
        header_contributing_upstreams: 1,
        api_contributing_upstreams: 0,
        max_observed_at_unix_millis: Some(at * 1_000),
        computed_at_unix_millis: at * 1_000,
        policy_version: 1,
    }
}

#[tokio::test]
async fn pool_history_retains_anchors_and_clamps_idle_and_bucket_starts() {
    let (_dir, storage) = storage("pool-anchor.sqlite").await;
    let window = SubscriptionQuotaWindow::FiveHour;
    storage
        .record_pool_quota_snapshots(&[
            pool_record(10, window, 0.1),
            pool_record(20, window, 0.2),
            pool_record(30, window, 0.3),
            pool_record(40, window, 0.4),
            pool_record(5, SubscriptionQuotaWindow::SevenDay, 0.5),
        ])
        .await
        .expect("seed pool history");
    assert_eq!(
        storage
            .prune_pool_quota_history_before(30, 0)
            .await
            .expect("zero batch"),
        0
    );
    assert_eq!(
        storage
            .prune_pool_quota_history_before(30, 1)
            .await
            .expect("bounded batch"),
        1
    );
    assert_eq!(
        storage
            .prune_pool_quota_history_before(30, 10)
            .await
            .expect("remaining batch"),
        1
    );
    assert_eq!(
        storage
            .prune_pool_quota_history_before(30, 10)
            .await
            .expect("stable anchors"),
        0
    );
    let summaries = storage
        .list_pool_quota_snapshot_summaries_in_range(&[window], 35, 39)
        .await
        .expect("idle summary anchor");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].snapshot_at_unix_secs, 35);
    assert_eq!(summaries[0].utilization, Some(0.3));
    for (bucket, expected_start) in [(None, 35), (Some(10), 30)] {
        let points = storage
            .list_pool_quota_chart_points_in_range(&[window], 35, 39, bucket)
            .await
            .expect("idle chart anchor");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].snapshot_at_unix_secs, expected_start);
        assert_eq!(points[0].utilization, Some(0.3));
        let exact = storage
            .list_pool_quota_chart_points_in_range(&[window], 40, 40, bucket)
            .await
            .expect("exact boundary");
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].snapshot_at_unix_secs, 40);
        assert_eq!(exact[0].utilization, Some(0.4));
    }
    let exact = storage
        .list_pool_quota_snapshot_summaries_in_range(&[window], 40, 40)
        .await
        .expect("exact summary boundary");
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].utilization, Some(0.4));
    let other = storage
        .list_pool_quota_snapshot_summaries_in_range(&[SubscriptionQuotaWindow::SevenDay], 35, 39)
        .await
        .expect("other partition anchor");
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].utilization, Some(0.5));
}

#[tokio::test]
async fn rollup_and_keepalive_pruners_bound_total_rows_across_both_tables() {
    let (_dir, storage) = storage("retention-total-batches.sqlite").await;
    for (resolution, at) in [
        ("minute", 10_i64),
        ("minute", 20),
        ("hour", 10),
        ("hour", 20),
    ] {
        sqlx::query("INSERT INTO usage_rollups_v2 (resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, model, updated_at) VALUES (?, ?, 'principal', 'upstream', 'upstream', 'model', 0)")
            .bind(resolution).bind(at).execute(storage.pool()).await.expect("seed rollup");
        sqlx::query("INSERT INTO overview_excluded_error_rollups_v1 (resolution, bucket_start_unix_secs, error_count, updated_at) VALUES (?, ?, 1, 0)")
            .bind(resolution).bind(at).execute(storage.pool()).await.expect("seed excluded rollup");
    }
    assert_eq!(
        storage
            .prune_usage_rollups_before(20, 20, 0)
            .await
            .expect("zero batch"),
        0
    );
    assert_eq!(
        storage
            .prune_usage_rollups_before(20, 20, 3)
            .await
            .expect("bounded cross-table batch"),
        3
    );
    assert_eq!(
        storage
            .prune_usage_rollups_before(20, 20, 3)
            .await
            .expect("remaining row"),
        1
    );
    assert_eq!(
        storage
            .prune_usage_rollups_before(20, 20, 3)
            .await
            .expect("cutoff rows retained"),
        0
    );
    for at in [10_i64, 20] {
        sqlx::query("INSERT INTO cache_keepalive_decisions (source_ref_id, decision, reason, generation, ts, last_message_at_ms) VALUES (?, 'skip', 'test', 1, ?, ?)")
            .bind(format!("decision-{at}")).bind(at).bind(at * 1_000)
            .execute(storage.pool()).await.expect("seed decision");
        sqlx::query("INSERT INTO cache_keepalive_turns (source_ref_id, session_key_hash, principal_id, upstream_id, model, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, cost_micros, hit_miss, ts) VALUES (?, 'session', 'principal', 'upstream', 'model', 1, 1, 1, 1, 0, 1, 1, 'hit', ?)")
            .bind(format!("turn-{at}")).bind(at).execute(storage.pool()).await.expect("seed turn");
    }
    assert_eq!(
        storage
            .prune_cache_keepalive_history_before(20, 0)
            .await
            .expect("zero batch"),
        0
    );
    assert_eq!(
        storage
            .prune_cache_keepalive_history_before(20, 1)
            .await
            .expect("bounded history batch"),
        1
    );
    assert_eq!(
        storage
            .prune_cache_keepalive_history_before(20, 3)
            .await
            .expect("remaining history row"),
        1
    );
    assert_eq!(
        storage
            .prune_cache_keepalive_history_before(20, 3)
            .await
            .expect("cutoff rows retained"),
        0
    );
}

#[tokio::test]
async fn relational_migration_preserves_existing_columns_and_cursor_high_water() {
    use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
    use std::str::FromStr;

    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!(
        "sqlite://{}",
        dir.path().join("migration99.sqlite").display()
    );
    let options = SqliteConnectOptions::from_str(&url)
        .expect("connect options")
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .expect("connect sqlite");
    let migrator = sqlx::migrate!("./migrations");
    migrator
        .run_direct(Some(98), &mut connection, false)
        .await
        .expect("migrate through 98");
    sqlx::query(
        "INSERT INTO request_events_v1 \
         (id, request_id, ts, event_id, payload, list_ts_ms, list_event_key, list_status, list_duration_ms) \
         VALUES (42, 'old-request', 1, 'old-event', \
         '{\"principal_kind\":\"backfilled-machine\",\"sse_event_count\":12,\"upstream_request_id\":\"req_up_42\",\"status\":201,\"duration_ms\":50}', \
         1000, 'old-event', 201, 50)",
    ).execute(&mut connection).await.expect("seed pre-relational request");
    sqlx::query("UPDATE sqlite_sequence SET seq = 99 WHERE name = 'request_events_v1'")
        .execute(&mut connection)
        .await
        .expect("advance allocation high water");
    migrator
        .run_direct(Some(99), &mut connection, false)
        .await
        .expect("apply relational migration");
    let row = sqlx::query(
        "SELECT id, request_id, list_status, list_duration_ms, principal_kind, sse_event_count, upstream_request_id \
         FROM request_events_v1 WHERE event_id = 'old-event'",
    )
    .fetch_one(&mut connection)
    .await
    .expect("preserved row");
    assert_eq!(row.get::<i64, _>("id"), 42);
    assert_eq!(row.get::<String, _>("request_id"), "old-request");
    assert_eq!(row.get::<i64, _>("list_status"), 201);
    assert_eq!(row.get::<i64, _>("list_duration_ms"), 50);
    assert_eq!(
        row.get::<Option<String>, _>("principal_kind").as_deref(),
        Some("backfilled-machine"),
    );
    assert_eq!(row.get::<Option<i64>, _>("sse_event_count"), Some(12));
    assert_eq!(
        row.get::<Option<String>, _>("upstream_request_id")
            .as_deref(),
        Some("req_up_42"),
    );
    let high_water: i64 =
        sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'request_events_v1'")
            .fetch_one(&mut connection)
            .await
            .expect("allocation high water");
    assert_eq!(high_water, 99);
    let columns = sqlx::query("PRAGMA table_info(request_events_v1)")
        .fetch_all(&mut connection)
        .await
        .expect("relational schema");
    assert!(
        columns
            .iter()
            .all(|row| row.get::<String, _>("name") != "payload")
    );
    let index_names =
        sqlx::query_scalar::<_, String>("SELECT name FROM sqlite_master WHERE type = 'index'")
            .fetch_all(&mut connection)
            .await
            .expect("indexes");
    assert!(
        !index_names
            .iter()
            .any(|name| name == "usage_rollups_v2_token_interval_idx"
                || name == "idx_audit_log_actor_subject")
    );
}
