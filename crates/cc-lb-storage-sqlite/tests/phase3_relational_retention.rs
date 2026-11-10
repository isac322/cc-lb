use std::{path::Path, str::FromStr, sync::Arc};

use cc_lb_storage_api::{
    CacheKeepaliveEvidenceRow, CacheKeepaliveReplaceRequest, CacheKeepaliveSessionStore, CacheTtl,
    MetaStore, PoolQuotaHistoryStore, PoolQuotaSnapshotRecord, RequestEvent,
    RequestEventExportPage, RequestEventKind, RequestEventStore, RequestEventStreamFilters,
    SubscriptionQuotaWindow, UsageRollupResolution, UsageRollupStore,
};
use sqlx::{Connection, Row, migrate::Migrator, sqlite::SqliteConnectOptions};
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

#[tokio::test]
async fn canonical_gemini_events_share_one_rollup_series_and_controls_stay_unchanged() {
    let (_dir, storage) = storage("canonical-gemini-rollup.sqlite").await;
    let upstream_id = Uuid::from_u128(1_204_001);
    let rows = [
        ("gemini-cached", Some("gemini-2.5-flash")),
        ("gemini-native", Some("gemini-2.5-flash")),
        ("openai-control", Some("gpt-4o")),
        ("unknown-control", None),
    ];
    for (index, (request_id, model)) in rows.into_iter().enumerate() {
        storage
            .append_request_event(&RequestEvent {
                ts: 1_800_000_000,
                ts_ms: Some(1_800_000_000_000 + index as u64),
                request_id: request_id.to_owned(),
                event_id: Some(format!("canonical-gemini-{request_id}")),
                principal_id: Some("principal-test".to_owned()),
                upstream_id: Some(upstream_id),
                upstream_name: Some("gemini-identity-events".to_owned()),
                model: model.map(str::to_owned),
                status: 200,
                input_tokens: Some(2),
                output_tokens: Some(1),
                duration_ms: 1,
                ..RequestEvent::default()
            })
            .await
            .expect("append canonical identity event");
    }

    let events = storage
        .query_request_events_between_cursors(
            0,
            storage
                .current_request_event_cursor()
                .await
                .expect("cursor"),
            10,
            &RequestEventStreamFilters::default(),
        )
        .await
        .expect("query request events");
    assert_eq!(
        events
            .iter()
            .map(|(_, event)| event.model.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("gemini-2.5-flash"),
            Some("gemini-2.5-flash"),
            Some("gpt-4o"),
            None
        ]
    );

    let first_run = storage
        .rollup_usage_once()
        .await
        .expect("roll up canonical identity events");
    assert_eq!(first_run.processed_events, 4);
    let rollups = storage
        .query_usage_rollups_in_range(UsageRollupResolution::Minute, 0, 1_800_000_060)
        .await
        .expect("query canonical identity rollups");
    assert_eq!(rollups.len(), 3);
    let gemini = rollups
        .iter()
        .find(|rollup| rollup.model == "gemini-2.5-flash")
        .expect("one Gemini model series");
    assert_eq!(gemini.request_count, 2);
    assert_eq!(gemini.input_tokens, 4);
    assert_eq!(gemini.output_tokens, 2);
    assert!(rollups.iter().any(|rollup| rollup.model == "gpt-4o"));
    assert!(rollups.iter().any(|rollup| rollup.model == "unknown"));

    let second_run = storage
        .rollup_usage_once()
        .await
        .expect("second rollup is idempotent");
    assert_eq!(second_run.processed_events, 0);
    assert_eq!(
        storage
            .query_usage_rollups_in_range(UsageRollupResolution::Minute, 0, 1_800_000_060)
            .await
            .expect("query rollups after idempotent run"),
        rollups
    );
}

#[tokio::test]
async fn canonicalize_gemini_model_identity_migration_preserves_historical_rollups() {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!(
        "sqlite://{}",
        dir.path().join("canonicalize.sqlite").display()
    );
    let options = SqliteConnectOptions::from_str(&url)
        .expect("connect options")
        .create_if_missing(true)
        .foreign_keys(true);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .expect("connect sqlite");
    let migrator = Migrator::new(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/migrations"
    )))
    .await
    .expect("load migrations");
    migrator
        .run_direct(Some(113), &mut connection, false)
        .await
        .expect("migrate through 113");

    let gemini_upstream_id = Uuid::from_u128(1_204_011);
    let unrelated_upstream_id = Uuid::from_u128(1_204_012);
    for (id, name, kind) in [
        (gemini_upstream_id, "gemini-history", "gemini_api_key"),
        (unrelated_upstream_id, "unrelated-history", "openai_api_key"),
    ] {
        sqlx::query(
            "INSERT INTO upstream_spec_v1 (
                id, name, kind, base_url, enabled, warmup_enabled,
                warmup_dialect_plugin, spec_revision, created_at, updated_at,
                deleted_at, workspace_id
            ) VALUES (?, ?, ?, NULL, 1, 0, NULL, 1, 0, 0, NULL, NULL)",
        )
        .bind(id.to_string())
        .bind(name)
        .bind(kind)
        .execute(&mut connection)
        .await
        .expect("seed upstream");
    }
    connection.close().await.expect("close seed connection");

    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open pre-migration sqlite");
    sqlx::raw_sql(include_str!(
        "../migrations/0114_canonicalize_gemini_model_identity.sql"
    ))
    .execute(storage.pool())
    .await
    .expect("install reconciliation state");
    sqlx::raw_sql(include_str!(
        "../migrations/0117_request_events_upstream_error_code.sql"
    ))
    .execute(storage.pool())
    .await
    .expect("install provider error code column for current writer");
    sqlx::query(
        "UPDATE usage_rollup_reconciliations_v1
         SET status = 'complete'
         WHERE id = 'canonicalize_gemini_model_identity'",
    )
    .execute(storage.pool())
    .await
    .expect("mark reconciliation complete while seeding history");
    let events = [
        (
            "gemini-prefixed",
            Some(gemini_upstream_id),
            Some("gemini-history"),
            Some("models/gemini-2.5-flash"),
            401,
            10,
            5,
            5,
        ),
        (
            "gemini-canonical",
            Some(gemini_upstream_id),
            Some("gemini-history"),
            Some("gemini-2.5-flash"),
            200,
            7,
            3,
            7,
        ),
        (
            "gemini-tuned",
            Some(gemini_upstream_id),
            Some("gemini-history"),
            Some("models/tunedModels/tuned-1"),
            200,
            17,
            2,
            4,
        ),
        (
            "gemini-nested",
            Some(gemini_upstream_id),
            Some("gemini-history"),
            Some("models/models/gemini-2.5-flash"),
            200,
            19,
            1,
            3,
        ),
        (
            "unrelated-prefixed",
            Some(unrelated_upstream_id),
            Some("unrelated-history"),
            Some("models/gemini-2.5-flash"),
            200,
            11,
            4,
            11,
        ),
        ("unknown-model", None, None, None, 404, 13, 6, 13),
    ];
    for (
        request_id,
        upstream_id,
        upstream_name,
        model,
        status,
        input_tokens,
        output_tokens,
        duration_ms,
    ) in events
    {
        storage
            .append_request_event(&RequestEvent {
                ts: 1_800_000_000,
                ts_ms: Some(1_800_000_000_000),
                request_id: request_id.to_owned(),
                event_id: Some(format!("canonicalize-{request_id}")),
                principal_id: Some("historical-principal".to_owned()),
                upstream_id,
                upstream_name: upstream_name.map(str::to_owned),
                model: model.map(str::to_owned),
                status,
                input_tokens: Some(input_tokens),
                output_tokens: Some(output_tokens),
                duration_ms,
                ..RequestEvent::default()
            })
            .await
            .expect("seed historical request event");
    }

    let seeded = storage
        .rollup_usage_once()
        .await
        .expect("seed historical rollups");
    assert_eq!(seeded.processed_events, 6);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM usage_rollups_v2 \
             WHERE resolution = 'minute'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("count seeded rollups"),
        6
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT error_count FROM overview_excluded_error_rollups_v1 \
             WHERE resolution = 'minute' AND bucket_start_unix_secs = ?",
        )
        .bind(1_800_000_000_i64)
        .fetch_one(storage.pool())
        .await
        .expect("read seeded excluded errors"),
        2
    );
    sqlx::query(
        "INSERT INTO usage_rollups_v2 (
            resolution, bucket_start_unix_secs, principal_id, upstream_id,
            upstream_name, model, request_count, input_tokens, output_tokens,
            error_count, latency_count, latency_ms_sum, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("minute")
    .bind(1_800_000_000_i64)
    .bind("rollup-only-principal")
    .bind(gemini_upstream_id.to_string())
    .bind("gemini-history")
    .bind("models_gemini-2.5-flash")
    .bind(5_i64)
    .bind(23_i64)
    .bind(11_i64)
    .bind(0_i64)
    .bind(1_i64)
    .bind(9_i64)
    .bind(0_i64)
    .execute(storage.pool())
    .await
    .expect("seed mixed-history rollup-only row");
    sqlx::query(
        "INSERT INTO usage_rollups_v2 (
            resolution, bucket_start_unix_secs, principal_id, upstream_id,
            upstream_name, model, request_count, input_tokens, output_tokens,
            error_count, latency_count, latency_ms_sum, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("minute")
    .bind(1_800_000_000_i64)
    .bind("rollup-only-triple-prefix")
    .bind(gemini_upstream_id.to_string())
    .bind("gemini-history")
    .bind("models_models_gemini-2.5-flash")
    .bind(2_i64)
    .bind(11_i64)
    .bind(5_i64)
    .bind(0_i64)
    .bind(1_i64)
    .bind(6_i64)
    .bind(0_i64)
    .execute(storage.pool())
    .await
    .expect("seed two-prefix rollup-only row");
    sqlx::query(
        "INSERT INTO usage_rollups_v2 (
            resolution, bucket_start_unix_secs, principal_id, upstream_id,
            upstream_name, model, request_count, input_tokens, output_tokens,
            error_count, latency_count, latency_ms_sum, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("minute")
    .bind(1_800_000_000_i64)
    .bind("rollup-only-triple-prefix")
    .bind(gemini_upstream_id.to_string())
    .bind("gemini-history")
    .bind("models_models_models_gemini-2.5-flash")
    .bind(7_i64)
    .bind(31_i64)
    .bind(13_i64)
    .bind(0_i64)
    .bind(1_i64)
    .bind(10_i64)
    .bind(0_i64)
    .execute(storage.pool())
    .await
    .expect("seed triple-prefix rollup-only row");

    sqlx::query(
        "UPDATE usage_rollup_reconciliations_v1
         SET status = 'pending', cursor = 0, event_upper_bound = 0, rollup_upper_bound = 0
         WHERE id = 'canonicalize_gemini_model_identity'",
    )
    .execute(storage.pool())
    .await
    .expect("reset reconciliation after seeding history");

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM usage_rollup_reconciliations_v1 \
             WHERE id = 'canonicalize_gemini_model_identity'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("read pending reconciliation"),
        "pending"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM usage_rollups_v2 WHERE resolution = 'minute'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("count retained minute rollups"),
        9
    );

    let first_reconciliation_run = storage
        .rollup_usage_once()
        .await
        .expect("start historical reconciliation");
    assert_eq!(first_reconciliation_run.processed_events, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM usage_rollups_v2 WHERE resolution = 'minute'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("count retained minute rollups"),
        9
    );
    let second_reconciliation_run = storage
        .rollup_usage_once()
        .await
        .expect("rewrite and merge one reconciliation batch");
    assert_eq!(second_reconciliation_run.processed_events, 0);
    let nested_reconciliation_run = storage
        .rollup_usage_once()
        .await
        .expect("finish nested reconciliation batch");
    assert_eq!(nested_reconciliation_run.processed_events, 0);
    let deep_reconciliation_run = storage
        .rollup_usage_once()
        .await
        .expect("finish deeper nested reconciliation batch");
    assert_eq!(deep_reconciliation_run.processed_events, 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM usage_rollup_reconciliations_v1 \
             WHERE id = 'canonicalize_gemini_model_identity'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("read completed reconciliation"),
        "complete"
    );

    for (event_id, expected_model) in [
        ("canonicalize-gemini-prefixed", Some("gemini-2.5-flash")),
        ("canonicalize-gemini-canonical", Some("gemini-2.5-flash")),
        (
            "canonicalize-gemini-nested",
            Some("models/gemini-2.5-flash"),
        ),
        (
            "canonicalize-gemini-tuned",
            Some("models/tunedModels/tuned-1"),
        ),
        (
            "canonicalize-unrelated-prefixed",
            Some("models/gemini-2.5-flash"),
        ),
        ("canonicalize-unknown-model", None),
    ] {
        let event = storage
            .get_request_event(event_id)
            .await
            .expect("read migrated event")
            .expect("migrated event exists");
        assert_eq!(event.model.as_deref(), expected_model);
    }

    let first_run = storage
        .rollup_usage_once()
        .await
        .expect("roll up migrated history");
    assert_eq!(first_run.processed_events, 0);
    let rollups = storage
        .query_usage_rollups_in_range(UsageRollupResolution::Minute, 0, 1_800_000_060)
        .await
        .expect("query migrated rollups");
    assert_eq!(rollups.len(), 8);
    let gemini = rollups
        .iter()
        .find(|rollup| rollup.model == "gemini-2.5-flash")
        .expect("canonical Gemini rollup");
    assert_eq!(gemini.request_count, 2);
    assert_eq!(gemini.input_tokens, 17);
    assert_eq!(gemini.output_tokens, 8);
    assert_eq!(gemini.error_count, 1);
    let tuned = rollups
        .iter()
        .find(|rollup| rollup.model == "models_tunedModels_tuned-1")
        .expect("tuned Gemini rollup is preserved");
    assert_eq!(tuned.request_count, 1);
    assert_eq!(tuned.input_tokens, 17);
    let rollup_only = rollups
        .iter()
        .find(|rollup| {
            rollup.model == "gemini-2.5-flash" && rollup.principal == "rollup-only-principal"
        })
        .expect("rollup-only mixed history is preserved");
    assert_eq!(rollup_only.request_count, 5);
    assert_eq!(rollup_only.input_tokens, 23);
    let nested = rollups
        .iter()
        .find(|rollup| {
            rollup.model == "models_gemini-2.5-flash" && rollup.upstream_id == gemini_upstream_id
        })
        .expect("nested Gemini rollup is canonicalized exactly once");
    assert_eq!(nested.request_count, 1);
    assert_eq!(nested.input_tokens, 19);
    let triple_prefix = rollups
        .iter()
        .find(|rollup| {
            rollup.model == "models_models_gemini-2.5-flash"
                && rollup.principal == "rollup-only-triple-prefix"
        })
        .expect("triple-prefix Gemini rollup is canonicalized exactly once");
    assert_eq!(triple_prefix.request_count, 7);
    assert_eq!(triple_prefix.input_tokens, 31);
    let two_prefix_source = rollups
        .iter()
        .find(|rollup| {
            rollup.model == "models_gemini-2.5-flash"
                && rollup.principal == "rollup-only-triple-prefix"
        })
        .expect("two-prefix source is processed before its deeper destination");
    assert_eq!(two_prefix_source.request_count, 2);
    assert_eq!(two_prefix_source.input_tokens, 11);
    assert!(
        !rollups
            .iter()
            .any(|rollup| rollup.model.starts_with("models_models_models_"))
    );
    assert_eq!(gemini.latency_ms_sum, 12);
    assert!(rollups.iter().any(|rollup| {
        rollup.model == "models_gemini-2.5-flash" && rollup.upstream_id == unrelated_upstream_id
    }));
    assert!(rollups.iter().any(|rollup| rollup.model == "unknown"));

    let excluded = storage
        .query_overview_excluded_error_buckets_in_range(
            UsageRollupResolution::Minute,
            0,
            1_800_000_060,
        )
        .await
        .expect("query migrated excluded errors");
    assert_eq!(excluded.len(), 1);
    assert_eq!(excluded[0].error_count, 2);

    let second_run = storage
        .rollup_usage_once()
        .await
        .expect("run migrated rollup again");
    assert_eq!(second_run.processed_events, 0);
    let third_run = storage
        .rollup_usage_once()
        .await
        .expect("run completed reconciliation again");
    assert_eq!(third_run.processed_events, 0);
    let nested_event = storage
        .get_request_event("canonicalize-gemini-nested")
        .await
        .expect("read nested event after repeat")
        .expect("nested event exists");
    assert_eq!(
        nested_event.model.as_deref(),
        Some("models/gemini-2.5-flash")
    );
    assert_eq!(
        storage
            .query_usage_rollups_in_range(UsageRollupResolution::Minute, 0, 1_800_000_060)
            .await
            .expect("query idempotent migrated rollups"),
        rollups
    );
}

#[tokio::test]
async fn canonicalize_gemini_rollup_only_history_merges_prefixed_rows() {
    let (_dir, storage) = storage("canonicalize-rollup-only.sqlite").await;
    let upstream_id = Uuid::from_u128(1_204_013);
    sqlx::query(
        "INSERT INTO upstream_spec_v1 (
            id, name, kind, base_url, enabled, warmup_enabled,
            warmup_dialect_plugin, spec_revision, created_at, updated_at,
            deleted_at, workspace_id
        ) VALUES (?, ?, 'gemini_api_key', NULL, 1, 0, NULL, 1, 0, 0, NULL, NULL)",
    )
    .bind(upstream_id.to_string())
    .bind("rollup-only-gemini")
    .execute(storage.pool())
    .await
    .expect("seed Gemini upstream");

    for (
        model,
        request_count,
        input_tokens,
        output_tokens,
        cache_write,
        error_count,
        latency_sum,
        latency_min,
        latency_max,
        virtual_cost,
        cost_input,
    ) in [
        (
            "gemini-2.5-flash",
            2,
            10,
            4,
            None,
            1,
            20,
            Some(8),
            Some(12),
            100,
            30,
        ),
        (
            "models_gemini-2.5-flash",
            3,
            7,
            5,
            Some(9),
            2,
            15,
            Some(3),
            Some(9),
            70,
            20,
        ),
    ] {
        sqlx::query(
            "INSERT INTO usage_rollups_v2 (
                resolution, bucket_start_unix_secs, principal_id, upstream_id,
                upstream_name, model, request_count, input_tokens, output_tokens,
                cache_write_input_tokens, error_count, latency_count, latency_ms_sum,
                latency_ms_min, latency_ms_max, virtual_cost_micros, cost_input_micros,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("minute")
        .bind(1_800_000_000_i64)
        .bind("rollup-only-principal")
        .bind(upstream_id.to_string())
        .bind("rollup-only-gemini")
        .bind(model)
        .bind(request_count)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(cache_write)
        .bind(error_count)
        .bind(request_count)
        .bind(latency_sum)
        .bind(latency_min)
        .bind(latency_max)
        .bind(virtual_cost)
        .bind(cost_input)
        .bind(0_i64)
        .execute(storage.pool())
        .await
        .expect("seed rollup");
    }
    sqlx::query(
        "UPDATE usage_rollup_reconciliations_v1
         SET status = 'pending', cursor = 0, event_upper_bound = 0, rollup_upper_bound = 0
         WHERE id = 'canonicalize_gemini_model_identity'",
    )
    .execute(storage.pool())
    .await
    .expect("reset reconciliation");

    storage
        .rollup_usage_once()
        .await
        .expect("merge stale rollup-only history");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT marker_model FROM usage_rollup_reconciliation_markers_v1
             WHERE principal_id = ? ORDER BY marker_model LIMIT 1",
        )
        .bind("rollup-only-principal")
        .fetch_one(storage.pool())
        .await
        .expect("read source rollup marker"),
        "models_gemini-2.5-flash"
    );
    storage
        .rollup_usage_once()
        .await
        .expect("finish nested stale rollup-only history");

    let merged = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            i64,
            Option<i64>,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
        ),
    >(
        "SELECT request_count, input_tokens, output_tokens, cache_write_input_tokens,
                error_count, latency_count, latency_ms_sum, latency_ms_min, latency_ms_max,
                virtual_cost_micros, cost_input_micros
         FROM usage_rollups_v2
         WHERE resolution = 'minute' AND bucket_start_unix_secs = ?
           AND principal_id = ? AND upstream_id = ? AND model = ?",
    )
    .bind(1_800_000_000_i64)
    .bind("rollup-only-principal")
    .bind(upstream_id.to_string())
    .bind("gemini-2.5-flash")
    .fetch_one(storage.pool())
    .await
    .expect("read merged rollup");
    assert_eq!(merged, (5, 17, 9, Some(9), 3, 5, 35, 3, 12, 170, 50));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM usage_rollups_v2 WHERE model = 'models_gemini-2.5-flash'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("count removed source rollups"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM usage_rollup_reconciliations_v1
             WHERE id = 'canonicalize_gemini_model_identity'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("read reconciliation status"),
        "complete"
    );

    storage
        .rollup_usage_once()
        .await
        .expect("run idempotent rollup-only reconciliation");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT request_count FROM usage_rollups_v2
             WHERE resolution = 'minute' AND bucket_start_unix_secs = ?
               AND principal_id = ? AND upstream_id = ? AND model = ?",
        )
        .bind(1_800_000_000_i64)
        .bind("rollup-only-principal")
        .bind(upstream_id.to_string())
        .bind("gemini-2.5-flash")
        .fetch_one(storage.pool())
        .await
        .expect("read idempotent merged rollup"),
        5
    );
}

#[tokio::test]
async fn usage_rollup_conflict_accumulates_nullable_provider_cache_writes() {
    let (_dir, storage) = storage("usage-rollup-cache-writes.sqlite").await;
    let upstream_id = Uuid::from_u128(7);
    let batches = [
        ("null-cache-writes", None, None),
        ("zero-cache-writes", Some((0, 0, 0)), Some((0, 0, 0))),
        ("first-cache-writes", Some((3, 5, 7)), Some((3, 5, 7))),
        (
            "second-cache-writes",
            Some((11, 13, 17)),
            Some((14, 18, 24)),
        ),
    ];

    for (request_id, cache_writes, expected) in batches {
        let (cache_write_input_tokens, cache_write_input_tokens_30m, cache_write_input_tokens_12h) =
            match cache_writes {
                Some((five_minute, thirty_minute, twelve_hour)) => {
                    (Some(five_minute), Some(thirty_minute), Some(twelve_hour))
                }
                None => (None, None, None),
            };
        let event = RequestEvent {
            ts: 1_800_000_000,
            ts_ms: Some(1_800_000_000_000),
            request_id: request_id.to_owned(),
            event_id: Some(format!("rollup-cache-write-{request_id}")),
            upstream_id: Some(upstream_id),
            upstream_name: Some("rollup-upstream".to_owned()),
            model: Some("rollup-model".to_owned()),
            status: 200,
            duration_ms: 1,
            cache_write_input_tokens,
            cache_write_input_tokens_30m,
            cache_write_input_tokens_12h,
            ..RequestEvent::default()
        };
        storage
            .append_request_event(&event)
            .await
            .expect("append cache-write rollup event");
        storage
            .rollup_usage_once()
            .await
            .expect("roll up cache-write event");

        let rollups = storage
            .query_usage_rollups_in_range(
                UsageRollupResolution::Minute,
                0,
                u64::try_from(i64::MAX).expect("max"),
            )
            .await
            .expect("query cache-write rollup");
        assert_eq!(rollups.len(), 1);
        let rollup = &rollups[0];
        match expected {
            Some((five_minute, thirty_minute, twelve_hour)) => {
                assert_eq!(rollup.cache_write_input_tokens, Some(five_minute));
                assert_eq!(rollup.cache_write_input_tokens_30m, Some(thirty_minute));
                assert_eq!(rollup.cache_write_input_tokens_12h, Some(twelve_hour));
            }
            None => {
                assert_eq!(rollup.cache_write_input_tokens, None);
                assert_eq!(rollup.cache_write_input_tokens_30m, None);
                assert_eq!(rollup.cache_write_input_tokens_12h, None);
            }
        }
    }
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
async fn cache_keepalive_evidence_pruning_obeys_the_shared_batch_and_cutoff() {
    let (_dir, storage) = storage("cache-keepalive-evidence.sqlite").await;
    for (id, request_start_ms) in [("old-evidence", 10_000_i64), ("boundary-evidence", 20_000)] {
        sqlx::query(
            "INSERT INTO cache_keepalive_evidence
             (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
              prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
              upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
              request_ref, event_ref, created_at_ms)
             VALUES (?, NULL, 'follow_up', ?, 'fingerprint', 'prefix', 'policy', 'schema',
                     'principal', ?, 'model', '5m', 'hit', 1, 1, ?, ?, ?)",
        )
        .bind(id)
        .bind(request_start_ms)
        .bind(Uuid::from_u128(7).to_string())
        .bind(format!("request-{id}"))
        .bind(format!("event-{id}"))
        .bind(request_start_ms)
        .execute(storage.pool())
        .await
        .expect("seed evidence");
    }

    assert_eq!(
        storage
            .prune_cache_keepalive_history_before(3_620, 1)
            .await
            .expect("prune one evidence row"),
        1
    );
    let remaining: Vec<String> =
        sqlx::query_scalar("SELECT evidence_id FROM cache_keepalive_evidence ORDER BY evidence_id")
            .fetch_all(storage.pool())
            .await
            .expect("read remaining evidence");
    assert_eq!(remaining, vec!["boundary-evidence"]);
}
#[tokio::test]
async fn append_evidence_filters_follow_ups_and_deduplicates_overlapping_cycles() {
    let (_dir, storage) = storage("cache-keepalive-evidence-filter.sqlite").await;
    let upstream_id = Uuid::from_u128(7);
    let cycle_id = Uuid::from_u128(8);
    let overlapping_cycle_id = Uuid::from_u128(9);
    let evidence =
        |id: &str, kind: &str, start: u64, cycle_id: Option<Uuid>| CacheKeepaliveEvidenceRow {
            evidence_id: id.to_owned(),
            cycle_id,
            evidence_kind: kind.to_owned(),
            request_start_ms: start,
            prefix_fingerprint: Some("fingerprint".to_owned()),
            prefix_hash: Some("prefix".to_owned()),
            prefix_policy_version: Some("policy".to_owned()),
            prefix_schema_version: Some("schema".to_owned()),
            principal_id: "principal".to_owned(),
            upstream_id,
            model: "model".to_owned(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some("hit".to_owned()),
            cache_read_input_tokens: Some(1),
            cost_micros: Some(1),
            request_ref: Some(id.to_owned()),
            event_ref: Some(id.to_owned()),
        };
    let mut invalid = evidence("invalid", "follow_up", 1_400, None);
    invalid.cache_read_input_tokens = Some(0);
    let mut unmatched = evidence("unmatched", "follow_up", 1_500, None);
    unmatched.prefix_hash = Some("other-prefix".to_owned());
    let cursor = storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 1,
                ts_ms: Some(1_000),
                event_id: Some("filtered-event".to_owned()),
                ..RequestEvent::default()
            },
            &[
                evidence("renewal", "renewal", 1_000, Some(cycle_id)),
                evidence(
                    "overlapping-renewal",
                    "renewal",
                    1_100,
                    Some(overlapping_cycle_id),
                ),
                evidence("first-follow-up", "follow_up", 1_200, None),
                evidence("second-follow-up", "follow_up", 1_300, None),
                invalid,
                unmatched,
            ],
        )
        .await
        .expect("append request and filtered evidence");
    assert!(cursor > 0);
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT evidence_id FROM cache_keepalive_evidence ORDER BY evidence_id")
            .fetch_all(storage.pool())
            .await
            .expect("read filtered evidence");
    assert_eq!(
        ids,
        vec!["first-follow-up", "overlapping-renewal", "renewal",]
    );
    let running = storage
        .replace_from_real_request(&CacheKeepaliveReplaceRequest {
            session_key_hash: "running-evidence-session".to_owned(),
            principal_id: "principal".to_owned(),
            upstream_id,
            cycle_id,
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 0,
            run_at_unix_secs: 1,
            expires_at_unix_secs: 1_000,
            encrypted_payload: b"payload".to_vec(),
            accounting_key_id: None,
            display_reason: "test".to_owned(),
            config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            },
            now_unix_secs: 0,
        })
        .await
        .expect("seed running session");
    storage
        .mark_cache_keepalive_enqueued(&running.session_key_hash, running.generation, 1)
        .await
        .expect("enqueue running session");
    storage
        .claim_cache_keepalive_turn(&running.session_key_hash, running.generation, 2)
        .await
        .expect("claim running session");
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 2,
                ts_ms: Some(2_000),
                event_id: Some("running-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence("running-follow-up", "follow_up", 2_000, None)],
        )
        .await
        .expect("append running-session follow-up");
    let running_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cache_keepalive_evidence WHERE evidence_id = 'running-follow-up'",
    )
    .fetch_one(storage.pool())
    .await
    .expect("read running-session follow-up");
    assert_eq!(running_count, 1);
}
#[tokio::test]
async fn append_evidence_deduplicates_follow_ups_across_sibling_renewals() {
    let (_dir, storage) = storage("cache-keepalive-evidence-sibling-filter.sqlite").await;
    let upstream_id = Uuid::from_u128(7);
    let cycle_id = Uuid::from_u128(80);
    let evidence = |id: &str, kind: &str, start: u64, prefix: &str| CacheKeepaliveEvidenceRow {
        evidence_id: id.to_owned(),
        cycle_id: (kind == "renewal").then_some(cycle_id),
        evidence_kind: kind.to_owned(),
        request_start_ms: start,
        prefix_fingerprint: Some("fingerprint".to_owned()),
        prefix_hash: Some(prefix.to_owned()),
        prefix_policy_version: Some("policy".to_owned()),
        prefix_schema_version: Some("schema".to_owned()),
        principal_id: "principal".to_owned(),
        upstream_id,
        model: "model".to_owned(),
        ttl: CacheTtl::Ttl5m,
        hit_miss: Some("hit".to_owned()),
        cache_read_input_tokens: Some(1),
        cost_micros: Some(1),
        request_ref: Some(id.to_owned()),
        event_ref: Some(id.to_owned()),
    };

    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 1,
                ts_ms: Some(1_000),
                event_id: Some("sibling-append-first-event".to_owned()),
                ..RequestEvent::default()
            },
            &[
                evidence("sibling-renewal-a", "renewal", 1_000, "prefix-a"),
                evidence("sibling-follow-up-a", "follow_up", 1_100, "prefix-a"),
            ],
        )
        .await
        .expect("append first sibling renewal and follow-up");
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 2,
                ts_ms: Some(2_000),
                event_id: Some("sibling-append-second-event".to_owned()),
                ..RequestEvent::default()
            },
            &[
                evidence("sibling-renewal-b", "renewal", 1_050, "prefix-b"),
                evidence("sibling-follow-up-b", "follow_up", 1_200, "prefix-b"),
            ],
        )
        .await
        .expect("append second sibling renewal and follow-up");

    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id
         FROM cache_keepalive_evidence
         WHERE evidence_id LIKE 'sibling-%'
         ORDER BY request_start_ms, evidence_id",
    )
    .fetch_all(storage.pool())
    .await
    .expect("read sibling evidence");
    assert_eq!(
        ids,
        vec![
            "sibling-renewal-a",
            "sibling-renewal-b",
            "sibling-follow-up-a",
        ]
    );
}

#[tokio::test]
async fn append_evidence_rejects_duplicate_follow_up_with_long_lived_proof() {
    let (_dir, storage) = storage("cache-keepalive-evidence-long-lived.sqlite").await;
    let upstream_id = Uuid::from_u128(7);
    let cycle_id = Uuid::from_u128(82);
    let evidence = |id: &str, kind: &str, start: u64, prefix: &str| CacheKeepaliveEvidenceRow {
        evidence_id: id.to_owned(),
        cycle_id: (kind == "renewal").then_some(cycle_id),
        evidence_kind: kind.to_owned(),
        request_start_ms: start,
        prefix_fingerprint: Some("fingerprint".to_owned()),
        prefix_hash: Some(prefix.to_owned()),
        prefix_policy_version: Some("policy".to_owned()),
        prefix_schema_version: Some("schema".to_owned()),
        principal_id: "principal".to_owned(),
        upstream_id,
        model: "model".to_owned(),
        ttl: CacheTtl::Ttl1h,
        hit_miss: Some("hit".to_owned()),
        cache_read_input_tokens: Some(1),
        cost_micros: Some(1),
        request_ref: Some(id.to_owned()),
        event_ref: Some(id.to_owned()),
    };

    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 1,
                ts_ms: Some(3_300_001),
                event_id: Some("long-lived-anchor-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence(
                "long-lived-renewal-a",
                "renewal",
                3_300_001,
                "prefix-a",
            )],
        )
        .await
        .expect("append long-lived anchor renewal");
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 2,
                ts_ms: Some(3_400_000),
                event_id: Some("long-lived-proof-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence(
                "long-lived-proof",
                "follow_up",
                3_400_000,
                "prefix-a",
            )],
        )
        .await
        .expect("append long-lived proof follow-up");
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 3,
                ts_ms: Some(18_000_000),
                event_id: Some("long-lived-sibling-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence(
                "long-lived-renewal-b",
                "renewal",
                18_000_000,
                "prefix-b",
            )],
        )
        .await
        .expect("append long-lived sibling renewal");
    storage
        .append_request_event_with_evidence(
            &RequestEvent {
                ts: 4,
                ts_ms: Some(21_500_000),
                event_id: Some("long-lived-duplicate-event".to_owned()),
                ..RequestEvent::default()
            },
            &[evidence(
                "long-lived-duplicate",
                "follow_up",
                21_500_000,
                "prefix-b",
            )],
        )
        .await
        .expect("append duplicate follow-up");

    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id
         FROM cache_keepalive_evidence
         WHERE evidence_id LIKE 'long-lived-%'
         ORDER BY request_start_ms, evidence_id",
    )
    .fetch_all(storage.pool())
    .await
    .expect("read long-lived evidence");
    assert_eq!(
        ids,
        vec![
            "long-lived-renewal-a",
            "long-lived-proof",
            "long-lived-renewal-b",
        ]
    );
}

#[tokio::test]
async fn purge_expired_prunes_unmatched_and_invalid_follow_ups_without_changing_count() {
    let (_dir, storage) = storage("cache-keepalive-evidence-purge.sqlite").await;
    let upstream_id = Uuid::from_u128(7);
    let cycle_id = Uuid::from_u128(8);
    let session = storage
        .replace_from_real_request(&CacheKeepaliveReplaceRequest {
            session_key_hash: "expired-session".to_owned(),
            principal_id: "principal".to_owned(),
            upstream_id,
            cycle_id,
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 0,
            run_at_unix_secs: 1,
            expires_at_unix_secs: 50,
            encrypted_payload: b"payload".to_vec(),
            accounting_key_id: None,
            display_reason: "test".to_owned(),
            config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            },
            now_unix_secs: 0,
        })
        .await
        .expect("seed expired session");
    storage
        .mark_cache_keepalive_enqueued(&session.session_key_hash, session.generation, 1)
        .await
        .expect("enqueue expired session");
    storage
        .claim_cache_keepalive_turn(&session.session_key_hash, session.generation, 2)
        .await
        .expect("claim expired session");
    let zero_cycle_id = Uuid::from_u128(9);
    let null_cycle_id = Uuid::from_u128(10);
    for (id, cycle, kind, start, prefix, read_tokens) in [
        (
            "renewal",
            Some(cycle_id),
            "renewal",
            1_000_i64,
            Some("prefix"),
            Some(1_i64),
        ),
        (
            "earliest",
            None,
            "follow_up",
            1_100,
            Some("prefix"),
            Some(1),
        ),
        ("later", None, "follow_up", 1_200, Some("prefix"), Some(1)),
        ("invalid", None, "follow_up", 1_300, Some("prefix"), Some(0)),
        (
            "zero-renewal",
            Some(zero_cycle_id),
            "renewal",
            2_000,
            Some("prefix"),
            Some(1),
        ),
        (
            "zero-only",
            None,
            "follow_up",
            2_100,
            Some("prefix"),
            Some(0),
        ),
        (
            "null-renewal",
            Some(null_cycle_id),
            "renewal",
            3_000,
            Some("prefix"),
            Some(1),
        ),
        ("null-only", None, "follow_up", 3_100, None, Some(1)),
    ] {
        sqlx::query(
            "INSERT INTO cache_keepalive_evidence
             (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
              prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
              upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
              request_ref, event_ref, created_at_ms)
             VALUES (?, ?, ?, ?, 'fingerprint', ?, 'policy', 'schema', ?, ?, 'model', '5m',
                     'hit', ?, 1, ?, ?, ?)",
        )
        .bind(id)
        .bind(cycle.map(|value| value.to_string()))
        .bind(kind)
        .bind(start)
        .bind(prefix)
        .bind("principal")
        .bind(upstream_id.to_string())
        .bind(read_tokens)
        .bind(id)
        .bind(id)
        .bind(start)
        .execute(storage.pool())
        .await
        .expect("seed evidence");
    }
    assert_eq!(
        storage
            .purge_cache_keepalive_expired(100)
            .await
            .expect("purge expired session and evidence"),
        1
    );
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT evidence_id FROM cache_keepalive_evidence ORDER BY evidence_id")
            .fetch_all(storage.pool())
            .await
            .expect("read retained evidence");
    assert_eq!(
        ids,
        vec!["earliest", "null-renewal", "renewal", "zero-renewal"]
    );
}
#[tokio::test]
async fn purge_expired_prunes_later_follow_up_across_sibling_renewals() {
    let (_dir, storage) = storage("cache-keepalive-evidence-sibling-purge.sqlite").await;
    let upstream_id = Uuid::from_u128(7);
    let cycle_id = Uuid::from_u128(81);
    let session = storage
        .replace_from_real_request(&CacheKeepaliveReplaceRequest {
            session_key_hash: "sibling-purge-session".to_owned(),
            principal_id: "principal".to_owned(),
            upstream_id,
            cycle_id,
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 0,
            run_at_unix_secs: 1,
            expires_at_unix_secs: 50,
            encrypted_payload: b"payload".to_vec(),
            accounting_key_id: None,
            display_reason: "test".to_owned(),
            config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            },
            now_unix_secs: 0,
        })
        .await
        .expect("seed sibling purge session");
    storage
        .mark_cache_keepalive_enqueued(&session.session_key_hash, session.generation, 1)
        .await
        .expect("enqueue sibling purge session");
    storage
        .claim_cache_keepalive_turn(&session.session_key_hash, session.generation, 2)
        .await
        .expect("claim sibling purge session");

    for (id, kind, start, prefix_hash) in [
        ("sibling-purge-renewal-a", "renewal", 1_000_i64, "prefix-a"),
        ("sibling-purge-renewal-b", "renewal", 1_050, "prefix-b"),
        ("sibling-purge-earliest", "follow_up", 1_100, "prefix-a"),
        ("sibling-purge-later", "follow_up", 1_200, "prefix-b"),
    ] {
        sqlx::query(
            "INSERT INTO cache_keepalive_evidence
             (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
              prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
              upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
              request_ref, event_ref, created_at_ms)
             VALUES (?, ?, ?, ?, 'fingerprint', ?, 'policy', 'schema', ?, ?, 'model', '5m',
                     'hit', 1, 1, ?, ?, ?)",
        )
        .bind(id)
        .bind((kind == "renewal").then_some(cycle_id.to_string()))
        .bind(kind)
        .bind(start)
        .bind(prefix_hash)
        .bind("principal")
        .bind(upstream_id.to_string())
        .bind(id)
        .bind(id)
        .bind(start)
        .execute(storage.pool())
        .await
        .expect("seed sibling purge evidence");
    }

    assert_eq!(
        storage
            .purge_cache_keepalive_expired(100)
            .await
            .expect("purge sibling follow-ups"),
        1
    );
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id
         FROM cache_keepalive_evidence
         WHERE evidence_id LIKE 'sibling-purge-%'
         ORDER BY evidence_id",
    )
    .fetch_all(storage.pool())
    .await
    .expect("read sibling purge evidence");
    assert_eq!(
        ids,
        vec![
            "sibling-purge-earliest",
            "sibling-purge-renewal-a",
            "sibling-purge-renewal-b",
        ]
    );
}

#[tokio::test]
async fn purge_expired_preserves_follow_up_earliest_for_one_matching_cycle() {
    let (_dir, storage) = storage("cache-keepalive-evidence-multi-cycle-purge.sqlite").await;
    let upstream_id = Uuid::from_u128(7);
    let cycle_one = Uuid::from_u128(83);
    let cycle_two = Uuid::from_u128(84);
    let session = storage
        .replace_from_real_request(&CacheKeepaliveReplaceRequest {
            session_key_hash: "multi-cycle-purge-session".to_owned(),
            principal_id: "principal".to_owned(),
            upstream_id,
            cycle_id: cycle_one,
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 0,
            run_at_unix_secs: 1,
            expires_at_unix_secs: 50,
            encrypted_payload: b"payload".to_vec(),
            accounting_key_id: None,
            display_reason: "test".to_owned(),
            config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            },
            now_unix_secs: 0,
        })
        .await
        .expect("seed multi-cycle purge session");
    storage
        .mark_cache_keepalive_enqueued(&session.session_key_hash, session.generation, 1)
        .await
        .expect("enqueue multi-cycle purge session");
    storage
        .claim_cache_keepalive_turn(&session.session_key_hash, session.generation, 2)
        .await
        .expect("claim multi-cycle purge session");

    for (id, cycle, kind, start) in [
        (
            "multi-cycle-renewal-earlier",
            Some(cycle_two),
            "renewal",
            1_000_i64,
        ),
        ("multi-cycle-earlier-proof", None, "follow_up", 1_500),
        (
            "multi-cycle-renewal-later",
            Some(cycle_one),
            "renewal",
            2_000,
        ),
        ("multi-cycle-later-proof", None, "follow_up", 2_500),
    ] {
        sqlx::query(
            "INSERT INTO cache_keepalive_evidence
             (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
              prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
              upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
              request_ref, event_ref, created_at_ms)
             VALUES (?, ?, ?, ?, 'fingerprint', 'shared-prefix', 'policy', 'schema',
                     ?, ?, 'model', '5m', 'hit', 1, 1, ?, ?, ?)",
        )
        .bind(id)
        .bind(cycle.map(|value| value.to_string()))
        .bind(kind)
        .bind(start)
        .bind("principal")
        .bind(upstream_id.to_string())
        .bind(id)
        .bind(id)
        .bind(start)
        .execute(storage.pool())
        .await
        .expect("seed multi-cycle purge evidence");
    }

    assert_eq!(
        storage
            .purge_cache_keepalive_expired(100)
            .await
            .expect("purge multi-cycle follow-ups"),
        1
    );
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id
         FROM cache_keepalive_evidence
         WHERE evidence_id LIKE 'multi-cycle-%'
         ORDER BY request_start_ms, evidence_id",
    )
    .fetch_all(storage.pool())
    .await
    .expect("read multi-cycle purge evidence");
    assert_eq!(
        ids,
        vec![
            "multi-cycle-renewal-earlier",
            "multi-cycle-earlier-proof",
            "multi-cycle-renewal-later",
            "multi-cycle-later-proof",
        ]
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
