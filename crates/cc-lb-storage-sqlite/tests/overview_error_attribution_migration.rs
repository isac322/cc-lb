use std::{str::FromStr, sync::Arc, time::Instant};

use cc_lb_storage_api::{
    MetaStore, RequestEvent, RequestEventStore, UpstreamCreate, UpstreamKind, UpstreamStore,
    UsageRollupStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use uuid::Uuid;

const AT: u64 = 1_800_000_000;
const UPSTREAM: &str = "00000000-0000-0000-0000-000000001300";

type Row = (String, i64, String, i64);

#[tokio::test]
async fn startup_attribution_resumes_committed_batches_without_double_counting() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}",
        dir.path().join("attribution.sqlite").display()
    );
    let options = SqliteConnectOptions::from_str(&url)
        .unwrap()
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let migrator = sqlx::migrate!("./migrations");
    migrator
        .run_direct(Some(125), &mut connection, false)
        .await
        .unwrap();
    sqlx::raw_sql(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10001)
         INSERT INTO request_events_v1
             (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,upstream_id,upstream_name)
         SELECT 'excluded-'||i,'excluded-'||i,1800000000,1800000000000,403,0,
                '00000000-0000-0000-0000-000000001300','captured' FROM n;
         INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
         VALUES ('minute',1800000000,10001,0),('hour',1800000000,10001,0);
         INSERT INTO usage_rollup_checkpoints_v1 (id,value)
         VALUES ('high_water',10001),('cost_components_backfill_upper',10001),
                ('cost_components_backfill_cursor',10001);
         UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
    ).execute(&mut connection).await.unwrap();
    // Install the fault after real SQLx schema migration, before the startup hook.
    migrator
        .run_direct(None, &mut connection, false)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TRIGGER interrupt_attribution BEFORE UPDATE OF error_count
         ON overview_excluded_error_rollups_v1
         WHEN NEW.upstream_id <> '' AND NEW.error_count > 1000
         BEGIN SELECT RAISE(ABORT, 'controlled attribution interruption'); END;",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    let error = storage
        .initialize()
        .await
        .expect_err("second attribution batch must fail");
    assert!(
        error
            .to_string()
            .contains("controlled attribution interruption"),
        "{error}"
    );
    for resolution in ["minute", "hour"] {
        let attributed: i64 = sqlx::query_scalar(
            "SELECT error_count FROM overview_excluded_error_rollups_v1
             WHERE resolution=? AND upstream_id=?",
        )
        .bind(resolution)
        .bind(UPSTREAM)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            attributed, 1000,
            "first batch must have committed: {resolution}"
        );
        let total: i64 = sqlx::query_scalar(
            "SELECT SUM(error_count) FROM overview_excluded_error_rollups_v1 WHERE resolution=?",
        )
        .bind(resolution)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(total, 10001, "interruption must preserve global totals");
    }
    sqlx::query("DROP TRIGGER interrupt_attribution")
        .execute(storage.pool())
        .await
        .unwrap();
    storage.pool().close().await;
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    storage.initialize().await.unwrap();
    storage.verify_schema_current().await.unwrap();
    let expected = vec![
        ("hour".to_owned(), AT as i64, UPSTREAM.to_owned(), 10001),
        ("minute".to_owned(), AT as i64, UPSTREAM.to_owned(), 10001),
    ];
    assert_eq!(snapshot(&storage).await, expected);
    assert_eq!(
        storage.usage_rollup_checkpoint().await.unwrap(),
        Some(10001)
    );
    let before = storage.usage_rollup_revision().await.unwrap();
    let started = Instant::now();
    storage.initialize().await.unwrap();
    assert!(
        started.elapsed().as_secs_f64() < 5.0,
        "completed initialization must be cheap"
    );
    assert_eq!(snapshot(&storage).await, expected);
    storage.rollup_usage_once().await.unwrap();
    assert_eq!(snapshot(&storage).await, expected);
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), before);
    storage
        .append_request_event(&RequestEvent {
            ts: AT,
            ts_ms: Some(AT * 1000),
            event_id: Some("post-upgrade".to_owned()),
            request_id: "post-upgrade".to_owned(),
            upstream_id: Some(Uuid::from_u128(0x1301)),
            status: 403,
            ..RequestEvent::default()
        })
        .await
        .unwrap();
    assert_eq!(
        storage.rollup_usage_once().await.unwrap().processed_events,
        1
    );
    assert_eq!(
        storage.rollup_usage_once().await.unwrap().processed_events,
        0
    );
    for resolution in ["minute", "hour"] {
        let total: i64 = sqlx::query_scalar(
            "SELECT SUM(error_count) FROM overview_excluded_error_rollups_v1 WHERE resolution=?",
        )
        .bind(resolution)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(total, 10002);
    }
    let fresh_url = format!("sqlite://{}", dir.path().join("fresh.sqlite").display());
    let fresh = cc_lb_storage_sqlite::open_sqlite(&fresh_url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .unwrap();
    fresh.initialize().await.unwrap();
    fresh.verify_schema_current().await.unwrap();
    assert_eq!(shape(&storage).await, shape(&fresh).await);
    assert!(snapshot(&fresh).await.is_empty());
}

#[tokio::test]
async fn retained_and_api_pruned_history_keep_exact_upstream_identity_and_checkpoint() {
    for prune_history in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            dir.path().join("identities.sqlite").display()
        );
        let options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::migrate!("./migrations")
            .run_direct(Some(125), &mut connection, false)
            .await
            .unwrap();
        for (id, name, kind) in [
            (0x1300_u128, "named", "openai_oauth"),
            (0x1301_u128, "other", "gemini_oauth"),
        ] {
            sqlx::query(
                "INSERT INTO upstream_spec_v1
                 (id,name,kind,base_url,enabled,warmup_enabled,warmup_dialect_plugin,
                  spec_revision,created_at,updated_at,deleted_at,workspace_id,owner_epoch)
                 VALUES (?,?,?,NULL,1,0,NULL,1,1800000000,1800000000,NULL,NULL,0)",
            )
            .bind(Uuid::from_u128(id).to_string())
            .bind(name)
            .bind(kind)
            .execute(&mut connection)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,4,0),('hour',1800000000,4,0)",
        )
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::migrate!("./migrations")
            .run_direct(Some(128), &mut connection, false)
            .await
            .unwrap();
        connection.close().await.unwrap();
        let storage =
            cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .unwrap();
        for (index, upstream_id, name, status) in [
            (0, Some(Uuid::from_u128(0x1300)), Some("named"), 401),
            (1, Some(Uuid::from_u128(0x1301)), Some("other"), 403),
            (2, Some(Uuid::nil()), None, 404),
            (3, None, Some("named"), 403),
        ] {
            let ts = AT + if index == 0 { 1 } else { 10 };
            storage
                .append_request_event(&RequestEvent {
                    ts,
                    ts_ms: Some(ts * 1000),
                    request_id: format!("identity-{index}"),
                    event_id: Some(format!("identity-{index}")),
                    principal_id: Some(
                        match index {
                            0 | 3 => "principal-a",
                            1 => "principal-b",
                            _ => "principal-unknown",
                        }
                        .to_owned(),
                    ),
                    upstream_id,
                    upstream_name: name.map(str::to_owned),
                    status,
                    ..RequestEvent::default()
                })
                .await
                .unwrap();
        }
        sqlx::raw_sql(
            "INSERT INTO usage_rollup_checkpoints_v1 (id,value)
             VALUES ('high_water',4),('cost_components_backfill_upper',4),
                    ('cost_components_backfill_cursor',4);
             UPDATE usage_rollup_reconciliations_v1 SET status='complete';
             INSERT INTO usage_rollups_v2
             (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,error_count,updated_at)
             SELECT resolution,1800000000,principal_id,upstream_id,upstream_name,'unknown',requests,requests,0
             FROM (SELECT 'minute' AS resolution UNION ALL SELECT 'hour')
             CROSS JOIN (
                 SELECT '00000000-0000-0000-0000-000000001300' AS upstream_id,'named' AS upstream_name,2 AS requests,'principal-a' AS principal_id
                 UNION ALL SELECT '00000000-0000-0000-0000-000000001301','other',1,'principal-b'
                 UNION ALL SELECT '00000000-0000-0000-0000-000000000000','unknown',1,'principal-unknown'
             );",
        )
        .execute(storage.pool())
        .await
        .unwrap();
        if prune_history {
            assert_eq!(
                storage
                    .prune_request_events_before((AT + 5) * 1_000_000_000, 100)
                    .await
                    .unwrap(),
                1,
            );
        }
        storage
            .append_request_event(&RequestEvent {
                ts: AT + 10,
                ts_ms: Some((AT + 10) * 1000),
                request_id: "after-checkpoint".to_owned(),
                event_id: Some("after-checkpoint".to_owned()),
                upstream_id: Some(Uuid::from_u128(0x1301)),
                status: 404,
                ..RequestEvent::default()
            })
            .await
            .unwrap();
        sqlx::query(
            "UPDATE overview_excluded_error_attribution_v1
             SET upper_bound=4,cursor=0,phase='copy'
             WHERE id=1",
        )
        .execute(storage.pool())
        .await
        .unwrap();
        storage.initialize().await.unwrap();
        let mut expected = Vec::new();
        for resolution in ["hour", "minute"] {
            if prune_history {
                expected.push((resolution.to_owned(), AT as i64, String::new(), 1));
            }
            expected.push((resolution.to_owned(), AT as i64, Uuid::nil().to_string(), 1));
            if !prune_history {
                expected.push((resolution.to_owned(), AT as i64, UPSTREAM.to_owned(), 1));
            }
            expected.push((
                resolution.to_owned(),
                AT as i64,
                Uuid::from_u128(0x1301).to_string(),
                1,
            ));
            expected.push((
                resolution.to_owned(),
                AT as i64,
                "legacy-unattributed".to_owned(),
                1,
            ));
        }
        assert_eq!(snapshot(&storage).await, expected);
        for resolution in [
            cc_lb_storage_api::UsageRollupResolution::Minute,
            cc_lb_storage_api::UsageRollupResolution::Hour,
        ] {
            let buckets = storage
                .query_overview_excluded_error_buckets_in_range(resolution, AT, AT + 60)
                .await
                .unwrap();
            let unknown = buckets
                .iter()
                .find(|bucket| bucket.upstream_id == Some(Uuid::nil()))
                .expect("actual unknown identity is Some(nil), not global residual");
            assert_eq!(unknown.error_count, 1);
            assert_eq!(
                buckets
                    .iter()
                    .filter(|bucket| bucket.upstream_id.is_none())
                    .map(|bucket| bucket.error_count)
                    .sum::<u64>(),
                if prune_history { 2 } else { 1 },
            );
        }
        assert_eq!(storage.usage_rollup_checkpoint().await.unwrap(), Some(4));
        storage.initialize().await.unwrap();
        assert_eq!(snapshot(&storage).await, expected);
        assert_eq!(
            storage.rollup_usage_once().await.unwrap().processed_events,
            1
        );
        for row in &mut expected {
            if row.2 == Uuid::from_u128(0x1301).to_string() {
                row.3 = 2;
            }
        }
        assert_eq!(snapshot(&storage).await, expected);
        assert_eq!(
            storage.rollup_usage_once().await.unwrap().processed_events,
            0
        );
        assert_eq!(snapshot(&storage).await, expected);
        for status in [400, 429, 500] {
            storage
                .append_request_event(&RequestEvent {
                    ts: AT + 10,
                    ts_ms: Some((AT + 10) * 1000),
                    request_id: format!("included-{status}"),
                    event_id: Some(format!("included-{status}")),
                    upstream_id: Some(Uuid::from_u128(0x1301)),
                    status,
                    ..RequestEvent::default()
                })
                .await
                .unwrap();
        }
        assert_eq!(
            storage.rollup_usage_once().await.unwrap().processed_events,
            3
        );
        assert_eq!(
            snapshot(&storage).await,
            expected,
            "400/429/500 must not be excluded"
        );
        for resolution in [
            cc_lb_storage_api::UsageRollupResolution::Minute,
            cc_lb_storage_api::UsageRollupResolution::Hour,
        ] {
            let usage = storage
                .query_usage_rollups_in_range(resolution, AT, AT + 60)
                .await
                .unwrap();
            assert_eq!(usage.iter().map(|row| row.request_count).sum::<u64>(), 8);
            assert_eq!(usage.iter().map(|row| row.error_count).sum::<u64>(), 8);
        }
    }
}

async fn snapshot(storage: &SqliteStorage) -> Vec<Row> {
    sqlx::query_as(
        "SELECT resolution,bucket_start_unix_secs,upstream_id,error_count
         FROM overview_excluded_error_rollups_v1 ORDER BY resolution,bucket_start_unix_secs,upstream_id",
    ).fetch_all(storage.pool()).await.unwrap()
}

async fn shape(storage: &SqliteStorage) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT name,sql FROM sqlite_master WHERE type='table'
         AND name LIKE 'overview_excluded_error%' ORDER BY name",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn startup_attribution_rejects_insufficient_and_exhausted_legacy_totals() {
    for (legacy_count, ambiguous) in [(1_i64, false), (1000, false), (1, true), (1000, true)] {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            dir.path().join("undercount.sqlite").display()
        );
        let options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::migrate!("./migrations")
            .run_direct(Some(125), &mut connection, false)
            .await
            .unwrap();
        sqlx::query(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
             INSERT INTO request_events_v1
             (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,upstream_id)
             SELECT 'undercount-'||i,'undercount-'||i,1800000000,1800000000000,403,0,
                    CASE WHEN ? THEN NULL ELSE '00000000-0000-0000-0000-000000001300' END FROM n",
        )
        .bind(legacy_count + 1)
        .bind(ambiguous)
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,?,0),('hour',1800000000,?,0)",
        )
        .bind(legacy_count)
        .bind(legacy_count)
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::query("INSERT INTO usage_rollup_checkpoints_v1 (id,value) VALUES ('high_water',?)")
            .bind(legacy_count + 1)
            .execute(&mut connection)
            .await
            .unwrap();
        if ambiguous {
            sqlx::query(
                "INSERT INTO usage_rollups_v2
                 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,error_count,updated_at)
                 SELECT resolution,1800000000,'unknown',upstream_id,'unknown','unknown',requests,requests,0
                 FROM (SELECT 'minute' AS resolution UNION ALL SELECT 'hour')
                 CROSS JOIN (
                     SELECT '00000000-0000-0000-0000-000000001300' AS upstream_id,? AS requests
                     UNION ALL SELECT '00000000-0000-0000-0000-000000001301',?
                 )",
            ).bind((legacy_count + 1) / 2).bind(legacy_count + 1 - (legacy_count + 1) / 2)
                .execute(&mut connection).await.unwrap();
        }
        connection.close().await.unwrap();
        let storage =
            cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .unwrap();
        let error = storage
            .initialize()
            .await
            .expect_err("retained events exceed legacy exclusion total");
        match error {
            cc_lb_storage_api::StorageError::Corrupted { message } => {
                assert!(
                    message.contains("overview excluded error attribution undercount"),
                    "{message}"
                );
            }
            other => panic!("expected explicit data inconsistency, not SQL/setup failure: {other}"),
        }
        let expected_id = if legacy_count == 1 {
            ""
        } else if ambiguous {
            "legacy-unattributed"
        } else {
            UPSTREAM
        };
        assert_eq!(
            snapshot(&storage).await,
            vec![
                (
                    "hour".to_owned(),
                    AT as i64,
                    expected_id.to_owned(),
                    legacy_count
                ),
                (
                    "minute".to_owned(),
                    AT as i64,
                    expected_id.to_owned(),
                    legacy_count
                ),
            ]
        );
        let cursor: i64 = sqlx::query_scalar(
            "SELECT cursor FROM overview_excluded_error_attribution_v1 WHERE id=1",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(cursor, if legacy_count == 1 { 0 } else { 1000 });
    }
}

#[tokio::test]
async fn missing_legacy_uuid_stays_global_only_despite_surviving_identity_candidates() {
    for scenario in [
        "renamed",
        "ambiguous",
        "captured-unknown",
        "explicit-nil",
        "model-drift",
        "no-history",
        "hour-distractor",
        "hour-fallback",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            dir.path().join("name-reuse.sqlite").display()
        );
        let options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::migrate!("./migrations")
            .run_direct(Some(125), &mut connection, false)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,1,0),('hour',1800000000,1,0)",
        )
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::migrate!("./migrations")
            .run_direct(Some(128), &mut connection, false)
            .await
            .unwrap();
        connection.close().await.unwrap();
        let storage =
            cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .unwrap();
        let create = |id, name: &str, kind| UpstreamCreate {
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            id,
            name: name.to_owned(),
            kind,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        };
        let original_id = Uuid::from_u128(0x1300);
        let reused_id = Uuid::from_u128(0x1301);
        let raw_model = if scenario == "model-drift" {
            "models/gemini-captured"
        } else {
            "gpt-captured"
        };
        let captured_model = if scenario == "model-drift" {
            "gemini-captured"
        } else {
            "gpt-captured"
        };
        storage
            .append_request_event(&RequestEvent {
                ts: AT + 10,
                ts_ms: Some((AT + 10) * 1000),
                request_id: "historical-name-only".to_owned(),
                event_id: Some("historical-name-only".to_owned()),
                principal_id: Some(" principal/a ".to_owned()),
                upstream_id: if scenario == "explicit-nil" {
                    Some(Uuid::nil())
                } else {
                    None
                },
                upstream_name: Some("foo".to_owned()),
                model: Some(raw_model.to_owned()),
                status: 403,
                ..RequestEvent::default()
            })
            .await
            .unwrap();
        let kind = if scenario == "model-drift" {
            UpstreamKind::GeminiOauth
        } else {
            UpstreamKind::OpenaiOauth
        };
        let original = storage
            .create(create(original_id, "foo", kind))
            .await
            .unwrap();
        storage
            .update(
                original_id,
                original.revision,
                cc_lb_storage_api::UpstreamUpdate {
                    name: Some("renamed-original".to_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        if !matches!(scenario, "captured-unknown" | "explicit-nil") {
            // A later event with the same rollup key overwrites its stored display name.
            storage
                .append_request_event(&RequestEvent {
                    ts: AT + 10,
                    ts_ms: Some((AT + 10) * 1000),
                    request_id: "renamed-original-success".to_owned(),
                    event_id: Some("renamed-original-success".to_owned()),
                    principal_id: Some(" principal/a ".to_owned()),
                    upstream_id: Some(original_id),
                    upstream_name: Some("renamed-original".to_owned()),
                    model: Some(raw_model.to_owned()),
                    status: 200,
                    ..RequestEvent::default()
                })
                .await
                .unwrap();
        }
        storage
            .create(create(reused_id, "foo", UpstreamKind::GeminiOauth))
            .await
            .unwrap();
        storage
            .append_request_event(&RequestEvent {
                ts: AT + 10,
                ts_ms: Some((AT + 10) * 1000),
                request_id: "reused-name-server-error".to_owned(),
                event_id: Some("reused-name-server-error".to_owned()),
                principal_id: Some("other-principal".to_owned()),
                upstream_id: Some(reused_id),
                upstream_name: Some("foo".to_owned()),
                model: Some("gemini-captured".to_owned()),
                status: 500,
                ..RequestEvent::default()
            })
            .await
            .unwrap();
        let captured_id = if matches!(scenario, "captured-unknown" | "explicit-nil") {
            Uuid::nil()
        } else {
            original_id
        };
        for resolution in ["minute", "hour"] {
            if scenario != "no-history" && !(scenario == "hour-fallback" && resolution == "minute")
            {
                sqlx::query(
                    "INSERT INTO usage_rollups_v2
                     (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,error_count,updated_at)
                     VALUES (?,1800000000,'principal_a',?,'renamed-original',?,?,1,0)",
                ).bind(resolution).bind(captured_id.to_string()).bind(captured_model)
                    .bind(if matches!(scenario, "captured-unknown" | "explicit-nil") { 1 } else { 2 })
                    .execute(storage.pool()).await.unwrap();
            }
            sqlx::query(
                "INSERT INTO usage_rollups_v2
                 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,error_count,updated_at)
                 VALUES (?,1800000000,'other-principal',?,'foo','gemini-captured',1,1,0)",
            ).bind(resolution).bind(reused_id.to_string()).execute(storage.pool()).await.unwrap();
            if matches!(scenario, "ambiguous" | "hour-distractor") {
                sqlx::query(
                    "INSERT INTO usage_rollups_v2
                     (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,error_count,updated_at)
                     VALUES (?,?,'principal_a',?,'foo','different-model',1,1,0)",
                ).bind(resolution).bind(if scenario == "hour-distractor" && resolution == "minute" { AT as i64 + 120 } else { AT as i64 })
                    .bind(Uuid::from_u128(0x1302).to_string())
                    .execute(storage.pool()).await.unwrap();
            }
        }
        if matches!(scenario, "ambiguous" | "hour-distractor") {
            let ts = AT
                + if scenario == "hour-distractor" {
                    130
                } else {
                    10
                };
            storage
                .append_request_event(&RequestEvent {
                    ts,
                    ts_ms: Some(ts * 1000),
                    request_id: "ambiguous-captured-name".to_owned(),
                    event_id: Some("ambiguous-captured-name".to_owned()),
                    principal_id: Some(" principal/a ".to_owned()),
                    upstream_id: Some(Uuid::from_u128(0x1302)),
                    upstream_name: Some("foo".to_owned()),
                    model: Some("different-model".to_owned()),
                    status: 500,
                    ..RequestEvent::default()
                })
                .await
                .unwrap();
        }
        sqlx::raw_sql(
            "INSERT INTO usage_rollup_checkpoints_v1 (id,value)
             SELECT id,(SELECT MAX(id) FROM request_events_v1)
             FROM (SELECT 'high_water' AS id UNION ALL SELECT 'cost_components_backfill_upper'
                   UNION ALL SELECT 'cost_components_backfill_cursor');
             UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
        )
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE overview_excluded_error_attribution_v1
             SET upper_bound=(SELECT MAX(id) FROM request_events_v1),cursor=0,phase='copy'
             WHERE id=1",
        )
        .execute(storage.pool())
        .await
        .unwrap();
        storage.initialize().await.unwrap();
        for resolution in [
            cc_lb_storage_api::UsageRollupResolution::Minute,
            cc_lb_storage_api::UsageRollupResolution::Hour,
        ] {
            let usage = storage
                .query_usage_rollups_in_range(resolution, AT, AT + 60)
                .await
                .unwrap();
            let reused = usage
                .iter()
                .find(|row| row.upstream_id == reused_id)
                .unwrap();
            assert_eq!(reused.error_count, 1, "Gemini 500 remains an error");
            let excluded = storage
                .query_overview_excluded_error_buckets_in_range(resolution, AT, AT + 60)
                .await
                .unwrap();
            assert_eq!(excluded.len(), 1, "{scenario}");
            assert_eq!(
                excluded[0].error_count, 1,
                "{scenario}: global count is conserved"
            );
            let expected = if scenario == "explicit-nil" {
                Some(Uuid::nil())
            } else {
                None
            };
            assert_eq!(
                excluded[0].upstream_id, expected,
                "{scenario}: only the captured raw UUID proves ownership; aggregates cannot fill it in"
            );
            assert!(
                !excluded
                    .iter()
                    .any(|row| row.upstream_id == Some(reused_id))
            );
        }
        let before = snapshot(&storage).await;
        storage.initialize().await.unwrap();
        assert_eq!(snapshot(&storage).await, before);
    }
}

#[tokio::test]
async fn global_only_sentinels_remain_distinct_from_unknown_uuid_and_invalid_ids() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("sentinels.sqlite").display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .unwrap();
    storage.initialize().await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO overview_excluded_error_rollups_v1
         (resolution,bucket_start_unix_secs,upstream_id,error_count,updated_at)
         VALUES ('minute',1800000000,'',2,0),
                ('minute',1800000000,'legacy-unattributed',3,0),
                ('minute',1800000000,'00000000-0000-0000-0000-000000000000',5,0);",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    let buckets = storage
        .query_overview_excluded_error_buckets_in_range(
            cc_lb_storage_api::UsageRollupResolution::Minute,
            AT,
            AT + 60,
        )
        .await
        .unwrap();
    assert_eq!(
        buckets
            .iter()
            .filter(|row| row.upstream_id.is_none())
            .map(|row| row.error_count)
            .sum::<u64>(),
        5
    );
    assert_eq!(
        buckets
            .iter()
            .find(|row| row.upstream_id == Some(Uuid::nil()))
            .unwrap()
            .error_count,
        5
    );
    assert_eq!(
        storage
            .prune_usage_rollups_before(AT + 60, AT + 60, 10)
            .await
            .unwrap(),
        3
    );
    sqlx::query(
        "INSERT INTO overview_excluded_error_rollups_v1
         (resolution,bucket_start_unix_secs,upstream_id,error_count,updated_at)
         VALUES ('minute',1800000000,'not-a-recognized-identity',1,0)",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    let error = storage
        .query_overview_excluded_error_buckets_in_range(
            cc_lb_storage_api::UsageRollupResolution::Minute,
            AT,
            AT + 60,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        cc_lb_storage_api::StorageError::Corrupted { .. }
    ));
}

#[tokio::test]
async fn partial_retention_batch_cannot_certify_a_name_only_event_owner() {
    for partial_prune in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            dir.path().join("partial-retention.sqlite").display()
        );
        let options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::migrate!("./migrations")
            .run_direct(Some(125), &mut connection, false)
            .await
            .unwrap();
        // The retention cutoff lies inside the current minute. Old raw events
        // expire, but events at +30/+40/+90 seconds remain legitimate history.
        sqlx::raw_sql(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<999)
             INSERT INTO request_events_v1
             (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,principal_id)
             SELECT 'expired-'||i,'expired-'||i,1799999940,1799999940000,200,0,'filler-'||i FROM n;
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<999)
             INSERT INTO usage_rollups_v2
             (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,error_count,updated_at)
             SELECT 'minute',1799999940,'filler-'||i,'00000000-0000-0000-0000-000000000000','unknown','unknown',1,0,0 FROM n;
             INSERT INTO usage_rollups_v2
             (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,error_count,updated_at)
             VALUES
             ('minute',1800000000,'boundary-principal','00000000-0000-0000-0000-000000001300','original','model',1,1,0),
             ('minute',1800000000,'boundary-principal','00000000-0000-0000-0000-000000001301','stable','model',1,1,0),
             ('minute',1800000060,'boundary-principal','00000000-0000-0000-0000-000000001300','original','model',1,1,0),
             ('hour',1800000000,'boundary-principal','00000000-0000-0000-0000-000000001300','original','model',2,2,0),
             ('hour',1800000000,'boundary-principal','00000000-0000-0000-0000-000000001301','stable','model',1,1,0);",
        ).execute(&mut connection).await.unwrap();
        connection.close().await.unwrap();
        let storage =
            cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .unwrap();
        let original = Uuid::from_u128(0x1300);
        let stable = Uuid::from_u128(0x1301);
        for (id, offset, upstream_id, name, status) in [
            ("legacy-name-only", 30, None, "original", 403),
            ("stable-server-error", 40, Some(stable), "stable", 500),
            ("stable-original-id", 90, Some(original), "original", 403),
        ] {
            storage
                .append_request_event(&RequestEvent {
                    ts: AT + offset,
                    ts_ms: Some((AT + offset) * 1000),
                    request_id: id.to_owned(),
                    event_id: Some(id.to_owned()),
                    principal_id: Some("boundary-principal".to_owned()),
                    upstream_id,
                    upstream_name: Some(name.to_owned()),
                    model: Some("model".to_owned()),
                    status,
                    ..RequestEvent::default()
                })
                .await
                .unwrap();
        }
        sqlx::raw_sql(
            "INSERT INTO usage_rollup_checkpoints_v1 (id,value)
             SELECT id,(SELECT MAX(id) FROM request_events_v1)
             FROM (SELECT 'high_water' AS id UNION ALL SELECT 'cost_components_backfill_upper'
                   UNION ALL SELECT 'cost_components_backfill_cursor');
             INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,1,0),('minute',1800000060,1,0),('hour',1800000000,2,0);
             UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
        )
        .execute(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            storage
                .prune_request_events_before((AT + 20) * 1_000_000_000, 1000)
                .await
                .unwrap(),
            999
        );
        if partial_prune {
            assert_eq!(
                storage
                    .prune_usage_rollups_before(AT + 20, AT + 20, 1000)
                    .await
                    .unwrap(),
                1000
            );
        }
        let before = storage
            .query_usage_rollups_in_range(
                cc_lb_storage_api::UsageRollupResolution::Minute,
                AT,
                AT + 60,
            )
            .await
            .unwrap();
        assert_eq!(
            before
                .iter()
                .filter(|row| row.upstream_id == original)
                .count(),
            if partial_prune { 0 } else { 1 }
        );
        assert_eq!(
            before
                .iter()
                .find(|row| row.upstream_id == stable)
                .unwrap()
                .error_count,
            1
        );
        storage.initialize().await.unwrap();
        for resolution in [
            cc_lb_storage_api::UsageRollupResolution::Minute,
            cc_lb_storage_api::UsageRollupResolution::Hour,
        ] {
            let excluded = storage
                .query_overview_excluded_error_buckets_in_range(resolution, AT, AT + 120)
                .await
                .unwrap();
            assert_eq!(
                excluded.iter().map(|row| row.error_count).sum::<u64>(),
                2,
                "global total is conserved"
            );
            assert_eq!(
                excluded
                    .iter()
                    .filter(|row| row.upstream_id.is_none())
                    .map(|row| row.error_count)
                    .sum::<u64>(),
                1,
                "partial_prune={partial_prune}: missing event identity is not proved by prunable aggregates"
            );
            assert_eq!(
                excluded
                    .iter()
                    .filter(|row| row.upstream_id == Some(original))
                    .map(|row| row.error_count)
                    .sum::<u64>(),
                1,
                "explicit UUID control remains exact"
            );
            assert!(
                !excluded.iter().any(|row| row.upstream_id == Some(stable)),
                "partial_prune={partial_prune}: never subtract A's legacy 403 from B's 500"
            );
            let usage = storage
                .query_usage_rollups_in_range(resolution, AT, AT + 120)
                .await
                .unwrap();
            assert_eq!(
                usage
                    .iter()
                    .filter(|row| row.upstream_id == stable)
                    .map(|row| row.error_count)
                    .sum::<u64>(),
                1
            );
        }
        let migrated = snapshot(&storage).await;
        storage.initialize().await.unwrap();
        assert_eq!(snapshot(&storage).await, migrated);
        storage.pool().close().await;
    }
}
