use std::{collections::HashMap, str::FromStr};

use sqlx::{AssertSqlSafe, Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

const LIVE_UPSTREAM_ID: &str = "00000000-0000-0000-0000-000000000101";
const DELETED_UPSTREAM_ID: &str = "00000000-0000-0000-0000-000000000102";
const RECREATED_UPSTREAM_ID: &str = "00000000-0000-0000-0000-000000000103";
const DUPLICATE_UPSTREAM_ID: &str = "00000000-0000-0000-0000-000000000104";

const LIVE_PRINCIPAL_ID: &str = "00000000-0000-0000-0000-000000000201";
const DELETED_PRINCIPAL_ID: &str = "00000000-0000-0000-0000-000000000202";
const RECREATED_PRINCIPAL_ID: &str = "00000000-0000-0000-0000-000000000203";
const DUPLICATE_PRINCIPAL_ID: &str = "00000000-0000-0000-0000-000000000204";

#[tokio::test]
async fn t3__name_active_unique_migrations_preserve_data_and_allow_name_reuse() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("name-active-unique.sqlite").display()
    );
    let options = SqliteConnectOptions::from_str(&database_url)
        .expect("sqlite connect options")
        .create_if_missing(true)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .expect("connect sqlite database");
    let migrator = sqlx::migrate!("./migrations");

    migrator
        .run_direct(Some(74), &mut connection, false)
        .await
        .expect("apply migrations through version 74");
    assert_eq!(foreign_keys(&mut connection).await, 1);

    seed_upstreams(&mut connection).await;
    seed_upstream_children(&mut connection).await;
    seed_principals(&mut connection).await;

    migrator
        .run_direct(Some(76), &mut connection, false)
        .await
        .expect("apply name-active uniqueness migrations");

    assert_preserved_rows(&mut connection).await;
    assert_indexes(&mut connection).await;

    let foreign_key_violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut connection)
        .await
        .expect("check foreign keys");
    assert!(
        foreign_key_violations.is_empty(),
        "migration left foreign-key violations: {foreign_key_violations:?}"
    );
    assert_eq!(foreign_keys(&mut connection).await, 1);

    assert_upstream_name_reuse(&mut connection).await;
    assert_principal_name_reuse(&mut connection).await;
}

async fn seed_upstreams(connection: &mut SqliteConnection) {
    sqlx::query(
        "INSERT INTO upstream_spec_v1 (
            id, name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin,
            spec_revision, created_at, updated_at, deleted_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(LIVE_UPSTREAM_ID)
    .bind("live-upstream")
    .bind("anthropic_api_key")
    .bind("https://live.example.test")
    .bind(1_i64)
    .bind(1_i64)
    .bind(r#"{"wasm_registry_id":"live-plugin","config":{"mode":"live"}}"#)
    .bind(7_i64)
    .bind(1_000_i64)
    .bind(1_010_i64)
    .bind(Option::<i64>::None)
    .bind(DELETED_UPSTREAM_ID)
    .bind("deleted-upstream")
    .bind("anthropic_oauth")
    .bind("https://deleted.example.test")
    .bind(0_i64)
    .bind(1_i64)
    .bind(r#"{"wasm_registry_id":"deleted-plugin","config":{"mode":"deleted"}}"#)
    .bind(8_i64)
    .bind(2_000_i64)
    .bind(2_010_i64)
    .bind(2_020_i64)
    .execute(&mut *connection)
    .await
    .expect("seed upstream specs");
}

async fn seed_upstream_children(connection: &mut SqliteConnection) {
    sqlx::query(
        "INSERT INTO upstream_api_key_secret_v1 (
            upstream_id, api_key_ciphertext, secret_revision, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)",
    )
    .bind(LIVE_UPSTREAM_ID)
    .bind(vec![0x10_u8, 0x11, 0x12])
    .bind(11_i64)
    .bind(1_100_i64)
    .bind(1_110_i64)
    .bind(DELETED_UPSTREAM_ID)
    .bind(vec![0x20_u8, 0x21, 0x22, 0x23])
    .bind(12_i64)
    .bind(2_100_i64)
    .bind(2_110_i64)
    .execute(&mut *connection)
    .await
    .expect("seed API-key child rows");

    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (
            upstream_id, oauth_credentials_ciphertext, token_revision,
            oauth_token_generation, refreshed_at, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(LIVE_UPSTREAM_ID)
    .bind(vec![0x30_u8, 0x31])
    .bind(21_i64)
    .bind(31_i64)
    .bind(1_200_i64)
    .bind(1_210_i64)
    .bind(1_220_i64)
    .bind(DELETED_UPSTREAM_ID)
    .bind(vec![0x40_u8, 0x41, 0x42])
    .bind(22_i64)
    .bind(32_i64)
    .bind(2_200_i64)
    .bind(2_210_i64)
    .bind(2_220_i64)
    .execute(&mut *connection)
    .await
    .expect("seed OAuth child rows");

    sqlx::query(
        "INSERT INTO upstream_status_v1 (
            upstream_id, last_apply_error, last_apply_at, observed_spec_revision,
            observed_api_key_secret_revision, observed_oauth_token_revision,
            last_warmup_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(LIVE_UPSTREAM_ID)
    .bind("live apply error")
    .bind(1_300_i64)
    .bind(7_i64)
    .bind(11_i64)
    .bind(21_i64)
    .bind(1_310_i64)
    .bind(1_320_i64)
    .bind(DELETED_UPSTREAM_ID)
    .bind("deleted apply error")
    .bind(2_300_i64)
    .bind(8_i64)
    .bind(12_i64)
    .bind(22_i64)
    .bind(2_310_i64)
    .bind(2_320_i64)
    .execute(&mut *connection)
    .await
    .expect("seed status child rows");

    insert_warmup_attempt(
        connection,
        "00000000-0000-0000-0000-000000000301",
        LIVE_UPSTREAM_ID,
        1_400,
        "success",
        "cycle_advanced",
        Some("http"),
        Some(200),
        "live-success",
    )
    .await;
    insert_warmup_attempt(
        connection,
        "00000000-0000-0000-0000-000000000302",
        LIVE_UPSTREAM_ID,
        1_500,
        "skipped",
        "window_already_active",
        Some("not_dispatched"),
        None,
        "live-skipped",
    )
    .await;
    insert_warmup_attempt(
        connection,
        "00000000-0000-0000-0000-000000000303",
        DELETED_UPSTREAM_ID,
        2_400,
        "permanent_failure",
        "oauth_credentials_missing",
        None,
        None,
        "deleted-failure",
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn insert_warmup_attempt(
    connection: &mut SqliteConnection,
    id: &str,
    upstream_id: &str,
    attempted_at: i64,
    outcome: &str,
    reason: &str,
    dispatch_kind: Option<&str>,
    http_status: Option<i64>,
    error_detail: &str,
) {
    sqlx::query(
        "INSERT INTO warmup_attempts_v1 (
            id, upstream_id, attempted_at_unix_secs, completed_at_unix_secs,
            scheduled_for_unix_secs, trigger, outcome, reason, dispatch_kind,
            http_status, cycle_key, expected_cycle_key, idle_secs_since_prev_window,
            replica_id, lease_holder, upstream_spec_revision, dialect_plugin_snapshot,
            error_detail
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(upstream_id)
    .bind(attempted_at)
    .bind(attempted_at + 5)
    .bind(attempted_at - 10)
    .bind("scheduled")
    .bind(outcome)
    .bind(reason)
    .bind(dispatch_kind)
    .bind(http_status)
    .bind(attempted_at / 100)
    .bind(attempted_at / 100 + 1)
    .bind(30_i64)
    .bind("00000000-0000-0000-0000-000000000401")
    .bind("migration-test-holder")
    .bind(7_i64)
    .bind(r#"{"plugin":"snapshot"}"#)
    .bind(error_detail)
    .execute(&mut *connection)
    .await
    .expect("seed warmup attempt");
}

async fn seed_principals(connection: &mut SqliteConnection) {
    sqlx::query(
        "INSERT INTO principals_v1 (
            id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits,
            router_terminal_strategy, revision, created_at, updated_at,
            last_apply_error, last_apply_at, deleted_at, cache_keepalive_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(LIVE_PRINCIPAL_ID)
    .bind("live-principal")
    .bind("human")
    .bind(1_i64)
    .bind(r#"["claude-live"]"#)
    .bind(format!(r#"["{LIVE_UPSTREAM_ID}"]"#))
    .bind(r#"[{"kind":"requests","limit":10}]"#)
    .bind("random")
    .bind(41_i64)
    .bind(3_000_i64)
    .bind(3_010_i64)
    .bind("live principal apply error")
    .bind(3_020_i64)
    .bind(Option::<i64>::None)
    .bind(r#"{"enabled":true,"ttl_secs":60}"#)
    .bind(DELETED_PRINCIPAL_ID)
    .bind("deleted-principal")
    .bind("admin")
    .bind(0_i64)
    .bind(r#"["claude-deleted"]"#)
    .bind(format!(r#"["{DELETED_UPSTREAM_ID}"]"#))
    .bind(r#"[{"kind":"tokens","limit":20}]"#)
    .bind("first-pick")
    .bind(42_i64)
    .bind(4_000_i64)
    .bind(4_010_i64)
    .bind("deleted principal apply error")
    .bind(4_020_i64)
    .bind(4_030_i64)
    .bind(r#"{"enabled":false,"ttl_secs":120}"#)
    .execute(&mut *connection)
    .await
    .expect("seed principals");
}

async fn assert_preserved_rows(connection: &mut SqliteConnection) {
    assert_eq!(row_count(connection, "upstream_spec_v1").await, 2);
    assert_eq!(row_count(connection, "upstream_api_key_secret_v1").await, 2);
    assert_eq!(row_count(connection, "upstream_oauth_token_v1").await, 2);
    assert_eq!(row_count(connection, "upstream_status_v1").await, 2);
    assert_eq!(row_count(connection, "warmup_attempts_v1").await, 3);
    assert_eq!(row_count(connection, "principals_v1").await, 2);

    let live_upstream = sqlx::query(
        "SELECT base_url, enabled, warmup_enabled, warmup_dialect_plugin,
                spec_revision, deleted_at
         FROM upstream_spec_v1 WHERE id = ?",
    )
    .bind(LIVE_UPSTREAM_ID)
    .fetch_one(&mut *connection)
    .await
    .expect("read preserved live upstream");
    assert_eq!(
        live_upstream.get::<String, _>("base_url"),
        "https://live.example.test"
    );
    assert_eq!(live_upstream.get::<i64, _>("enabled"), 1);
    assert_eq!(live_upstream.get::<i64, _>("warmup_enabled"), 1);
    assert_eq!(
        live_upstream.get::<String, _>("warmup_dialect_plugin"),
        r#"{"wasm_registry_id":"live-plugin","config":{"mode":"live"}}"#
    );
    assert_eq!(live_upstream.get::<i64, _>("spec_revision"), 7);
    assert_eq!(live_upstream.get::<Option<i64>, _>("deleted_at"), None);

    let upstream = sqlx::query(
        "SELECT name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin,
                spec_revision, created_at, updated_at, deleted_at
         FROM upstream_spec_v1 WHERE id = ?",
    )
    .bind(DELETED_UPSTREAM_ID)
    .fetch_one(&mut *connection)
    .await
    .expect("read preserved deleted upstream");
    assert_eq!(upstream.get::<String, _>("name"), "deleted-upstream");
    assert_eq!(upstream.get::<String, _>("kind"), "anthropic_oauth");
    assert_eq!(
        upstream.get::<String, _>("base_url"),
        "https://deleted.example.test"
    );
    assert_eq!(upstream.get::<i64, _>("enabled"), 0);
    assert_eq!(upstream.get::<i64, _>("warmup_enabled"), 1);
    assert_eq!(
        upstream.get::<String, _>("warmup_dialect_plugin"),
        r#"{"wasm_registry_id":"deleted-plugin","config":{"mode":"deleted"}}"#
    );
    assert_eq!(upstream.get::<i64, _>("spec_revision"), 8);
    assert_eq!(upstream.get::<i64, _>("created_at"), 2_000);
    assert_eq!(upstream.get::<i64, _>("updated_at"), 2_010);
    assert_eq!(upstream.get::<i64, _>("deleted_at"), 2_020);

    let api_key = sqlx::query(
        "SELECT api_key_ciphertext, secret_revision, created_at, updated_at
         FROM upstream_api_key_secret_v1 WHERE upstream_id = ?",
    )
    .bind(DELETED_UPSTREAM_ID)
    .fetch_one(&mut *connection)
    .await
    .expect("read preserved API-key child");
    assert_eq!(
        api_key.get::<Vec<u8>, _>("api_key_ciphertext"),
        vec![0x20, 0x21, 0x22, 0x23]
    );
    assert_eq!(api_key.get::<i64, _>("secret_revision"), 12);
    assert_eq!(api_key.get::<i64, _>("created_at"), 2_100);
    assert_eq!(api_key.get::<i64, _>("updated_at"), 2_110);

    let oauth = sqlx::query(
        "SELECT oauth_credentials_ciphertext, token_revision, oauth_token_generation,
                refreshed_at, created_at, updated_at
         FROM upstream_oauth_token_v1 WHERE upstream_id = ?",
    )
    .bind(DELETED_UPSTREAM_ID)
    .fetch_one(&mut *connection)
    .await
    .expect("read preserved OAuth child");
    assert_eq!(
        oauth.get::<Vec<u8>, _>("oauth_credentials_ciphertext"),
        vec![0x40, 0x41, 0x42]
    );
    assert_eq!(oauth.get::<i64, _>("token_revision"), 22);
    assert_eq!(oauth.get::<i64, _>("oauth_token_generation"), 32);
    assert_eq!(oauth.get::<i64, _>("refreshed_at"), 2_200);
    assert_eq!(oauth.get::<i64, _>("created_at"), 2_210);
    assert_eq!(oauth.get::<i64, _>("updated_at"), 2_220);

    let status = sqlx::query(
        "SELECT last_apply_error, last_apply_at, observed_spec_revision,
                observed_api_key_secret_revision, observed_oauth_token_revision,
                last_warmup_at, updated_at
         FROM upstream_status_v1 WHERE upstream_id = ?",
    )
    .bind(DELETED_UPSTREAM_ID)
    .fetch_one(&mut *connection)
    .await
    .expect("read preserved status child");
    assert_eq!(
        status.get::<String, _>("last_apply_error"),
        "deleted apply error"
    );
    assert_eq!(status.get::<i64, _>("last_apply_at"), 2_300);
    assert_eq!(status.get::<i64, _>("observed_spec_revision"), 8);
    assert_eq!(status.get::<i64, _>("observed_api_key_secret_revision"), 12);
    assert_eq!(status.get::<i64, _>("observed_oauth_token_revision"), 22);
    assert_eq!(status.get::<i64, _>("last_warmup_at"), 2_310);
    assert_eq!(status.get::<i64, _>("updated_at"), 2_320);

    let warmup_attempts = sqlx::query(
        "SELECT id, upstream_id, outcome, reason, error_detail
         FROM warmup_attempts_v1 ORDER BY id",
    )
    .fetch_all(&mut *connection)
    .await
    .expect("read preserved warmup attempts");
    let warmup_payloads = warmup_attempts
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("id"),
                row.get::<String, _>("upstream_id"),
                row.get::<String, _>("outcome"),
                row.get::<String, _>("reason"),
                row.get::<String, _>("error_detail"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        warmup_payloads,
        vec![
            (
                "00000000-0000-0000-0000-000000000301".to_owned(),
                LIVE_UPSTREAM_ID.to_owned(),
                "success".to_owned(),
                "cycle_advanced".to_owned(),
                "live-success".to_owned(),
            ),
            (
                "00000000-0000-0000-0000-000000000302".to_owned(),
                LIVE_UPSTREAM_ID.to_owned(),
                "skipped".to_owned(),
                "window_already_active".to_owned(),
                "live-skipped".to_owned(),
            ),
            (
                "00000000-0000-0000-0000-000000000303".to_owned(),
                DELETED_UPSTREAM_ID.to_owned(),
                "permanent_failure".to_owned(),
                "oauth_credentials_missing".to_owned(),
                "deleted-failure".to_owned(),
            ),
        ]
    );

    let live_principal = sqlx::query(
        "SELECT enabled, allowed_models, allowed_upstreams, default_limits,
                router_terminal_strategy, revision, deleted_at, cache_keepalive_json
         FROM principals_v1 WHERE id = ?",
    )
    .bind(LIVE_PRINCIPAL_ID)
    .fetch_one(&mut *connection)
    .await
    .expect("read preserved live principal");
    assert_eq!(live_principal.get::<i64, _>("enabled"), 1);
    assert_eq!(
        live_principal.get::<String, _>("allowed_models"),
        r#"["claude-live"]"#
    );
    assert_eq!(
        live_principal.get::<String, _>("allowed_upstreams"),
        format!(r#"["{LIVE_UPSTREAM_ID}"]"#)
    );
    assert_eq!(
        live_principal.get::<String, _>("default_limits"),
        r#"[{"kind":"requests","limit":10}]"#
    );
    assert_eq!(
        live_principal.get::<String, _>("router_terminal_strategy"),
        "random"
    );
    assert_eq!(live_principal.get::<i64, _>("revision"), 41);
    assert_eq!(live_principal.get::<Option<i64>, _>("deleted_at"), None);
    assert_eq!(
        live_principal.get::<String, _>("cache_keepalive_json"),
        r#"{"enabled":true,"ttl_secs":60}"#
    );

    let principal = sqlx::query(
        "SELECT name, kind, enabled, allowed_models, allowed_upstreams, default_limits,
                router_terminal_strategy, revision, created_at, updated_at,
                last_apply_error, last_apply_at, deleted_at, cache_keepalive_json
         FROM principals_v1 WHERE id = ?",
    )
    .bind(DELETED_PRINCIPAL_ID)
    .fetch_one(&mut *connection)
    .await
    .expect("read preserved deleted principal");
    assert_eq!(principal.get::<String, _>("name"), "deleted-principal");
    assert_eq!(principal.get::<String, _>("kind"), "admin");
    assert_eq!(principal.get::<i64, _>("enabled"), 0);
    assert_eq!(
        principal.get::<String, _>("allowed_models"),
        r#"["claude-deleted"]"#
    );
    assert_eq!(
        principal.get::<String, _>("allowed_upstreams"),
        format!(r#"["{DELETED_UPSTREAM_ID}"]"#)
    );
    assert_eq!(
        principal.get::<String, _>("default_limits"),
        r#"[{"kind":"tokens","limit":20}]"#
    );
    assert_eq!(
        principal.get::<String, _>("router_terminal_strategy"),
        "first-pick"
    );
    assert_eq!(principal.get::<i64, _>("revision"), 42);
    assert_eq!(principal.get::<i64, _>("created_at"), 4_000);
    assert_eq!(principal.get::<i64, _>("updated_at"), 4_010);
    assert_eq!(
        principal.get::<String, _>("last_apply_error"),
        "deleted principal apply error"
    );
    assert_eq!(principal.get::<i64, _>("last_apply_at"), 4_020);
    assert_eq!(principal.get::<i64, _>("deleted_at"), 4_030);
    assert_eq!(
        principal.get::<String, _>("cache_keepalive_json"),
        r#"{"enabled":false,"ttl_secs":120}"#
    );
}

async fn assert_indexes(connection: &mut SqliteConnection) {
    let upstream_indexes = index_flags(connection, "upstream_spec_v1").await;
    assert_eq!(
        upstream_indexes.get("upstream_spec_v1_name_active_uniq"),
        Some(&(true, true))
    );
    assert_eq!(
        upstream_indexes.get("upstream_spec_v1_warmup_wasm_registry_id_idx"),
        Some(&(false, true))
    );

    let principal_indexes = index_flags(connection, "principals_v1").await;
    assert_eq!(
        principal_indexes.get("principals_v1_name_active_uniq"),
        Some(&(true, true))
    );
}

async fn assert_upstream_name_reuse(connection: &mut SqliteConnection) {
    let active_before: Option<String> = sqlx::query_scalar(
        "SELECT id FROM upstream_spec_v1
         WHERE name = 'deleted-upstream' AND deleted_at IS NULL",
    )
    .fetch_optional(&mut *connection)
    .await
    .expect("look up active upstream before recreation");
    assert_eq!(active_before, None);

    insert_upstream_for_reuse(connection, RECREATED_UPSTREAM_ID)
        .await
        .expect("reuse soft-deleted upstream name");

    let active_after: String = sqlx::query_scalar(
        "SELECT id FROM upstream_spec_v1
         WHERE name = 'deleted-upstream' AND deleted_at IS NULL",
    )
    .fetch_one(&mut *connection)
    .await
    .expect("look up recreated upstream");
    assert_eq!(active_after, RECREATED_UPSTREAM_ID);

    let old_deleted_at: i64 =
        sqlx::query_scalar("SELECT deleted_at FROM upstream_spec_v1 WHERE id = ?")
            .bind(DELETED_UPSTREAM_ID)
            .fetch_one(&mut *connection)
            .await
            .expect("read original upstream tombstone");
    assert_eq!(old_deleted_at, 2_020);

    let duplicate_error = insert_upstream_for_reuse(connection, DUPLICATE_UPSTREAM_ID)
        .await
        .expect_err("second active upstream with the same name must conflict");
    assert_unique_violation(duplicate_error);
}

async fn insert_upstream_for_reuse(
    connection: &mut SqliteConnection,
    id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO upstream_spec_v1 (
            id, name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin,
            spec_revision, created_at, updated_at, deleted_at
         ) VALUES (?, 'deleted-upstream', 'anthropic_oauth', NULL, 1, 0, NULL, 1, 5000, 5000, NULL)",
    )
    .bind(id)
    .execute(&mut *connection)
    .await
    .map(|_| ())
}

async fn assert_principal_name_reuse(connection: &mut SqliteConnection) {
    let active_before: Option<String> = sqlx::query_scalar(
        "SELECT id FROM principals_v1
         WHERE name = 'deleted-principal' AND deleted_at IS NULL",
    )
    .fetch_optional(&mut *connection)
    .await
    .expect("look up active principal before recreation");
    assert_eq!(active_before, None);

    insert_principal_for_reuse(connection, RECREATED_PRINCIPAL_ID)
        .await
        .expect("reuse soft-deleted principal name");

    let active_after: String = sqlx::query_scalar(
        "SELECT id FROM principals_v1
         WHERE name = 'deleted-principal' AND deleted_at IS NULL",
    )
    .fetch_one(&mut *connection)
    .await
    .expect("look up recreated principal");
    assert_eq!(active_after, RECREATED_PRINCIPAL_ID);

    let old_deleted_at: i64 =
        sqlx::query_scalar("SELECT deleted_at FROM principals_v1 WHERE id = ?")
            .bind(DELETED_PRINCIPAL_ID)
            .fetch_one(&mut *connection)
            .await
            .expect("read original principal tombstone");
    assert_eq!(old_deleted_at, 4_030);

    let duplicate_error = insert_principal_for_reuse(connection, DUPLICATE_PRINCIPAL_ID)
        .await
        .expect_err("second active principal with the same name must conflict");
    assert_unique_violation(duplicate_error);
}

async fn insert_principal_for_reuse(
    connection: &mut SqliteConnection,
    id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO principals_v1 (
            id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits,
            router_terminal_strategy, revision, created_at, updated_at, deleted_at,
            cache_keepalive_json
         ) VALUES (?, 'deleted-principal', 'machine', 1, '[]', '[]', '[]',
                   'first-pick', 0, 6000, 6000, NULL, NULL)",
    )
    .bind(id)
    .execute(&mut *connection)
    .await
    .map(|_| ())
}

async fn row_count(connection: &mut SqliteConnection, table: &str) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(&mut *connection)
        .await
        .unwrap_or_else(|error| panic!("count rows in {table}: {error}"))
}

async fn index_flags(
    connection: &mut SqliteConnection,
    table: &str,
) -> HashMap<String, (bool, bool)> {
    sqlx::query(AssertSqlSafe(format!("PRAGMA index_list('{table}')")))
        .fetch_all(&mut *connection)
        .await
        .unwrap_or_else(|error| panic!("list indexes for {table}: {error}"))
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("name"),
                (
                    row.get::<i64, _>("unique") != 0,
                    row.get::<i64, _>("partial") != 0,
                ),
            )
        })
        .collect()
}

async fn foreign_keys(connection: &mut SqliteConnection) -> i64 {
    sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut *connection)
        .await
        .expect("read foreign_keys pragma")
}

fn assert_unique_violation(error: sqlx::Error) {
    assert!(
        error
            .as_database_error()
            .is_some_and(|database_error| database_error.is_unique_violation()),
        "expected a unique-constraint violation, got {error}"
    );
}
