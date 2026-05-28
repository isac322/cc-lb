//! Task 16 sub-cases:
//! 1. Issue a managed key on instance A via the in-process `KeyStore::create` (or whatever public accessor `App` exposes — NOT an HTTP roundtrip).
//! 2. On instance B: authenticate (call `app_b.builtin_authn().authenticate(...)`) with the same token → succeed.
//! 3. On instance A: revoke the key (`KeyStore::revoke` or equivalent).
//! 4. On instance B: authenticate same token → fail (NOT `Unavailable`; expect `InvalidKey` / `Revoked` / whatever variant means "no such active key").
//! 5. Concurrent issue: spawn 50 tasks on A + 50 tasks on B issuing for the SAME principal → assert all 100 succeed AND 100 UNIQUE `key_id` values.
//! 6. Cleanup: TRUNCATE `managed_api_key_index_v1, managed_api_keys_v1` in test setup OR a guard.
//!
//! QA Scenarios (MANDATORY):
//! Scenario: Cross-instance issue + auth + revoke
//! Scenario: 100 concurrent issues across two instances
//! Scenario: CI_POSTGRES_URL unset → SKIP (not FAIL)

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use cc_lb_config::{
    Config, DownstreamAuthMode, Limit as ConfigLimit, LimitKind as ConfigLimitKind, PrincipalSpec,
    PrincipalType,
};
use cc_lb_core::api_keys::{
    builtin_authn::{BuiltinAuthError, BuiltinAuthn},
    key_store::{CreateParams, KeyStore},
    principal_view::PrincipalView,
    secret,
};
use cc_lb_server::app::{App, build_app_for_testing_postgres};
use cc_lb_storage_api::types::{Limit, LimitKind, PrincipalKindLite, UpstreamKind};
use cc_lb_storage_postgres::{PostgresManagedKeyStore, adapter::retry::RetryPolicy};
use http::{HeaderMap, HeaderValue};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

static TEST_LOCK: Mutex<()> = Mutex::const_new(());

const PRINCIPAL_ID: &str = "multi-instance-principal";
const TASKS_PER_INSTANCE: usize = 50;
const TOTAL_EXPECTED_KEYS: usize = TASKS_PER_INSTANCE * 2;

// App has no public key_store/builtin_authn accessor, so each test keeps two
// real App fixtures alive and attaches per-instance handles to the same DB.
struct TestInstance {
    _app: App,
    key_store: Arc<KeyStore>,
    builtin_authn: BuiltinAuthn,
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn cross_instance_issue_auth_revoke() {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skipped: CI_POSTGRES_URL unset");
        return;
    };

    let _guard = TEST_LOCK.lock().await;

    if let Err(error) = run_cross_instance_issue_auth_revoke(&url).await {
        panic!("cross-instance issue/auth/revoke failed: {error}");
    }
}

#[tokio::test]
async fn concurrent_cross_instance_issue() {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skipped: CI_POSTGRES_URL unset");
        return;
    };

    let _guard = TEST_LOCK.lock().await;

    if let Err(error) = run_concurrent_cross_instance_issue(&url).await {
        panic!("concurrent cross-instance issue failed: {error}");
    }
}

async fn run_cross_instance_issue_auth_revoke(database_url: &str) -> TestResult<()> {
    let instance_a = build_test_instance(database_url, 4).await?;
    let instance_b = build_test_instance(database_url, 4).await?;
    clean_database(database_url).await?;

    let (record, plaintext) = instance_a
        .key_store
        .create(PRINCIPAL_ID, create_params("issued-on-instance-a"))
        .await?;
    let headers = headers_with_key(plaintext.expose())?;
    let (key_id, _) = secret::parse(plaintext.expose())?;

    let success = instance_b.builtin_authn.authenticate(&headers).await?;
    assert_eq!(success.principal_id, PRINCIPAL_ID);
    assert_eq!(success.key_id, key_id);
    assert_eq!(success.record, record);

    instance_a.key_store.revoke(PRINCIPAL_ID, &key_id).await?;

    let error = instance_b
        .builtin_authn
        .authenticate(&headers)
        .await
        .expect_err("revoked key must not authenticate on instance B");
    assert_ne!(error, BuiltinAuthError::Unavailable);
    assert!(
        matches!(
            error,
            BuiltinAuthError::NotFound | BuiltinAuthError::KeyRevoked
        ),
        "expected inactive-key auth failure after revoke, got {error:?}"
    );

    Ok(())
}

async fn run_concurrent_cross_instance_issue(database_url: &str) -> TestResult<()> {
    let instance_a = build_test_instance(database_url, 32).await?;
    let instance_b = build_test_instance(database_url, 32).await?;
    clean_database(database_url).await?;

    let mut tasks = JoinSet::new();
    for index in 0..TASKS_PER_INSTANCE {
        let key_store = instance_a.key_store.clone();
        tasks.spawn(issue_key(key_store, format!("instance-a-{index}")));
    }
    for index in 0..TASKS_PER_INSTANCE {
        let key_store = instance_b.key_store.clone();
        tasks.spawn(issue_key(key_store, format!("instance-b-{index}")));
    }

    let mut key_ids = HashSet::new();
    while let Some(result) = tasks.join_next().await {
        let key_id = result??;
        assert!(key_ids.insert(key_id), "duplicate key_id issued");
    }

    assert_eq!(key_ids.len(), TOTAL_EXPECTED_KEYS);
    println!("issued={} unique={}", TOTAL_EXPECTED_KEYS, key_ids.len());

    Ok(())
}

async fn build_test_instance(database_url: &str, max_connections: u32) -> TestResult<TestInstance> {
    let app = build_app_for_testing_postgres(database_url).await?;
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await?;
    let managed_store = Arc::new(PostgresManagedKeyStore::new(
        pool,
        Arc::new(RetryPolicy::default()),
    ));
    let key_store = Arc::new(KeyStore::new(managed_store));
    let builtin_authn = BuiltinAuthn::new(
        DownstreamAuthMode::ApiKey,
        None,
        key_store.clone(),
        principal_view(),
    );

    Ok(TestInstance {
        _app: app,
        key_store,
        builtin_authn,
    })
}

async fn issue_key(key_store: Arc<KeyStore>, label: String) -> TestResult<String> {
    let (_record, plaintext) = key_store.create(PRINCIPAL_ID, create_params(label)).await?;
    let (key_id, _) = secret::parse(plaintext.expose())?;
    Ok(key_id)
}

async fn clean_database(database_url: &str) -> sqlx::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    sqlx::query("TRUNCATE managed_api_key_index_v1, managed_api_keys_v1")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

fn create_params(label: impl Into<String>) -> CreateParams {
    CreateParams {
        upstream_kind: UpstreamKind::AnthropicKey,
        upstream_credential_ref: "anthropic-prod".to_owned(),
        label: label.into(),
        description: Some("multi-instance integration test".to_owned()),
        expires_at_unix_secs: Some(4_102_444_800),
        limit_overrides: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        }],
        principal_kind: PrincipalKindLite::Machine,
    }
}

fn principal_view() -> Arc<ArcSwap<PrincipalView>> {
    Arc::new(ArcSwap::from(PrincipalView::from_config(
        &principal_config(),
    )))
}

fn principal_config() -> Config {
    let mut principals = HashMap::new();
    principals.insert(
        PRINCIPAL_ID.to_owned(),
        PrincipalSpec {
            principal_type: PrincipalType::Machine,
            default_limits: vec![ConfigLimit {
                kind: ConfigLimitKind::Requests,
                window: Duration::from_secs(60),
                cap_micros: 1_000,
            }],
            enabled: true,
            allowed_models: vec!["*".to_owned()],
            credentials_ref: None,
        },
    );

    Config {
        principals,
        ..Config::default()
    }
}

fn headers_with_key(api_key: &str) -> TestResult<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_str(api_key)?);
    Ok(headers)
}
