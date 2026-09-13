use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DownstreamAuthMode, StorageConfig};
use cc_lb_engine::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_engine::{ClockHandle, TestClock};
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion, global_catalog};
use cc_lb_server::app::build_app_with_storage;
use cc_lb_server::{BuildError, signal::SignalHandle};
use cc_lb_storage_api::{
    AuditEntry, AuditStore, BackendKind, ManagedKeyStore, MetaStore, RequestEvent,
    RequestEventStore, Storage, SubscriptionQuotaLatestRecord, UpstreamSubscriptionQuotaStore,
    principal::{
        Limit as PrincipalLimit, LimitKind as PrincipalLimitKind, PrincipalCreate, PrincipalKind,
        PrincipalStore,
    },
    types::{PrincipalKindLite, UpstreamKind as KeyUpstreamKind},
    upstream::{UpstreamCreate, UpstreamKind, UpstreamStore},
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use fake_anthropic::{AppConfig, MessageScript, ScriptedMessageResponse, WeatherConfig};
use http::{HeaderMap, StatusCode};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;
use uuid::Uuid;

const ADMIN_TOKEN: &str = "parity-characterization-admin-token";
const MODEL: &str = "claude-3-5-sonnet-20241022";
const FIXED_UNIX_SECS: u64 = 1_700_000_000;
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

const REQUEST_LABELS: [&str; 8] = [
    "non-stream messages 200",
    "stream messages exact SSE body/order",
    "count_tokens exact body",
    "models exact body",
    "upstream 401 exact status/body",
    "upstream 429 unified quota headers",
    "local request-limit rejection",
    "subscription-quota observation from 200 headers",
];

const REQUEST_IDS: [&str; 8] = [
    "golden-01-non-stream",
    "golden-02-stream",
    "golden-03-count-tokens",
    "golden-04-models",
    "golden-05-upstream-401",
    "golden-06-upstream-429",
    "golden-07-local-limit",
    "golden-08-quota-200",
];
const REQUEST_EVENT_IDS: [&str; 9] = [
    "golden-01-non-stream",
    "golden-02-stream",
    "golden-03-count-tokens",
    "golden-04-models",
    "golden-05-upstream-401",
    "golden-06-upstream-429",
    "golden-07-limit-prime",
    "golden-07-local-limit",
    "golden-08-quota-200",
];

#[derive(Debug, Serialize)]
struct GoldenSnapshot {
    fixed_clock_unix_secs: u64,
    observations: Vec<ObservationSnapshot>,
    request_events: Vec<Value>,
    audit_rows: Vec<Value>,
    subscription_quota_rows: Vec<Value>,
    fixed_clock_values: FixedClockValues,
}

#[derive(Debug, Serialize)]
struct ObservationSnapshot {
    label: &'static str,
    status: u16,
    stable_headers: BTreeMap<String, String>,
    body_utf8: String,
    body_bytes: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct FixedClockValues {
    audit_ts: u64,
    subscription_quota_observed_at_unix_millis: u64,
    subscription_quota_ingested_at_unix_millis: u64,
}

struct SeededKey {
    plaintext: String,
    key_id: String,
}

struct RunningApp {
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    signal: SignalHandle,
    task: Option<JoinHandle<Result<(), BuildError>>>,
}

impl RunningApp {
    async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.signal.start_shutdown();
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(WAIT_TIMEOUT, &mut task).await {
                Ok(result) => {
                    result
                        .map_err(|error| format!("App task failed during shutdown: {error}"))?
                        .map_err(|error| {
                            format!("App returned an error during shutdown: {error}")
                        })?;
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    return Err("App did not shut down within 5s".into());
                }
            }
        }
        Ok(())
    }
}

impl Drop for RunningApp {
    fn drop(&mut self) {
        self.signal.start_shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct FakeTask {
    task: Option<JoinHandle<Result<(), std::io::Error>>>,
}

impl FakeTask {
    fn new(task: JoinHandle<Result<(), std::io::Error>>) -> Self {
        Self { task: Some(task) }
    }

    async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut task) = self.task.take() {
            if !task.is_finished() {
                task.abort();
            }
            match tokio::time::timeout(WAIT_TIMEOUT, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => return Err(error.into()),
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => return Err(format!("fake task panicked: {error}").into()),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    return Err("fake task did not cancel within 5s".into());
                }
            }
        }
        Ok(())
    }
}

impl Drop for FakeTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[tokio::test]
async fn t4__parity_characterization_golden() -> Result<(), Box<dyn std::error::Error>> {
    install_price_catalog_fixture();

    let dir = tempfile::tempdir()?;
    let sqlite_path = dir.path().join("parity-characterization.sqlite");
    let config_path = dir.path().join("cc-lb.toml");
    let clock = Arc::new(TestClock::new_at_secs(FIXED_UNIX_SECS));
    let clock_handle: ClockHandle = clock.clone();
    let storage = open_test_storage(&sqlite_path, clock_handle.clone()).await?;

    let message_script = MessageScript::new();
    let mut fake_config = AppConfig {
        message_script: Some(message_script.clone()),
        ..AppConfig::default()
    };
    fake_config.weather = WeatherConfig {
        delta_count: 1,
        ..WeatherConfig::default()
    };
    let fake_listener = TcpListener::bind("127.0.0.1:0").await?;
    let fake_addr = fake_listener.local_addr()?;
    let fake_task = FakeTask::new(tokio::spawn(async move {
        axum::serve(fake_listener, fake_anthropic::app(fake_config)).await
    }));
    let fake_ready = tokio::time::timeout(
        WAIT_TIMEOUT,
        raw_http(
            fake_addr,
            "GET",
            "/v1/models",
            &[("x-api-key", "sk-ant-readiness")],
            &[],
        ),
    )
    .await
    .map_err(|_| "fake readiness timed out after 5s")??;
    assert_eq!(fake_ready.status, StatusCode::OK);

    let upstream = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "parity-fake-anthropic".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse(&format!("http://{fake_addr}"))?),
            api_key_ciphertext: Some(Vec::new()),
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await?;
    let regular_key = seed_key(storage.clone(), "parity-regular", regular_limits()).await?;
    let limited_key = seed_key(storage.clone(), "parity-limited", one_request_limit()).await?;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let admin_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let admin_addr = admin_listener.local_addr()?;

    let mut config = Config::default();
    config.listener.proxy_addr = proxy_addr;
    config.listener.admin_addr = admin_addr;
    config.listener.metrics_addr = "127.0.0.1:0".parse()?;
    config.storage = StorageConfig::Sqlite {
        path: sqlite_path.clone(),
    };
    config.runtime.data_dir = Some(dir.path().join("data"));
    config.aead.key_env = "__CC_LB_PARITY_EXPLICIT_KEY__".to_owned();
    config.admin.token = Some(ADMIN_TOKEN.to_owned());
    config.downstream_auth.mode = DownstreamAuthMode::ApiKey;
    config.downstream_auth.none_mode = None;
    config.timeouts.upstream_total_secs = 30;
    config.api_keys.price_catalog.url = format!("http://{fake_addr}/prices");
    config.api_keys.price_catalog.refresh_interval = Duration::from_secs(24 * 60 * 60);
    config.api_keys.price_catalog.cache_path = dir.path().join("prices.json");
    config.subscription_quota.writer_batch_max_records = 1;
    config.subscription_quota.writer_flush_ms = 10;
    for job in config.scheduler.recurring_jobs.values_mut() {
        job.enabled = false;
    }
    std::fs::write(&config_path, toml::to_string_pretty(&config)?)?;

    let managed_store: Arc<dyn ManagedKeyStore> = storage.clone();
    let storage_handle: Arc<dyn Storage> = storage.clone();
    let aead = Arc::new(AeadService::from_master_key([0x33; 32]));
    let app = build_app_with_storage(
        config,
        Some(&config_path),
        managed_store,
        storage_handle,
        aead,
        clock_handle,
    )
    .await?;
    let signal = app.signal_handle();
    let task = tokio::spawn(async move {
        app.start_with_listeners(proxy_listener, admin_listener)
            .await
    });
    let mut running = RunningApp {
        proxy_addr,
        admin_addr,
        signal,
        task: Some(task),
    };

    wait_until_ready(&mut running).await?;
    assert_listener_separation(&running).await?;
    assert_storage_is_empty(storage.as_ref(), upstream.id).await?;

    let mut observations = Vec::with_capacity(8);

    observations.push(
        observe(
            &running,
            REQUEST_LABELS[0],
            REQUEST_IDS[0],
            "POST",
            "/v1/messages",
            &regular_key.plaintext,
            &message_body(false),
            &[],
            StatusCode::OK,
            &json_body(&happy_message_body()),
        )
        .await?,
    );

    observations.push(
        observe(
            &running,
            REQUEST_LABELS[1],
            REQUEST_IDS[1],
            "POST",
            "/v1/messages",
            &regular_key.plaintext,
            &message_body(true),
            &[],
            StatusCode::OK,
            expected_sse_body().as_bytes(),
        )
        .await?,
    );

    observations.push(
        observe(
            &running,
            REQUEST_LABELS[2],
            REQUEST_IDS[2],
            "POST",
            "/v1/messages/count_tokens",
            &regular_key.plaintext,
            &message_body(false),
            &[],
            StatusCode::OK,
            &json_body(&json!({"input_tokens": 100})),
        )
        .await?,
    );

    observations.push(
        observe(
            &running,
            REQUEST_LABELS[3],
            REQUEST_IDS[3],
            "GET",
            "/v1/models",
            &regular_key.plaintext,
            &[],
            &[],
            StatusCode::OK,
            &json_body(&models_body()),
        )
        .await?,
    );

    let unauthorized_body = json!({
        "type": "error",
        "error": {"type": "authentication_error", "message": "scripted upstream unauthorized"}
    });
    message_script.push_response(ScriptedMessageResponse::error(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        "scripted upstream unauthorized",
    ));
    observations.push(
        observe(
            &running,
            REQUEST_LABELS[4],
            REQUEST_IDS[4],
            "POST",
            "/v1/messages",
            &regular_key.plaintext,
            &message_body(false),
            &[],
            StatusCode::UNAUTHORIZED,
            &json_body(&unauthorized_body),
        )
        .await?,
    );

    let rate_limited_body = json!({
        "type": "error",
        "error": {"type": "rate_limit_error", "message": "scripted upstream quota rejection"}
    });
    message_script.push_response(
        ScriptedMessageResponse::error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "scripted upstream quota rejection",
        )
        .with_header("retry-after", "17")
        .with_header("anthropic-ratelimit-unified-5h-utilization", "0.42")
        .with_header("anthropic-ratelimit-unified-5h-status", "allowed_warning")
        .with_header("anthropic-ratelimit-unified-5h-reset", "1800000000"),
    );
    observations.push(
        observe(
            &running,
            REQUEST_LABELS[5],
            REQUEST_IDS[5],
            "POST",
            "/v1/messages",
            &regular_key.plaintext,
            &message_body(false),
            &[],
            StatusCode::TOO_MANY_REQUESTS,
            &json_body(&rate_limited_body),
        )
        .await?,
    );

    let prime = request(
        running.proxy_addr,
        "POST",
        "/v1/messages",
        &[
            ("content-type", "application/json"),
            ("x-api-key", &limited_key.plaintext),
            ("request-id", "golden-07-limit-prime"),
        ],
        &message_body(false),
    )
    .await?;
    assert_eq!(prime.status, StatusCode::OK, "limited key priming request");
    assert_eq!(
        prime.body,
        json_body(&happy_message_body()),
        "limited key priming response body"
    );

    let local_limit_body = json!({
        "type": "error",
        "error": {
            "type": "rate_limit_error",
            "message": "requests/window cap exceeded",
            "limit_kind": "requests",
            "retry_after_seconds": 60
        }
    });
    observations.push(
        observe(
            &running,
            REQUEST_LABELS[6],
            REQUEST_IDS[6],
            "POST",
            "/v1/messages",
            &limited_key.plaintext,
            &message_body(false),
            &[],
            StatusCode::TOO_MANY_REQUESTS,
            &json_body(&local_limit_body),
        )
        .await?,
    );

    let quota_success_body = json!({
        "id": "msg_quota_observation",
        "type": "message",
        "role": "assistant",
        "model": MODEL,
        "content": [{"type": "text", "text": "quota observed"}],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {"input_tokens": 7, "output_tokens": 2}
    });
    message_script.push_response(
        ScriptedMessageResponse {
            status: StatusCode::OK,
            headers: BTreeMap::new(),
            body: quota_success_body.clone(),
            delay: Duration::ZERO,
        }
        .with_header("anthropic-ratelimit-unified-7d-utilization", "0.25")
        .with_header("anthropic-ratelimit-unified-7d-status", "allowed")
        .with_header("anthropic-ratelimit-unified-7d-reset", "1800001000"),
    );
    observations.push(
        observe(
            &running,
            REQUEST_LABELS[7],
            REQUEST_IDS[7],
            "POST",
            "/v1/messages",
            &regular_key.plaintext,
            &message_body(false),
            &[],
            StatusCode::OK,
            &json_body(&quota_success_body),
        )
        .await?,
    );

    assert_eq!(
        observations.len(),
        8,
        "golden must contain exactly eight observations"
    );
    assert_eq!(
        message_script.request_count(),
        6,
        "exactly six requests reach the messages upstream; the local rejection must not dispatch"
    );

    let request_events = wait_for_request_events(storage.as_ref(), 9).await;
    let audit_rows = wait_for_audit_rows(storage.as_ref(), 1).await;
    let quota_rows = wait_for_quota_rows(storage.as_ref(), upstream.id, 2).await;

    let limit_event_id = request_events
        .iter()
        .find(|row| row.request_id == REQUEST_IDS[6])
        .and_then(|row| row.event_id.clone())
        .expect("local limit RequestEvent has an event_id");
    Uuid::parse_str(&limit_event_id).expect("local limit event_id must be a UUID");

    let normalized_events = normalize_request_events(
        request_events,
        &regular_key.key_id,
        &limited_key.key_id,
        upstream.id,
    );
    let normalized_audits = normalize_audits(audit_rows, &limited_key.key_id, &limit_event_id);
    let normalized_quotas = normalize_quotas(quota_rows, upstream.id);

    insta::assert_json_snapshot!(GoldenSnapshot {
        fixed_clock_unix_secs: FIXED_UNIX_SECS,
        observations,
        request_events: normalized_events,
        audit_rows: normalized_audits,
        subscription_quota_rows: normalized_quotas,
        fixed_clock_values: FixedClockValues {
            audit_ts: FIXED_UNIX_SECS,
            subscription_quota_observed_at_unix_millis: FIXED_UNIX_SECS * 1_000,
            subscription_quota_ingested_at_unix_millis: FIXED_UNIX_SECS * 1_000,
        },
    });

    running.shutdown().await?;
    fake_task.shutdown().await?;
    storage.pool().close().await;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the golden observation helper keeps every asserted HTTP input explicit"
)]
async fn observe(
    app: &RunningApp,
    label: &'static str,
    request_id: &str,
    method: &str,
    path: &str,
    key: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
    expected_status: StatusCode,
    expected_body: &[u8],
) -> Result<ObservationSnapshot, Box<dyn std::error::Error>> {
    let mut headers = vec![
        ("content-type", "application/json"),
        ("x-api-key", key),
        ("request-id", request_id),
    ];
    headers.extend_from_slice(extra_headers);
    let response = request(app.proxy_addr, method, path, &headers, body).await?;
    assert_eq!(response.status, expected_status, "label={label}");
    assert_eq!(response.body, expected_body, "label={label}");
    assert_eq!(
        response
            .headers
            .get("request-id")
            .and_then(|value| value.to_str().ok()),
        Some(request_id),
        "label={label}: request-id must be present and preserve the valid caller-supplied format"
    );
    Ok(ObservationSnapshot {
        label,
        status: response.status.as_u16(),
        stable_headers: stable_headers(&response.headers),
        body_utf8: String::from_utf8(response.body.clone())?,
        body_bytes: response.body,
    })
}

async fn wait_until_ready(app: &mut RunningApp) -> Result<(), Box<dyn std::error::Error>> {
    let readiness = async {
        let proxy = raw_http(app.proxy_addr, "GET", "/healthz", &[], &[]).await?;
        if proxy.status != StatusCode::OK {
            return Err(format!("proxy readiness returned {}", proxy.status).into());
        }
        let admin = raw_http(app.admin_addr, "GET", "/admin/health", &[], &[]).await?;
        if admin.status != StatusCode::OK {
            return Err(format!("admin readiness returned {}", admin.status).into());
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    match tokio::time::timeout(WAIT_TIMEOUT, readiness).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            if app.task.as_ref().is_some_and(JoinHandle::is_finished) {
                let task = app.task.take().expect("finished App task exists");
                let result = task.await.expect("App task panicked before readiness");
                return Err(format!("App failed before readiness: {result:?}").into());
            }
            Err(error)
        }
        Err(_) => {
            if app.task.as_ref().is_some_and(JoinHandle::is_finished) {
                let task = app.task.take().expect("finished App task exists");
                let result = task.await.expect("App task panicked before readiness");
                return Err(format!("App failed before readiness: {result:?}").into());
            }
            Err(format!("App readiness was not observed within {WAIT_TIMEOUT:?}").into())
        }
    }
}

async fn assert_listener_separation(app: &RunningApp) -> Result<(), Box<dyn std::error::Error>> {
    let admin = request(app.admin_addr, "GET", "/admin/health", &[], &[]).await?;
    assert_eq!(admin.status, StatusCode::OK);
    let proxy = request(app.proxy_addr, "GET", "/admin/health", &[], &[]).await?;
    assert_eq!(proxy.status, StatusCode::NOT_FOUND);
    Ok(())
}

async fn assert_storage_is_empty(
    storage: &SqliteStorage,
    upstream_id: Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(
        storage
            .query_request_events(0, u64::MAX, 16)
            .await?
            .is_empty(),
        "readiness must not create RequestEvent rows"
    );
    assert!(
        storage.query_audit(None, 0, u64::MAX, 16).await?.is_empty(),
        "readiness must not create audit rows"
    );
    assert!(
        storage
            .list_latest_subscription_quota_for_upstreams(&[upstream_id])
            .await?
            .is_empty(),
        "readiness must not create subscription quota rows"
    );
    Ok(())
}

async fn wait_for_request_events(storage: &SqliteStorage, expected: usize) -> Vec<RequestEvent> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let rows = storage
                .query_request_events(FIXED_UNIX_SECS - 1, FIXED_UNIX_SECS + 1, expected + 1)
                .await
                .expect("query RequestEvent rows");
            assert!(rows.len() <= expected, "unexpected extra RequestEvent row");
            if rows.len() == expected {
                return rows;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request events were not persisted within 5s")
}

async fn wait_for_audit_rows(storage: &SqliteStorage, expected: usize) -> Vec<AuditEntry> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let rows = storage
                .query_audit(None, 0, u64::MAX, expected + 1)
                .await
                .expect("query audit rows");
            assert!(rows.len() <= expected, "unexpected extra audit row");
            if rows.len() == expected {
                return rows;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("limit audit row was not persisted within 5s")
}

async fn wait_for_quota_rows(
    storage: &SqliteStorage,
    upstream_id: Uuid,
    expected: usize,
) -> Vec<SubscriptionQuotaLatestRecord> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let rows = storage
                .list_latest_subscription_quota_for_upstreams(&[upstream_id])
                .await
                .expect("query subscription quota rows");
            assert!(
                rows.len() <= expected,
                "unexpected extra subscription quota row"
            );
            if rows.len() == expected {
                return rows;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscription quota rows were not persisted within 5s")
}

fn normalize_request_events(
    rows: Vec<RequestEvent>,
    regular_key_id: &str,
    limited_key_id: &str,
    upstream_id: Uuid,
) -> Vec<Value> {
    let mut by_request_id = rows
        .into_iter()
        .map(|row| (row.request_id.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let mut normalized = Vec::with_capacity(REQUEST_EVENT_IDS.len());
    for request_id in REQUEST_EVENT_IDS {
        let row = by_request_id
            .remove(request_id)
            .unwrap_or_else(|| panic!("missing RequestEvent for {request_id}"));
        assert_eq!(row.ts, FIXED_UNIX_SECS);
        assert_eq!(row.ts_ms, Some(FIXED_UNIX_SECS * 1_000));
        let mut value = serde_json::to_value(row).expect("RequestEvent serializes");
        normalize_dynamic_ids(&mut value, regular_key_id, limited_key_id, upstream_id);
        normalize_monotonic_timings(&mut value);
        normalized.push(value);
    }
    assert!(
        by_request_id.is_empty(),
        "all RequestEvent rows must match an observation"
    );
    normalized
}

fn normalize_audits(
    rows: Vec<AuditEntry>,
    limited_key_id: &str,
    limit_event_id: &str,
) -> Vec<Value> {
    rows.into_iter()
        .map(|row| {
            assert_eq!(row.request_id, limit_event_id);
            assert_eq!(row.api_key_id.as_deref(), Some(limited_key_id));
            assert_fixed_seconds(row.ts, "audit.ts");
            let mut value = serde_json::to_value(row).expect("AuditEntry serializes");
            assert_eq!(value["ts"], Value::from(FIXED_UNIX_SECS));
            normalize_dynamic_ids(&mut value, "", limited_key_id, Uuid::nil());
            normalize_monotonic_timings(&mut value);
            value
        })
        .collect()
}

fn normalize_quotas(rows: Vec<SubscriptionQuotaLatestRecord>, upstream_id: Uuid) -> Vec<Value> {
    let mut rows = rows;
    rows.sort_by_key(|row| row.window.code());
    rows.into_iter()
        .map(|row| {
            assert_eq!(row.upstream_id, upstream_id);
            assert_fixed_millis(
                row.observed_at_unix_millis,
                "subscription_quota.observed_at_unix_millis",
            );
            assert_fixed_millis(
                row.ingested_at_unix_millis,
                "subscription_quota.ingested_at_unix_millis",
            );
            assert_ne!(row.sample_id, Uuid::nil(), "quota sample_id must be a UUID");
            let mut value = serde_json::to_value(row).expect("quota row serializes");
            assert_eq!(
                value["observed_at_unix_millis"],
                Value::from(FIXED_UNIX_SECS * 1_000)
            );
            assert_eq!(
                value["ingested_at_unix_millis"],
                Value::from(FIXED_UNIX_SECS * 1_000)
            );
            normalize_dynamic_ids(&mut value, "", "", upstream_id);
            value
        })
        .collect()
}

fn normalize_monotonic_timings(value: &mut Value) {
    const FIELDS: &[&str] = &[
        "auth_ms",
        "route_ms",
        "limit_reserve_ms",
        "json_parse_ms",
        "cache_structure_ms",
        "cache_token_key_ms",
        "cache_count_lookup_ms",
        "cache_tokenizer_queue_ms",
        "cache_serialize_ms",
        "cache_tokenize_ms",
        "prepare_signer_ms",
        "bulkhead_wait_ms",
        "dns_ms",
        "connect_ms",
        "limit_reconcile_ms",
        "observability_post_ms",
        "duration_ms",
        "request_body_read_ms",
        "finalize_ms",
        "proxy_setup_ms",
        "shape_ms",
        "sign_ms",
        "upstream_ttfb_ms",
        "upstream_body_ms",
        "first_body_chunk_ms",
        "request_body_first_chunk_ms",
        "request_body_receive_ms",
        "request_body_wait_ms",
        "request_body_process_ms",
        "response_body_wait_ms",
        "response_body_process_ms",
        "response_body_downstream_poll_gap_ms",
        "retry_overhead_ms",
        "stream_message_start_ms",
        "stream_content_block_start_ms",
        "stream_first_content_delta_ms",
        "stream_last_content_delta_ms",
        "stream_message_stop_ms",
        "stream_last_chunk_ms",
        "stream_total_ms",
        "inter_token_avg_ms",
        "duration_us",
    ];

    match value {
        Value::Array(items) => {
            for item in items {
                normalize_monotonic_timings(item);
            }
        }
        Value::Object(fields) => {
            for (name, field) in fields.iter_mut() {
                if FIELDS.contains(&name.as_str()) && field.is_number() {
                    let duration = field
                        .as_f64()
                        .expect("monotonic duration must serialize as a finite number");
                    assert!(
                        duration.is_finite() && duration >= 0.0,
                        "{name} must be a finite non-negative duration, got {duration}"
                    );
                    *field = Value::String("<monotonic-duration>".to_owned());
                } else {
                    normalize_monotonic_timings(field);
                }
            }
            if fields.contains_key("stage_name") {
                fields.insert(
                    "duration_us".to_owned(),
                    Value::String("<monotonic-duration>".to_owned()),
                );
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn normalize_dynamic_ids(
    value: &mut Value,
    regular_key_id: &str,
    limited_key_id: &str,
    upstream_id: Uuid,
) {
    match value {
        Value::String(text) => {
            if text == regular_key_id || text == limited_key_id {
                assert_key_id_format(text);
                *text = "<key-id>".to_owned();
            } else if text == &upstream_id.to_string() || Uuid::parse_str(text).is_ok() {
                Uuid::parse_str(text).expect("normalized UUID must parse");
                *text = "<uuid>".to_owned();
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_dynamic_ids(item, regular_key_id, limited_key_id, upstream_id);
            }
        }
        Value::Object(fields) => {
            for field in fields.values_mut() {
                normalize_dynamic_ids(field, regular_key_id, limited_key_id, upstream_id);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn assert_key_id_format(key_id: &str) {
    assert_eq!(
        key_id.len(),
        26,
        "managed key id must be a 26-character ULID"
    );
    assert!(
        key_id.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z')),
        "managed key id must use Crockford base32"
    );
}

fn assert_fixed_seconds(value: u64, field: &str) {
    assert_eq!(
        value, FIXED_UNIX_SECS,
        "{field} must use the injected fixed clock"
    );
}

fn assert_fixed_millis(value: u64, field: &str) {
    assert_eq!(
        value,
        FIXED_UNIX_SECS * 1_000,
        "{field} must use the injected fixed clock"
    );
}

fn stable_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    const STABLE: &[&str] = &[
        "content-length",
        "content-type",
        "transfer-encoding",
        "request-id",
        "retry-after",
        "anthropic-organization-id",
        "anthropic-ratelimit-requests-remaining",
        "anthropic-ratelimit-tokens-remaining",
        "anthropic-ratelimit-unified-5h-utilization",
        "anthropic-ratelimit-unified-5h-status",
        "anthropic-ratelimit-unified-5h-reset",
        "anthropic-ratelimit-unified-7d-utilization",
        "anthropic-ratelimit-unified-7d-status",
        "anthropic-ratelimit-unified-7d-reset",
    ];
    STABLE
        .iter()
        .filter_map(|name| {
            headers
                .get(*name)
                .and_then(|value| value.to_str().ok())
                .map(|value| ((*name).to_owned(), value.to_owned()))
        })
        .collect()
}

async fn seed_key(
    storage: Arc<SqliteStorage>,
    principal_name: &str,
    default_limits: Vec<PrincipalLimit>,
) -> Result<SeededKey, Box<dyn std::error::Error>> {
    PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: principal_name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec![],
            allowed_upstreams: vec![],
            default_limits,
            cache_keepalive: None,
        },
        FIXED_UNIX_SECS,
    )
    .await?;
    let (_record, plaintext) = KeyStore::new(storage)
        .create(
            principal_name,
            CreateParams {
                upstream_kind: KeyUpstreamKind::AnthropicKey,
                label: "parity-golden".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![],
                principal_kind: PrincipalKindLite::Machine,
            },
        )
        .await?;
    let (key_id, _) = cc_lb_engine::api_keys::secret::parse(plaintext.expose())?;
    assert_key_id_format(&key_id);
    Ok(SeededKey {
        plaintext: plaintext.expose().to_owned(),
        key_id,
    })
}

fn regular_limits() -> Vec<PrincipalLimit> {
    vec![]
}

fn one_request_limit() -> Vec<PrincipalLimit> {
    vec![PrincipalLimit {
        kind: PrincipalLimitKind::Requests,
        window_secs: 60 * 60,
        cap_micros: 1,
    }]
}

async fn open_test_storage(
    path: &Path,
    clock: ClockHandle,
) -> Result<Arc<SqliteStorage>, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = Arc::new(open_sqlite(&database_url, clock).await?);
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

fn install_price_catalog_fixture() {
    let models = HashMap::from([(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            by_tier: Default::default(),
        },
    )]);
    global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "parity-characterization-price-catalog".to_owned(),
        fetched_at_ms: FIXED_UNIX_SECS * 1_000,
        models,
        raw_json: serde_json::to_vec(&json!({
            MODEL: {
                "input_cost_per_token": 0.000003,
                "output_cost_per_token": 0.000015,
                "mode": "chat",
                "max_tokens": 8192
            }
        }))
        .expect("price catalog serializes"),
        cache_creation_per_million_usd: std::collections::HashMap::new(),
        cache_read_per_million_usd: std::collections::HashMap::new(),
        cache_creation_per_million_usd_by_tier: std::collections::HashMap::new(),
        cache_read_per_million_usd_by_tier: std::collections::HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn message_body(stream: bool) -> Vec<u8> {
    let mut body = json!({
        "model": MODEL,
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}]
    });
    if stream {
        body["stream"] = Value::Bool(true);
    }
    json_body(&body)
}

fn happy_message_body() -> Value {
    json!({
        "id": "msg_fake_000000000000000000000000",
        "type": "message",
        "role": "assistant",
        "model": MODEL,
        "content": [{"type": "text", "text": "fake anthropic fixture response HELLO"}],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {"input_tokens": 100, "output_tokens": 50}
    })
}

fn models_body() -> Value {
    let model = |id: &str| {
        json!({
            "id": id,
            "type": "model",
            "display_name": id,
            "created_at": "2026-05-20T00:00:00Z"
        })
    };
    json!({
        "type": "list",
        "data": [
            model("claude-fable-5"),
            model("claude-3-5-sonnet-20241022"),
            model("claude-3-5-haiku-20241022"),
            model("claude-3-opus-20240229")
        ],
        "first_id": "claude-fable-5",
        "last_id": "claude-3-opus-20240229",
        "has_more": false
    })
}

fn expected_sse_body() -> String {
    let events = [
        (
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": "msg_fake_000000000000000000000000",
                    "type": "message",
                    "role": "assistant",
                    "model": MODEL,
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {"input_tokens": 100, "output_tokens": 0}
                }
            }),
        ),
        (
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""}
            }),
        ),
        (
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "fake anthropic fixture response HELLO "}
            }),
        ),
        (
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 0}),
        ),
        (
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": null},
                "usage": {"input_tokens": 100, "output_tokens": 50}
            }),
        ),
        ("message_stop", json!({"type": "message_stop"})),
    ];
    let mut body = String::new();
    for (event, data) in events {
        body.push_str("event: ");
        body.push_str(event);
        body.push('\n');
        body.push_str("data: ");
        body.push_str(&data.to_string());
        body.push_str("\n\n");
    }
    body
}

fn json_body(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("JSON fixture serializes")
}

struct TestResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<TestResponse, Box<dyn std::error::Error>> {
    tokio::time::timeout(WAIT_TIMEOUT, raw_http(addr, method, path, headers, body))
        .await
        .map_err(|_| format!("HTTP request timed out after {WAIT_TIMEOUT:?}: {method} {path}"))??
        .pipe(Ok)
}

async fn raw_http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<TestResponse> {
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    let stream = TcpStream::connect(addr).await?;
    let (mut reader, mut writer) = stream.into_split();
    writer.write_all(request.as_bytes()).await?;
    writer.write_all(body).await?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await?;
    parse_raw_response(&bytes)
}

fn parse_raw_response(bytes: &[u8]) -> std::io::Result<TestResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing headers"))?;
    let head = std::str::from_utf8(&bytes[..header_end])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid status"))?;
    let mut headers = HeaderMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = http::header::HeaderName::from_bytes(name.trim().as_bytes())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            let value = http::HeaderValue::from_str(value.trim())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            headers.insert(name, value);
        }
    }
    let encoded_body = &bytes[header_end + 4..];
    let body = if headers
        .get(http::header::TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked(encoded_body)?
    } else {
        encoded_body.to_vec()
    };
    Ok(TestResponse {
        status,
        headers,
        body,
    })
}

fn decode_chunked(mut bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    loop {
        let line_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "chunk size"))?;
        let size_text = std::str::from_utf8(&bytes[..line_end])
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let size = usize::from_str_radix(size_text.trim(), 16)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        bytes = &bytes[line_end + 2..];
        if size == 0 {
            return Ok(decoded);
        }
        if bytes.len() < size + 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk body",
            ));
        }
        decoded.extend_from_slice(&bytes[..size]);
        bytes = &bytes[size + 2..];
    }
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}
