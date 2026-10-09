use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use http::{HeaderMap, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use cc_lb_aead::AeadService;
use cc_lb_config::{Config, StorageConfig};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_engine::DrainController;
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion, global_catalog};
use cc_lb_server::{BuildError, build_app, signal::SignalHandle};
use cc_lb_storage_api::{
    Limit as KeyLimit, LimitKind as KeyLimitKind, MetaStore, RequestEvent, RequestEventStore,
    principal::{
        Limit as PrincipalLimit, LimitKind as PrincipalLimitKind, PrincipalCreate, PrincipalKind,
        PrincipalStore,
    },
    upstream::{UpstreamCreate, UpstreamKind, UpstreamStore},
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use url::Url;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const MASTER_KEY_ENV: &str = "CC_LB_TERMINAL_OBS_MASTER_KEY";
const MASTER_KEY_HEX: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const MODEL: &str = "claude-3-5-sonnet-20241022";

#[tokio::test(flavor = "multi_thread")]
async fn body_too_large_after_auth_persists_one_row() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(happy_response()))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _key_id) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let big_body = vec![b'x'; 33 * 1024 * 1024];
    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                ("x-api-key", &plaintext_key),
            ],
            &big_body,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    // Authentication now runs before the body is read, so an oversized body
    // from a caller who presented a valid credential is rejected *after*
    // authentication and is attributable: it persists exactly one row.
    let rows = {
        let storage = sqlite_storage(&sqlite_path).await?;
        wait_for_request_event_count(storage.as_ref(), 1).await?
    };
    assert_eq!(rows[0].status, 413);
    assert_eq!(rows[0].error_code.as_deref(), Some("body_too_large"));
    // #849: the persisted row must carry the typed ingress diagnostic, not
    // just the coarse error_code — the cap value is part of the reason.
    assert_eq!(
        internal_errors_json(&rows[0]),
        json!([{
            "stage": "ingress",
            "kind": "invalid_input",
            "message": "request body exceeded limit of 33554432 bytes"
        }])
    );
    assert!(rows[0].upstream_error_type.is_none());
    assert!(rows[0].upstream_error_message.is_none());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_body_with_bad_credential_is_rejected_without_reading_body()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    // The project rule: authentication comes first, so nothing else happens.
    // This request is both unauthenticated AND oversized. Because the
    // credential is checked before the body is touched, the caller gets 401 —
    // not 413 — which is the observable proof that the 33 MiB was never
    // buffered on behalf of an anonymous caller.
    let big_body = vec![b'x'; 33 * 1024 * 1024];
    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                ("x-api-key", "sk-cclb-does-not-exist"),
            ],
            &big_body,
        )
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "an oversized body must not be read before the credential is checked"
    );

    let rows = {
        let storage = sqlite_storage(&sqlite_path).await?;
        wait_for_request_event_count(storage.as_ref(), 1).await?
    };
    assert_eq!(rows[0].status, 401);
    assert_eq!(rows[0].error_code.as_deref(), Some("authentication_failed"));
    // #849: auth rejection persists the typed authn diagnostic with the
    // exact BuiltinAuthError reason — "sk-cclb-does-not-exist" fails secret
    // parsing, so the persisted message is "invalid api key format".
    assert_eq!(
        internal_errors_json(&rows[0]),
        json!([{
            "stage": "authn",
            "kind": "invalid_input",
            "message": "invalid api key format"
        }])
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_authentication_failed() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (_key, _key_id) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                (
                    "x-api-key",
                    "sk-cclb-invalid_bogusbogusbogusbogusbogusbogusbogusbogusbogus",
                ),
            ],
            &sample_request_body(false),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("authentication_failed"));
    assert_eq!(row.status, 401);
    assert!(row.event_id.is_some());
    // #849: the authn stage/kind and the exact BuiltinAuthError display
    // string must be persisted — the bogus fixture key fails secret
    // parsing, so the message is "invalid api key format".
    assert_eq!(
        internal_errors_json(&row),
        json!([{
            "stage": "authn",
            "kind": "invalid_input",
            "message": "invalid api key format"
        }])
    );
    assert!(row.upstream_error_type.is_none());
    assert!(row.upstream_error_message.is_none());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_api_key_authentication_failed_persists_one_row()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (_key, _key_id) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    // A request with no credential still reaches the authentication attempt,
    // so its rejection is an observed auth failure — not pre-auth traffic —
    // and must persist exactly one row. This is the boundary that separates
    // "before authentication" (no row) from "authentication failed" (a row).
    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[("content-type", "application/json")],
            &sample_request_body(false),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let storage = sqlite_storage(&sqlite_path).await?;
    let rows = wait_for_request_event_count(storage.as_ref(), 1).await?;
    assert_eq!(rows[0].status, 401);
    assert_eq!(rows[0].error_code.as_deref(), Some("authentication_failed"));
    // #849: missing credential persists the exact authn reason.
    assert_eq!(
        internal_errors_json(&rows[0]),
        json!([{
            "stage": "authn",
            "kind": "invalid_input",
            "message": "missing x-api-key header"
        }])
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_4xx() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "type": "error",
            "error": {"type": "invalid_request_error", "message": "bad model"}
        })))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(row.status, 400);
    assert!(row.event_id.is_some());
    // #849: upstream diagnostics live in upstream_error_* — the typed
    // provider error replaces the old provider_error_observed metric.
    assert_eq!(
        row.upstream_error_type.as_deref(),
        Some("invalid_request_error")
    );
    assert_eq!(row.upstream_error_message.as_deref(), Some("bad model"));
    // Upstream failures are not cc-lb internal failures.
    assert!(row.internal_errors.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_5xx() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(529).set_body_json(json!({
            "type": "error",
            "error": {"type": "overloaded_error", "message": "overloaded"}
        })))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(
        response.status().is_server_error() || response.status() == StatusCode::from_u16(529)?,
        "expected 5xx, got {}",
        response.status()
    );

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_5xx"));
    // #849: typed upstream error replaces the provider_error_observed metric.
    assert_eq!(row.upstream_error_type.as_deref(), Some("overloaded_error"));
    assert_eq!(row.upstream_error_message.as_deref(), Some("overloaded"));
    assert!(row.internal_errors.is_empty());
    assert!(row.status >= 500);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_dispatch_failed() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let closed_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let closed_addr = closed_listener.local_addr()?;
    let closed_url = format!("http://{closed_addr}");
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, closed_url, "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;
    drop(closed_listener);

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(
        response.status().is_server_error(),
        "expected 5xx from dispatch failure, got {}",
        response.status()
    );

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_dispatch_failed"));
    // #849: dispatch failure persists the typed relay diagnostic — the
    // transport error chain is the message, kind is unavailable.
    let errors = internal_errors_json(&row);
    assert_eq!(errors[0]["stage"], "relay");
    assert_eq!(errors[0]["kind"], "unavailable");
    assert!(
        errors[0]["message"].as_str().is_some_and(|m| !m.is_empty()),
        "relay diagnostic must carry the transport error chain: {errors}"
    );
    assert!(row.upstream_error_type.is_none());
    assert!(row.upstream_error_message.is_none());
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_route_no_upstream_after_filter() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let bogus_upstream_id = uuid::Uuid::nil();
    let (plaintext_key, _) = seed_runtime_state_upstream_restricted(
        &sqlite_path,
        upstream.uri(),
        "u1",
        vec![bogus_upstream_id],
    )
    .await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(
        response.status().is_client_error() || response.status().is_server_error(),
        "expected non-2xx, got {}",
        response.status()
    );

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(
        row.error_code.as_deref(),
        Some("route_no_upstream_after_filter")
    );
    assert!(row.event_id.is_some());
    // #849: the router_filter stage diagnostic explains why no upstream was
    // eligible — previously this reason was discarded entirely.
    assert_eq!(
        internal_errors_json(&row),
        json!([{
            "stage": "router_filter",
            "kind": "unavailable",
            "message": "no upstream candidates remain after routing filters"
        }])
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_limit_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(happy_response()))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) =
        seed_runtime_state_tight_limit(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let first = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(first.status(), StatusCode::OK);

    let second = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);

    let row = wait_for_request_event_status(&sqlite_path, 429).await?;
    assert_eq!(row.error_code.as_deref(), Some("limit_rejected"));
    assert_eq!(row.status, 429);
    assert!(row.event_id.is_some());
    // #849: the limit rejection persists the typed router diagnostic — the
    // LimitDecision reason was previously discarded.
    let errors = internal_errors_json(&row);
    assert_eq!(errors[0]["stage"], "router");
    assert_eq!(errors[0]["kind"], "unavailable");
    assert!(
        errors[0]["message"].as_str().is_some_and(|m| !m.is_empty()),
        "limit rejection must carry the typed reason: {errors}"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_success_non_stream() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(happy_response()))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert!(
        row.error_code.is_none(),
        "error_code must be NULL on success"
    );
    assert_eq!(row.status, 200);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_success_stream() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = ChunkedSseMock::start(happy_sse_stream()).await?;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, true).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert!(
        row.error_code.is_none(),
        "SSE happy path must have NULL error_code; got {:?}, row={:?}",
        row.error_code,
        row,
    );
    assert_eq!(row.status, 200);
    assert!(row.event_id.is_some());

    server.shutdown().await;
    upstream.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_stream_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("content-type", "text/event-stream")
                .set_body_string(mid_stream_error_sse()),
        )
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, true).await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "upstream returned 200 before injecting mid-stream error"
    );

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(row.status, 200);
    assert!(row.event_id.is_some());
    // #849: the mid-stream provider error is persisted as typed upstream
    // diagnostics, not an internal error.
    assert_eq!(row.upstream_error_type.as_deref(), Some("overloaded_error"));
    assert_eq!(
        row.upstream_error_message.as_deref(),
        Some("stream aborted by upstream")
    );
    assert!(row.internal_errors.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_tower_timeout() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(happy_response())
                .set_delay(Duration::from_secs(5)),
        )
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let mut config = base_config(sqlite_path.clone(), litellm.uri());
    config.config.timeouts.upstream_total_secs = 1;
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("tower_timeout"));
    assert_eq!(row.status, 504);
    assert!(row.event_id.is_some(), "event_id must be populated");
    // #849: tower timeout persists the typed relay/timeout diagnostic.
    let errors = internal_errors_json(&row);
    assert_eq!(errors[0]["stage"], "relay");
    assert_eq!(errors[0]["kind"], "timeout");
    assert!(
        errors[0]["message"].as_str().is_some_and(|m| !m.is_empty()),
        "timeout diagnostic must carry a message: {errors}"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn route_not_found_before_auth_persists_no_row() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;

    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request("GET", &format!("{}/models", server.proxy_url), &[], &[])
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8_lossy(&response.body);
    assert!(
        body.contains("requested proxy path was not found"),
        "unexpected 404 body: {body}"
    );

    // A 404 router fallback never reaches authentication, so it must not
    // persist a request_events row — pre-auth traffic is unauthenticated
    // external input and logging it would let scanners amplify writes.
    // Give the async request-event writer a window to (incorrectly) persist
    // a row before asserting the table stayed empty.
    sleep(Duration::from_millis(500)).await;
    let storage = sqlite_storage(&sqlite_path).await?;
    let rows = all_request_events(storage.as_ref()).await?;
    assert!(
        rows.is_empty(),
        "pre-auth 404 must not persist request events, found {} row(s)",
        rows.len()
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn method_not_allowed_before_auth_persists_no_row() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;

    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "PUT",
            &format!("{}/v1/messages", server.proxy_url),
            &[("content-type", "application/json")],
            &sample_request_body(false),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let body = String::from_utf8_lossy(&response.body);
    assert!(
        body.contains("method is not allowed for this proxy path"),
        "unexpected 405 body: {body}"
    );

    // A 405 rejection never reaches authentication, so it must not persist a
    // request_events row.
    sleep(Duration::from_millis(500)).await;
    let storage = sqlite_storage(&sqlite_path).await?;
    let rows = all_request_events(storage.as_ref()).await?;
    assert!(
        rows.is_empty(),
        "pre-auth 405 must not persist request events, found {} row(s)",
        rows.len()
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_rejected_before_auth_persists_no_row() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _key_id) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;

    server.drain_controller.set_draining(true);

    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                ("x-api-key", &plaintext_key),
            ],
            &sample_request_body(false),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.body, b"draining");
    assert_eq!(
        response
            .headers
            .get(http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("60")
    );
    // The layer reorder that put request_id_middleware outermost means a
    // drain rejection now carries a request-id header it did not carry before.
    // Pin it so the change is deliberate rather than incidental.
    assert!(
        response
            .headers
            .get("request-id")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("req_server_")),
        "drain rejection must carry the assigned request id"
    );

    // A drain rejection never reaches authentication, so it must not persist
    // a request_events row.
    sleep(Duration::from_millis(500)).await;
    let storage = sqlite_storage(&sqlite_path).await?;
    let rows = all_request_events(storage.as_ref()).await?;
    assert!(
        rows.is_empty(),
        "pre-auth drain rejection must not persist request events, found {} row(s)",
        rows.len()
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn health_endpoints_persist_no_request_events() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;

    let client = TestClient::new(Duration::from_secs(10));
    assert_eq!(
        client
            .get_status(&format!("{}/healthz", server.proxy_url))
            .await?,
        StatusCode::OK
    );
    // /readyz only reports ready once the dynamic view marks an upstream
    // active, which can lag wait_ready's /healthz probe — poll briefly.
    let readyz_url = format!("{}/readyz", server.proxy_url);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if client.get_status(&readyz_url).await? == StatusCode::OK {
            break;
        }
        if Instant::now() >= deadline {
            return Err("/readyz never reported ready".into());
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Give the async request-event writer a window to (incorrectly) persist
    // rows for the health probes before asserting the table stayed empty.
    sleep(Duration::from_millis(500)).await;
    let storage = sqlite_storage(&sqlite_path).await?;
    let rows = all_request_events(storage.as_ref()).await?;
    assert!(
        rows.is_empty(),
        "health endpoints must not persist request events, found {} row(s)",
        rows.len()
    );

    server.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread")]
async fn terminal_expired_key_records_authn_reason() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    // Issue an expired key for the principal. The seeded key stays valid so
    // the server has a working view.
    let storage = sqlite_storage(&sqlite_path).await?;
    let key_store =
        KeyStore::new(Arc::clone(&storage) as Arc<dyn cc_lb_storage_api::ManagedKeyStore>);
    let (_expired_record, expired_plaintext) = key_store
        .create(
            "u1",
            CreateParams {
                label: "expired".to_owned(),
                description: None,
                expires_at_unix_secs: Some(1),
                limit_overrides: vec![],
            },
        )
        .await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let client = TestClient::new(Duration::from_secs(10));
    let expired = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                ("x-api-key", expired_plaintext.expose()),
            ],
            &sample_request_body(false),
        )
        .await?;
    assert_eq!(expired.status(), StatusCode::UNAUTHORIZED);
    let row = wait_for_request_event_status(&sqlite_path, 401).await?;
    assert_eq!(
        internal_errors_json(&row),
        json!([{
            "stage": "authn",
            "kind": "invalid_input",
            "message": "api key expired"
        }])
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_key_for_missing_principal_records_authn_reason()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    // Issue a key under a principal that does not exist: the credential
    // verifies, but the principal lookup fails after authentication.
    let storage = sqlite_storage(&sqlite_path).await?;
    let key_store =
        KeyStore::new(Arc::clone(&storage) as Arc<dyn cc_lb_storage_api::ManagedKeyStore>);
    let (_record, orphan_plaintext) = key_store
        .create(
            "ghost-principal",
            CreateParams {
                label: "orphan".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![],
            },
        )
        .await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                ("x-api-key", orphan_plaintext.expose()),
            ],
            &sample_request_body(false),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("authentication_failed"));
    assert_eq!(
        internal_errors_json(&row),
        json!([{
            "stage": "authn",
            "kind": "invalid_input",
            "message": "principal not found"
        }])
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_4xx_error_code_fallback() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // A non-Anthropic-shaped error body: `error.code` instead of `error.type`.
    // The persisted upstream_error_type must come from `code` when `type` is
    // absent — previously this field was dropped entirely.
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"code": "model_not_found", "message": "no such model"}
        })))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(row.upstream_error_type.as_deref(), Some("model_not_found"));
    assert_eq!(row.upstream_error_message.as_deref(), Some("no such model"));
    assert!(row.internal_errors.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_401_error_code_parsed_and_body_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // The API-key signer never retries on 401 (RetryDecision::Fail), so the
    // upstream response passes through untouched — but the persisted row
    // must still parse `error.code` into upstream_error_type.
    let upstream = MockServer::start().await;
    let error_body = json!({
        "error": {"code": "authentication_error", "message": "upstream key rejected"}
    });
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(401).set_body_json(error_body.clone()))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    // The client-visible body is the upstream's own — byte-identical.
    let body_json: Value = serde_json::from_slice(&response.body)?;
    assert_eq!(body_json, error_body);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        row.upstream_error_type.as_deref(),
        Some("authentication_error")
    );
    assert_eq!(
        row.upstream_error_message.as_deref(),
        Some("upstream key rejected")
    );
    assert!(row.internal_errors.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_4xx_streaming_request() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // A streaming request whose upstream rejects with a non-2xx status: the
    // error body is buffered and must still populate upstream_error_*.
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({
            "type": "error",
            "error": {"type": "rate_limit_error", "message": "slow down"}
        })))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, true).await?;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(row.upstream_error_type.as_deref(), Some("rate_limit_error"));
    assert_eq!(row.upstream_error_message.as_deref(), Some("slow down"));
    assert!(row.internal_errors.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_5xx_malformed_body() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // A 500 whose body is not JSON at all: upstream_error_type stays null and
    // the bounded raw body is preserved as the message.
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("<html>gateway exploded</html>"))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(response.status().is_server_error());

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_5xx"));
    assert!(row.upstream_error_type.is_none());
    assert_eq!(
        row.upstream_error_message.as_deref(),
        Some("<html>gateway exploded</html>")
    );
    assert!(row.internal_errors.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_refusal() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = ChunkedSseMock::start(refusal_sse_stream()).await?;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, true).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_refusal"));
    assert_eq!(row.status, 200);
    assert_eq!(row.upstream_error_type.as_deref(), Some("refusal"));
    assert!(row.upstream_error_message.is_some());
    assert!(row.internal_errors.is_empty());

    server.shutdown().await;
    upstream.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_success_body_with_error_key_not_misclassified()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // A 200 body that happens to contain an "error" key must NOT populate
    // upstream_error_* — the status gate is what prevents misclassification.
    let upstream = MockServer::start().await;
    let mut body = happy_response();
    body["error"] = json!({"type": "bogus", "message": "not a real error"});
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert!(row.error_code.is_none());
    assert_eq!(row.status, 200);
    assert!(row.upstream_error_type.is_none());
    assert!(row.upstream_error_message.is_none());
    assert!(row.internal_errors.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_dispatch_failed_dns() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // `.invalid` is guaranteed to never resolve (RFC 2606), so dispatch fails
    // at the DNS stage — the persisted row must record dns_ms.
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) =
        seed_runtime_state(&sqlite_path, "http://nonexistent.invalid".to_owned(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(response.status().is_server_error());

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_dispatch_failed"));
    let errors = internal_errors_json(&row);
    assert_eq!(errors[0]["stage"], "relay");
    assert_eq!(errors[0]["kind"], "unavailable");
    assert!(row.dns_ms.is_some(), "dns failure must record dns_ms");
    assert!(row.connect_ms.is_none());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_upstream_dispatch_failed_tls() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // Plain-HTTP mock behind an https:// URL: TCP connect succeeds, the TLS
    // handshake fails — connect_ms is recorded, dns_ms is not (literal IP).
    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let https_url = upstream.uri().replacen("http://", "https://", 1);
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, https_url, "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let response = send_messages(&server, &plaintext_key, false).await?;
    assert!(response.status().is_server_error());

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_dispatch_failed"));
    let errors = internal_errors_json(&row);
    assert_eq!(errors[0]["stage"], "relay");
    assert!(
        row.connect_ms.is_some(),
        "tls failure must record connect_ms"
    );
    assert!(row.dns_ms.is_none(), "literal IP must not record dns_ms");

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_bulkhead_full() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // One upstream slot + a slow upstream: the second request overflows the
    // bulkhead queue and is rejected with 503 overloaded_error.
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(happy_response())
                .set_delay(Duration::from_secs(5)),
        )
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let mut config = base_config(sqlite_path.clone(), litellm.uri());
    config.config.bulkhead.semaphore_per_upstream = 1;
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    let key = plaintext_key.clone();
    let proxy = server.proxy_url.clone();
    let first = tokio::spawn(async move {
        let client = TestClient::new(Duration::from_secs(15));
        client
            .request(
                "POST",
                &format!("{proxy}/v1/messages"),
                &[("content-type", "application/json"), ("x-api-key", &key)],
                &sample_request_body(false),
            )
            .await
    });
    // Give the first request a head start so it holds the only permit.
    sleep(Duration::from_millis(300)).await;
    let second = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);

    let row = wait_for_request_event_status(&sqlite_path, 503).await?;
    assert_eq!(row.error_code.as_deref(), Some("upstream_dispatch_failed"));
    let errors = internal_errors_json(&row);
    assert_eq!(errors[0]["stage"], "relay");
    assert_eq!(errors[0]["kind"], "unavailable");
    assert!(
        errors[0]["message"]
            .as_str()
            .is_some_and(|m| m.contains("bulkhead")),
        "bulkhead diagnostic must name the queue: {errors}"
    );

    let _ = first.await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_client_closed_mid_stream() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    // The upstream sends one SSE event then stalls; the client disconnects
    // mid-stream. The drop guard must classify this as client_closed_request
    // — a downstream cancellation, not an internal or upstream error.
    let upstream = ChunkedSseMock::start_stalled(stalled_sse_stream()).await?;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    // Raw client: read the response head + first chunk, then drop.
    let url = Url::parse(&format!("{}/v1/messages", server.proxy_url))?;
    let host = url.host_str().expect("host");
    let port = url.port_or_known_default().expect("port");
    let mut stream = TcpStream::connect((host, port)).await?;
    let body = sample_request_body(true);
    let request = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nx-api-key: {plaintext_key}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&body).await?;
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await?;
    assert!(n > 0, "expected response head before disconnect");
    drop(stream);

    let row = wait_for_request_event(&sqlite_path).await?;
    assert_eq!(row.error_code.as_deref(), Some("client_closed_request"));
    assert_eq!(row.status, 499);
    // Client cancellation is neither an internal failure nor an upstream
    // error — all three diagnostic fields stay empty.
    assert!(row.internal_errors.is_empty());
    assert!(row.upstream_error_type.is_none());
    assert!(row.upstream_error_message.is_none());

    server.shutdown().await;
    upstream.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_connection_reused() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(happy_response()))
        .mount(&upstream)
        .await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    let (plaintext_key, _) = seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let config = base_config(sqlite_path.clone(), litellm.uri());
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    // Two sequential requests to the same upstream: the second must reuse the
    // pooled connection — connection_reused=true, no fresh connect_ms.
    let first = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(first.status(), StatusCode::OK);
    let second = send_messages(&server, &plaintext_key, false).await?;
    assert_eq!(second.status(), StatusCode::OK);

    // Both rows are status 200 and the writer commits asynchronously, so the
    // latest row alone may still be the first request's. Wait for both rows
    // and select the second request by its start time.
    let storage = sqlite_storage(&sqlite_path).await?;
    let rows = wait_for_request_event_count(&storage, 2).await?;
    let row = rows
        .into_iter()
        .max_by_key(|row| row.ts_ms)
        .expect("two request_event rows");
    assert_eq!(row.status, 200);
    assert_eq!(row.connection_reused, Some(true));
    assert!(row.connect_ms.is_none());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_event_detail_propagates_internal_errors() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    ensure_env();

    let upstream = MockServer::start().await;
    let litellm = start_price_mock().await;

    let sqlite_path = dir.path().join("term-obs.sqlite");
    seed_runtime_state(&sqlite_path, upstream.uri(), "u1").await?;

    let mut config = base_config(sqlite_path.clone(), litellm.uri());
    config.config.admin.auth.providers = vec![cc_lb_config::AdminAuthProviderConfig::StaticToken {
        id: "test-admin".to_owned(),
        token_env: "CC_LB_TERMINAL_OBS_ADMIN_TOKEN".to_owned(),
    }];
    unsafe {
        std::env::set_var("CC_LB_TERMINAL_OBS_ADMIN_TOKEN", "test-admin-token");
    }
    let server = StartedServer::start(config).await?;
    wait_for_price_catalog().await?;

    // Trigger an auth failure so a row with internal_errors exists.
    let client = TestClient::new(Duration::from_secs(10));
    let response = client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[
                ("content-type", "application/json"),
                (
                    "x-api-key",
                    "sk-cclb-invalid_bogusbogusbogusbogusbogusbogusbogusbogusbogus",
                ),
            ],
            &sample_request_body(false),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let row = wait_for_request_event(&sqlite_path).await?;
    let event_id = row.event_id.clone().expect("event_id persisted");

    // The admin detail endpoint must surface the same typed diagnostics.
    let detail = client
        .request(
            "GET",
            &format!("{}/admin/v1/events/detail/{event_id}", server.admin_url),
            &[("authorization", "Bearer test-admin-token")],
            &[],
        )
        .await?;
    assert_eq!(detail.status(), StatusCode::OK);
    let detail_json: Value = serde_json::from_slice(&detail.body)?;
    assert_eq!(
        detail_json["internal_errors"],
        json!([{
            "stage": "authn",
            "kind": "invalid_input",
            "message": "invalid api key format"
        }])
    );

    server.shutdown().await;
    Ok(())
}

fn refusal_sse_stream() -> String {
    concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-20241022\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":8,\"output_tokens\":1}}}\n",
        "\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"refusal\"},\"usage\":{\"output_tokens\":4}}\n",
        "\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n",
        "\n",
    )
    .to_owned()
}

fn stalled_sse_stream() -> String {
    concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-20241022\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":8,\"output_tokens\":1}}}\n",
        "\n",
    )
    .to_owned()
}

fn happy_sse_stream() -> String {
    concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-20241022\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":8,\"output_tokens\":1}}}\n",
        "\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":12}}\n",
        "\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n",
        "\n",
    )
    .to_owned()
}

fn mid_stream_error_sse() -> String {
    concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-20241022\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":8,\"output_tokens\":1}}}\n",
        "\n",
        "event: error\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"stream aborted by upstream\"}}\n",
        "\n",
    )
    .to_owned()
}

fn sample_request_body(stream: bool) -> Vec<u8> {
    let mut body = serde_json::Map::new();
    body.insert("model".to_owned(), Value::String(MODEL.to_owned()));
    body.insert("max_tokens".to_owned(), json!(64));
    body.insert(
        "messages".to_owned(),
        json!([{"role": "user", "content": "hi"}]),
    );
    if stream {
        body.insert("stream".to_owned(), Value::Bool(true));
    }
    serde_json::to_vec(&Value::Object(body)).expect("sample body serializes")
}

async fn send_messages(
    server: &StartedServer,
    key: &str,
    stream: bool,
) -> std::io::Result<TestResponse> {
    let client = TestClient::new(Duration::from_secs(15));
    client
        .request(
            "POST",
            &format!("{}/v1/messages", server.proxy_url),
            &[("content-type", "application/json"), ("x-api-key", key)],
            &sample_request_body(stream),
        )
        .await
}

fn ensure_env() {
    unsafe {
        std::env::set_var(MASTER_KEY_ENV, MASTER_KEY_HEX);
    }
}

async fn start_price_mock() -> MockServer {
    let litellm = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/prices"))
        .respond_with(ResponseTemplate::new(200).set_body_json(price_catalog_fixture()))
        .mount(&litellm)
        .await;
    litellm
}

fn happy_response() -> Value {
    json!({
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "model": MODEL,
        "content": [{"type": "text", "text": "ok"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 5, "output_tokens": 3}
    })
}

struct ReservedConfig {
    config: Config,
    listener_reservations: [std::net::TcpListener; 2],
}

struct StartedServer {
    proxy_url: String,
    admin_url: String,
    signal: SignalHandle,
    drain_controller: DrainController,
    task: Option<JoinHandle<Result<(), BuildError>>>,
}

impl StartedServer {
    async fn start(reserved: ReservedConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let ReservedConfig {
            config,
            listener_reservations,
        } = reserved;
        let proxy_addr = config.listener.proxy_addr;
        let admin_addr = config.listener.admin_addr;
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let app = build_app(config, clock).await?;
        let signal = app.signal_handle();
        let drain_controller = app.drain_controller();
        // Keep all selected ports reserved while build_app performs its async setup.
        // App::start owns the real listeners immediately after this handoff.
        drop(listener_reservations);
        let task = tokio::spawn(async move { app.start().await });
        let mut server = Self {
            proxy_url: format!("http://{proxy_addr}"),
            admin_url: format!("http://{admin_addr}"),
            signal,
            drain_controller,
            task: Some(task),
        };
        server.wait_ready().await?;
        Ok(server)
    }

    async fn wait_ready(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let client = TestClient::new(Duration::from_secs(1));
        let ready_secs = std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(60);
        let deadline = Instant::now() + Duration::from_secs(ready_secs);
        let proxy_url = self.proxy_url.clone();
        let admin_url = self.admin_url.clone();
        let task = self.task.as_mut().expect("server task must exist");
        loop {
            tokio::select! {
                result = &mut *task => {
                    let message = match result {
                        Ok(Ok(())) => "server exited before becoming ready".to_owned(),
                        Ok(Err(error)) => format!("server failed before becoming ready: {error}"),
                        Err(error) => format!("server task failed before becoming ready: {error}"),
                    };
                    return Err(std::io::Error::other(message).into());
                }
                (proxy_ok, admin_ok) = async {
                    let proxy_ok = client
                        .get_status(&format!("{proxy_url}/healthz"))
                        .await
                        .is_ok_and(|status| status == StatusCode::OK);
                    let admin_ok = client
                        .get_status(&format!("{admin_url}/admin/health"))
                        .await
                        .is_ok_and(|status| status == StatusCode::OK);
                    (proxy_ok, admin_ok)
                } => {
                    if proxy_ok && admin_ok {
                        return Ok(());
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err("server did not become ready".into());
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn shutdown(mut self) {
        self.signal.start_shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for StartedServer {
    fn drop(&mut self) {
        self.signal.start_shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct TestClient {
    timeout: Duration,
}

struct TestResponse {
    status: StatusCode,
    #[allow(dead_code)]
    headers: HeaderMap,
    #[allow(dead_code)]
    body: Vec<u8>,
}

impl TestClient {
    fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    async fn get_status(&self, url: &str) -> std::io::Result<StatusCode> {
        self.request("GET", url, &[], &[])
            .await
            .map(|response| response.status)
    }

    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> std::io::Result<TestResponse> {
        tokio::time::timeout(self.timeout, raw_http(method, url, headers, body))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "request timed out"))?
    }
}

impl TestResponse {
    fn status(&self) -> StatusCode {
        self.status
    }
}

async fn raw_http(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<TestResponse> {
    let url = Url::parse(url).expect("test url");
    let host = url.host_str().expect("test url host");
    let port = url.port_or_known_default().expect("test url port");
    let mut target = url.path().to_owned();
    if let Some(query) = url.query() {
        target.push('?');
        target.push_str(query);
    }
    let authority = if url.port().is_some() {
        format!("{host}:{port}")
    } else {
        host.to_owned()
    };
    let mut request = format!(
        "{method} {target} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    let stream = TcpStream::connect((host, port)).await?;
    let (mut reader, mut writer) = stream.into_split();
    writer.write_all(request.as_bytes()).await?;
    let mut bytes = Vec::new();
    let (write_result, read_result) =
        tokio::join!(writer.write_all(body), reader.read_to_end(&mut bytes),);
    match read_result {
        Ok(_) => {}
        Err(error)
            if is_early_close_error(&error)
                && bytes.windows(4).any(|window| window == b"\r\n\r\n") => {}
        Err(error) => return Err(error),
    }
    let response = parse_raw_response(&bytes)?;

    match write_result {
        Ok(()) => Ok(response),
        Err(error) if is_early_close_error(&error) => Ok(response),
        Err(error) => Err(error),
    }
}

fn is_early_close_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
    )
}

fn parse_raw_response(bytes: &[u8]) -> std::io::Result<TestResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing headers"))?;
    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing status"))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
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
    let body = bytes[header_end + 4..].to_vec();
    Ok(TestResponse {
        status,
        headers,
        body,
    })
}

async fn wait_for_request_event(
    sqlite_path: &Path,
) -> Result<RequestEvent, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(row) = query_latest_request_event(sqlite_path).await? {
            return Ok(row);
        }
        if Instant::now() >= deadline {
            return Err("no request_event row was persisted within 5s".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

// Multi-request tests must poll until the row with the *expected* status
// is the latest one — the async request_event writer commits after the
// downstream response returns, so a naive `wait_for_request_event` right
// after the second request can race and pick up the FIRST request's row.
// Callers that only ever fire one request should keep using
// `wait_for_request_event`.
async fn wait_for_request_event_status(
    sqlite_path: &Path,
    expected_status: u16,
) -> Result<RequestEvent, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_seen: Option<RequestEvent> = None;
    loop {
        if let Some(row) = query_latest_request_event(sqlite_path).await?
            && row.status == expected_status
        {
            return Ok(row);
        } else if let Some(row) = query_latest_request_event(sqlite_path).await? {
            last_seen = Some(row);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "no request_event row with status={expected_status} within 10s (last_seen={last_seen:?})",
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn query_latest_request_event(
    sqlite_path: &Path,
) -> Result<Option<RequestEvent>, Box<dyn std::error::Error>> {
    use sqlx::Row;
    use sqlx::sqlite::SqlitePoolOptions;

    let database_url = format!("sqlite://{}", sqlite_path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    let row_opt = sqlx::query("SELECT payload FROM request_events_v1 ORDER BY id DESC LIMIT 1")
        .fetch_optional(&pool)
        .await?;
    pool.close().await;

    let Some(row) = row_opt else {
        return Ok(None);
    };
    let payload: String = row.try_get("payload")?;
    Ok(Some(serde_json::from_str(&payload)?))
}

/// Serialize `internal_errors` for assertions. `RequestEvent` stores typed
/// `InternalError` values; comparing against `serde_json::json!` literals
/// keeps these tests free of a `cc-lb-domain` dependency.
fn internal_errors_json(row: &RequestEvent) -> Value {
    serde_json::to_value(&row.internal_errors).expect("internal_errors serializes")
}

async fn all_request_events(
    storage: &SqliteStorage,
) -> Result<Vec<RequestEvent>, Box<dyn std::error::Error>> {
    let cursor = storage.current_request_event_cursor().await?;
    Ok(storage
        .query_request_events_between_cursors(
            0,
            cursor,
            500,
            &cc_lb_storage_api::RequestEventStreamFilters::default(),
        )
        .await?
        .into_iter()
        .map(|(_, event)| event)
        .collect())
}

/// Poll until at least `expected` request-event rows exist, then settle and
/// assert the count is *exactly* `expected` — a double-finalize regression
/// that writes two rows for one request must fail this.
async fn wait_for_request_event_count(
    storage: &SqliteStorage,
    expected: usize,
) -> Result<Vec<RequestEvent>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = all_request_events(storage).await?;
        if rows.len() >= expected {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "expected {expected} request_event row(s), found {}",
                rows.len()
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
    sleep(Duration::from_millis(300)).await;
    let rows = all_request_events(storage).await?;
    assert_eq!(
        rows.len(),
        expected,
        "request must persist exactly {expected} row(s)"
    );
    Ok(rows)
}

async fn wait_for_price_catalog() -> Result<(), Box<dyn std::error::Error>> {
    seed_price_catalog();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if global_catalog().lookup(MODEL, None).is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("price catalog was not loaded".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn seed_price_catalog() {
    let mut models = std::collections::HashMap::new();
    models.insert(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            by_tier: Default::default(),
        },
    );
    global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "test-fixture-hash".to_owned(),
        fetched_at_ms: now_secs() * 1000,
        models,
        raw_json: serde_json::to_vec(&price_catalog_fixture()).expect("price fixture serializes"),
        cache_creation_per_million_usd: std::collections::HashMap::new(),
        cache_read_per_million_usd: std::collections::HashMap::new(),
        cache_creation_per_million_usd_by_tier: std::collections::HashMap::new(),
        cache_read_per_million_usd_by_tier: std::collections::HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn now_secs() -> u64 {
    use cc_lb_clock::Clock as _;
    let clock = cc_lb_clock::SystemClock;
    clock
        .now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn price_catalog_fixture() -> Value {
    json!({
        MODEL: {
            "input_cost_per_token": 0.000003,
            "output_cost_per_token": 0.000015,
            "mode": "chat",
            "max_tokens": 8192
        }
    })
}

async fn seed_runtime_state(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    seed_runtime_state_full(
        sqlite_path,
        upstream_url,
        principal_name,
        vec!["claude-3-5-sonnet-*".to_owned()],
        default_limits(),
    )
    .await
}

async fn seed_runtime_state_upstream_restricted(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
    allowed_upstreams: Vec<cc_lb_storage_api::UpstreamRecordId>,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    seed_runtime_state_full_v2(
        sqlite_path,
        upstream_url,
        principal_name,
        vec!["claude-3-5-sonnet-*".to_owned()],
        allowed_upstreams,
        default_limits(),
    )
    .await
}

async fn seed_runtime_state_tight_limit(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let tight = vec![
        PrincipalLimit {
            kind: PrincipalLimitKind::CostUsd,
            window_secs: 60 * 60,
            cap_micros: 1_000_000,
        },
        PrincipalLimit {
            kind: PrincipalLimitKind::Requests,
            window_secs: 60 * 60,
            cap_micros: 1,
        },
        PrincipalLimit {
            kind: PrincipalLimitKind::TotalTokens,
            window_secs: 60 * 60,
            cap_micros: 1_000_000,
        },
    ];
    seed_runtime_state_full(
        sqlite_path,
        upstream_url,
        principal_name,
        vec!["claude-3-5-sonnet-*".to_owned()],
        tight,
    )
    .await
}

fn default_limits() -> Vec<PrincipalLimit> {
    vec![
        PrincipalLimit {
            kind: PrincipalLimitKind::CostUsd,
            window_secs: 60 * 60,
            cap_micros: 1_000_000,
        },
        PrincipalLimit {
            kind: PrincipalLimitKind::Requests,
            window_secs: 60 * 60,
            cap_micros: 1_000,
        },
        PrincipalLimit {
            kind: PrincipalLimitKind::TotalTokens,
            window_secs: 60 * 60,
            cap_micros: 1_000_000,
        },
    ]
}

async fn seed_runtime_state_full(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
    allowed_models: Vec<String>,
    default_limits: Vec<PrincipalLimit>,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    seed_runtime_state_full_v2(
        sqlite_path,
        upstream_url,
        principal_name,
        allowed_models,
        vec![],
        default_limits,
    )
    .await
}

async fn seed_runtime_state_full_v2(
    sqlite_path: &Path,
    upstream_url: String,
    principal_name: &str,
    allowed_models: Vec<String>,
    allowed_upstreams: Vec<cc_lb_storage_api::UpstreamRecordId>,
    default_limits: Vec<PrincipalLimit>,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let storage = sqlite_storage(sqlite_path).await?;
    let created = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "anthropic-mock".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse(&upstream_url)?),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await?;
    // Must match the 32 bytes decoded from MASTER_KEY_HEX, which the server
    // loads via config.aead.key_env.
    let aead = AeadService::from_master_key([0x22; 32]);
    let ciphertext = aead.encrypt(b"sk-ant-fixture-secret", created.id.as_bytes())?;
    UpstreamStore::update_api_key_secret(storage.as_ref(), created.id, Some(ciphertext)).await?;
    PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: principal_name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models,
            allowed_upstreams,
            default_limits,
            cache_keepalive: None,
        },
        now_secs(),
    )
    .await?;
    let (_record, plaintext) = KeyStore::new(storage)
        .create(
            principal_name,
            CreateParams {
                label: "prod".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![KeyLimit {
                    kind: KeyLimitKind::CostUsd,
                    window_secs: 60 * 60,
                    cap_micros: 1_000_000,
                }],
            },
        )
        .await?;
    let (key_id, _) = cc_lb_control::api_keys::secret::parse(plaintext.expose())?;
    Ok((plaintext.expose().to_owned(), key_id))
}

fn base_config(sqlite_path: std::path::PathBuf, litellm_url: String) -> ReservedConfig {
    let proxy_reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve proxy port");
    let admin_reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve admin port");
    let metrics_addr = free_addr();
    let mut config = Config::default();
    config.listener.proxy_addr = proxy_reservation.local_addr().expect("proxy addr");
    config.listener.admin_addr = admin_reservation.local_addr().expect("admin addr");
    config.listener.metrics_addr = metrics_addr;
    config.timeouts.upstream_total_secs = 10;
    config.storage = StorageConfig::Sqlite { path: sqlite_path };
    config.aead.key_env = MASTER_KEY_ENV.to_owned();
    config.price_catalog.url = format!("{litellm_url}/prices");
    config.price_catalog.cache_path = tempfile::tempdir()
        .expect("price cache tempdir")
        .keep()
        .join("prices.json");
    ReservedConfig {
        config,
        listener_reservations: [proxy_reservation, admin_reservation],
    }
}
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free addr")
}

async fn sqlite_storage(path: &Path) -> Result<Arc<SqliteStorage>, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock)).await?;
    storage.initialize().await?;
    Ok(Arc::new(storage))
}

/// TCP-level mock that reproduces Anthropic's chunked SSE streaming shape.
/// `wiremock::set_body_raw` collapses this into a single Content-Length blob
/// which races with cc-lb's async_stream tail `finish()` — see PR #241.
struct ChunkedSseMock {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl ChunkedSseMock {
    async fn start(sse_body: String) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(chunked_sse_accept_loop(listener, shutdown_rx, sse_body));
        Ok(Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }
    /// Variant whose handler writes the SSE head and the given frames, then
    /// holds the connection open without completing the stream — used to
    /// exercise downstream disconnect mid-stream.
    async fn start_stalled(sse_body: String) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(stalled_sse_accept_loop(listener, shutdown_rx, sse_body));
        Ok(Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }

    fn uri(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ChunkedSseMock {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn chunked_sse_accept_loop(
    listener: TcpListener,
    mut shutdown: oneshot::Receiver<()>,
    sse_body: String,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            accept = listener.accept() => {
                let Ok((socket, _)) = accept else { return };
                let body = sse_body.clone();
                tokio::spawn(chunked_sse_handle(socket, body));
            }
        }
    }
}

async fn chunked_sse_handle(mut socket: tokio::net::TcpStream, sse_body: String) {
    if drain_http_request(&mut socket).await.is_err() {
        return;
    }
    let head = "HTTP/1.1 200 OK\r\n\
                Content-Type: text/event-stream\r\n\
                Transfer-Encoding: chunked\r\n\
                Connection: close\r\n\
                \r\n";
    if socket.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    for frame in sse_body.split_inclusive("\n\n") {
        let framed = format!("{:x}\r\n{}\r\n", frame.len(), frame);
        if socket.write_all(framed.as_bytes()).await.is_err() {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    let _ = socket.write_all(b"0\r\n\r\n").await;
    let _ = socket.shutdown().await;
}
async fn stalled_sse_accept_loop(
    listener: TcpListener,
    mut shutdown: oneshot::Receiver<()>,
    sse_body: String,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            accept = listener.accept() => {
                let Ok((socket, _)) = accept else { return };
                let body = sse_body.clone();
                tokio::spawn(stalled_sse_handle(socket, body));
            }
        }
    }
}

/// Writes the SSE head and the given frames, then parks the connection: the
/// stream never completes, so a downstream disconnect is the only way the
/// request resolves.
async fn stalled_sse_handle(mut socket: tokio::net::TcpStream, sse_body: String) {
    if drain_http_request(&mut socket).await.is_err() {
        return;
    }
    let head = "HTTP/1.1 200 OK\r\n\
                Content-Type: text/event-stream\r\n\
                Transfer-Encoding: chunked\r\n\
                Connection: close\r\n\
                \r\n";
    if socket.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    for frame in sse_body.split_inclusive("\n\n") {
        let framed = format!("{:x}\r\n{}\r\n", frame.len(), frame);
        if socket.write_all(framed.as_bytes()).await.is_err() {
            return;
        }
    }
    // Hold the connection open; the task is abandoned when the test runtime
    // shuts down.
    sleep(Duration::from_secs(300)).await;
}

async fn drain_http_request(socket: &mut tokio::net::TcpStream) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    let mut acc: Vec<u8> = Vec::new();
    let mut content_length: Option<usize> = None;
    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        acc.extend_from_slice(&buf[..n]);
        if let Some(header_end) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
            if content_length.is_none() {
                let head = std::str::from_utf8(&acc[..header_end]).unwrap_or_default();
                for line in head.split("\r\n") {
                    if let Some(rest) = strip_header_prefix(line, "content-length") {
                        content_length = rest.trim().parse::<usize>().ok();
                        break;
                    }
                }
            }
            let body_start = header_end + 4;
            let body_end = body_start + content_length.unwrap_or(0);
            if acc.len() >= body_end {
                return Ok(());
            }
        }
    }
}

fn strip_header_prefix<'a>(line: &'a str, name_lower: &str) -> Option<&'a str> {
    let colon = line.find(':')?;
    let (name, rest) = line.split_at(colon);
    if name.trim().eq_ignore_ascii_case(name_lower) {
        Some(&rest[1..])
    } else {
        None
    }
}
