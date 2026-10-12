//! Secret hygiene on the API-key proxy path: provider text that echoes the
//! upstream key is masked before any stored sink, the relayed body stays
//! untouched, and neither logs nor metrics carry the key or the workspace.

use std::io::Write;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use fake_anthropic::ScriptedMessageResponse;
use serde_json::{Value, json};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt;

use crate::api_key_proxy::{KEY, MESSAGE_BODY, ProxyHarness, stored_request_events};

const WORKSPACE: &str = "wrkspc_hygiene";
/// Request-log store cap on upstream error text (engine constant).
const STORE_CAP: usize = 8 * 1024;

/// The secret body of `KEY`: everything after the public family prefix that
/// a `key_hint` may show.
fn secret_body() -> &'static str {
    KEY.strip_prefix("sk-ant-api03-").expect("api03 key")
}

/// A sink is clean when no 12-byte window of the secret body appears in it.
fn assert_clean(sink: &str, label: &str) {
    for window in secret_body().as_bytes().windows(12) {
        let window = std::str::from_utf8(window).expect("ascii key");
        assert!(
            !sink.contains(window),
            "{label} keeps key fragment {window:?}"
        );
    }
}

/// Every byte the SQLite store wrote, including the WAL.
fn database_bytes(harness: &ProxyHarness) -> String {
    let mut bytes = Vec::new();
    for suffix in ["", "-wal"] {
        let path = format!(
            "{}{suffix}",
            harness.storage_path().to_str().expect("utf-8 storage path")
        );
        if let Ok(file) = std::fs::read(&path) {
            bytes.extend_from_slice(&file);
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// SEC-05: a 401 or 400 that echoes the key is relayed to the client
/// unchanged, while the request event and the database hold only the
/// masked text.
#[tokio::test]
async fn sec_05_provider_echo_is_masked_in_stored_sinks_only() {
    for status in [StatusCode::UNAUTHORIZED, StatusCode::BAD_REQUEST] {
        let harness = ProxyHarness::new().await;
        let upstream = harness
            .api_key_upstream("key", Some(WORKSPACE), None, harness.fake_url.clone())
            .await;
        let echoed = format!("invalid x-api-key: {KEY}");
        harness.script.push_response(ScriptedMessageResponse::error(
            status,
            "authentication_error",
            &echoed,
        ));
        let mut proxy = harness
            .proxy(&[&upstream])
            .await
            .recording(harness.storage.clone());

        let (relayed_status, body) = proxy.send(MESSAGE_BODY, &[]).await;

        assert_eq!(relayed_status, status);
        let relayed: Value = serde_json::from_slice(&body).expect("relayed json");
        assert_eq!(relayed["error"]["message"], echoed.as_str(), "{status}");
        let events = stored_request_events(&harness.storage).await;
        let [event] = events.as_slice() else {
            panic!("{status}: one request event expected: {events:?}");
        };
        assert_eq!(
            event.upstream_error_message.as_deref(),
            Some("invalid x-api-key: [REDACTED]"),
            "{status}"
        );
        assert_clean(
            &serde_json::to_string(&events).expect("events serialize"),
            &format!("{status} request event"),
        );
        assert_clean(&database_bytes(&harness), &format!("{status} database"));
    }
}

/// SEC-05: success responses are never rewritten, even when the model's
/// text happens to contain the key.
#[tokio::test]
async fn sec_05_success_body_is_relayed_byte_identical() {
    let harness = ProxyHarness::new().await;
    let upstream = harness
        .api_key_upstream("key", None, None, harness.fake_url.clone())
        .await;
    let mut ok = ScriptedMessageResponse::ok();
    ok.body["content"] = json!([{"type": "text", "text": format!("the key is {KEY}")}]);
    let expected = serde_json::to_vec(&ok.body).expect("success body");
    harness.script.push_response(ok);
    let mut proxy = harness.proxy(&[&upstream]).await;

    let (status, body) = proxy.send(MESSAGE_BODY, &[]).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), expected.as_slice());
}

/// SEC-13: a non-JSON error body with the key at the start, middle, end,
/// or straddling the store cap leaves no key fragment in the stored text,
/// which stays within the cap.
#[tokio::test]
async fn sec_13_key_straddling_store_cap_is_masked_before_truncation() {
    let marker = "\n...[truncated]";
    for offset in [
        0,
        STORE_CAP / 2,
        STORE_CAP - marker.len() - 30,
        STORE_CAP - 2,
    ] {
        let harness = ProxyHarness::new().await;
        let upstream = harness
            .api_key_upstream("key", None, None, harness.fake_url.clone())
            .await;
        let mut text = "x".repeat(offset);
        text.push_str(KEY);
        text.push_str(&"y".repeat(STORE_CAP));
        let mut response =
            ScriptedMessageResponse::error(StatusCode::BAD_REQUEST, "unused", "unused");
        // A bare JSON string is not an Anthropic error object, so the stored
        // message is the bounded raw body.
        response.body = Value::String(text);
        harness.script.push_response(response);
        let mut proxy = harness
            .proxy(&[&upstream])
            .await
            .recording(harness.storage.clone());

        let (status, _body) = proxy.send(MESSAGE_BODY, &[]).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "offset {offset}");
        let events = stored_request_events(&harness.storage).await;
        let [event] = events.as_slice() else {
            panic!("offset {offset}: one request event expected: {events:?}");
        };
        let stored = event
            .upstream_error_message
            .as_deref()
            .expect("raw error body stored");
        assert!(stored.len() <= STORE_CAP, "offset {offset}");
        assert!(stored.ends_with(marker), "offset {offset}");
        assert_clean(stored, &format!("offset {offset} stored message"));
        assert_clean(
            &database_bytes(&harness),
            &format!("offset {offset} database"),
        );
    }
}

/// Collects every formatted log line written while installed.
#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl LogBuffer {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer lock")).into_owned()
    }
}

/// SEC-02: at TRACE, the proxy's events and span fields carry neither the key
/// nor the managed key, on success and on an error that echoes the key.
#[tokio::test(flavor = "current_thread")]
async fn sec_02_trace_logs_never_carry_the_key() {
    let logs = LogBuffer::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
            .with_writer(move || writer.clone())
            .with_filter(
                Targets::new()
                    .with_default(LevelFilter::TRACE)
                    .with_target("fake_anthropic", LevelFilter::OFF),
            ),
    );
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);
    let harness = ProxyHarness::new().await;
    let upstream = harness
        .api_key_upstream("key", Some(WORKSPACE), None, harness.fake_url.clone())
        .await;
    harness.script.push_response(ScriptedMessageResponse::error(
        StatusCode::BAD_REQUEST,
        "invalid_request_error",
        &format!("echo {KEY}"),
    ));
    let mut proxy = harness
        .proxy(&[&upstream])
        .await
        .recording(harness.storage.clone());
    let managed_key = proxy.managed_key.clone();
    // Callsites another test thread registered before this scoped subscriber
    // existed may have cached "disabled"; recompute them against it.
    tracing::callsite::rebuild_interest_cache();

    let (error_status, _body) = proxy.send(MESSAGE_BODY, &[]).await;
    let (ok_status, _body) = proxy.send(MESSAGE_BODY, &[]).await;

    assert_eq!(error_status, StatusCode::BAD_REQUEST);
    assert_eq!(ok_status, StatusCode::OK);
    let text = logs.text();
    assert!(
        text.contains("about to dispatch signed request"),
        "trace capture is live"
    );
    assert_clean(&text, "trace logs");
    assert!(
        !text.contains(&managed_key),
        "trace logs carry the managed key"
    );
}

/// SEC-07: after proxied success and error traffic, no metric label carries
/// the key, its hint, or the workspace id.
#[tokio::test]
async fn sec_07_metric_labels_never_carry_key_or_workspace() {
    let metrics = crate::common::install_prometheus();
    let harness = ProxyHarness::new().await;
    let upstream = harness
        .api_key_upstream("key", Some(WORKSPACE), None, harness.fake_url.clone())
        .await;
    harness.script.push_response(ScriptedMessageResponse::error(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        &format!("invalid x-api-key: {KEY}"),
    ));
    let mut proxy = harness
        .proxy(&[&upstream])
        .await
        .recording(harness.storage.clone());

    let (error_status, _body) = proxy.send(MESSAGE_BODY, &[]).await;
    let (ok_status, _body) = proxy.send(MESSAGE_BODY, &[]).await;

    assert_eq!(error_status, StatusCode::UNAUTHORIZED);
    assert_eq!(ok_status, StatusCode::OK);
    let rendered = metrics.render();
    assert!(!rendered.is_empty(), "metrics are recorded");
    assert_clean(&rendered, "metrics");
    assert!(
        !rendered.contains(WORKSPACE),
        "metrics carry the workspace id"
    );
    assert!(
        !rendered.contains("sk-ant-api03-"),
        "metrics carry a key hint"
    );
}
