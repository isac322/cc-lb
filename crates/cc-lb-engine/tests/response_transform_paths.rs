use crate::common;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{LifecycleBusReceiver, RequestEventBus};
use cc_lb_domain::{Principal, PrincipalKind, TerminalStrategy, Upstream};
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalRoutingArtifacts, PrincipalView,
    RouterPipelineCache, ShapePluginCache,
};
use cc_lb_engine::{
    DispatchError, DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig,
    UpstreamDispatch,
};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_observability::ObserveEvent;
use cc_lb_storage_api::principal::PrincipalRecord;
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{BackendKind, MetaStore, RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, ResponseTransformHook,
    ShapedRequest, ShapedRequestBuilder, SignedRequest, SseEvent, SseEventTransformHook,
    TransformResponseRequest, TransformResponseResult, TransformSseEventRequest,
    TransformSseEventResult, UpstreamDialect,
};
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use url::Url;
use uuid::Uuid;

use common::{
    RecordingHook, TestAuthn, TestLifecycleBus, TestRouter, TestState, collect_body,
    messages_request,
};

#[tokio::test]
async fn buffered_transform_rewrites_tool_name_and_sanitizes_headers() {
    let transform = Arc::new(BufferedToolNameTransform::default());
    let lifecycle = lifecycle_with_transforms(Some(transform.clone()), None, buffered_dispatch());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles buffered response");
    let (_status, headers, body) = collect_body(response).await;

    let body_text = std::str::from_utf8(&body).expect("body is utf8");
    assert!(body_text.contains(r#""name":"bash""#));
    assert!(!body_text.contains(r#""name":"Bash""#));
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        Some(body.len().to_string())
    );
    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert!(headers.get("x-cc-lb-spoof").is_none());
    assert!(headers.get("x-cc-lb-secret").is_none());
    assert!(headers.get("x-ratelimit-limit").is_none());
    assert!(headers.get("authorization").is_none());
    assert!(headers.get("x-api-key").is_none());
    assert!(headers.get("anthropic-ratelimit-requests-limit").is_none());
    assert_eq!(
        headers.get(CONTENT_TYPE),
        Some(&HeaderValue::from_static("application/json"))
    );
    assert_eq!(transform.seen_bodies(), vec![buffered_upstream_body()]);
}

#[tokio::test]
async fn buffered_transform_rewrites_upstream_error_response() {
    let transform = Arc::new(BufferedToolNameTransform::default());
    let lifecycle = lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::BAD_REQUEST,
            headers: json_headers(buffered_upstream_body().len(), false),
            body: buffered_upstream_body(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles buffered error response");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = std::str::from_utf8(&body).expect("body is utf8");
    assert!(body_text.contains(r#""name":"bash""#));
    assert_eq!(transform.seen_bodies(), vec![buffered_upstream_body()]);
}

#[tokio::test]
async fn buffered_unchanged_preserves_compressed_upstream_bytes() {
    let compressed = gzip_bytes(&buffered_upstream_body());
    let transform = Arc::new(UnchangedBufferedTransform::default());
    let lifecycle = lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: json_headers(compressed.len(), true),
            body: compressed.clone(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles compressed buffered response");
    let (_status, headers, body) = collect_body(response).await;

    assert_eq!(body, compressed);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(body.len().to_string().as_str())
    );
    assert_eq!(transform.seen_bodies(), vec![buffered_upstream_body()]);
}

#[tokio::test]
async fn buffered_header_only_transform_preserves_compressed_upstream_bytes() {
    let compressed = gzip_bytes(&buffered_upstream_body());
    let transform = Arc::new(HeaderOnlyBufferedTransform::default());
    let lifecycle = lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: json_headers(compressed.len(), true),
            body: compressed.clone(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles compressed header-only transform");
    let (_status, headers, body) = collect_body(response).await;

    assert_eq!(body, compressed);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(body.len().to_string().as_str())
    );
    assert_eq!(
        headers.get("x-plugin-header"),
        Some(&HeaderValue::from_static("kept"))
    );
    assert!(headers.get("x-api-key").is_none());
    assert_eq!(transform.seen_bodies(), vec![buffered_upstream_body()]);
}

#[tokio::test]
async fn buffered_unsupported_encoding_skips_transform() {
    let transform = Arc::new(BufferedToolNameTransform::default());
    let mut headers = json_headers(buffered_upstream_body().len(), false);
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("snappy"));
    let lifecycle = lifecycle_with_transforms(
        Some(transform.clone()),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: buffered_upstream_body(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles unsupported encoded response");
    let (_status, headers, body) = collect_body(response).await;

    assert_eq!(body, buffered_upstream_body());
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("snappy"))
    );
    assert!(transform.seen_bodies().is_empty());
}

#[tokio::test]
async fn buffered_transform_failure_fails_open_to_original_response() {
    let transform = Arc::new(FailingBufferedTransform);
    let lifecycle = lifecycle_with_transforms(Some(transform), None, buffered_dispatch());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles buffered response");
    let (_status, headers, body) = collect_body(response).await;

    assert_eq!(body, buffered_upstream_body());
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        Some(body.len().to_string())
    );
    assert!(headers.get(CONTENT_ENCODING).is_none());
}

#[tokio::test]
async fn buffered_transform_failure_is_observed_as_error() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "response-transform-observation.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let transform = Arc::new(FailingBufferedTransform);
    let lifecycle = lifecycle_with_transforms(Some(transform), None, buffered_dispatch())
        .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles buffered response");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, buffered_upstream_body());

    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.status, StatusCode::OK.as_u16());
    assert_eq!(event.error_code.as_deref(), Some("upstream_stream_error"));
    Ok(())
}

#[tokio::test]
async fn buffered_http_error_preserves_canonical_error_over_transform_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let body = Bytes::from_static(
        br#"{"type":"error","error":{"type":"rate_limit_error","message":"forced fake rate limit response"}}"#,
    );
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "buffered-error-precedence.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        Some(Arc::new(FailingBufferedTransform)),
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers: json_headers(body.len(), false),
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles buffered upstream error");
    let (status, _headers, output) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(output, body);
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("rate_limit_error")
    );
    assert_eq!(
        events[0].upstream_error_message.as_deref(),
        Some("forced fake rate limit response")
    );
    Ok(())
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
    file_name: &str,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
            .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

async fn wait_for_events(
    storage: &dyn RequestEventStore,
    expected: usize,
) -> Result<Vec<cc_lb_storage_api::types::RequestEvent>, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let events = RequestEventStore::query_request_events(storage, 0, u64::MAX, 10).await?;
        if events.len() >= expected {
            return Ok(events);
        }
        if std::time::Instant::now() >= deadline {
            panic!("expected {expected} request event(s), got {}", events.len());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn buffered_accounting_uses_pre_transform_usage() {
    let transform = Arc::new(BufferedUsageMutatingTransform);
    let recording = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with_transforms_and_hook(
        Some(transform),
        None,
        buffered_dispatch(),
        recording.clone(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles buffered response");
    let (_status, _headers, body) = collect_body(response).await;

    let body_text = std::str::from_utf8(&body).expect("body is utf8");
    assert!(body_text.contains(r#""input_tokens":999"#));
    let events = recording.events.lock().expect("events lock").clone();
    let usage = events.iter().find_map(|event| match event {
        ObserveEvent::RequestFinished {
            input_tokens,
            output_tokens,
            ..
        } => Some((*input_tokens, *output_tokens)),
        _ => None,
    });
    assert_eq!(usage, Some((Some(3), Some(5))));
}

#[tokio::test]
async fn sse_transform_rewrites_content_block_start_event() {
    let transform = Arc::new(SseToolNameTransform::default());
    let lifecycle = lifecycle_with_transforms(None, Some(transform.clone()), sse_dispatch(false));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles SSE response by upstream content-type");
    let (_status, _headers, body) = collect_body(response).await;

    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#"event: content_block_start"#));
    assert!(text.contains(r#""name":"bash""#));
    assert!(!text.contains(r#""name":"Bash""#));
    assert_eq!(
        transform.seen_events(),
        vec!["content_block_start".to_owned(), "message_stop".to_owned()]
    );
}

#[tokio::test]
async fn sse_transform_receives_sanitized_response_headers() {
    let transform = Arc::new(HeaderCapturingSseTransform::default());
    let mut headers = sse_headers();
    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("123"));
    headers.insert("x-cc-lb-secret", HeaderValue::from_static("remove-me"));
    headers.insert("x-ratelimit-limit", HeaderValue::from_static("remove-me"));
    headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
    headers.insert("x-safe-header", HeaderValue::from_static("keep-me"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: sse_upstream_body(false),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, _body) = collect_body(response).await;
    let seen = transform.seen_headers();
    assert!(!seen.is_empty());
    let first = &seen[0];
    assert!(first.get(CONTENT_LENGTH).is_none());
    assert!(first.get("x-cc-lb-secret").is_none());
    assert!(first.get("x-ratelimit-limit").is_none());
    assert!(first.get("authorization").is_none());
    assert_eq!(
        first.get("x-safe-header"),
        Some(&HeaderValue::from_static("keep-me"))
    );
}

#[tokio::test]
async fn sse_unchanged_transform_preserves_raw_event_bytes() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let body =
        Bytes::from_static(b": keep-this-comment\nevent: ping\ndata: {\"type\":\"ping\"}\n\n");
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: body.clone(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles unchanged SSE response");
    let (_status, _headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    assert_eq!(transform.seen_events(), vec!["ping".to_owned()]);
}

#[tokio::test]
async fn sse_transform_decodes_gzip_and_emits_identity_sse() {
    let transform = Arc::new(SseToolNameTransform::default());
    let body = gzip_bytes(&sse_upstream_body(false));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).expect("content length header"),
    );
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body,
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles gzip SSE response");
    let (_status, headers, body) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert!(headers.get(CONTENT_LENGTH).is_none());
    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#""name":"bash""#));
    assert_eq!(
        transform.seen_events(),
        vec!["content_block_start".to_owned(), "message_stop".to_owned()]
    );
}

#[tokio::test]
async fn sse_transform_decodes_brotli_and_emits_identity_sse() {
    let transform = Arc::new(SseToolNameTransform::default());
    let body = brotli_bytes(&sse_upstream_body(false));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("br"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).expect("content length header"),
    );
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body,
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles brotli SSE response");
    let (_status, headers, body) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert!(headers.get(CONTENT_LENGTH).is_none());
    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#""name":"bash""#));
    assert_eq!(
        transform.seen_events(),
        vec!["content_block_start".to_owned(), "message_stop".to_owned()]
    );
}

#[tokio::test]
async fn truncated_gzip_after_transformed_output_is_upstream_decode_error()
-> Result<(), Box<dyn std::error::Error>> {
    let transform = Arc::new(UnchangedSseTransform::default());
    let plaintext = Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n");
    let body = incomplete_gzip_prefix(&plaintext);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "truncated-gzip.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let LifecycleBusReceiver::InMemory(mut lifecycle_rx) = test_bus.bus.subscribe_lifecycle()
    else {
        panic!("expected in-memory lifecycle receiver");
    };
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles truncated gzip SSE response");
    let (_status, headers, body) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&body).expect("SSE body is utf8");
    assert!(text.contains("event: ping\n"));
    assert!(text.contains("event: error\n"));
    assert!(text.contains("\"api_error\""));
    assert!(!text.contains("response_transform_error"));
    assert_eq!(transform.seen_events(), vec!["ping".to_owned()]);

    let error = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let LifecycleEvent::StreamCompleted {
                result: Err(error), ..
            } = lifecycle_rx
                .recv()
                .await
                .expect("lifecycle event delivered")
            {
                break error;
            }
        }
    })
    .await
    .expect("stream error observed");
    assert_eq!(error.error_type, "upstream_response_decode_error");
    assert!(error.error_message.contains("matching checksum"));

    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        event.upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    assert!(
        event
            .upstream_error_message
            .as_deref()
            .is_some_and(|message| message.contains("matching checksum"))
    );
    Ok(())
}

#[tokio::test]
async fn truncated_gzip_before_transformed_output_emits_decode_error()
-> Result<(), Box<dyn std::error::Error>> {
    let transform = Arc::new(UnchangedSseTransform::default());
    let plaintext = Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}");
    let body = incomplete_gzip_prefix(&plaintext);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "truncated-gzip-before-output.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles truncated gzip before transform output");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("\"api_error\""));
    assert!(text.contains("matching checksum"));
    assert!(transform.seen_events().is_empty());
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    Ok(())
}

#[tokio::test]
async fn gzip_transform_failure_preserves_transform_error() -> Result<(), Box<dyn std::error::Error>>
{
    let transform = Arc::new(FailAfterFirstSseTransform::default());
    let chunks = gzip_event_chunks(
        b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
        b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
    );
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "gzip-transform-failure.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform),
        Arc::new(ChunkedDispatch {
            status: StatusCode::OK,
            headers,
            chunks,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles gzip transform failure");
    let (_status, _headers, body) = collect_body(response).await;

    let text = std::str::from_utf8(&body).expect("SSE body is utf8");
    assert_eq!(text.matches("event: error\n").count(), 1);
    assert!(text.contains("response_transform_error"));
    assert!(!text.contains("\"api_error\""));

    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("response_transform_error")
    );
    Ok(())
}

#[tokio::test]
async fn raw_deflate_passthrough_decode_failure_remains_success()
-> Result<(), Box<dyn std::error::Error>> {
    let plaintext = Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n");
    let body = raw_deflate_bytes(&plaintext);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("deflate"));
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "raw-deflate-passthrough.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: body.clone(),
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles raw deflate passthrough");
    let (_status, headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("deflate"))
    );
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].error_code, None);
    assert_eq!(events[0].upstream_error_type, None);
    Ok(())
}

#[tokio::test]
async fn non_sse_gzip_decode_failure_skips_sse_transform() -> Result<(), Box<dyn std::error::Error>>
{
    let transform = Arc::new(UnchangedSseTransform::default());
    let body = incomplete_gzip_prefix(&Bytes::from_static(br#"{"partial":true}"#));
    let headers = json_headers(body.len(), true);
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "non-sse-gzip.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: body.clone(),
        }),
    )
    .with_static_limit_subject(
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_engine::SystemClock),
        ),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles non-SSE gzip response");
    let (_status, headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    assert!(transform.seen_events().is_empty());
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].error_code, None);
    assert_eq!(events[0].upstream_error_type, None);
    Ok(())
}

#[tokio::test]
async fn sse_unsupported_encoding_skips_transform_and_raw_passes_through() {
    let transform = Arc::new(SseToolNameTransform::default());
    let body = sse_upstream_body(false);
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("snappy"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: body.clone(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles unsupported encoded SSE response");
    let (_status, headers, output) = collect_body(response).await;

    assert_eq!(output, body);
    assert_eq!(
        headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("snappy"))
    );
    assert!(transform.seen_events().is_empty());
}

#[tokio::test]
async fn sse_transform_failure_before_output_fails_open_raw() {
    let transform = Arc::new(AlwaysFailingSseTransform);
    let lifecycle = lifecycle_with_transforms(None, Some(transform), sse_dispatch(false));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, body) = collect_body(response).await;

    assert_eq!(body, sse_upstream_body(false));
}

#[tokio::test]
async fn sse_unterminated_final_event_is_upstream_framing_error() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let body = Bytes::from_static(b"event: message_stop\ndata: {}\n");
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: body.clone(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles unterminated identity SSE event");
    let (_status, _headers, output) = collect_body(response).await;

    assert_ne!(output, body);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains(r#""type":"api_error""#));
    assert!(text.contains("incomplete event before transform output"));
    assert!(transform.seen_events().is_empty());
}

#[tokio::test]
async fn sse_gzip_unterminated_final_event_emits_upstream_framing_error() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let compressed = gzip_bytes(&Bytes::from_static(b"event: message_stop\ndata: {}\n"));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform.clone()),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed.clone(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles unterminated compressed SSE event");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert_ne!(output, compressed);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains(r#""type":"api_error""#));
    assert!(text.contains("incomplete event before transform output"));
    assert!(transform.seen_events().is_empty());
}

#[tokio::test]
async fn sse_unterminated_http_error_preserves_upstream_status()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "unterminated-http-error.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let transform = Arc::new(UnchangedSseTransform::default());
    let body = Bytes::from_static(
        b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"forced fake rate limit response\"}}\n",
    );
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers: sse_headers(),
            body,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles unterminated HTTP error SSE event");
    let (status, _headers, output) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains(r#""type":"api_error""#));
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, StatusCode::TOO_MANY_REQUESTS.as_u16());
    assert_eq!(events[0].error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("upstream_response_framing_error")
    );
    Ok(())
}

#[tokio::test]
async fn sse_gzip_transform_failure_before_output_emits_error_frame() {
    assert_gzip_transform_failure_before_output(Arc::new(AlwaysFailingSseTransform)).await;
}

#[tokio::test]
async fn sse_gzip_transform_failure_preserves_http_error_status()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "transform-http-error.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let compressed = gzip_bytes(&sse_upstream_body(false));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(Arc::new(AlwaysFailingSseTransform)),
        Arc::new(FixedDispatch {
            status: StatusCode::TOO_MANY_REQUESTS,
            headers,
            body: compressed,
        }),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles transform failure on HTTP error response");
    let (status, _headers, output) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("response_transform_error"));
    let events = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, StatusCode::TOO_MANY_REQUESTS.as_u16());
    assert_eq!(events[0].error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        events[0].upstream_error_type.as_deref(),
        Some("response_transform_error")
    );
    Ok(())
}

#[tokio::test]
async fn sse_gzip_invalid_event_name_before_output_emits_error_frame() {
    assert_gzip_transform_failure_before_output(Arc::new(InvalidEventNameSseTransform)).await;
}

async fn assert_gzip_transform_failure_before_output(transform: Arc<dyn SseEventTransformHook>) {
    let compressed = gzip_bytes(&sse_upstream_body(false));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let lifecycle = lifecycle_with_transforms(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed.clone(),
        }),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles compressed SSE transform failure");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert_ne!(output, compressed);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
}

#[tokio::test]
async fn sse_transform_invalid_event_name_fails_open_without_injection() {
    let transform = Arc::new(InvalidEventNameSseTransform);
    let lifecycle = lifecycle_with_transforms(None, Some(transform), sse_dispatch(false));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, body) = collect_body(response).await;

    assert_eq!(body, sse_upstream_body(false));
    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(!text.contains("event: injected"));
}

#[tokio::test]
async fn sse_transform_failure_after_transformed_output_terminates_classified() {
    let transform = Arc::new(FailAfterFirstSseTransform::default());
    let lifecycle = lifecycle_with_transforms(None, Some(transform), sse_dispatch(false));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, body) = collect_body(response).await;

    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#""name":"bash""#));
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(!text.contains("message_stop"));
}

#[tokio::test]
async fn sse_decompression_budget_before_output_emits_transform_error() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let plaintext = vec![b'x'; 1024];
    let compressed = gzip_bytes(&Bytes::from(plaintext));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed,
        }),
        config,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles over-budget gzip SSE response");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(text.contains("before transform output"));
}

#[tokio::test]
async fn sse_gzip_incomplete_event_budget_before_output_emits_transform_error() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let mut plaintext = Vec::from(&b"data: "[..]);
    plaintext.extend(std::iter::repeat_n(b'x', 256));
    let compressed = gzip_bytes(&Bytes::from(plaintext));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed,
        }),
        config,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles compressed incomplete SSE event");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(text.contains("incomplete SSE event"));
    assert!(text.contains("before transform output"));
}

#[tokio::test]
async fn sse_brotli_incomplete_event_budget_never_replays_compressed_bytes() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let mut plaintext = Vec::from(&b"data: "[..]);
    plaintext.extend(std::iter::repeat_n(b'x', 256));
    let compressed = brotli_bytes(&Bytes::from(plaintext));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("br"));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers,
            body: compressed.clone(),
        }),
        config,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles brotli incomplete SSE event");
    let (_status, headers, output) = collect_body(response).await;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    assert_ne!(output, compressed);
    let text = std::str::from_utf8(&output).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(text.contains("incomplete SSE event"));
}

#[tokio::test]
async fn sse_incomplete_event_budget_after_output_uses_existing_error_frame() {
    let transform = Arc::new(UnchangedSseTransform::default());
    let mut body = Vec::from(&b"event: ping\ndata: {\"type\":\"ping\"}\n\n"[..]);
    body.extend_from_slice(b"data: ");
    body.extend(std::iter::repeat_n(b'x', 256));
    let config = LifecycleConfig {
        messages_body_cap_bytes: 128,
        ..LifecycleConfig::default()
    };
    let lifecycle = lifecycle_with_transforms_and_config(
        None,
        Some(transform),
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: Bytes::from(body),
        }),
        config,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles over-budget incomplete SSE event");
    let (_status, _headers, output) = collect_body(response).await;
    let text = std::str::from_utf8(&output).expect("output is SSE text");

    assert!(text.starts_with("event: ping\n"));
    assert!(text.contains("event: error\n"));
    assert!(text.contains("response_transform_error"));
    assert!(!text.contains(&"x".repeat(256)));
}

#[tokio::test]
async fn sse_accounting_uses_pre_transform_usage() {
    let transform = Arc::new(SseUsageMutatingTransform);
    let recording = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with_transforms_and_hook(
        None,
        Some(transform),
        sse_dispatch(true),
        recording.clone(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles SSE response");
    let (_status, _headers, body) = collect_body(response).await;

    let text = std::str::from_utf8(&body).expect("sse body is utf8");
    assert!(text.contains(r#""input_tokens":999"#));
    let events = recording.events.lock().expect("events lock").clone();
    let usage = events.iter().find_map(|event| match event {
        ObserveEvent::RequestFinished {
            input_tokens,
            output_tokens,
            ..
        } => Some((*input_tokens, *output_tokens)),
        _ => None,
    });
    assert_eq!(usage, Some((Some(7), Some(11))));
}

fn lifecycle_with_transforms(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
) -> Lifecycle {
    lifecycle_with_transforms_and_config(
        response_transform,
        sse_transform,
        dispatcher,
        LifecycleConfig::default(),
    )
}

fn lifecycle_with_transforms_and_config(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    config: LifecycleConfig,
) -> Lifecycle {
    lifecycle_with_transforms_and_hook_and_config(
        response_transform,
        sse_transform,
        dispatcher,
        Arc::new(RecordingHook::default()),
        config,
    )
}

fn lifecycle_with_transforms_and_hook(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    hook: Arc<RecordingHook>,
) -> Lifecycle {
    lifecycle_with_transforms_and_hook_and_config(
        response_transform,
        sse_transform,
        dispatcher,
        hook,
        LifecycleConfig::default(),
    )
}

fn lifecycle_with_transforms_and_hook_and_config(
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    hook: Arc<RecordingHook>,
    config: LifecycleConfig,
) -> Lifecycle {
    let dialect = Arc::new(ShapeTransformDialect::new(
        response_transform,
        sse_transform,
    ));
    let state = TestState::default();
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(Arc::new(RouterPipelineCache::empty(
                TerminalStrategy::FirstPick,
            ))),
            ObservabilityHooksCache::Inherit,
            DialectCache::Explicit(ShapePluginCache { dialect }),
        ),
    );
    let principal = PrincipalRecord {
        id: Uuid::new_v4(),
        name: "principal-test".to_owned(),
        kind: cc_lb_storage_api::PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: vec![default_upstream_id()],
        default_limits: Vec::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: None,
    };
    let authn = TestAuthn::with_principal_view(
        state,
        Arc::new(PrincipalView::from_db(&[principal], chains)),
    );
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }))
        .global_observability_hooks(vec![hook])
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![default_upstream_record()])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        config,
        Arc::new(cc_lb_engine::SystemClock),
    )
}

struct ShapeTransformDialect {
    response_transform: Option<Arc<dyn ResponseTransformHook>>,
    sse_transform: Option<Arc<dyn SseEventTransformHook>>,
}

impl ShapeTransformDialect {
    fn new(
        response_transform: Option<Arc<dyn ResponseTransformHook>>,
        sse_transform: Option<Arc<dyn SseEventTransformHook>>,
    ) -> Self {
        Self {
            response_transform,
            sse_transform,
        }
    }
}

impl UpstreamDialect for ShapeTransformDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut url = Url::parse("http://upstream.local/").expect("test URL parses");
        url.set_path(context.path.trim_start_matches('/'));
        url.set_query(context.query.as_deref());
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }

    fn response_transform_hook(&self) -> Option<&dyn ResponseTransformHook> {
        self.response_transform.as_deref()
    }

    fn sse_event_transform_hook(&self) -> Option<&dyn SseEventTransformHook> {
        self.sse_transform.as_deref()
    }
}

fn buffered_dispatch() -> Arc<dyn UpstreamDispatch> {
    Arc::new(FixedDispatch {
        status: StatusCode::OK,
        headers: json_headers(buffered_upstream_body().len(), false),
        body: buffered_upstream_body(),
    })
}

fn sse_dispatch(with_usage_mutation_case: bool) -> Arc<dyn UpstreamDispatch> {
    Arc::new(FixedDispatch {
        status: StatusCode::OK,
        headers: sse_headers(),
        body: sse_upstream_body(with_usage_mutation_case),
    })
}

struct FixedDispatch {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for FixedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        Ok(response)
    }
}

struct ChunkedDispatch {
    status: StatusCode,
    headers: HeaderMap,
    chunks: Vec<Bytes>,
}

#[async_trait]
impl UpstreamDispatch for ChunkedDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let chunks = self.chunks.clone();
        let stream = async_stream::stream! {
            for chunk in chunks {
                yield Ok::<Bytes, std::convert::Infallible>(chunk);
            }
        };
        let mut response = Response::new(Body::from_stream(stream));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        Ok(response)
    }
}

#[derive(Default)]
struct BufferedToolNameTransform {
    seen: Mutex<Vec<Bytes>>,
}

impl BufferedToolNameTransform {
    fn seen_bodies(&self) -> Vec<Bytes> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl ResponseTransformHook for BufferedToolNameTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.body.clone());
        let body = replace_bytes(&request.body, br#""name":"Bash""#, br#""name":"bash""#);
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: Some(spoofed_plugin_headers(body.len())),
            body: Some(body),
        })
    }
}

struct FailingBufferedTransform;

impl ResponseTransformHook for FailingBufferedTransform {
    fn transform_response(
        &self,
        _request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        Err(ResponseTransformError::Trap {
            reason: "forced buffered failure".to_owned(),
        })
    }
}

struct BufferedUsageMutatingTransform;

impl ResponseTransformHook for BufferedUsageMutatingTransform {
    fn transform_response(
        &self,
        _request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: None,
            body: Some(Bytes::from_static(
                br#"{"type":"message","usage":{"input_tokens":999,"output_tokens":999},"content":[]}"#,
            )),
        })
    }
}

#[derive(Default)]
struct UnchangedBufferedTransform {
    seen: Mutex<Vec<Bytes>>,
}

impl UnchangedBufferedTransform {
    fn seen_bodies(&self) -> Vec<Bytes> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl ResponseTransformHook for UnchangedBufferedTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.body.clone());
        Ok(TransformResponseResult::Unchanged)
    }
}

#[derive(Default)]
struct HeaderOnlyBufferedTransform {
    seen: Mutex<Vec<Bytes>>,
}

impl HeaderOnlyBufferedTransform {
    fn seen_bodies(&self) -> Vec<Bytes> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl ResponseTransformHook for HeaderOnlyBufferedTransform {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.body.clone());
        let mut headers = HeaderMap::new();
        headers.insert("x-plugin-header", HeaderValue::from_static("kept"));
        headers.insert("x-api-key", HeaderValue::from_static("secret"));
        Ok(TransformResponseResult::Replace {
            status: None,
            headers: Some(headers),
            body: None,
        })
    }
}

#[derive(Default)]
struct SseToolNameTransform {
    seen: Mutex<Vec<String>>,
}

impl SseToolNameTransform {
    fn seen_events(&self) -> Vec<String> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl SseEventTransformHook for SseToolNameTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.event.event.clone());
        if request.event.event == "content_block_start" {
            return Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: request.event.event,
                    data: replace_bytes(
                        &request.event.data,
                        br#""name":"Bash""#,
                        br#""name":"bash""#,
                    ),
                }],
            });
        }
        Ok(TransformSseEventResult::Unchanged)
    }
}

#[derive(Default)]
struct UnchangedSseTransform {
    seen: Mutex<Vec<String>>,
}

impl UnchangedSseTransform {
    fn seen_events(&self) -> Vec<String> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl SseEventTransformHook for UnchangedSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        self.seen
            .lock()
            .expect("seen lock")
            .push(request.event.event);
        Ok(TransformSseEventResult::Unchanged)
    }
}

#[derive(Default)]
struct HeaderCapturingSseTransform {
    seen_headers: Mutex<Vec<HeaderMap>>,
}

impl HeaderCapturingSseTransform {
    fn seen_headers(&self) -> Vec<HeaderMap> {
        self.seen_headers.lock().expect("seen headers lock").clone()
    }
}

impl SseEventTransformHook for HeaderCapturingSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        self.seen_headers
            .lock()
            .expect("seen headers lock")
            .push(request.response_headers);
        Ok(TransformSseEventResult::Unchanged)
    }
}

struct AlwaysFailingSseTransform;

impl SseEventTransformHook for AlwaysFailingSseTransform {
    fn transform_sse_event(
        &self,
        _request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        Err(ResponseTransformError::Runtime {
            reason: "forced sse failure".to_owned(),
        })
    }
}

#[derive(Default)]
struct FailAfterFirstSseTransform {
    calls: Mutex<u64>,
}

impl SseEventTransformHook for FailAfterFirstSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        let mut calls = self.calls.lock().expect("calls lock");
        *calls = calls.saturating_add(1);
        if *calls > 1 {
            return Err(ResponseTransformError::Runtime {
                reason: "forced post-output failure".to_owned(),
            });
        }
        Ok(TransformSseEventResult::Replace {
            events: vec![SseEvent {
                event: request.event.event,
                data: replace_bytes(
                    &request.event.data,
                    br#""name":"Bash""#,
                    br#""name":"bash""#,
                ),
            }],
        })
    }
}

struct SseUsageMutatingTransform;

impl SseEventTransformHook for SseUsageMutatingTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if request.event.event == "message_stop" {
            return Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: request.event.event,
                    data: Bytes::from_static(
                        br#"{"type":"message_stop","usage":{"input_tokens":999,"output_tokens":999}}"#,
                    ),
                }],
            });
        }
        Ok(TransformSseEventResult::Unchanged)
    }
}

struct InvalidEventNameSseTransform;

impl SseEventTransformHook for InvalidEventNameSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        Ok(TransformSseEventResult::Replace {
            events: vec![SseEvent {
                event: format!("{}\nevent: injected", request.event.event),
                data: request.event.data,
            }],
        })
    }
}

fn buffered_upstream_body() -> Bytes {
    Bytes::from_static(
        br#"{"type":"message","usage":{"input_tokens":3,"output_tokens":5},"content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{}}]}"#,
    )
}

fn sse_upstream_body(with_usage_mutation_case: bool) -> Bytes {
    let usage = if with_usage_mutation_case {
        r#"{"type":"message_stop","usage":{"input_tokens":7,"output_tokens":11}}"#
    } else {
        r#"{"type":"message_stop"}"#
    };
    Bytes::from(format!(
        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"Bash\",\"input\":{{}}}}}}\n\nevent: message_stop\ndata: {usage}\n\n"
    ))
}

fn replace_bytes(input: &Bytes, needle: &[u8], replacement: &[u8]) -> Bytes {
    let mut output = Vec::with_capacity(input.len());
    let mut cursor = input.as_ref();
    while let Some(index) = cursor
        .windows(needle.len())
        .position(|window| window == needle)
    {
        output.extend_from_slice(&cursor[..index]);
        output.extend_from_slice(replacement);
        cursor = &cursor[index + needle.len()..];
    }
    output.extend_from_slice(cursor);
    Bytes::from(output)
}

fn json_headers(content_len: usize, compressed: bool) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_len.to_string()).expect("content length header"),
    );
    if compressed {
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    headers
}

fn spoofed_plugin_headers(content_len: usize) -> HeaderMap {
    let mut headers = json_headers(content_len, true);
    headers.insert("connection", HeaderValue::from_static("x-sneaky"));
    headers.insert("x-sneaky", HeaderValue::from_static("remove-me"));
    headers.insert("x-cc-lb-spoof", HeaderValue::from_static("remove-me"));
    headers.insert("x-cc-lb-secret", HeaderValue::from_static("remove-me"));
    headers.insert("x-ratelimit-limit", HeaderValue::from_static("remove-me"));
    headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
    headers.insert("x-api-key", HeaderValue::from_static("secret"));
    headers.insert(
        "anthropic-ratelimit-requests-limit",
        HeaderValue::from_static("1"),
    );
    headers
}

fn gzip_bytes(body: &Bytes) -> Bytes {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("gzip write succeeds");
    Bytes::from(encoder.finish().expect("gzip finish succeeds"))
}

fn brotli_bytes(body: &Bytes) -> Bytes {
    let mut compressed = Vec::new();
    {
        let mut encoder = brotli::CompressorWriter::new(&mut compressed, 4096, 5, 22);
        encoder.write_all(body).expect("brotli write succeeds");
    }
    Bytes::from(compressed)
}

fn incomplete_gzip_prefix(body: &Bytes) -> Bytes {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("gzip write succeeds");
    encoder.flush().expect("gzip flush succeeds");
    Bytes::copy_from_slice(encoder.get_ref())
}

fn raw_deflate_bytes(body: &Bytes) -> Bytes {
    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).expect("deflate write succeeds");
    Bytes::from(encoder.finish().expect("deflate finish succeeds"))
}

fn gzip_event_chunks(first: &[u8], second: &[u8]) -> Vec<Bytes> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(first).expect("first gzip event writes");
    encoder.flush().expect("first gzip event flushes");
    let first_end = encoder.get_ref().len();
    encoder.write_all(second).expect("second gzip event writes");
    encoder.flush().expect("second gzip event flushes");
    let second_end = encoder.get_ref().len();
    let compressed = encoder.finish().expect("gzip finish succeeds");
    vec![
        Bytes::copy_from_slice(&compressed[..first_end]),
        Bytes::copy_from_slice(&compressed[first_end..second_end]),
        Bytes::copy_from_slice(&compressed[second_end..]),
    ]
}

fn sse_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    headers
}

fn default_upstream_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("default upstream id parses")
}

fn default_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: default_upstream_id(),
        name: "test-upstream".to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: Some(Vec::new()),
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

fn _principal_for_doc() -> Principal {
    Principal {
        id: "principal-test".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    }
}
