use crate::common;

use std::sync::Arc;

use bytes::Bytes;
use cc_lb_engine::event_bus::{BusReceiver, RequestEventBus, RequestEventUpdate};
use cc_lb_observability::ObserveEvent;
use cc_lb_storage_api::types::RequestEvent;
use cc_lb_storage_api::{BackendKind, MetaStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::SqliteStorage;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, StatusCode};
use tokio::time::{Duration, timeout};

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestLifecycleBus, TestState,
    collect_body, lifecycle_with, messages_request,
};

const CANONICAL_RATE_LIMIT_BODY: &[u8] = br#"{"type":"error","error":{"type":"rate_limit_error","message":"forced fake rate limit response"}}"#;

#[tokio::test]
async fn non_stream_429_preserves_client_status_and_body_bytes() {
    // Given
    let state = TestState::default();
    let upstream_body = Bytes::from_static(CANONICAL_RATE_LIMIT_BODY);
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::Raw {
                status: StatusCode::TOO_MANY_REQUESTS,
                headers,
                body: upstream_body.clone(),
            },
        },
        Arc::new(RecordingHook::default()),
    );

    // When
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":false}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _headers, downstream_body) = collect_body(response).await;

    // Then
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(downstream_body, upstream_body);
}

#[tokio::test]
async fn canonical_non_stream_429_records_structured_error_and_preserves_client_response()
-> Result<(), Box<dyn std::error::Error>> {
    // Given
    let upstream_body = Bytes::from_static(CANONICAL_RATE_LIMIT_BODY);

    // When
    let observed = observe_non_stream_429(upstream_body.clone()).await?;

    // Then
    assert_eq!(observed.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(observed.body, upstream_body);
    assert_eq!(observed.event.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        observed.event.upstream_error_type.as_deref(),
        Some("rate_limit_error")
    );
    assert_eq!(
        observed.event.upstream_error_message.as_deref(),
        Some("forced fake rate limit response")
    );
    assert_eq!(
        observed.event.body_bytes,
        Some(u64::try_from(upstream_body.len()).expect("body length fits u64"))
    );
    assert_eq!(observed.event.body_chunk_count, Some(1));
    Ok(())
}

#[tokio::test]
async fn noncanonical_non_stream_429_extracts_partial_fields_or_raw_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    // Given — each body exercises one branch of the approved parser
    // contract: valid `error` objects are read regardless of the outer
    // `type`; partial objects yield the present field only; non-JSON falls
    // back to the raw body.
    let cases: [(Bytes, Option<&str>, Option<&str>); 4] = [
        // non-JSON: no type, raw body as message
        (Bytes::from_static(b"not-json"), None, Some("not-json")),
        // valid error object regardless of outer type: type + message
        (
            Bytes::from_static(
                br#"{"type":"message","error":{"type":"rate_limit_error","message":"no"}}"#,
            ),
            Some("rate_limit_error"),
            Some("no"),
        ),
        // message only: no type, extracted message
        (
            Bytes::from_static(br#"{"type":"error","error":{"message":"missing type"}}"#),
            None,
            Some("missing type"),
        ),
        // type only: extracted type, raw body fallback as message
        (
            Bytes::from_static(br#"{"type":"error","error":{"type":"rate_limit_error"}}"#),
            Some("rate_limit_error"),
            Some(r#"{"type":"error","error":{"type":"rate_limit_error"}}"#),
        ),
    ];

    // When / Then
    for (upstream_body, expected_type, expected_message) in cases {
        let observed = observe_non_stream_429(upstream_body.clone()).await?;
        assert_eq!(observed.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(observed.body, upstream_body);
        assert_eq!(observed.event.error_code.as_deref(), Some("upstream_4xx"));
        assert_eq!(
            observed.event.upstream_error_type.as_deref(),
            expected_type,
            "upstream_error_type mismatch for body: {upstream_body:?}"
        );
        assert_eq!(
            observed.event.upstream_error_message.as_deref(),
            expected_message,
            "upstream_error_message mismatch for body: {upstream_body:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn empty_non_stream_429_records_no_error_body() -> Result<(), Box<dyn std::error::Error>> {
    // Given / When
    let observed = observe_non_stream_429(Bytes::new()).await?;

    // Then
    assert_eq!(observed.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(observed.event.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(observed.event.upstream_error_type, None);
    assert_eq!(observed.event.upstream_error_message, None);
    Ok(())
}

#[tokio::test]
async fn oversized_non_stream_429_truncates_captured_error_body()
-> Result<(), Box<dyn std::error::Error>> {
    // Given
    let upstream_body = Bytes::from(vec![b'x'; 10 * 1024]);

    // When
    let observed = observe_non_stream_429(upstream_body.clone()).await?;

    // Then
    assert_eq!(observed.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(observed.body, upstream_body);
    let message = observed
        .event
        .upstream_error_message
        .expect("raw error body captured");
    assert!(message.len() <= 8 * 1024, "message stays within cap");
    assert!(
        message.ends_with("...[truncated]"),
        "oversized body is marked truncated"
    );
    assert_eq!(observed.event.upstream_error_type, None);
    Ok(())
}

#[tokio::test]
async fn canonical_non_stream_429_preserves_usage_and_body_metrics()
-> Result<(), Box<dyn std::error::Error>> {
    // Given
    let upstream_body = Bytes::from_static(
        br#"{"type":"error","error":{"type":"rate_limit_error","message":"forced fake rate limit response"},"usage":{"input_tokens":12,"output_tokens":3}}"#,
    );

    // When
    let observed = observe_non_stream_429(upstream_body.clone()).await?;

    // Then
    assert_eq!(observed.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(observed.body, upstream_body);
    assert_eq!(observed.event.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(observed.event.input_tokens, Some(12));
    assert_eq!(observed.event.output_tokens, Some(3));
    assert_eq!(
        observed.event.body_bytes,
        Some(u64::try_from(upstream_body.len()).expect("body length fits u64"))
    );
    assert_eq!(observed.event.body_chunk_count, Some(1));
    assert_eq!(
        observed.event.upstream_error_type.as_deref(),
        Some("rate_limit_error")
    );
    Ok(())
}

#[tokio::test]
async fn canonical_non_stream_429_exposes_only_broad_error_to_observe_plugin()
-> Result<(), Box<dyn std::error::Error>> {
    // Given / When
    let observed = observe_non_stream_429(Bytes::from_static(CANONICAL_RATE_LIMIT_BODY)).await?;

    // Then
    let errors = observed
        .observe_events
        .iter()
        .filter_map(|event| match event {
            ObserveEvent::Error {
                code,
                message,
                source,
            } => Some((code.as_str(), message.as_str(), source.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(errors.len(), 1, "canonical text reached observe plugin");
    assert_eq!(errors[0], ("upstream_4xx", "429", "upstream"));
    Ok(())
}

#[tokio::test]
async fn noncanonical_stream_429_extracts_message_without_type()
-> Result<(), Box<dyn std::error::Error>> {
    // Given — a valid `error` object carrying only `message`: the streaming
    // path extracts the message and leaves the type absent.
    let upstream_body =
        Bytes::from_static(br#"{"type":"error","error":{"message":"missing type"}}"#);

    // When
    let observed = observe_429(true, upstream_body.clone()).await?;

    // Then
    assert_eq!(observed.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(observed.body, upstream_body);
    assert_eq!(observed.event.error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(observed.event.upstream_error_type, None);
    assert_eq!(
        observed.event.upstream_error_message.as_deref(),
        Some("missing type"),
    );
    Ok(())
}

struct ObservedErrorResponse {
    status: StatusCode,
    body: Bytes,
    event: RequestEvent,
    observe_events: Vec<ObserveEvent>,
}

async fn observe_non_stream_429(
    upstream_body: Bytes,
) -> Result<ObservedErrorResponse, Box<dyn std::error::Error>> {
    observe_429(false, upstream_body).await
}

async fn observe_429(
    client_stream: bool,
    upstream_body: Bytes,
) -> Result<ObservedErrorResponse, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let hook = Arc::new(RecordingHook::default());
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let BusReceiver::InMemory(mut event_updates) = test_bus.bus.subscribe() else {
        panic!("expected in-memory request-event receiver");
    };
    let state = TestState::default();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::Raw {
                status: StatusCode::TOO_MANY_REQUESTS,
                headers,
                body: upstream_body,
            },
        },
        hook.clone(),
    )
    .with_event_bus(test_bus.bus_arc());
    let request_body = if client_stream {
        Bytes::from_static(br#"{"model":"claude-test","messages":[],"stream":true}"#)
    } else {
        Bytes::from_static(br#"{"model":"claude-test","messages":[],"stream":false}"#)
    };
    let request = messages_request(request_body);
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, body) = collect_body(response).await;
    let event = timeout(Duration::from_secs(1), async {
        loop {
            match event_updates
                .recv()
                .await
                .expect("request-event bus remains open")
            {
                RequestEventUpdate::Final(update) => break update.event,
                RequestEventUpdate::Partial(_) => {}
            }
        }
    })
    .await
    .expect("final request event arrives");
    timeout(
        Duration::from_secs(1),
        hook.wait_for_event(
            |event| matches!(event, ObserveEvent::Error { code, .. } if code == "upstream_4xx"),
        ),
    )
    .await
    .expect("broad provider error reaches observe plugin");
    let observe_events = hook.events.lock().expect("observe events lock").clone();
    Ok(ObservedErrorResponse {
        status,
        body,
        event,
        observe_events,
    })
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("provider-error.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
            .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}
