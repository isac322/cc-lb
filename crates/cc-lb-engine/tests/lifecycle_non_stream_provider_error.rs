use crate::common;

use std::sync::Arc;

use bytes::Bytes;
use cc_lb_engine::event_bus::{BusReceiver, RequestEventBus, RequestEventUpdate};
use cc_lb_plugin_api::ObserveEvent;
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
    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
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
async fn noncanonical_non_stream_429_keeps_only_broad_error_classification()
-> Result<(), Box<dyn std::error::Error>> {
    // Given
    let controls = [
        Bytes::new(),
        Bytes::from_static(b"not-json"),
        Bytes::from_static(
            br#"{"type":"message","error":{"type":"rate_limit_error","message":"no"}}"#,
        ),
        Bytes::from_static(br#"{"type":"error","error":{"message":"missing type"}}"#),
        Bytes::from_static(br#"{"type":"error","error":{"type":"rate_limit_error"}}"#),
    ];

    // When / Then
    for upstream_body in controls {
        let observed = observe_non_stream_429(upstream_body.clone()).await?;
        assert_eq!(observed.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(observed.body, upstream_body);
        assert_eq!(observed.event.error_code.as_deref(), Some("upstream_4xx"));
        assert_eq!(observed.event.upstream_error_type, None);
        assert_eq!(observed.event.upstream_error_message, None);
    }
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

struct ObservedErrorResponse {
    status: StatusCode,
    body: Bytes,
    event: RequestEvent,
    observe_events: Vec<ObserveEvent>,
}

async fn observe_non_stream_429(
    upstream_body: Bytes,
) -> Result<ObservedErrorResponse, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let hook = Arc::new(RecordingHook::default());
    let test_bus = TestLifecycleBus::new()
        .with_assembler(storage.clone() as Arc<dyn StorageTrait>)
        .with_hook_adapter(vec![hook.clone()]);
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
    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await?;
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
