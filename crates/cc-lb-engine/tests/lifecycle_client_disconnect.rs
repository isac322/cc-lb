mod client_disconnect_support;
use crate::common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{BusReceiver, RequestEventBus};
use cc_lb_engine::LifecycleContext;
use cc_lb_storage_api::{RequestEventStore, Storage as StorageTrait};
use http::StatusCode;
use http_body_util::BodyExt;

use client_disconnect_support::{
    assert_dropped_terminal, assert_error_terminal, assert_one_final, assert_stream_error,
    assert_success_terminal, canonical_error_frame, json_dispatch, lifecycle, lifecycle_receiver,
    normal_sse_frame, pending_sse, sqlite_storage, sse_dispatch, transform_body,
    transform_lifecycle, upstream_frame_error, upstream_frame_error_after,
};
use common::{TestLifecycleBus, messages_request};

#[tokio::test]
async fn unpolled_stream_body_drop_persists_one_client_closed_final()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let BusReceiver::InMemory(mut update_rx) = test_bus.bus.subscribe() else {
        panic!("expected in-memory update receiver");
    };
    let waiting = Arc::new(tokio::sync::Notify::new());
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::OK, pending_sse(waiting)),
        &test_bus,
    );

    let response = lifecycle.handle(stream_request()).await?;
    drop(response.into_body());

    assert_error_terminal(&mut lifecycle_rx, 499, "client_closed_request").await;
    assert_one_final(&mut update_rx).await;
    let rows = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, 499);
    assert_eq!(rows[0].error_code.as_deref(), Some("client_closed_request"));
    Ok(())
}

#[tokio::test]
async fn stream_body_drop_after_normal_frame_is_client_closed() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let waiting = Arc::new(tokio::sync::Notify::new());
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::OK, pending_sse(waiting)),
        &test_bus,
    );

    let mut body = lifecycle
        .handle(stream_request())
        .await
        .expect("lifecycle handles request")
        .into_body();
    let frame = body.frame().await.expect("normal frame").expect("frame ok");
    assert_eq!(frame.into_data().expect("data frame"), normal_sse_frame());
    drop(body);

    assert_error_terminal(&mut lifecycle_rx, 499, "client_closed_request").await;
}

#[tokio::test]
async fn stream_body_drop_while_awaiting_upstream_is_client_closed() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let waiting = Arc::new(tokio::sync::Notify::new());
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::OK, pending_sse(Arc::clone(&waiting))),
        &test_bus,
    );

    let mut body = lifecycle
        .handle(stream_request())
        .await
        .expect("lifecycle handles request")
        .into_body();
    let _ = body.frame().await.expect("normal frame").expect("frame ok");
    let poll_task = tokio::spawn(async move { body.frame().await });
    tokio::time::timeout(Duration::from_secs(1), waiting.notified())
        .await
        .expect("relay awaits upstream");
    poll_task.abort();
    let _ = poll_task.await;

    assert_error_terminal(&mut lifecycle_rx, 499, "client_closed_request").await;
}

#[tokio::test]
async fn normal_stream_eof_remains_success() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::OK, Body::from(normal_sse_frame())),
        &test_bus,
    );

    let response = lifecycle
        .handle(stream_request())
        .await
        .expect("lifecycle handles request");
    let _ = response.into_body().collect().await.expect("body collects");

    assert_success_terminal(&mut lifecycle_rx, StatusCode::OK.as_u16()).await;
}

#[tokio::test]
async fn streaming_http_error_body_drop_remains_upstream_error() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let waiting = Arc::new(tokio::sync::Notify::new());
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::TOO_MANY_REQUESTS, pending_sse(waiting)),
        &test_bus,
    );

    let response = lifecycle
        .handle(stream_request())
        .await
        .expect("lifecycle handles request");
    drop(response.into_body());

    assert_error_terminal(
        &mut lifecycle_rx,
        StatusCode::TOO_MANY_REQUESTS.as_u16(),
        "upstream_4xx",
    )
    .await;
}

#[tokio::test]
async fn canonical_sse_error_then_drop_remains_upstream_stream_error() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::OK, Body::from(canonical_error_frame())),
        &test_bus,
    );

    let mut body = lifecycle
        .handle(stream_request())
        .await
        .expect("lifecycle handles request")
        .into_body();
    let _ = body.frame().await.expect("error frame").expect("frame ok");
    drop(body);

    assert_error_terminal(&mut lifecycle_rx, 200, "upstream_stream_error").await;
}

#[tokio::test]
async fn fatal_transform_frame_then_drop_remains_transform_error() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let lifecycle = transform_lifecycle(sse_dispatch(StatusCode::OK, transform_body()), &test_bus);

    let mut body = lifecycle
        .handle(stream_request())
        .await
        .expect("lifecycle handles request")
        .into_body();
    let _ = body
        .frame()
        .await
        .expect("transformed frame")
        .expect("frame ok");
    let terminal = body
        .frame()
        .await
        .expect("terminal frame")
        .expect("frame ok");
    let terminal_data = terminal.into_data().expect("data frame");
    assert!(
        terminal_data.starts_with(b"event: error"),
        "unexpected second frame: {terminal_data:?}",
    );
    drop(body);

    assert_error_terminal(&mut lifecycle_rx, 200, "upstream_stream_error").await;
}

#[tokio::test]
async fn upstream_frame_error_is_recorded_as_upstream_stream_error() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::OK, upstream_frame_error()),
        &test_bus,
    );

    let response = lifecycle
        .handle(stream_request())
        .await
        .expect("lifecycle handles request");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("relay body collects")
        .to_bytes();
    let text = std::str::from_utf8(&body).expect("error frame is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("\"api_error\""));

    assert_stream_error(
        &mut lifecycle_rx,
        "upstream_response_body_error",
        "forced upstream frame error",
    )
    .await;
    assert_error_terminal(&mut lifecycle_rx, 200, "upstream_stream_error").await;
}

#[tokio::test]
async fn upstream_frame_error_preserves_http_error_classification()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let BusReceiver::InMemory(mut update_rx) = test_bus.bus.subscribe() else {
        panic!("expected in-memory update receiver");
    };
    let lifecycle = lifecycle(
        sse_dispatch(
            StatusCode::TOO_MANY_REQUESTS,
            upstream_frame_error_after(normal_sse_frame()),
        ),
        &test_bus,
    );

    let response = lifecycle
        .handle(stream_request())
        .await
        .expect("lifecycle handles request");
    let _ = response
        .into_body()
        .collect()
        .await
        .expect("relay body collects");

    assert_stream_error(
        &mut lifecycle_rx,
        "upstream_response_body_error",
        "forced upstream frame error",
    )
    .await;
    assert_error_terminal(
        &mut lifecycle_rx,
        StatusCode::TOO_MANY_REQUESTS.as_u16(),
        "upstream_4xx",
    )
    .await;
    assert_one_final(&mut update_rx).await;
    let rows = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].error_code.as_deref(), Some("upstream_4xx"));
    assert_eq!(
        rows[0].upstream_error_type.as_deref(),
        Some("upstream_response_body_error")
    );
    assert_eq!(
        rows[0].upstream_error_message.as_deref(),
        Some("forced upstream frame error")
    );
    Ok(())
}

#[tokio::test]
async fn timeout_before_stream_guard_drop_remains_timeout() {
    timeout_ordering(true).await;
}

#[tokio::test]
async fn timeout_after_stream_guard_drop_remains_timeout() {
    timeout_ordering(false).await;
}

#[tokio::test]
async fn generic_observer_drop_remains_terminal_dropped() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let observer = LifecycleContext::new("generic-drop".to_owned(), test_bus.bus_arc(), &clock);

    drop(observer);

    assert_dropped_terminal(&mut lifecycle_rx).await;
}

#[tokio::test]
async fn non_stream_body_drop_remains_success() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let lifecycle = lifecycle(
        json_dispatch(
            StatusCode::OK,
            Bytes::from_static(
                br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#,
            ),
        ),
        &test_bus,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":false}"#,
        )))
        .await
        .expect("lifecycle handles request");
    drop(response.into_body());

    assert_success_terminal(&mut lifecycle_rx, 200).await;
}

async fn timeout_ordering(timeout_first: bool) {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let observer = LifecycleContext::new("timeout-ordering".to_owned(), test_bus.bus_arc(), &clock);
    let waiting = Arc::new(tokio::sync::Notify::new());
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::OK, pending_sse(waiting)),
        &test_bus,
    );
    let mut request = stream_request();
    request.extensions_mut().insert(observer.clone());
    let body = lifecycle
        .handle(request)
        .await
        .expect("lifecycle handles request")
        .into_body();

    if timeout_first {
        observer.terminate_tower_timeout();
        drop(body);
    } else {
        drop(body);
        observer.terminate_tower_timeout();
    }
    drop(observer);

    assert_error_terminal(&mut lifecycle_rx, 504, "tower_timeout").await;
}

fn stream_request() -> http::Request<Bytes> {
    messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ))
}
