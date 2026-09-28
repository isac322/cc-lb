mod client_disconnect_support;
use crate::common;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{BusReceiver, RequestEventBus};
use cc_lb_domain::{
    InternalError, InternalErrorKind, InternalErrorStage, Principal, TerminalStrategy, TtlClass,
    Upstream,
};
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_engine::{
    DynamicViewBuilder, DynamicViewHolder, InternalFailure, Lifecycle, LifecycleConfig,
    LifecycleContext, PromptCacheObservationEnqueueError, PromptCacheObservationSinkLike,
};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason, UsageSource};
use cc_lb_storage_api::principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore, RequestEventStore,
    Storage as StorageTrait, StorageResult,
};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, ShapedRequest, ShapedRequestBuilder,
    SseEventTransformHook, TransformSseEventRequest, TransformSseEventResult, UpstreamDialect,
};

use http::header::CONTENT_ENCODING;
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;

use client_disconnect_support::{
    assert_dropped_terminal, assert_error_terminal, assert_one_final, assert_stream_error,
    assert_success_terminal, canonical_error_frame, encoded_sse_dispatch, json_dispatch, lifecycle,
    lifecycle_receiver, normal_sse_frame, pending_sse, persistent_upstream_frame_error,
    refusal_sse_body, sqlite_storage, sse_dispatch, transform_body, transform_lifecycle,
    upstream_frame_error, upstream_frame_error_after,
};
use common::{TestAuthn, TestLifecycleBus, TestRouter, TestState, messages_request};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Notify, mpsc};
use url::Url;

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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let _ = response.into_body().collect().await.expect("body collects");

    assert_success_terminal(&mut lifecycle_rx, StatusCode::OK.as_u16()).await;
}

#[tokio::test]
async fn aborted_sse_with_only_below_threshold_cache_candidates_emits_no_observation_event() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let sink = Arc::new(RecordingObservationSink::default());
    let lifecycle = prompt_cache_lifecycle_with(
        sse_dispatch(
            StatusCode::OK,
            Body::from(Bytes::from_static(
                b"event: message_start\n\
                  data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":12,\"cache_creation_input_tokens\":12,\"output_tokens\":0}}}\n\n",
            )),
        ),
        &test_bus,
        Some(sink.clone() as Arc<dyn PromptCacheObservationSinkLike>),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-sonnet-4-5-20250929","stream":true,"system":[{"type":"text","text":"brief system","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles aborted prompt-cache stream");
    assert_eq!(response.status(), StatusCode::OK);
    response
        .into_body()
        .collect()
        .await
        .expect("aborted prompt-cache stream body collects");

    let (client_status, reason, prefix_token_counts, cache_usage_observed) =
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut prefix_token_counts = None;
            let mut cache_usage_observed = false;
            loop {
                match lifecycle_rx
                    .recv()
                    .await
                    .expect("lifecycle event delivered")
                {
                    LifecycleEvent::ParseCompleted {
                        result: Ok(info), ..
                    } => {
                        prefix_token_counts = Some(
                            info.cache_breakpoints
                                .into_iter()
                                .map(|breakpoint| breakpoint.prefix_token_count)
                                .collect::<Vec<_>>(),
                        );
                    }
                    LifecycleEvent::UsageObserved {
                        usage,
                        source: UsageSource::MessageStart,
                        ..
                    } => {
                        cache_usage_observed = usage.cache_creation_input_tokens == 12;
                    }
                    LifecycleEvent::RequestTerminated {
                        client_status,
                        reason,
                        ..
                    } => {
                        break (
                            client_status,
                            reason,
                            prefix_token_counts,
                            cache_usage_observed,
                        );
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("request terminates");

    assert_eq!(client_status, StatusCode::OK.as_u16());
    assert!(matches!(reason, TerminationReason::Success));
    let prefix_token_counts = prefix_token_counts.expect("request exposes cache breakpoints");
    assert_eq!(prefix_token_counts.len(), 2);
    assert!(
        prefix_token_counts.iter().all(|count| *count < 1_024),
        "every cache candidate must remain below the model threshold: {prefix_token_counts:?}",
    );
    assert!(
        cache_usage_observed,
        "message_start cache creation usage was not observed",
    );
    assert!(sink.records().is_empty());
}
// QA-B1: the production stream loop enqueues the cache observation at
// message_start — while the final upstream frames are still gated — and
// publishes exactly once per request.
#[tokio::test]
async fn message_start_publishes_cache_observation_before_final_frames() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let sink = Arc::new(RecordingObservationSink::default());
    let release = Arc::new(Notify::new());
    let lifecycle = prompt_cache_lifecycle_with(
        sse_dispatch(StatusCode::OK, gated_cache_sse_body(Arc::clone(&release))),
        &test_bus,
        Some(sink.clone() as Arc<dyn PromptCacheObservationSinkLike>),
    );

    let request = cacheable_stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request")
        .into_body();
    let frame = body
        .frame()
        .await
        .expect("first frame delivered")
        .expect("first frame ok");
    let frame = frame.into_data().expect("data frame");
    let text = std::str::from_utf8(&frame).expect("first frame is utf8");
    assert!(
        text.contains("message_start"),
        "first downstream frame must carry message_start: {text:?}",
    );

    // The final upstream frames are still gated: the observation was already
    // enqueued at message_start, not deferred to stream completion.
    let records = sink.records();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.upstream_id, uuid::Uuid::from_u128(1));
    assert_eq!(record.canonical_model_id, "claude-sonnet-4-5-20250929");
    assert_eq!(record.ttl_class, TtlClass::Ephemeral5m);
    assert!(
        record.estimated_prefix_tokens >= 1_024,
        "published prefix must clear the model cache minimum: {record:?}",
    );

    release.notify_one();
    let rest = body.collect().await.expect("body collects").to_bytes();
    let rest = std::str::from_utf8(&rest).expect("stream tail is utf8");
    assert!(
        rest.contains("message_stop"),
        "stream completes with message_stop: {rest:?}",
    );
    assert_eq!(
        sink.records().len(),
        1,
        "message_stop must not publish a duplicate observation",
    );

    assert_success_terminal(&mut lifecycle_rx, StatusCode::OK.as_u16()).await;
}

// QA-B2: the compat transform path reaches the same early publish at
// message_start.
#[tokio::test]
async fn transformed_sse_message_start_publishes_cache_observation_before_final_frames() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let sink = Arc::new(RecordingObservationSink::default());
    let release = Arc::new(Notify::new());
    let lifecycle = prompt_cache_transform_lifecycle(
        sse_dispatch(StatusCode::OK, gated_cache_sse_body(Arc::clone(&release))),
        &test_bus,
        sink.clone(),
    );

    let request = cacheable_stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request")
        .into_body();
    let frame = body
        .frame()
        .await
        .expect("first frame delivered")
        .expect("first frame ok");
    let frame = frame.into_data().expect("data frame");
    let text = std::str::from_utf8(&frame).expect("first frame is utf8");
    assert!(
        text.contains("message_start"),
        "first transformed frame must carry message_start: {text:?}",
    );

    let records = sink.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].canonical_model_id, "claude-sonnet-4-5-20250929");

    release.notify_one();
    let rest = body.collect().await.expect("body collects").to_bytes();
    let rest = std::str::from_utf8(&rest).expect("stream tail is utf8");
    assert!(
        rest.contains("message_stop"),
        "transformed stream completes with message_stop: {rest:?}",
    );
    assert_eq!(
        sink.records().len(),
        1,
        "message_stop must not publish a duplicate observation",
    );

    assert_success_terminal(&mut lifecycle_rx, StatusCode::OK.as_u16()).await;
}

// QA-B3: a store write that is still blocked does not delay the first
// downstream frame — the sink enqueue is non-blocking and the writer drains
// to the store off the request path.
#[tokio::test]
async fn first_frame_arrives_while_observation_store_write_is_blocked() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let store = Arc::new(GatedObservationStore::default());
    let (sink, writer) = channel_observation_sink(store.clone());
    let lifecycle = prompt_cache_lifecycle_with(
        sse_dispatch(StatusCode::OK, Body::from(cache_sse_body())),
        &test_bus,
        Some(sink.clone() as Arc<dyn PromptCacheObservationSinkLike>),
    );

    let request = cacheable_stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request")
        .into_body();
    let frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("first frame is not delayed by the blocked store write")
        .expect("first frame delivered")
        .expect("first frame ok");
    let frame = frame.into_data().expect("data frame");
    let text = std::str::from_utf8(&frame).expect("first frame is utf8");
    assert!(
        text.contains("message_start"),
        "first downstream frame must carry message_start: {text:?}",
    );

    // The write did start (the enqueue reached the writer) but is still gated.
    tokio::time::timeout(Duration::from_secs(1), store.started.notified())
        .await
        .expect("observation write reached the store");
    assert!(
        store.records().is_empty(),
        "gated store must not have committed yet",
    );

    store.release.notify_one();
    let _ = body.collect().await.expect("body collects");
    tokio::time::timeout(Duration::from_secs(1), store.committed.notified())
        .await
        .expect("observation commits once the store unblocks");
    assert_eq!(sink.dropped_total(), 0);
    drop(lifecycle);
    drop(sink);
    tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .expect("writer drains all accepted observations")
        .expect("writer exits cleanly");
    assert_eq!(store.records().len(), 1);

    assert_success_terminal(&mut lifecycle_rx, StatusCode::OK.as_u16()).await;
}

// QA-B4: a downstream disconnect after message_start neither retracts nor
// duplicates the accepted observation — it still commits exactly once.
#[tokio::test]
async fn disconnect_after_message_start_keeps_single_committed_observation() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let store = Arc::new(RecordingObservationStore::default());
    let (sink, writer) = channel_observation_sink(store.clone());
    let waiting = Arc::new(Notify::new());
    let lifecycle = prompt_cache_lifecycle_with(
        sse_dispatch(StatusCode::OK, gated_cache_sse_body(Arc::clone(&waiting))),
        &test_bus,
        Some(sink.clone() as Arc<dyn PromptCacheObservationSinkLike>),
    );

    let request = cacheable_stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request")
        .into_body();
    let _ = body
        .frame()
        .await
        .expect("first frame delivered")
        .expect("first frame ok");
    drop(body);

    assert_error_terminal(&mut lifecycle_rx, 499, "client_closed_request").await;
    waiting.notify_one();
    tokio::time::timeout(Duration::from_secs(1), store.committed.notified())
        .await
        .expect("accepted observation still commits after disconnect");
    assert_eq!(sink.dropped_total(), 0);
    drop(lifecycle);
    drop(sink);
    tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .expect("writer drains after disconnect")
        .expect("writer exits cleanly");
    assert_eq!(store.records().len(), 1);
}

// QA-B4: a downstream disconnect before message_start publishes nothing.
#[tokio::test]
async fn disconnect_before_message_start_publishes_no_observation() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let store = Arc::new(RecordingObservationStore::default());
    let (sink, writer) = channel_observation_sink(store.clone());
    let release = Arc::new(Notify::new());
    let polled = Arc::new(Notify::new());
    let upstream = {
        let release = Arc::clone(&release);
        let polled = Arc::clone(&polled);
        Body::from_stream(async_stream::stream! {
            polled.notify_one();
            release.notified().await;
            yield Ok::<Bytes, Infallible>(cache_message_start_frame());
            yield Ok::<Bytes, Infallible>(cache_stream_tail_frame());
        })
    };
    let lifecycle = prompt_cache_lifecycle_with(
        sse_dispatch(StatusCode::OK, upstream),
        &test_bus,
        Some(sink.clone() as Arc<dyn PromptCacheObservationSinkLike>),
    );
    let request = cacheable_stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request")
        .into_body();
    let poll_task = tokio::spawn(async move { body.frame().await });
    tokio::time::timeout(Duration::from_secs(1), polled.notified())
        .await
        .expect("relay awaits upstream");
    poll_task.abort();
    let _ = poll_task.await;

    assert_error_terminal(&mut lifecycle_rx, 499, "client_closed_request").await;
    assert_eq!(sink.enqueued_total(), 0);
    assert!(store.records().is_empty());
    writer.abort();
    let _ = writer.await;
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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let mut body = lifecycle
        .handle(request, &auth)
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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
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
async fn uncompressed_sse_terminal_body_error_reaches_eos_once()
-> Result<(), Box<dyn std::error::Error>> {
    let (headers, body) = terminal_body_error_case(None).await?;

    assert!(headers.get(CONTENT_ENCODING).is_none());
    let text = std::str::from_utf8(&body).expect("identity SSE output is utf8");
    assert_eq!(
        text.matches("event: error\n").count(),
        1,
        "identity SSE emits one client-visible error frame before EOS"
    );
    assert!(text.contains("\"api_error\""));
    Ok(())
}

#[tokio::test]
async fn compressed_sse_terminal_body_error_reaches_eos_without_plaintext_frame()
-> Result<(), Box<dyn std::error::Error>> {
    let (headers, body) = terminal_body_error_case(Some("gzip")).await?;

    assert_eq!(
        headers
            .get(CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok()),
        Some("gzip")
    );
    assert!(
        body.is_empty(),
        "compressed passthrough must not inject a plaintext SSE error frame"
    );
    Ok(())
}

async fn terminal_body_error_case(
    content_encoding: Option<&'static str>,
) -> Result<(HeaderMap, Bytes), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let BusReceiver::InMemory(mut update_rx) = test_bus.bus.subscribe() else {
        panic!("expected in-memory update receiver");
    };
    let dispatcher = match content_encoding {
        Some(content_encoding) => encoded_sse_dispatch(
            StatusCode::OK,
            persistent_upstream_frame_error(),
            content_encoding,
        ),
        None => sse_dispatch(StatusCode::OK, persistent_upstream_frame_error()),
    };
    let lifecycle = lifecycle(dispatcher, &test_bus);

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles persistent upstream body error");
    let (parts, body) = response.into_parts();
    let body = body
        .collect()
        .await
        .expect("downstream response reaches EOS after the first upstream body error")
        .to_bytes();

    assert_stream_error(
        &mut lifecycle_rx,
        "upstream_response_body_error",
        "forced persistent upstream frame error",
    )
    .await;
    assert_error_terminal(&mut lifecycle_rx, 200, "upstream_stream_error").await;
    assert_one_final(&mut update_rx).await;

    let rows = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].error_code.as_deref(),
        Some("upstream_stream_error"),
        "terminal upstream errors must not be reclassified as client_closed_request"
    );
    assert_eq!(
        rows[0].upstream_error_type.as_deref(),
        Some("upstream_response_body_error")
    );
    assert_eq!(
        rows[0].upstream_error_message.as_deref(),
        Some("forced persistent upstream frame error")
    );

    Ok((parts.headers, body))
}

#[tokio::test]
async fn upstream_frame_error_after_partial_event_starts_separate_error_frame() {
    let test_bus = TestLifecycleBus::new();
    let lifecycle = lifecycle(
        sse_dispatch(
            StatusCode::OK,
            upstream_frame_error_after(Bytes::from_static(
                b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\"",
            )),
        ),
        &test_bus,
    );

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles partial upstream event");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("relay body collects")
        .to_bytes();
    let text = std::str::from_utf8(&body).expect("stream output is utf8");

    assert!(
        text.contains("\"content_block_delta\"\n\nevent: error\n"),
        "error frame must start after an SSE event boundary: {text:?}",
    );
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

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
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

/// An Anthropic refusal is delivered to the client as a perfectly ordinary
/// HTTP 200 stream, so before this was fixed it persisted as
/// `status = 200, error_code = NULL` and was indistinguishable from a
/// successful request. An operator had no way to see that a refusal had
/// happened at all. The relayed bytes must stay untouched while the persisted
/// row carries the abnormal outcome.
#[tokio::test]
async fn streaming_refusal_persists_distinguishable_row() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let BusReceiver::InMemory(mut update_rx) = test_bus.bus.subscribe() else {
        panic!("expected in-memory update receiver");
    };
    let lifecycle = lifecycle(sse_dispatch(StatusCode::OK, refusal_sse_body()), &test_bus);

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle relays a refusal stream");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("relay body collects")
        .to_bytes();
    let text = std::str::from_utf8(&body).expect("relay body is SSE text");
    // The client must receive the refusal verbatim: cc-lb observes, never edits.
    assert!(text.contains("\"stop_reason\":\"refusal\""));
    assert_eq!(text.matches("event: message_stop").count(), 1);

    assert_one_final(&mut update_rx).await;
    let rows = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, 200);
    assert_eq!(rows[0].error_code.as_deref(), Some("upstream_refusal"));
    assert_eq!(rows[0].upstream_error_type.as_deref(), Some("refusal"));
    let message = rows[0]
        .upstream_error_message
        .as_deref()
        .expect("refusal records an upstream error message");
    assert!(
        message.starts_with("category=reasoning_extraction;"),
        "refusal category must be queryable from the row, got {message:?}"
    );
    // Stream accounting is still recorded: a refusal is a complete response.
    assert!(rows[0].body_bytes.is_some());
    Ok(())
}

#[tokio::test]
async fn provider_error_before_body_failure_preserves_provider_error()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let BusReceiver::InMemory(mut update_rx) = test_bus.bus.subscribe() else {
        panic!("expected in-memory update receiver");
    };
    let lifecycle = lifecycle(
        sse_dispatch(
            StatusCode::OK,
            upstream_frame_error_after(canonical_error_frame()),
        ),
        &test_bus,
    );

    let request = stream_request();
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles provider error before body failure");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("relay body collects")
        .to_bytes();
    let text = std::str::from_utf8(&body).expect("relay body is SSE text");
    assert_eq!(text.matches("event: error\n").count(), 1);

    assert_one_final(&mut update_rx).await;
    let rows = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].upstream_error_type.as_deref(),
        Some("overloaded_error")
    );
    assert_eq!(
        rows[0].upstream_error_message.as_deref(),
        Some("forced stream error")
    );
    assert!(rows[0].body_bytes.is_some());
    assert!(rows[0].body_chunk_count.is_some());
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
    observer.mark_authn_reached();

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
    drop(response.into_body());

    assert_success_terminal(&mut lifecycle_rx, 200).await;
}

async fn timeout_ordering(timeout_first: bool) {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let observer = LifecycleContext::new("timeout-ordering".to_owned(), test_bus.bus_arc(), &clock);
    observer.mark_authn_reached();
    let waiting = Arc::new(tokio::sync::Notify::new());
    let lifecycle = lifecycle(
        sse_dispatch(StatusCode::OK, pending_sse(waiting)),
        &test_bus,
    );
    let mut request = stream_request();
    request.extensions_mut().insert(observer.clone());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let body = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request")
        .into_body();
    let timeout = InternalFailure::tower_timeout(InternalError {
        stage: InternalErrorStage::Relay,
        kind: InternalErrorKind::Timeout,
        message: Some("request timed out".to_owned()),
    });
    if timeout_first {
        observer.terminate_failure(timeout.clone());
        drop(body);
    } else {
        drop(body);
        observer.terminate_failure(timeout);
    }
    drop(observer);

    assert_error_terminal(&mut lifecycle_rx, 504, "tower_timeout").await;
}

fn prompt_cache_lifecycle_with(
    dispatcher: Arc<dyn cc_lb_engine::UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
) -> Lifecycle {
    let authn = TestAuthn::new(TestState::default());
    let mut builder = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![UpstreamRecord {
            id: uuid::Uuid::from_u128(1),
            name: "test-upstream".to_owned(),
            kind: StorageUpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
            enabled: true,
            revision: 1,
            ..UpstreamRecord::default()
        }])
        .prompt_cache_observation_store(Arc::new(NoopPromptCacheObservationStore));
    if let Some(sink) = sink {
        builder = builder.prompt_cache_observation_sink(sink);
    }
    let view = builder.build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc())
}

fn stream_request() -> http::Request<Bytes> {
    messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ))
}
#[derive(Default)]
struct NoopPromptCacheObservationStore;

#[async_trait]
impl PromptCacheObservationStore for NoopPromptCacheObservationStore {
    async fn list_active_for_candidates(
        &self,
        _upstream_ids: &[uuid::Uuid],
        _canonical_model_id: &str,
        _v3_prefix_keys: &[String],
        _not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        Ok(Vec::new())
    }
}

fn cacheable_stream_request() -> http::Request<Bytes> {
    // One system block large enough to clear the Sonnet 4.5 cache minimum
    // (1,024 tokens) on the serialized-prefix fast path.
    let system_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(300);
    let body = format!(
        r#"{{"model":"claude-sonnet-4-5","stream":true,"system":[{{"type":"text","text":"{system_text}","cache_control":{{"type":"ephemeral"}}}}],"messages":[{{"role":"user","content":"hi"}}]}}"#,
    );
    messages_request(Bytes::from(body))
}

fn cache_message_start_frame() -> Bytes {
    Bytes::from_static(
        b"event: message_start\n\
          data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":4096,\"cache_creation_input_tokens\":4096,\"output_tokens\":0}}}\n\n",
    )
}

fn cache_stream_tail_frame() -> Bytes {
    Bytes::from_static(
        b"event: content_block_delta\n\
          data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n\
          event: message_stop\n\
          data: {\"type\":\"message_stop\"}\n\n",
    )
}

fn cache_sse_body() -> Bytes {
    let mut body = cache_message_start_frame().to_vec();
    body.extend_from_slice(&cache_stream_tail_frame());
    Bytes::from(body)
}

// Upstream body that emits message_start, then holds the remaining frames
// until `release` fires — the seam that proves the observation is enqueued
// before stream completion.
fn gated_cache_sse_body(release: Arc<Notify>) -> Body {
    Body::from_stream(async_stream::stream! {
        yield Ok::<Bytes, Infallible>(cache_message_start_frame());
        release.notified().await;
        yield Ok::<Bytes, Infallible>(cache_stream_tail_frame());
    })
}

#[derive(Default)]
struct RecordingObservationSink {
    records: Mutex<Vec<PromptCacheObservationRecord>>,
}

impl RecordingObservationSink {
    fn records(&self) -> Vec<PromptCacheObservationRecord> {
        self.records.lock().expect("records lock").clone()
    }
}

impl PromptCacheObservationSinkLike for RecordingObservationSink {
    fn enqueue(
        &self,
        record: PromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError> {
        self.records.lock().expect("records lock").push(record);
        Ok(())
    }
}

#[derive(Default)]
struct RecordingObservationStore {
    records: Mutex<Vec<PromptCacheObservationRecord>>,
    committed: Notify,
}

impl RecordingObservationStore {
    fn records(&self) -> Vec<PromptCacheObservationRecord> {
        self.records.lock().expect("records lock").clone()
    }
}

#[async_trait]
impl PromptCacheObservationStore for RecordingObservationStore {
    async fn upsert_observation(&self, record: &PromptCacheObservationRecord) -> StorageResult<()> {
        self.records
            .lock()
            .expect("records lock")
            .push(record.clone());
        self.committed.notify_one();
        Ok(())
    }

    async fn list_active_for_candidates(
        &self,
        _upstream_ids: &[uuid::Uuid],
        _canonical_model_id: &str,
        _v3_prefix_keys: &[String],
        _not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct GatedObservationStore {
    records: Mutex<Vec<PromptCacheObservationRecord>>,
    started: Notify,
    release: Notify,
    committed: Notify,
}

impl GatedObservationStore {
    fn records(&self) -> Vec<PromptCacheObservationRecord> {
        self.records.lock().expect("records lock").clone()
    }
}

#[async_trait]
impl PromptCacheObservationStore for GatedObservationStore {
    async fn upsert_observation(&self, record: &PromptCacheObservationRecord) -> StorageResult<()> {
        self.started.notify_one();
        self.release.notified().await;
        self.records
            .lock()
            .expect("records lock")
            .push(record.clone());
        self.committed.notify_one();
        Ok(())
    }

    async fn list_active_for_candidates(
        &self,
        _upstream_ids: &[uuid::Uuid],
        _canonical_model_id: &str,
        _v3_prefix_keys: &[String],
        _not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        Ok(Vec::new())
    }
}

// Mirrors the production PromptCacheObservationSink contract — bounded
// try_send enqueue plus a background writer draining to the store — because
// the real sink lives in cc-lb-server, which engine integration tests cannot
// depend on.
struct ChannelObservationSink {
    tx: mpsc::Sender<PromptCacheObservationRecord>,
    enqueued: AtomicU64,
    dropped: AtomicU64,
}

impl ChannelObservationSink {
    fn enqueued_total(&self) -> u64 {
        self.enqueued.load(Ordering::Relaxed)
    }

    fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl PromptCacheObservationSinkLike for ChannelObservationSink {
    fn enqueue(
        &self,
        record: PromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError> {
        match self.tx.try_send(record) {
            Ok(()) => {
                self.enqueued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Err(PromptCacheObservationEnqueueError::ChannelFull)
            }
            Err(TrySendError::Closed(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Err(PromptCacheObservationEnqueueError::ChannelClosed)
            }
        }
    }
}

fn channel_observation_sink(
    store: Arc<dyn PromptCacheObservationStore>,
) -> (Arc<ChannelObservationSink>, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel(16);
    let writer = tokio::spawn(async move {
        while let Some(record) = rx.recv().await {
            store
                .upsert_observation(&record)
                .await
                .expect("test store write succeeds");
        }
    });
    (
        Arc::new(ChannelObservationSink {
            tx,
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }),
        writer,
    )
}

// Compat-path dialect: shapes like the test upstream and forwards every SSE
// event unchanged so the transform hook path is exercised end to end.
struct PassthroughTransformDialect;

impl UpstreamDialect for PassthroughTransformDialect {
    fn shape(
        &self,
        ctx: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut url = Url::parse("http://upstream.local/").expect("test URL parses");
        url.set_path(ctx.path.trim_start_matches('/'));
        Ok(builder.shaped_request(
            url,
            ctx.method.clone(),
            ctx.downstream_headers.clone(),
            ctx.body_bytes.clone(),
        ))
    }

    fn sse_event_transform_hook(&self) -> Option<&dyn SseEventTransformHook> {
        Some(self)
    }
}

impl SseEventTransformHook for PassthroughTransformDialect {
    fn transform_sse_event(
        &self,
        _request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        Ok(TransformSseEventResult::Unchanged)
    }
}

fn prompt_cache_transform_lifecycle(
    dispatcher: Arc<dyn cc_lb_engine::UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    sink: Arc<RecordingObservationSink>,
) -> Lifecycle {
    let state = TestState::default();
    let dialect = Arc::new(PassthroughTransformDialect);
    let upstream_id = uuid::Uuid::from_u128(1);
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(Arc::new(RouterPipelineCache::empty(
                TerminalStrategy::FirstPick,
            ))),
            DialectCache::Explicit(ShapePluginCache { dialect }),
        ),
    );
    let principal = PrincipalRecord {
        id: uuid::Uuid::from_u128(2),
        name: "principal-test".to_owned(),
        kind: StoragePrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: vec![upstream_id],
        default_limits: Vec::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: TerminalStrategy::FirstPick,
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
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![UpstreamRecord {
            id: upstream_id,
            name: "test-upstream".to_owned(),
            kind: StorageUpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            revision: 1,
            ..UpstreamRecord::default()
        }])
        .prompt_cache_observation_store(Arc::new(NoopPromptCacheObservationStore))
        .prompt_cache_observation_sink(sink)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc())
}
