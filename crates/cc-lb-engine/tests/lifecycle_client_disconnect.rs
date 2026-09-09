mod client_disconnect_support;
use crate::common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{BusReceiver, RequestEventBus};
use cc_lb_engine::{
    DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig, LifecycleContext,
    NoopSubscriptionQuotaCache,
};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason, UsageSource};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{RequestEventStore, Storage as StorageTrait};
use http::header::CONTENT_ENCODING;
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;

use client_disconnect_support::{
    assert_dropped_terminal, assert_error_terminal, assert_one_final, assert_stream_error,
    assert_success_terminal, canonical_error_frame, encoded_sse_dispatch, json_dispatch, lifecycle,
    lifecycle_receiver, normal_sse_frame, pending_sse, persistent_upstream_frame_error,
    sqlite_storage, sse_dispatch, transform_body, transform_lifecycle, upstream_frame_error,
    upstream_frame_error_after,
};
use common::{RecordingHook, TestAuthn, TestLifecycleBus, TestRouter, TestState, messages_request};
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
async fn aborted_sse_with_only_below_threshold_cache_candidates_emits_no_observation_event() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_rx = lifecycle_receiver(&test_bus);
    let lifecycle = prompt_cache_lifecycle(
        sse_dispatch(
            StatusCode::OK,
            Body::from(Bytes::from_static(
                b"event: message_start\n\
                  data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":12,\"cache_creation_input_tokens\":12,\"output_tokens\":0}}}\n\n",
            )),
        ),
        &test_bus,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-sonnet-4-5-20250929","stream":true,"system":[{"type":"text","text":"brief system","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}]}"#,
        )))
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
                    LifecycleEvent::PromptCacheObservationsProduced {
                        observations,
                        dropped_below_threshold,
                        dropped_aborted,
                        ..
                    } => panic!(
                        "aborted below-threshold stream published an observation event: \
                         observations={}, dropped_below_threshold={dropped_below_threshold}, \
                         dropped_aborted={dropped_aborted}",
                        observations.len(),
                    ),
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
    assert!(
        std::iter::from_fn(|| lifecycle_rx.try_recv().ok()).all(|event| !matches!(
            event,
            LifecycleEvent::PromptCacheObservationsProduced { .. }
        )),
        "prompt-cache observation event was published after request termination",
    );
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

    let response = lifecycle
        .handle(stream_request())
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

    let response = lifecycle
        .handle(stream_request())
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

    let response = lifecycle
        .handle(stream_request())
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

fn prompt_cache_lifecycle(
    dispatcher: Arc<dyn cc_lb_engine::UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
) -> Lifecycle {
    let authn = TestAuthn::new(TestState::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }))
        .global_observability_hooks(vec![Arc::new(RecordingHook::default())])
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![UpstreamRecord {
            id: uuid::Uuid::from_u128(1),
            name: "test-upstream".to_owned(),
            kind: StorageUpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            revision: 1,
            ..UpstreamRecord::default()
        }])
        .prompt_cache_observation_cache(Arc::new(NoopSubscriptionQuotaCache))
        .build();
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
