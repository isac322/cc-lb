mod events;

pub use events::{
    assert_dropped_terminal, assert_error_terminal, assert_one_final, assert_stream_error,
    assert_success_terminal, lifecycle_receiver, sqlite_storage,
};

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{Principal, TerminalStrategy, Upstream};
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_storage_api::principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, SseEvent, SseEventTransformHook, TransformSseEventRequest,
    TransformSseEventResult, UpstreamDialect,
};
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderValue, Response, StatusCode};
use http_body::{Body as HttpBody, Frame};
use tokio::sync::Notify;
use url::Url;

use super::common::{TestAuthn, TestLifecycleBus, TestState};

pub fn lifecycle(dispatcher: Arc<dyn UpstreamDispatch>, test_bus: &TestLifecycleBus) -> Lifecycle {
    lifecycle_with_upstream_kind(dispatcher, test_bus, StorageUpstreamKind::AnthropicApiKey)
}

pub fn lifecycle_with_upstream_kind(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    upstream_kind: StorageUpstreamKind,
) -> Lifecycle {
    lifecycle_with_upstream_kind_and_refresh(dispatcher, test_bus, upstream_kind, true)
}

pub fn lifecycle_with_upstream_kind_and_refresh(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    upstream_kind: StorageUpstreamKind,
    refresh_allowed: bool,
) -> Lifecycle {
    let state = TestState::default();
    let mut authn = TestAuthn::new(state.clone());
    authn.refresh_allowed = refresh_allowed;
    let upstream_id = uuid::Uuid::from_u128(1);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![UpstreamRecord {
            id: upstream_id,
            name: "test-upstream".to_owned(),
            kind: upstream_kind,
            base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            revision: 1,
            ..UpstreamRecord::default()
        }])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc())
}

pub fn transform_lifecycle(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
) -> Lifecycle {
    transform_lifecycle_with_kind(dispatcher, test_bus, StorageUpstreamKind::AnthropicApiKey)
}

pub fn transform_lifecycle_with_kind(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    upstream_kind: StorageUpstreamKind,
) -> Lifecycle {
    transform_lifecycle_with_dialect(
        dispatcher,
        test_bus,
        upstream_kind,
        Arc::new(FailAfterFirstDialect::default()),
    )
}

pub fn responses_replace_lifecycle_with_kind(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    upstream_kind: StorageUpstreamKind,
) -> Lifecycle {
    transform_lifecycle_with_dialect(
        dispatcher,
        test_bus,
        upstream_kind,
        Arc::new(ResponsesSplitDialect {
            provider_failure: ProviderFailureTransform::PassThrough,
        }),
    )
}

pub fn responses_replace_provider_failure_lifecycle_with_kind(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    upstream_kind: StorageUpstreamKind,
) -> Lifecycle {
    transform_lifecycle_with_dialect(
        dispatcher,
        test_bus,
        upstream_kind,
        Arc::new(ResponsesSplitDialect {
            provider_failure: ProviderFailureTransform::Replace,
        }),
    )
}

pub fn responses_drop_provider_failure_lifecycle_with_kind(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    upstream_kind: StorageUpstreamKind,
) -> Lifecycle {
    transform_lifecycle_with_dialect(
        dispatcher,
        test_bus,
        upstream_kind,
        Arc::new(ResponsesSplitDialect {
            provider_failure: ProviderFailureTransform::Drop,
        }),
    )
}

fn transform_lifecycle_with_dialect(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
    upstream_kind: StorageUpstreamKind,
    dialect: Arc<dyn UpstreamDialect>,
) -> Lifecycle {
    let state = TestState::default();
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
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![UpstreamRecord {
            id: upstream_id,
            name: "test-upstream".to_owned(),
            kind: upstream_kind,
            base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            revision: 1,
            ..UpstreamRecord::default()
        }])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc())
}

pub fn sse_dispatch(status: StatusCode, body: Body) -> Arc<dyn UpstreamDispatch> {
    sse_dispatch_with_encoding(status, body, None)
}

pub fn encoded_sse_dispatch(
    status: StatusCode,
    body: Body,
    content_encoding: &'static str,
) -> Arc<dyn UpstreamDispatch> {
    sse_dispatch_with_encoding(status, body, Some(content_encoding))
}

fn sse_dispatch_with_encoding(
    status: StatusCode,
    body: Body,
    content_encoding: Option<&'static str>,
) -> Arc<dyn UpstreamDispatch> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    if let Some(content_encoding) = content_encoding {
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static(content_encoding));
    }
    Arc::new(OneResponseDispatch::new(response))
}

pub fn json_dispatch(status: StatusCode, body: Bytes) -> Arc<dyn UpstreamDispatch> {
    json_body_dispatch(status, Body::from(body))
}

pub fn json_body_dispatch(status: StatusCode, body: Body) -> Arc<dyn UpstreamDispatch> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Arc::new(OneResponseDispatch::new(response))
}

pub fn pending_sse(waiting: Arc<Notify>) -> Body {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, Infallible>(normal_sse_frame());
        waiting.notify_one();
        std::future::pending::<()>().await;
    };
    Body::from_stream(stream)
}

pub fn upstream_frame_error() -> Body {
    let stream = async_stream::stream! {
        yield Err::<Bytes, std::io::Error>(std::io::Error::other(
            "forced upstream frame error",
        ));
    };
    Body::from_stream(stream)
}

// Models an h2 body that remains error-ready after a reset. The relay must treat the first
// error as terminal; a second poll panics so the regression fails promptly instead of hanging.
pub fn persistent_upstream_frame_error() -> Body {
    Body::new(PersistentReadyErrorBody::default())
}

#[derive(Default)]
struct PersistentReadyErrorBody {
    error_returned: bool,
}

impl HttpBody for PersistentReadyErrorBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        assert!(
            !self.error_returned,
            "upstream body was polled after its first terminal error"
        );
        self.error_returned = true;
        Poll::Ready(Some(Err(std::io::Error::other(
            "forced persistent upstream frame error",
        ))))
    }
}

pub fn upstream_frame_error_after(frame: Bytes) -> Body {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(frame);
        yield Err::<Bytes, std::io::Error>(std::io::Error::other(
            "forced upstream frame error",
        ));
    };
    Body::from_stream(stream)
}

pub fn normal_sse_frame() -> Bytes {
    Bytes::from_static(
        b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0}\n\n",
    )
}

pub fn canonical_error_frame() -> Bytes {
    Bytes::from_static(
        b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"forced stream error\"}}\n\n",
    )
}

/// A complete, well-formed Anthropic refusal stream: HTTP 200 with
/// `stop_reason: "refusal"` and populated `stop_details`, and no content
/// blocks. Captured from `api.anthropic.com`.
pub fn refusal_sse_body() -> Body {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, Infallible>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"id\":\"msg_refusal\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"stop_reason\":null,\"stop_details\":null,\"usage\":{\"input_tokens\":80,\"output_tokens\":0}}}\n\n",
        ));
        yield Ok::<Bytes, Infallible>(Bytes::from_static(
            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"refusal\",\"stop_sequence\":null,\"stop_details\":{\"type\":\"refusal\",\"category\":\"reasoning_extraction\",\"explanation\":\"This request was blocked as it seems to violate Anthropic's Terms of Service.\",\"fallback_credit_token\":null}},\"usage\":{\"input_tokens\":80,\"output_tokens\":0}}\n\n",
        ));
        yield Ok::<Bytes, Infallible>(Bytes::from_static(
            b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ));
    };
    Body::from_stream(stream)
}

pub fn transform_body() -> Body {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, Infallible>(normal_sse_frame());
        yield Ok::<Bytes, Infallible>(normal_sse_frame());
    };
    Body::from_stream(stream)
}

struct OneResponseDispatch {
    response: Mutex<Option<Response<Body>>>,
}

impl OneResponseDispatch {
    fn new(response: Response<Body>) -> Self {
        Self {
            response: Mutex::new(Some(response)),
        }
    }
}

#[async_trait]
impl UpstreamDispatch for OneResponseDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.response
            .lock()
            .map_err(|_| DispatchError::Transport {
                source: Box::new(std::io::Error::other("test response lock poisoned")),
            })?
            .take()
            .ok_or_else(|| DispatchError::Transport {
                source: Box::new(std::io::Error::other("test response already consumed")),
            })
    }
}

#[derive(Default)]
struct FailAfterFirstDialect {
    calls: AtomicU64,
}

impl UpstreamDialect for FailAfterFirstDialect {
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

impl SseEventTransformHook for FailAfterFirstDialect {
    fn transform_sse_event(
        &self,
        _request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            return Ok(TransformSseEventResult::Unchanged);
        }
        Err(ResponseTransformError::Runtime {
            reason: "forced post-output failure".to_owned(),
        })
    }
}

#[derive(Clone, Copy)]
enum ProviderFailureTransform {
    PassThrough,
    Replace,
    Drop,
}

struct ResponsesSplitDialect {
    provider_failure: ProviderFailureTransform,
}

impl UpstreamDialect for ResponsesSplitDialect {
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

impl SseEventTransformHook for ResponsesSplitDialect {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        const DOWNSTREAM_ID: &str = "resp_downstream";
        match request.event.event.as_str() {
            "response.created" | "response.completed" => {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&request.event.data).expect("response JSON");
                value["response"]["id"] = serde_json::Value::String(DOWNSTREAM_ID.to_owned());
                Ok(TransformSseEventResult::Replace {
                    events: vec![SseEvent {
                        event: request.event.event,
                        data: Bytes::from(
                            serde_json::to_vec(&value).expect("response replacement JSON"),
                        ),
                    }],
                })
            }
            "response.output_text.delta" => {
                let mut first: serde_json::Value =
                    serde_json::from_slice(&request.event.data).expect("delta JSON");
                first["response_id"] = serde_json::Value::String(DOWNSTREAM_ID.to_owned());
                let sequence = first["sequence_number"]
                    .as_u64()
                    .expect("delta sequence number");
                let mut second = first.clone();
                second["sequence_number"] = serde_json::json!(sequence.saturating_add(1));
                second["delta"] = serde_json::Value::String("n+1".to_owned());
                Ok(TransformSseEventResult::Replace {
                    events: vec![
                        SseEvent {
                            event: request.event.event.clone(),
                            data: Bytes::from(
                                serde_json::to_vec(&first).expect("first delta JSON"),
                            ),
                        },
                        SseEvent {
                            event: request.event.event,
                            data: Bytes::from(
                                serde_json::to_vec(&second).expect("second delta JSON"),
                            ),
                        },
                    ],
                })
            }
            "response.failed" => match self.provider_failure {
                ProviderFailureTransform::PassThrough => Ok(TransformSseEventResult::Unchanged),
                ProviderFailureTransform::Drop => Ok(TransformSseEventResult::Drop),
                ProviderFailureTransform::Replace => {
                    let value: serde_json::Value =
                        serde_json::from_slice(&request.event.data).expect("failure JSON");
                    let sequence = value["sequence_number"]
                        .as_u64()
                        .expect("failure sequence number");
                    Ok(TransformSseEventResult::Replace {
                        events: vec![SseEvent {
                            event: "response.output_text.delta".to_owned(),
                            data: Bytes::from(
                                serde_json::json!({
                                    "type": "response.output_text.delta",
                                    "sequence_number": sequence,
                                    "response_id": DOWNSTREAM_ID,
                                    "delta": "provider failure transformed",
                                })
                                .to_string(),
                            ),
                        }],
                    })
                }
            },
            _ => Ok(TransformSseEventResult::Unchanged),
        }
    }
}

pub fn sse_json_events(body: &[u8]) -> Vec<(String, serde_json::Value)> {
    let text = std::str::from_utf8(body).expect("SSE body is UTF-8");
    let normalized = text.replace("\r\n", "\n");
    normalized
        .split("\n\n")
        .filter_map(|frame| {
            let mut name = "message";
            let mut data = Vec::new();
            for line in frame.lines() {
                if let Some(value) = line.strip_prefix("event:") {
                    name = value.trim_start();
                } else if let Some(value) = line.strip_prefix("data:") {
                    data.push(value.trim_start());
                }
            }
            if data.is_empty() {
                return None;
            }
            let data = data.join("\n");
            Some((
                name.to_owned(),
                serde_json::from_str(&data).expect("each relayed SSE event has valid JSON data"),
            ))
        })
        .collect()
}

pub fn assert_responses_failed(
    body: &[u8],
    response_id: Option<&str>,
    last_sequence: u64,
) -> serde_json::Value {
    let events = sse_json_events(body);
    assert_eq!(
        events
            .iter()
            .filter(|(name, _)| name == "response.failed")
            .count(),
        1,
        "one client-visible Responses failure: {events:?}",
    );
    assert!(
        events.iter().all(|(name, value)| {
            name != "error" && name != "response.completed" && value["type"] != "response.completed"
        }),
        "local Responses failure is neither a generic error nor completion: {events:?}",
    );
    let (name, failure) = events.last().expect("terminal SSE event");
    assert_eq!(name, "response.failed");
    assert_eq!(failure["type"], "response.failed");
    assert_eq!(failure["sequence_number"].as_u64(), Some(last_sequence + 1));
    assert_eq!(failure["response"]["object"], "response");
    assert_eq!(failure["response"]["status"], "failed");
    assert_eq!(failure["response"]["id"].as_str(), response_id);
    assert!(
        failure["response"]["error"]["code"]
            .as_str()
            .is_some_and(|code| !code.is_empty()),
        "failure supplies a native error code: {failure}",
    );
    assert!(
        failure["response"]["error"]["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()),
        "failure supplies a client-visible message: {failure}",
    );
    assert!(
        failure.get("error").is_none(),
        "no HTTP error envelope in SSE"
    );
    failure.clone()
}

pub async fn assert_stream_failure_once(
    receiver: &mut tokio::sync::broadcast::Receiver<LifecycleEvent>,
    expected_type: &str,
    expected_status: u16,
    expected_code: &str,
) {
    let mut events = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = receiver.recv().await.expect("lifecycle event delivered");
            let terminal = matches!(event, LifecycleEvent::RequestTerminated { .. });
            events.push(event);
            if terminal {
                break;
            }
        }
    })
    .await
    .expect("request terminates");
    events.extend(std::iter::from_fn(|| receiver.try_recv().ok()));
    let completions = events
        .iter()
        .filter_map(|event| match event {
            LifecycleEvent::StreamCompleted { result, .. } => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completions.len(),
        1,
        "one stream accounting result: {events:?}"
    );
    let error = completions[0]
        .as_ref()
        .expect_err("failure is never success");
    assert_eq!(error.error_type, expected_type);
    let terminals = events
        .iter()
        .filter_map(|event| match event {
            LifecycleEvent::RequestTerminated {
                client_status,
                reason,
                ..
            } => Some((*client_status, reason)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(terminals.len(), 1, "exactly one terminal outcome");
    assert_eq!(terminals[0].0, expected_status);
    assert!(matches!(terminals[0].1, TerminationReason::ErrorCode(code) if code == expected_code));
}
