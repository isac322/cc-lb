mod events;

pub use events::{
    assert_dropped_terminal, assert_error_terminal, assert_one_final, assert_success_terminal,
    lifecycle_receiver, sqlite_storage,
};

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalRoutingArtifacts, PrincipalView,
    RouterPipelineCache, ShapePluginCache,
};
use cc_lb_engine::{
    DispatchError, DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig,
    UpstreamDispatch,
};
use cc_lb_plugin_api::{Principal, RequestContext, TerminalStrategy, Upstream};
use cc_lb_storage_api::principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, SseEventTransformHook, TransformSseEventRequest, TransformSseEventResult,
    UpstreamDialect,
};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Response, StatusCode};
use tokio::sync::Notify;
use url::Url;

use crate::common::{RecordingHook, TestAuthn, TestLifecycleBus, TestRouter, TestState};

pub fn lifecycle(dispatcher: Arc<dyn UpstreamDispatch>, test_bus: &TestLifecycleBus) -> Lifecycle {
    let state = TestState::default();
    crate::common::lifecycle_with_parts(
        TestAuthn::new(state),
        Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        dispatcher,
        vec![Arc::new(RecordingHook::default())],
        cc_lb_engine::LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc())
}

pub fn transform_lifecycle(
    dispatcher: Arc<dyn UpstreamDispatch>,
    test_bus: &TestLifecycleBus,
) -> Lifecycle {
    let state = TestState::default();
    let dialect = Arc::new(FailAfterFirstDialect::default());
    let upstream_id = uuid::Uuid::from_u128(1);
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
        .global_observability_hooks(vec![Arc::new(RecordingHook::default())])
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

pub fn sse_dispatch(status: StatusCode, body: Body) -> Arc<dyn UpstreamDispatch> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    Arc::new(OneResponseDispatch::new(response))
}

pub fn json_dispatch(status: StatusCode, body: Bytes) -> Arc<dyn UpstreamDispatch> {
    let mut response = Response::new(Body::from(body));
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
                reason: "test response lock poisoned".to_owned(),
            })?
            .take()
            .ok_or_else(|| DispatchError::Transport {
                reason: "test response already consumed".to_owned(),
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
