use std::sync::{Arc, Mutex};

use ::http::{HeaderMap, HeaderValue, Response, StatusCode};
use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{ApiKeyAwareSignerFactory, UpstreamDispatch};
use cc_lb_plugin_api::{
    Principal, RequestContext, RetryDecision, RouteDecision, RouteError, RouterPlugin,
    ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, Upstream,
    UpstreamCandidate,
};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use serde_json::{Value, json};

use crate::cache_keepalive_enqueuer::CacheKeepaliveTaskPusher;

#[derive(Default)]
pub(in crate::scheduler_dispatch::tests) struct RecordingHttp {
    pub(in crate::scheduler_dispatch::tests) requests: Mutex<Vec<CapturedRequest>>,
    response: Mutex<RecordingHttpResponse>,
}

enum RecordingHttpResponse {
    Json(Value),
    Status(StatusCode, Value),
}

impl Default for RecordingHttpResponse {
    fn default() -> Self {
        Self::Json(json!({
            "content": [],
            "stop_reason": "max_tokens",
            "usage": {"cache_read_input_tokens": 10, "input_tokens": 0, "output_tokens": 0}
        }))
    }
}

impl RecordingHttp {
    pub(in crate::scheduler_dispatch::tests) fn return_cache_miss(&self) {
        *self.response.lock().expect("response lock") = RecordingHttpResponse::Json(json!({
            "content": [],
            "stop_reason": "max_tokens",
            "usage": {"cache_read_input_tokens": 0, "input_tokens": 0, "output_tokens": 0}
        }));
    }

    pub(in crate::scheduler_dispatch::tests) fn return_status(
        &self,
        status: StatusCode,
        body: Value,
    ) {
        *self.response.lock().expect("response lock") = RecordingHttpResponse::Status(status, body);
    }
}

#[derive(Clone)]
pub(in crate::scheduler_dispatch::tests) struct CapturedRequest {
    pub(in crate::scheduler_dispatch::tests) url: String,
    pub(in crate::scheduler_dispatch::tests) headers: HeaderMap,
    pub(in crate::scheduler_dispatch::tests) body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for RecordingHttp {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
        self.requests
            .lock()
            .expect("requests lock")
            .push(CapturedRequest {
                url: request.url().to_string(),
                headers: request.headers().clone(),
                body: request.body().clone(),
            });
        match &*self.response.lock().expect("response lock") {
            RecordingHttpResponse::Json(body) => Ok(json_response(StatusCode::OK, body.clone())),
            RecordingHttpResponse::Status(status, body) => Ok(json_response(*status, body.clone())),
        }
    }
}

pub(super) struct RecordingSignerFactory {
    pub(super) calls: Arc<Mutex<Vec<String>>>,
}

impl ApiKeyAwareSignerFactory for RecordingSignerFactory {
    fn with_router_choice(
        &self,
        api_key: String,
        router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        self.calls
            .lock()
            .expect("signer calls lock")
            .push(format!("{router_chosen_upstream_name}:{api_key}"));
        Arc::new(RecordingSigner)
    }
}

struct RecordingSigner;

pub(super) struct NoRouteRouter;

impl RouterPlugin for NoRouteRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "test routes through stored upstream".to_owned(),
        })
    }
}

#[async_trait]
impl SignerFactory for RecordingSigner {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(RecordingSigner))
    }
}

#[async_trait]
impl Signer for RecordingSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("sk-ant-rotated"));
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_plugin_api::UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

pub(in crate::scheduler_dispatch::tests) struct FailingPusher;

#[async_trait]
impl CacheKeepaliveTaskPusher for FailingPusher {
    async fn push_cache_keepalive_task(
        &self,
        _task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()> {
        Err(SchedulerError::Job(
            "forced reschedule enqueue failure".to_owned(),
        ))
    }
}

fn json_response(status: StatusCode, body: Value) -> Response<Body> {
    let mut response = Response::new(Body::from(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        ::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}
