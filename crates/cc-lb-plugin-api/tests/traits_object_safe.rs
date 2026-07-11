use async_trait::async_trait;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_plugin_api::{
    ObservabilityError, ObservabilityHook, ObserveEvent, Principal, Upstream, UpstreamCandidate,
};
use cc_lb_routing::{RouteDecision, RouteError, RouterPlugin, RoutingContext};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, ResponseTransformHook,
    RetryDecision, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, SseEventTransformHook, TransformResponseRequest,
    TransformResponseResult, TransformSseEventRequest, TransformSseEventResult, UpstreamDialect,
    UpstreamError,
};
use http::{HeaderMap, Method};

struct DummyRouter;
struct DummyDialect;
struct DummySigner;
struct DummyFactory;
struct DummyObserve;
struct DummyResponseTransform;
struct DummySseTransform;

impl RouterPlugin for DummyRouter {
    fn route(
        &self,
        _context: &RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Ok(RouteDecision {
            upstream_id: None,
            upstream: Upstream::AnthropicDirect { base_url: None },
            dialect: Arc::new(DummyDialect),
        })
    }
}

impl UpstreamDialect for DummyDialect {
    fn shape(
        &self,
        _context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            "https://api.anthropic.com/v1/messages".parse().unwrap(),
            Method::POST,
            HeaderMap::new(),
            Bytes::new(),
        ))
    }
}

#[async_trait]
impl Signer for DummySigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

#[async_trait]
impl SignerFactory for DummyFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(DummySigner))
    }
}

impl ObservabilityHook for DummyObserve {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}

impl ResponseTransformHook for DummyResponseTransform {
    fn transform_response(
        &self,
        _request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        Ok(TransformResponseResult::Unchanged)
    }
}

impl SseEventTransformHook for DummySseTransform {
    fn transform_sse_event(
        &self,
        _request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        Ok(TransformSseEventResult::Unchanged)
    }
}

#[test]
fn all_traits_are_object_safe() {
    let _: Box<dyn RouterPlugin> = Box::new(DummyRouter);
    let _: Box<dyn UpstreamDialect> = Box::new(DummyDialect);
    let _: Box<dyn Signer> = Box::new(DummySigner);
    let _: Box<dyn SignerFactory> = Box::new(DummyFactory);
    let _: Box<dyn ObservabilityHook> = Box::new(DummyObserve);
    let _: Box<dyn ResponseTransformHook> = Box::new(DummyResponseTransform);
    let _: Box<dyn SseEventTransformHook> = Box::new(DummySseTransform);
}
