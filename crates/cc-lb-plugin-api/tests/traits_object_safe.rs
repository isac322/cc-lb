use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_plugin_api::{
    AuthnError, AuthnOutcome, AuthnPlugin, DialectError, ObservabilityError, ObservabilityHook,
    ObserveEvent, PluginManifest, PluginRuntime, Principal, PrincipalKind, PrincipalQuotas,
    RequestContext, RetryDecision, RouteDecision, RouteError, RouterPlugin, ShapedRequest,
    ShapedRequestBuilder, SignedRequest, Signer, SignerError, SignerFactory, SigningCapability,
    Upstream, UpstreamDialect,
};
use http::{HeaderMap, Method, StatusCode};

struct DummyAuthn;
struct DummyRouter;
struct DummyDialect;
struct DummySigner;
struct DummyFactory;
struct DummyObserve;
struct DummyRuntime;

#[async_trait]
impl AuthnPlugin for DummyAuthn {
    async fn authenticate(&self, _ctx: &RequestContext) -> Result<AuthnOutcome, AuthnError> {
        Ok(AuthnOutcome {
            principal: Principal {
                id: "alice".to_owned(),
                kind: PrincipalKind::ApiKey,
                claims: serde_json::Map::new(),
            },
            signer_factory: Arc::new(DummyFactory),
            quotas: PrincipalQuotas {
                requests_per_window: 1,
                input_tokens_per_window: 1,
                output_tokens_per_window: 1,
                window: std::time::Duration::from_secs(1),
                allowed_models: Vec::new(),
            },
        })
    }
}

impl RouterPlugin for DummyRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
    ) -> Result<RouteDecision, RouteError> {
        Ok(RouteDecision {
            upstream: Upstream::AnthropicDirect,
            dialect: Box::new(DummyDialect),
        })
    }
}

impl UpstreamDialect for DummyDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
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

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
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

    async fn on_unauthorized(&self, _err: &cc_lb_plugin_api::UpstreamError) -> RetryDecision {
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

impl PluginRuntime for DummyRuntime {
    fn instantiate(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn AuthnPlugin>, cc_lb_plugin_api::RuntimeError> {
        Ok(Arc::new(DummyAuthn))
    }

    fn instantiate_router(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn RouterPlugin>, cc_lb_plugin_api::RuntimeError> {
        Ok(Arc::new(DummyRouter))
    }

    fn instantiate_dialect(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn UpstreamDialect>, cc_lb_plugin_api::RuntimeError> {
        Ok(Arc::new(DummyDialect))
    }

    fn instantiate_signer_factory(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn SignerFactory>, cc_lb_plugin_api::RuntimeError> {
        Ok(Arc::new(DummyFactory))
    }

    fn instantiate_observability(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn ObservabilityHook>, cc_lb_plugin_api::RuntimeError> {
        Ok(Arc::new(DummyObserve))
    }
}

#[test]
fn all_traits_are_object_safe() {
    let _: Box<dyn AuthnPlugin> = Box::new(DummyAuthn);
    let _: Box<dyn RouterPlugin> = Box::new(DummyRouter);
    let _: Box<dyn UpstreamDialect> = Box::new(DummyDialect);
    let _: Box<dyn Signer> = Box::new(DummySigner);
    let _: Box<dyn SignerFactory> = Box::new(DummyFactory);
    let _: Box<dyn ObservabilityHook> = Box::new(DummyObserve);
    let _: Box<dyn PluginRuntime> = Box::new(DummyRuntime);
}
