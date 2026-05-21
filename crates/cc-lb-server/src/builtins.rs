use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_config::{AuthStrategy as ConfigAuthStrategy, Config, UpstreamKind, UpstreamSpec};
use cc_lb_dialect_anthropic::{AnthropicDirectDialect, CustomAnthropicSpecDialect};
use cc_lb_dialect_bedrock::{BedrockMantleDialect, BedrockRuntimeDialect};
use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::{
    AuthStrategy, AuthnError, AuthnOutcome, AuthnPlugin, DialectError, ObservabilityError,
    ObservabilityHook, ObserveEvent, Principal, PrincipalKind, PrincipalQuotas, RequestContext,
    RouteDecision, RouteError, RouterPlugin, ShapedRequest, ShapedRequestBuilder, SignerError,
    SignerFactory, Upstream, UpstreamDialect,
};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactory;
use cc_lb_signer_aws::AwsSigV4SignerFactory;
use cc_lb_signer_gcp::GcpOAuthSignerFactory;
use cc_lb_storage_redb::Storage;
use http::StatusCode;
use oauth2::{ClientId, TokenUrl};
use serde_json::Map;

#[derive(Clone)]
pub struct BuiltinAuthn {
    defaults: PrincipalQuotas,
    principal_quotas: HashMap<String, PrincipalQuotas>,
    signer_factory: Arc<CompositeSignerFactory>,
}

impl BuiltinAuthn {
    pub fn new(config: &Config, storage: Option<Arc<Storage>>) -> Self {
        let defaults = default_quotas(config);
        let principal_quotas = config
            .principals
            .iter()
            .map(|(name, principal)| {
                let quotas = principal.quotas.as_ref().map_or_else(
                    || defaults.clone(),
                    |quotas| PrincipalQuotas {
                        requests_per_window: quotas.default_requests_per_window,
                        input_tokens_per_window: quotas.default_input_tokens,
                        output_tokens_per_window: quotas.default_output_tokens,
                        window: Duration::from_secs(quotas.default_window_secs.max(1)),
                        allowed_models: principal.allowed_models.clone(),
                    },
                );
                (name.clone(), quotas)
            })
            .collect();

        Self {
            defaults,
            principal_quotas,
            signer_factory: Arc::new(CompositeSignerFactory::new(config, storage)),
        }
    }
}

#[async_trait]
impl AuthnPlugin for BuiltinAuthn {
    async fn authenticate(&self, ctx: &RequestContext) -> Result<AuthnOutcome, AuthnError> {
        let Some(api_key) = ctx
            .downstream_headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
        else {
            return Err(AuthnError::InvalidCredentials {
                reason: "missing x-api-key header".to_owned(),
            });
        };

        let principal_id = "api-key".to_owned();
        let quotas = self
            .principal_quotas
            .get(&principal_id)
            .cloned()
            .unwrap_or_else(|| self.defaults.clone());
        let signer_factory = Arc::new(self.signer_factory.with_api_key(api_key.to_owned()));

        Ok(AuthnOutcome {
            principal: Principal {
                id: principal_id,
                kind: PrincipalKind::ApiKey,
                claims: Map::new(),
            },
            signer_factory,
            quotas,
        })
    }
}

#[derive(Clone)]
pub struct BuiltinRouter {
    route: BuiltinRoute,
}

impl BuiltinRouter {
    pub fn new(config: &Config) -> Result<Self, BuiltinError> {
        let (name, spec) = config
            .upstreams
            .iter()
            .next()
            .ok_or(BuiltinError::NoUpstreams)?;
        Ok(Self {
            route: BuiltinRoute {
                name: name.clone(),
                upstream: upstream_from_spec(spec)?,
                dialect: dialect_for_spec(spec)?,
            },
        })
    }
}

impl RouterPlugin for BuiltinRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
    ) -> Result<RouteDecision, RouteError> {
        tracing::debug!(
            upstream = self.route.name.as_str(),
            "builtin route selected"
        );
        Ok(RouteDecision {
            upstream: self.route.upstream.clone(),
            dialect: Box::new(SharedDialect(self.route.dialect.clone())),
        })
    }
}

#[derive(Clone)]
struct BuiltinRoute {
    name: String,
    upstream: Upstream,
    dialect: Arc<dyn UpstreamDialect>,
}

#[derive(Clone)]
pub struct NoopObservabilityHook;

impl ObservabilityHook for NoopObservabilityHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}

#[derive(Clone)]
struct SharedDialect(Arc<dyn UpstreamDialect>);

impl UpstreamDialect for SharedDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        self.0.shape(ctx, upstream, principal, builder)
    }

    fn normalize_error(&self, status: StatusCode, body: &Bytes) -> Option<Bytes> {
        self.0.normalize_error(status, body)
    }
}

#[derive(Clone)]
pub struct CompositeSignerFactory {
    upstreams: Vec<(Upstream, AuthStrategy)>,
    api_key: Option<String>,
    aws: Arc<AwsSigV4SignerFactory>,
    gcp: Arc<GcpOAuthSignerFactory>,
    oauth: Option<Arc<AnthropicOAuthSignerFactory>>,
}

impl CompositeSignerFactory {
    fn new(config: &Config, storage: Option<Arc<Storage>>) -> Self {
        let upstreams = config
            .upstreams
            .values()
            .filter_map(|spec| {
                let upstream = upstream_from_spec(spec).ok()?;
                Some((upstream, auth_strategy_from_config(&spec.auth_strategy)))
            })
            .collect();
        Self {
            upstreams,
            api_key: None,
            aws: Arc::new(AwsSigV4SignerFactory::new()),
            gcp: Arc::new(GcpOAuthSignerFactory::new()),
            oauth: anthropic_oauth_factory(config, storage, "api-key", "anthropic_oauth"),
        }
    }

    fn with_api_key(&self, api_key: String) -> Self {
        Self {
            upstreams: self.upstreams.clone(),
            api_key: Some(api_key),
            aws: self.aws.clone(),
            gcp: self.gcp.clone(),
            oauth: self.oauth.clone(),
        }
    }

    fn strategy_for(&self, upstream: &Upstream) -> AuthStrategy {
        self.upstreams
            .iter()
            .find_map(|(candidate, strategy)| (candidate == upstream).then(|| strategy.clone()))
            .unwrap_or(AuthStrategy::ApiKey)
    }
}

#[async_trait]
impl SignerFactory for CompositeSignerFactory {
    async fn build(
        &self,
        upstream: &Upstream,
    ) -> Result<Arc<dyn cc_lb_plugin_api::Signer>, SignerError> {
        match self.strategy_for(upstream) {
            AuthStrategy::ApiKey | AuthStrategy::InternalForwarded => {
                let api_key =
                    self.api_key
                        .clone()
                        .ok_or_else(|| SignerError::MissingCredentials {
                            reason: "x-api-key is required for api_key signing".to_owned(),
                        })?;
                AnthropicKeySignerFactory::new(api_key)
                    .build(upstream)
                    .await
            }
            AuthStrategy::OAuth => {
                let factory = self
                    .oauth
                    .as_ref()
                    .ok_or_else(|| SignerError::MissingCredentials {
                        reason: "oauth storage and CC_LB_OAUTH_TOKEN_URL/CC_LB_OAUTH_CLIENT_ID are required".to_owned(),
                    })?;
                factory.build(upstream).await
            }
            AuthStrategy::AwsSigV4 => self.aws.build(upstream).await,
            AuthStrategy::GcpOAuth => self.gcp.build(upstream).await,
        }
    }
}

pub fn anthropic_oauth_factory(
    config: &Config,
    storage: Option<Arc<Storage>>,
    principal_id: &str,
    provider: &str,
) -> Option<Arc<AnthropicOAuthSignerFactory>> {
    let storage = storage?;
    let token_url = std::env::var("CC_LB_OAUTH_TOKEN_URL")
        .ok()
        .unwrap_or_else(|| {
            oauth_endpoint(
                &config.signers.anthropic_oauth.issuer_base_url,
                "/v1/oauth/token",
            )
        });
    let client_id = oauth_client_id(config)?;
    let token_url = TokenUrl::new(token_url).ok()?;
    Some(Arc::new(AnthropicOAuthSignerFactory::new(
        principal_id.to_owned(),
        provider.to_owned(),
        storage,
        token_url,
        ClientId::new(client_id),
    )))
}

pub fn oauth_client_id(config: &Config) -> Option<String> {
    if !config.signers.anthropic_oauth.client_id.trim().is_empty() {
        return Some(config.signers.anthropic_oauth.client_id.clone());
    }
    std::env::var("CC_LB_OAUTH_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub fn oauth_endpoint(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

pub fn upstream_from_spec(spec: &UpstreamSpec) -> Result<Upstream, BuiltinError> {
    match spec.kind {
        UpstreamKind::AnthropicDirect => Ok(Upstream::AnthropicDirect),
        UpstreamKind::BedrockRuntime => Ok(Upstream::BedrockRuntime {
            region: required_field(spec.region.as_deref(), "region")?,
        }),
        UpstreamKind::BedrockMantle => Ok(Upstream::BedrockMantle {
            region: required_field(spec.region.as_deref(), "region")?,
        }),
        UpstreamKind::Vertex => Ok(Upstream::Vertex {
            project: required_field(spec.project.as_deref(), "project")?,
            region: required_field(spec.region.as_deref(), "region")?,
        }),
        UpstreamKind::Custom => Ok(Upstream::CustomAnthropicSpec {
            base_url: spec
                .base_url
                .clone()
                .ok_or(BuiltinError::MissingField("base_url"))?,
        }),
    }
}

pub fn dialect_for_spec(spec: &UpstreamSpec) -> Result<Arc<dyn UpstreamDialect>, BuiltinError> {
    match spec.kind {
        UpstreamKind::AnthropicDirect => Ok(Arc::new(AnthropicDirectDialect)),
        UpstreamKind::BedrockRuntime => Ok(Arc::new(BedrockRuntimeDialect)),
        UpstreamKind::BedrockMantle => Ok(Arc::new(BedrockMantleDialect)),
        UpstreamKind::Vertex => Ok(Arc::new(VertexDialect)),
        UpstreamKind::Custom => Ok(Arc::new(CustomAnthropicSpecDialect)),
    }
}

pub fn auth_strategy_from_config(strategy: &ConfigAuthStrategy) -> AuthStrategy {
    match strategy {
        ConfigAuthStrategy::ApiKey => AuthStrategy::ApiKey,
        ConfigAuthStrategy::OAuth => AuthStrategy::OAuth,
        ConfigAuthStrategy::AwsSigV4 => AuthStrategy::AwsSigV4,
        ConfigAuthStrategy::GcpOAuth => AuthStrategy::GcpOAuth,
        ConfigAuthStrategy::InternalForwarded => AuthStrategy::InternalForwarded,
    }
}

pub fn default_quotas(config: &Config) -> PrincipalQuotas {
    PrincipalQuotas {
        requests_per_window: config.quotas.default_requests_per_window,
        input_tokens_per_window: config.quotas.default_input_tokens,
        output_tokens_per_window: config.quotas.default_output_tokens,
        window: Duration::from_secs(config.quotas.default_window_secs.max(1)),
        allowed_models: Vec::new(),
    }
}

fn required_field(value: Option<&str>, field: &'static str) -> Result<String, BuiltinError> {
    value
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or(BuiltinError::MissingField(field))
}

#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("no upstreams configured")]
    NoUpstreams,
    #[error("missing upstream field {0}")]
    MissingField(&'static str),
    #[error("invalid upstream URL {0}")]
    InvalidUrl(#[from] url::ParseError),
}
