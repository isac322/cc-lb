use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_core::ApiKeyAwareSignerFactory;
use cc_lb_config::{AuthStrategy as ConfigAuthStrategy, Config, UpstreamKind, UpstreamSpec};
use cc_lb_dialect_anthropic::{AnthropicDirectDialect, CustomAnthropicSpecDialect};
use cc_lb_dialect_bedrock::{BedrockMantleDialect, BedrockRuntimeDialect};
use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::{
    AuthStrategy, ObservabilityError, ObservabilityHook, ObserveEvent, Principal, RequestContext,
    RouteDecision,
    RouteError, RouterPlugin, SignerError, SignerFactory, Upstream, UpstreamDialect,
};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactory;
use cc_lb_signer_aws::AwsSigV4SignerFactory;
use cc_lb_signer_gcp::GcpOAuthSignerFactory;
use cc_lb_storage_redb::Storage;
use oauth2::{ClientId, TokenUrl};


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
            dialect: self.route.dialect.clone(),
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
pub struct CompositeSignerFactory {
    upstreams: Vec<(Upstream, AuthStrategy)>,
    api_key: Option<String>,
    aws: Arc<AwsSigV4SignerFactory>,
    gcp: Arc<GcpOAuthSignerFactory>,
    oauth: Option<Arc<AnthropicOAuthSignerFactory>>,
}

impl CompositeSignerFactory {
    pub fn new(config: &Config, storage: Option<Arc<Storage>>, aead: Arc<AeadService>) -> Self {
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
            oauth: anthropic_oauth_factory(config, storage, aead, "api-key", "anthropic_oauth"),
        }
    }

    fn clone_with_api_key(&self, api_key: String) -> Self {
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

impl ApiKeyAwareSignerFactory for CompositeSignerFactory {
    fn with_api_key(&self, api_key: String) -> Arc<dyn SignerFactory> {
        Arc::new(self.clone_with_api_key(api_key))
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
    _config: &Config,
    _storage: Option<Arc<Storage>>,
    _aead: Arc<AeadService>,
    _principal_id: &str,
    _provider: &str,
) -> Option<Arc<AnthropicOAuthSignerFactory>> {
    None
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
        UpstreamKind::AnthropicDirect => Ok(Arc::new(AnthropicDirectDialect::with_base_url(
            spec.base_url.clone(),
        ))),
        UpstreamKind::BedrockRuntime => Ok(Arc::new(BedrockRuntimeDialect::with_base_url(
            spec.base_url.clone(),
        ))),
        UpstreamKind::BedrockMantle => Ok(Arc::new(BedrockMantleDialect::with_base_url(
            spec.base_url.clone(),
        ))),
        UpstreamKind::Vertex => Ok(Arc::new(VertexDialect::with_base_url(
            spec.base_url.clone(),
        ))),
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
