use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_config::{
    AuthStrategy as ConfigAuthStrategy, Config, DownstreamAuthMode, NoneModeConfig,
    NoneModeUpstreamKind, UpstreamKind, UpstreamSpec,
};
use cc_lb_core::api_keys::{
    key_store::KeyStore,
    principal_view::{PrincipalStatus, PrincipalView},
    secret,
};
use cc_lb_dialect_anthropic::{AnthropicDirectDialect, CustomAnthropicSpecDialect};
use cc_lb_dialect_bedrock::{BedrockMantleDialect, BedrockRuntimeDialect};
use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::{
    AuthStrategy, AuthnError as PluginAuthnError, AuthnOutcome, AuthnPlugin, ObservabilityError, ObservabilityHook,
    ObserveEvent, Principal, PrincipalKind, PrincipalQuotas, RequestContext, RouteDecision,
    RouteError, RouterPlugin, SignerError, SignerFactory, Upstream, UpstreamDialect,
};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactory;
use cc_lb_signer_aws::AwsSigV4SignerFactory;
use cc_lb_signer_gcp::GcpOAuthSignerFactory;
use cc_lb_storage_api::Storage;
use cc_lb_storage_redb::{KeyStatus, StoredApiKeyRecord};
use http::Method;
use oauth2::{ClientId, TokenUrl};
use serde_json::Map;


#[derive(Clone)]
pub struct BuiltinAuthn {
    mode: DownstreamAuthMode,
    none_mode: Option<NoneModeConfig>,
    key_store: Arc<KeyStore>,
    principal_view: Arc<arc_swap::ArcSwap<PrincipalView>>,
}

#[derive(Debug, Clone)]
pub struct AuthnSuccess {
    pub principal_id: String,
    pub key_id: String,
    pub upstream_kind: cc_lb_storage_redb::UpstreamKind,
    pub upstream_credential_ref: String,
    pub record: StoredApiKeyRecord,
    pub last_4: String,
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum AuthnError {
    #[error("missing x-api-key header")]
    MissingHeader,
    #[error("invalid api key format")]
    InvalidFormat,
    #[error("api key not found")]
    NotFound,
    #[error("api key signature mismatch")]
    SignatureMismatch,
    #[error("api key disabled")]
    KeyDisabled,
    #[error("api key revoked")]
    KeyRevoked,
    #[error("api key expired")]
    Expired,
    #[error("principal not found")]
    PrincipalMissing,
    #[error("principal disabled")]
    PrincipalDisabled,
}

impl AuthnError {
    pub fn http_status(&self) -> u16 {
        match self {
            Self::KeyDisabled | Self::PrincipalDisabled => 403,
            Self::MissingHeader
            | Self::InvalidFormat
            | Self::NotFound
            | Self::SignatureMismatch
            | Self::KeyRevoked
            | Self::Expired
            | Self::PrincipalMissing => 401,
        }
    }
}

impl BuiltinAuthn {
    pub fn new(
        mode: DownstreamAuthMode,
        none_mode: Option<NoneModeConfig>,
        key_store: Arc<KeyStore>,
        principal_view: Arc<arc_swap::ArcSwap<PrincipalView>>,
    ) -> Self {
        Self {
            mode,
            none_mode,
            key_store,
            principal_view,
        }
    }

    pub fn authenticate(&self, headers: &http::HeaderMap) -> Result<AuthnSuccess, AuthnError> {
        let input = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthnError::MissingHeader)?;
        let (parsed_key_id, secret_bytes) =
            secret::parse(input).map_err(|_| AuthnError::InvalidFormat)?;
        let index_hash = secret::compute_index_hash(&secret_bytes);
        let (principal_id, key_id_storage, record) = self
            .key_store
            .lookup_by_index_hash(&index_hash)
            .map_err(|_| AuthnError::NotFound)?
            .ok_or(AuthnError::NotFound)?;

        if parsed_key_id != key_id_storage {
            return Err(AuthnError::NotFound);
        }
        if !secret::verify_secret(&secret_bytes, &record.verify_hash, &record.secret_salt) {
            return Err(AuthnError::SignatureMismatch);
        }
        match record.status {
            KeyStatus::Active => {}
            KeyStatus::Disabled => return Err(AuthnError::KeyDisabled),
            KeyStatus::Revoked => return Err(AuthnError::KeyRevoked),
        }
        if let Some(expires_at_unix_secs) = record.expires_at_unix_secs {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if now > expires_at_unix_secs {
                return Err(AuthnError::Expired);
            }
        }

        let view = self.principal_view.load();
        view.get(&principal_id)
            .ok_or(AuthnError::PrincipalMissing)?;
        if view.principal_status(&principal_id) != PrincipalStatus::Active {
            return Err(AuthnError::PrincipalDisabled);
        }

        Ok(AuthnSuccess {
            principal_id,
            key_id: key_id_storage,
            upstream_kind: record.upstream_kind,
            upstream_credential_ref: record.upstream_credential_ref.clone(),
            record: record.clone(),
            last_4: record.last_4.clone(),
        })
    }

    pub fn authenticate_none_mode(&self) -> Option<AuthnSuccess> {
        if self.mode != DownstreamAuthMode::None {
            return None;
        }

        let none_mode = self.none_mode.as_ref()?;
        let record = StoredApiKeyRecord {
            status: KeyStatus::Active,
            upstream_kind: map_none_mode_upstream_kind(none_mode.upstream_kind.clone()),
            upstream_credential_ref: none_mode.upstream_credential_ref.clone(),
            verify_hash: [0; 32],
            secret_salt: [0; 16],
            last_4: String::new(),
            ..Default::default()
        };

        Some(AuthnSuccess {
            principal_id: none_mode.principal_id.clone(),
            key_id: "none-mode".to_owned(),
            upstream_kind: record.upstream_kind,
            upstream_credential_ref: record.upstream_credential_ref.clone(),
            record,
            last_4: String::new(),
        })
    }
}

pub fn map_none_mode_upstream_kind(kind: NoneModeUpstreamKind) -> cc_lb_storage_redb::UpstreamKind {
    match kind {
        NoneModeUpstreamKind::AnthropicKey => cc_lb_storage_redb::UpstreamKind::AnthropicKey,
        NoneModeUpstreamKind::AnthropicOAuth => cc_lb_storage_redb::UpstreamKind::AnthropicOAuth,
        NoneModeUpstreamKind::AwsSigV4 => cc_lb_storage_redb::UpstreamKind::AwsSigV4,
        NoneModeUpstreamKind::GcpOAuth => cc_lb_storage_redb::UpstreamKind::GcpOAuth,
    }
}

#[derive(Clone)]
pub struct BuiltinAuthnPluginAdapter {
    defaults: PrincipalQuotas,
    principal_quotas: HashMap<String, PrincipalQuotas>,
    signer_factory: Arc<CompositeSignerFactory>,
}

impl BuiltinAuthnPluginAdapter {
    pub fn new(config: &Config, storage: Arc<dyn Storage>, aead: Arc<AeadService>) -> Self {
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
            signer_factory: Arc::new(CompositeSignerFactory::new(config, storage, aead)),
        }
    }
}

#[async_trait]
impl AuthnPlugin for BuiltinAuthnPluginAdapter {
    async fn authenticate(&self, ctx: &RequestContext) -> Result<AuthnOutcome, PluginAuthnError> {
        let api_key = ctx
            .downstream_headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| {
                if ctx.method == Method::POST && ctx.path == "/api/event_logging/batch" {
                    "sk-ant-anonymous-event-logging".to_owned()
                } else {
                    String::new()
                }
            });

        if api_key.is_empty() {
            return Err(PluginAuthnError::InvalidCredentials {
                reason: "missing x-api-key header".to_owned(),
            });
        }

        let principal_id = "api-key".to_owned();
        let quotas = self
            .principal_quotas
            .get(&principal_id)
            .cloned()
            .unwrap_or_else(|| self.defaults.clone());
        let signer_factory = Arc::new(self.signer_factory.with_api_key(api_key));

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
    fn new(config: &Config, storage: Arc<dyn Storage>, aead: Arc<AeadService>) -> Self {
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
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    principal_id: &str,
    provider: &str,
) -> Option<Arc<AnthropicOAuthSignerFactory>> {
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
        aead,
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
