use super::cache::request_cache_evidence;
use super::routing::default_base_url;
use super::*;

pub type Body = AxumBody;

/// Prefix-hash schema persisted with prompt-cache observations.
///
/// Version 6 excludes the official Anthropic billing-header system block from
/// effective cache identity while preserving literal custom-upstream identity.
/// The version filter cold-starts older persisted observations.
pub use cc_lb_domain::HASH_SCHEMA_VERSION;

pub(super) struct CompatBodyTimingGuard {
    pub(super) observer: Option<LifecycleContext>,
    pub(super) timing: compat_response::CompatBodyTiming,
}

impl CompatBodyTimingGuard {
    pub(super) fn start(observer: LifecycleContext) -> Self {
        Self {
            observer: Some(observer),
            timing: compat_response::CompatBodyTiming::start(),
        }
    }

    pub(super) fn publish(&mut self) {
        let Some(observer) = self.observer.take() else {
            return;
        };
        let now = Instant::now();
        observer.set_io_timings(self.timing.snapshot(now));
        let first_body_chunk_ms = self
            .timing
            .first_body_chunk_at
            .map(|at| duration_to_ms(at.saturating_duration_since(self.timing.collect_started)));
        observer.set_termination_timings(None, None, first_body_chunk_ms);
    }
}

impl Drop for CompatBodyTimingGuard {
    fn drop(&mut self) {
        self.publish();
    }
}

pub(super) fn push_buffered_data(
    data: &[u8],
    body: &Body,
    body_cap: usize,
    body_buf: &mut Vec<u8>,
    body_collect_failed: &mut bool,
    body_limit_terminal: &mut bool,
) -> bool {
    if data.len() > body_cap.saturating_sub(body_buf.len()) {
        *body_collect_failed = true;
        *body_limit_terminal = body.is_end_stream();
        return false;
    }
    body_buf.extend_from_slice(data);
    true
}

pub(super) const DEFAULT_MESSAGES_CAP_BYTES: usize = 32 * 1024 * 1024;
pub(super) const DEFAULT_FILES_CAP_BYTES: usize = 100 * 1024 * 1024;
pub(super) const DEFAULT_MAX_BUFFERED_RESPONSE_BYTES: usize = 100 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_INPUT_ESTIMATE: i64 = 4000;
pub(super) static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestKind {
    AnthropicMessages,
    OpenaiResponses,
    GeminiGenerateContent,
    GeminiCachedContentsCreate,
    GeminiCountTokens,
    AnthropicModels,
    AnthropicComplete,
    OpenaiResponsesInputTokens,
    OpenaiResponsesCompact,
    OpenaiChatCompletions,
    OpenaiAudioTranscriptions,
    OpenaiRealtimeCall,
    OpenaiRealtimeHangup,
    OpenaiAccountOwned,
    OpenaiStoredResponse,
    OpenaiCompletions,
    OpenaiEmbeddings,
    OpenaiModerations,
    OpenaiAudio,
    OpenaiImages,
    OpenaiModels,
    OpenaiCodexAuxiliary,
    GeminiEmbedContent,
    GeminiPredict,
    GeminiModels,
    GeminiOpenaiChatCompletions,
    GeminiOpenaiEmbeddings,
    GeminiOpenaiImages,
    GeminiOpenaiModels,
    OpenaiImageVariations,
}

impl RequestKind {
    /// Returns whether this request can be sent to the concrete upstream record.
    ///
    /// Vertex API-key records intentionally support only generation/counting.
    /// Google rejects API keys for cache and model-catalog RPCs; those routes
    /// require OAuth credentials. Credential records support the full Vertex
    /// HTTP surface represented here.
    pub(crate) fn eligible(self, record: &UpstreamRecord) -> bool {
        use StorageUpstreamKind as K;
        if self.provider() != record.kind.provider() {
            return false;
        }
        match record.kind {
            K::VertexApiKey => {
                matches!(self, Self::GeminiGenerateContent | Self::GeminiCountTokens)
            }
            K::VertexCredential => matches!(
                self,
                Self::GeminiGenerateContent
                    | Self::GeminiCountTokens
                    | Self::GeminiCachedContentsCreate
                    | Self::GeminiModels
            ),
            _ => self.matches_upstream(record.kind),
        }
    }

    pub(super) fn matches_upstream(self, kind: StorageUpstreamKind) -> bool {
        use StorageUpstreamKind as K;
        if self.provider() != kind.provider() {
            return false;
        }
        match kind {
            K::AnthropicApiKey | K::GeminiApiKey => true,
            K::OpenaiApiKey => self != Self::OpenaiCodexAuxiliary,
            K::AnthropicOauth => matches!(self, Self::AnthropicMessages | Self::AnthropicModels),
            K::OpenaiOauth => matches!(
                self,
                Self::OpenaiResponses
                    | Self::OpenaiResponsesInputTokens
                    | Self::OpenaiResponsesCompact
                    | Self::OpenaiChatCompletions
                    | Self::OpenaiAudioTranscriptions
                    | Self::OpenaiRealtimeCall
                    | Self::OpenaiRealtimeHangup
                    | Self::OpenaiImages
                    | Self::OpenaiModels
                    | Self::OpenaiCodexAuxiliary
            ),
            K::GeminiOauth => matches!(
                self,
                Self::GeminiGenerateContent | Self::GeminiCountTokens | Self::GeminiModels
            ),
            _ => false,
        }
    }

    pub(super) fn capability(self) -> ProviderCapability {
        match self {
            Self::AnthropicMessages => ProviderCapability::AnthropicMessages,
            Self::OpenaiResponses => ProviderCapability::OpenaiResponses,
            Self::GeminiGenerateContent => ProviderCapability::GeminiGenerateContent,
            Self::GeminiCachedContentsCreate => ProviderCapability::GeminiCachedContentsCreate,
            Self::GeminiCountTokens => ProviderCapability::GeminiCountTokens,
            Self::AnthropicModels => ProviderCapability::AnthropicModels,
            Self::AnthropicComplete => ProviderCapability::AnthropicComplete,
            Self::OpenaiResponsesInputTokens => ProviderCapability::OpenaiResponsesInputTokens,
            Self::OpenaiResponsesCompact => ProviderCapability::OpenaiResponsesCompact,
            Self::OpenaiChatCompletions => ProviderCapability::OpenaiChatCompletions,
            Self::OpenaiAudioTranscriptions => ProviderCapability::OpenaiAudioTranscriptions,
            Self::OpenaiRealtimeCall => ProviderCapability::OpenaiRealtimeCall,
            Self::OpenaiRealtimeHangup => ProviderCapability::OpenaiRealtimeHangup,
            Self::OpenaiAccountOwned => ProviderCapability::OpenaiAccountOwned,
            Self::OpenaiStoredResponse => ProviderCapability::OpenaiStoredResponse,
            Self::OpenaiCompletions => ProviderCapability::OpenaiCompletions,
            Self::OpenaiEmbeddings => ProviderCapability::OpenaiEmbeddings,
            Self::OpenaiModerations => ProviderCapability::OpenaiModerations,
            Self::OpenaiAudio => ProviderCapability::OpenaiAudio,
            Self::OpenaiImages => ProviderCapability::OpenaiImages,
            Self::OpenaiModels => ProviderCapability::OpenaiModels,
            Self::OpenaiCodexAuxiliary => ProviderCapability::OpenaiCodexAuxiliary,
            Self::GeminiEmbedContent => ProviderCapability::GeminiEmbedContent,
            Self::GeminiPredict => ProviderCapability::GeminiPredict,
            Self::GeminiModels => ProviderCapability::GeminiModels,
            Self::GeminiOpenaiChatCompletions => ProviderCapability::GeminiOpenaiChatCompletions,
            Self::GeminiOpenaiEmbeddings => ProviderCapability::GeminiOpenaiEmbeddings,
            Self::GeminiOpenaiImages => ProviderCapability::GeminiOpenaiImages,
            Self::GeminiOpenaiModels => ProviderCapability::GeminiOpenaiModels,
            Self::OpenaiImageVariations => ProviderCapability::OpenaiImages,
        }
    }

    pub(super) fn provider(self) -> cc_lb_domain::Provider {
        self.capability().provider()
    }
}

impl From<ProviderCapability> for RequestKind {
    fn from(capability: ProviderCapability) -> Self {
        match capability {
            ProviderCapability::AnthropicMessages => Self::AnthropicMessages,
            ProviderCapability::AnthropicCountTokens => Self::AnthropicMessages,
            ProviderCapability::GeminiGenerateContent => Self::GeminiGenerateContent,
            ProviderCapability::GeminiCachedContentsCreate => Self::GeminiCachedContentsCreate,
            ProviderCapability::GeminiCountTokens => Self::GeminiCountTokens,
            ProviderCapability::AnthropicModels => Self::AnthropicModels,
            ProviderCapability::AnthropicComplete => Self::AnthropicComplete,
            ProviderCapability::OpenaiResponses => Self::OpenaiResponses,
            ProviderCapability::OpenaiResponsesInputTokens => Self::OpenaiResponsesInputTokens,
            ProviderCapability::OpenaiResponsesCompact => Self::OpenaiResponsesCompact,
            ProviderCapability::OpenaiChatCompletions => Self::OpenaiChatCompletions,
            ProviderCapability::OpenaiAudioTranscriptions => Self::OpenaiAudioTranscriptions,
            ProviderCapability::OpenaiRealtimeCall => Self::OpenaiRealtimeCall,
            ProviderCapability::OpenaiRealtimeHangup => Self::OpenaiRealtimeHangup,
            ProviderCapability::OpenaiAccountOwned => Self::OpenaiAccountOwned,
            ProviderCapability::OpenaiStoredResponse => Self::OpenaiStoredResponse,
            ProviderCapability::OpenaiCompletions => Self::OpenaiCompletions,
            ProviderCapability::OpenaiEmbeddings => Self::OpenaiEmbeddings,
            ProviderCapability::OpenaiModerations => Self::OpenaiModerations,
            ProviderCapability::OpenaiAudio => Self::OpenaiAudio,
            ProviderCapability::OpenaiImages => Self::OpenaiImages,
            ProviderCapability::OpenaiModels => Self::OpenaiModels,
            ProviderCapability::OpenaiCodexAuxiliary => Self::OpenaiCodexAuxiliary,
            ProviderCapability::GeminiEmbedContent => Self::GeminiEmbedContent,
            ProviderCapability::GeminiPredict => Self::GeminiPredict,
            ProviderCapability::GeminiModels => Self::GeminiModels,
            ProviderCapability::GeminiOpenaiChatCompletions => Self::GeminiOpenaiChatCompletions,
            ProviderCapability::GeminiOpenaiEmbeddings => Self::GeminiOpenaiEmbeddings,
            ProviderCapability::GeminiOpenaiImages => Self::GeminiOpenaiImages,
            ProviderCapability::GeminiOpenaiModels => Self::GeminiOpenaiModels,
        }
    }
}

pub(super) fn gemini_model_from_path(path: &str) -> Option<String> {
    let model_path = path
        .strip_prefix("/v1beta/models/")
        .or_else(|| path.strip_prefix("/v1/models/"))?;
    let (model, action) = model_path.rsplit_once(':')?;
    (matches!(
        action,
        "generateContent"
            | "streamGenerateContent"
            | "countTokens"
            | "embedContent"
            | "batchEmbedContents"
            | "predict"
    ) && !model.is_empty())
    .then(|| model.to_owned())
}

pub(super) fn lifecycle_request_model(
    capability: Option<ProviderCapability>,
    model: &str,
) -> String {
    if capability.is_some_and(|capability| capability.provider() == cc_lb_domain::Provider::Gemini)
    {
        canonical_gemini_model_id(model).to_owned()
    } else {
        model.to_owned()
    }
}

pub(super) fn canonical_model_for_capability(
    capability: Option<ProviderCapability>,
    model: &str,
) -> &str {
    let provider = capability
        .map(ProviderCapability::provider)
        .unwrap_or(Provider::Anthropic);
    if provider == cc_lb_domain::Provider::Gemini {
        canonical_gemini_model_id(model)
    } else {
        canonical_model_id(provider, model)
    }
}

#[derive(Clone, Debug)]
pub struct LifecycleConfig {
    pub messages_body_cap_bytes: usize,
    pub files_body_cap_bytes: usize,
    pub max_buffered_response_bytes: usize,
    pub replica_identity: Option<ReplicaIdentity>,
    pub upstream_affinity_ttl: Duration,
    pub unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy,
    pub unavailable_openai_reasoning_owner: cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy,
    pub openai_oauth_compat: cc_lb_domain::OpenaiOauthCompatPolicy,
    pub openai_realtime_origin: url::Url,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            messages_body_cap_bytes: DEFAULT_MESSAGES_CAP_BYTES,
            files_body_cap_bytes: DEFAULT_FILES_CAP_BYTES,
            max_buffered_response_bytes: DEFAULT_MAX_BUFFERED_RESPONSE_BYTES,
            replica_identity: None,
            upstream_affinity_ttl: std::time::Duration::from_secs(
                UpstreamAffinityConfig::default().ttl_secs(),
            ),
            unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::default(),
            unavailable_openai_reasoning_owner:
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::default(),
            openai_oauth_compat: cc_lb_domain::OpenaiOauthCompatPolicy::default(),
            openai_realtime_origin: url::Url::parse("https://api.openai.com/").expect("valid URL"),
        }
    }
}

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("response build failed: {reason}")]
    ResponseBuild { reason: String },
}
#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("invalid upstream uri: {reason}")]
    InvalidUri { reason: String },
    #[error("upstream request build failed: {reason}")]
    RequestBuild { reason: String },
    /// Transport failure with the typed source preserved: `source()` yields
    /// the hyper/connect/io error chain so terminal classification can
    /// distinguish DNS, TCP, TLS, and timeout failures without string
    /// matching.
    #[error("upstream dispatch failed: {source}")]
    Transport {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("upstream bulkhead queue full; retry after {retry_after:?}")]
    BulkheadFull { retry_after: Duration },
}

/// Failure of the raw-passthrough fallback used when dialect shaping fails.
#[derive(Debug, Error)]
pub(super) enum RawPassthroughError {
    /// The configured or default upstream base URL failed to parse.
    #[error("invalid raw passthrough base url: {0}")]
    BaseUrl(#[from] url::ParseError),
    /// The raw-passthrough dialect failed to shape the request.
    #[error("raw passthrough shaping failed: {0}")]
    Shape(#[from] DialectError),
}

/// A failed upstream attempt carrying the typed cause until the outer
/// response-conversion point.
///
/// The attempt boundary used to fold every failure into a boxed
/// `Response<Body>`, which destroyed the error chain and made the terminal
/// commit reasonless. This enum keeps the real cause so the caller can build
/// the client response and the [`InternalFailure`] diagnostic from the same
/// value.
#[derive(Debug)]
pub(super) enum AttemptFailure {
    /// Request shaping failed before dispatch.
    Shape(DialectError),
    /// The raw-passthrough fallback itself failed after dialect shaping had
    /// already failed.
    RawPassthrough(RawPassthroughError),
    /// Request signing failed.
    Sign(SignerError),
    /// Dispatch to the upstream failed.
    Dispatch(DispatchError),
    /// The OAuth compatibility layer rejected this request before dispatch.
    CompatReject(crate::openai_compat::CompatReject),
}
pub(super) struct AttemptResponse {
    pub(super) response: Response<Body>,
    pub(super) compat_response_plan: Option<CompatResponsePlan>,
    pub(super) openai_oauth_sent_service_tier: Option<Option<String>>,
}
pub(super) struct FanoutAttemptResponse {
    pub(super) response: Response<Body>,
    pub(super) openai_oauth_sent_service_tier: Option<Option<String>>,
}
pub(super) type ConcurrentAttemptFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AttemptResponse, AttemptFailure>> + Send + 'a>>;
pub(super) type ResponseBodyFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = (
                    StatusCode,
                    HeaderMap,
                    Result<Bytes, compat_response::CollectError>,
                    Option<Option<String>>,
                ),
            > + Send
            + 'a,
    >,
>;

pub(super) struct ConcurrentAttemptInputs<'a, 'r> {
    pub(super) authenticated: &'a crate::authn_rail::Authenticated,
    pub(super) reserved: Option<&'a Reserved>,
    pub(super) ctx: &'a RequestContext,
    pub(super) parsed_body: Option<&'a serde_json::Value>,
    pub(super) principal: &'a Principal,
    pub(super) routed: &'a RoutedUpstream<'r>,
    pub(super) signer: Arc<dyn Signer>,
    pub(super) observer: Option<&'a LifecycleContext>,
    pub(super) cache_metadata: &'a RequestCacheMetadata,
    pub(super) shared_request_started:
        Arc<tokio::sync::OnceCell<(Instant, Option<crate::cache_keepalive::RequestStartOrder>)>>,
}
pub(super) fn reduce_fanout(
    mut attempts: Vec<AttemptResponse>,
) -> (AttemptResponse, Option<Vec<FanoutAttemptResponse>>) {
    let retry_index = attempts.iter().position(|attempt| {
        matches!(
            attempt.response.status(),
            StatusCode::UNAUTHORIZED | StatusCode::TOO_MANY_REQUESTS
        )
    });
    if let Some(index) = retry_index {
        attempts.swap(0, index);
    }
    let mut attempts = attempts.into_iter();
    let first = attempts
        .next()
        .expect("fan-out produced at least one response");
    let rest = retry_index.is_none().then(|| {
        attempts
            .map(|attempt| FanoutAttemptResponse {
                response: attempt.response,
                openai_oauth_sent_service_tier: attempt.openai_oauth_sent_service_tier,
            })
            .collect()
    });
    (first, rest)
}

impl From<RawPassthroughError> for AttemptFailure {
    fn from(source: RawPassthroughError) -> Self {
        Self::RawPassthrough(source)
    }
}

impl AttemptFailure {
    /// Convert into the client-visible response and the terminal diagnostic.
    ///
    /// Response status, body, and `Retry-After` semantics are identical to the
    /// previous inline mapping.
    pub(super) fn into_response_and_failure(
        self,
        capability: ProviderCapability,
    ) -> (Response<Body>, InternalFailure) {
        match self {
            Self::Shape(source) => {
                let (response, kind) = match &source {
                    DialectError::PoolSaturated { .. } => (
                        provider_error_response_with_retry_after(
                            capability,
                            StatusCode::SERVICE_UNAVAILABLE,
                            "overloaded_error",
                            "plugin runtime saturated",
                            None,
                            1,
                        ),
                        InternalErrorKind::Unavailable,
                    ),
                    DialectError::UnsupportedRequest { .. } | DialectError::InvalidUrl { .. } => (
                        provider_error_response(
                            capability,
                            StatusCode::BAD_GATEWAY,
                            "api_error",
                            "failed to shape upstream request",
                            None,
                        ),
                        InternalErrorKind::PluginError,
                    ),
                };
                let failure = InternalFailure {
                    status: response.status(),
                    error_code: error_codes::UPSTREAM_DISPATCH_FAILED,
                    error: InternalError {
                        stage: InternalErrorStage::Shape,
                        kind,
                        message: Some(source.to_string()),
                    },
                };
                (response, failure)
            }
            Self::RawPassthrough(source) => {
                let response = provider_error_response(
                    capability,
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to prepare raw upstream request",
                    None,
                );
                let failure = InternalFailure {
                    status: StatusCode::BAD_GATEWAY,
                    error_code: error_codes::UPSTREAM_DISPATCH_FAILED,
                    error: InternalError {
                        stage: InternalErrorStage::Relay,
                        kind: InternalErrorKind::Unavailable,
                        message: Some(source.to_string()),
                    },
                };
                (response, failure)
            }
            Self::Sign(source) => {
                let response = provider_error_response(
                    capability,
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to sign upstream request",
                    None,
                );
                let failure = InternalFailure {
                    status: StatusCode::BAD_GATEWAY,
                    error_code: error_codes::SIGNER_FAILED,
                    error: InternalError {
                        stage: InternalErrorStage::Signer,
                        kind: signer_error_kind(&source),
                        message: Some(source.to_string()),
                    },
                };
                (response, failure)
            }
            Self::CompatReject(reject) => {
                let response = crate::error_format::compat_reject_response(&reject);
                let failure = InternalFailure {
                    status: StatusCode::BAD_REQUEST,
                    error_code: error_codes::COMPAT_UNSUPPORTED_PARAMETER,
                    error: InternalError {
                        stage: InternalErrorStage::Ingress,
                        kind: InternalErrorKind::InvalidInput,
                        message: Some(reject.message),
                    },
                };
                (response, failure)
            }
            Self::Dispatch(source) => {
                let (response, kind, message) = match &source {
                    DispatchError::BulkheadFull { retry_after } => (
                        provider_error_response_with_retry_after(
                            capability,
                            StatusCode::SERVICE_UNAVAILABLE,
                            "overloaded_error",
                            "upstream bulkhead queue is full",
                            None,
                            retry_after.as_secs().max(1),
                        ),
                        InternalErrorKind::Unavailable,
                        source.to_string(),
                    ),
                    DispatchError::InvalidUri { .. } | DispatchError::RequestBuild { .. } => (
                        provider_error_response(
                            capability,
                            StatusCode::BAD_GATEWAY,
                            "api_error",
                            "upstream request failed",
                            None,
                        ),
                        InternalErrorKind::ConfigError,
                        source.to_string(),
                    ),
                    DispatchError::Transport { source: transport } => {
                        let failure_kind =
                            crate::bulkhead::classify_transport_error(transport.as_ref());
                        let chain =
                            crate::bulkhead::transport_error_chain(transport.as_ref()).join(" <- ");
                        (
                            provider_error_response(
                                capability,
                                StatusCode::BAD_GATEWAY,
                                "api_error",
                                "upstream request failed",
                                None,
                            ),
                            match failure_kind {
                                crate::bulkhead::TransportFailureKind::Timeout => {
                                    InternalErrorKind::Timeout
                                }
                                _ => InternalErrorKind::Unavailable,
                            },
                            format!("{failure_kind}: {chain}"),
                        )
                    }
                };
                let failure = InternalFailure {
                    status: response.status(),
                    error_code: error_codes::UPSTREAM_DISPATCH_FAILED,
                    error: InternalError {
                        stage: InternalErrorStage::Relay,
                        kind,
                        message: Some(message),
                    },
                };
                (response, failure)
            }
        }
    }
}

/// Map a signing failure to its diagnostic kind.
pub(super) fn signer_error_kind(error: &SignerError) -> InternalErrorKind {
    match error {
        SignerError::MissingCredentials { .. } => InternalErrorKind::ConfigError,
        SignerError::StorageUnavailable { .. } | SignerError::ExpiredToken { .. } => {
            InternalErrorKind::Unavailable
        }
        SignerError::SigningFailed { .. } => InternalErrorKind::PluginError,
    }
}

#[async_trait]
pub trait UpstreamDispatch: Send + Sync {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError>;

    /// Sends only if the deadline still permits HTTP to start. Middleware that
    /// waits for admission must forward this operation after acquiring capacity.
    async fn dispatch_before(
        &self,
        request: SignedRequest,
        deadline: &crate::attempt_rail::DispatchDeadline,
    ) -> Result<Response<Body>, crate::attempt_rail::DispatchBeforeError> {
        deadline.check()?;
        self.dispatch(request).await.map_err(Into::into)
    }
}

#[async_trait]
pub trait LimitSubjectProvider: Send + Sync {
    async fn limit_subject(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        authn_success: &AuthnSuccess,
    ) -> Option<AuthLimitSubject>;
}

pub trait LimitCostEstimator: Send + Sync {
    fn estimate_max(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64>;

    /// Prices disjoint submitted-input regions without adding a prefix twice.
    fn estimate_max_with_local_cache(
        &self,
        model: &str,
        input: cc_lb_pricing::LocalCacheInputEstimate,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64>;

    fn estimate_max_with_local_cache_and_images(
        &self,
        model: &str,
        input: cc_lb_pricing::LocalCacheInputEstimate,
        max_output: u64,
        use_image_rates: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        let _ = use_image_rates;
        self.estimate_max_with_local_cache(model, input, max_output, service_tier)
    }

    fn estimate_max_with_cache(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        estimated_cache_tokens: u64,
        _can_cross_cache_cliff: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.estimate_max(
            model,
            max_input.saturating_add(estimated_cache_tokens),
            max_output,
            service_tier,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn estimate_max_with_cache_and_images(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        estimated_cache_tokens: u64,
        can_cross_cache_cliff: bool,
        use_image_rates: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        let _ = use_image_rates;
        self.estimate_max_with_cache(
            model,
            max_input,
            max_output,
            estimated_cache_tokens,
            can_cross_cache_cliff,
            service_tier,
        )
    }
}

#[derive(Clone, Debug)]
pub struct AuthLimitSubject {
    pub principal_id: String,
    pub key_id: String,
    pub record: StoredApiKeyRecord,
}

pub(super) struct LimitRejectionInfo {
    pub(super) subject: cc_lb_lifecycle::LimitSubject,
    pub(super) request_summary: cc_lb_lifecycle::LimitRequestSummary,
    pub(super) route_summary: cc_lb_lifecycle::RouteSummary,
    pub(super) limit_violation: Option<String>,
    pub(super) reason_label: String,
}

pub(super) struct LimitRejectionErr {
    pub(super) response: Response<Body>,
    pub(super) info: LimitRejectionInfo,
}

pub(super) struct RequestAffinityResolution {
    pub(super) target_upstream_id: Uuid,
    pub(super) owner_epoch: u64,
    pub(super) pending_keys: Vec<UpstreamAffinityKey>,
}

pub(super) struct MultiOwnerAffinityMatch {
    pub(super) matched: Vec<(Uuid, UpstreamAffinityKey, u64, u64)>,
    pub(super) pending_keys: Vec<UpstreamAffinityKey>,
}

pub(super) enum RequestAffinityOutcome {
    None,
    Single(RequestAffinityResolution),
    MultipleOwners(MultiOwnerAffinityMatch),
}

pub(super) struct RequestAffinityFailure {
    pub(super) status: StatusCode,
    pub(super) error_type: &'static str,
    pub(super) message: &'static str,
}

impl RequestAffinityFailure {
    pub(super) fn response(&self, capability: ProviderCapability) -> Response<Body> {
        provider_error_response(capability, self.status, self.error_type, self.message, None)
    }
}

#[derive(Clone)]
pub(super) struct ResponseAffinityContext {
    /// Provider protocol of the routed request; selects which response
    /// shapes create provider-owned state.
    pub(super) capability: ProviderCapability,
    pub(super) usage_shape: Option<crate::provider_capability::UsageShape>,
    pub(super) precomputed_usage: Option<UsageCounts>,
    /// Present (`Some(sent_tier)`) only when the routed upstream is `StorageUpstreamKind::OpenaiOauth`.
    pub(super) openai_oauth_sent_service_tier: Option<Option<String>>,
    pub(super) store: Option<Arc<dyn UpstreamAffinityStore>>,
    pub(super) principal_id: String,
    pub(super) upstream_id: Uuid,
    pub(super) pending_request_keys: Vec<UpstreamAffinityKey>,
    pub(super) observe_response_keys: bool,
    pub(super) owner_epoch: Option<u64>,
    pub(super) ttl_secs: u64,
    pub(super) session_hint_keys: Vec<String>,
}

pub(super) async fn bind_upstream_affinity_keys(
    context: &ResponseAffinityContext,
    keys: &[UpstreamAffinityKey],
    now_unix_secs: u64,
    expires_at_unix_secs: Option<u64>,
) -> Result<(), ()> {
    if keys.is_empty() {
        return Ok(());
    }
    let Some(store) = context.store.as_ref() else {
        return Err(());
    };
    let owner_epoch = context.owner_epoch.ok_or(())?;
    let mut seen = HashSet::with_capacity(keys.len());
    let mut bindings = Vec::with_capacity(keys.len());
    for key in keys {
        if seen.insert(key) {
            bindings.push(UpstreamAffinityBinding {
                key: key.clone(),
                upstream_id: context.upstream_id,
                owner_epoch,
                observed_at_unix_secs: now_unix_secs,
                expires_at_unix_secs,
            });
        }
    }
    store
        .bind_upstream_affinities(&bindings, now_unix_secs, context.ttl_secs)
        .await
        .map_err(|_| ())
}

#[async_trait]
impl LimitSubjectProvider for BuiltinAuthn {
    async fn limit_subject(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        authn_success: &AuthnSuccess,
    ) -> Option<AuthLimitSubject> {
        let mut record = authn_success.record.clone();
        record.key_hash_b64 = authn_success.key_id.clone();
        Some(AuthLimitSubject {
            principal_id: authn_success.principal_id.clone(),
            key_id: authn_success.key_id.clone(),
            record,
        })
    }
}
pub(super) struct CollectedResponse {
    pub(super) status: StatusCode,
    pub(super) headers: HeaderMap,
    /// The upstream body, or `None` when it could not be read to EOF within
    /// [`ERROR_BODY_COLLECT_CAP_BYTES`] and [`ERROR_BODY_COLLECT_DEADLINE`].
    /// `None` means "no trustworthy body"; it is never a truncated prefix.
    pub(super) body: Option<Bytes>,
}

pub(super) fn rewrite_json_request_body_headers(headers: &mut HeaderMap, body_len: usize) {
    headers.remove(CONTENT_LENGTH);
    headers.remove(http::header::CONTENT_ENCODING);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Ok(value) = HeaderValue::from_str(&body_len.to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
}

pub(super) fn record_shape_internal_error(observer: Option<&LifecycleContext>, message: &str) {
    if let Some(observer) = observer {
        observer.record_internal_error(InternalError {
            stage: InternalErrorStage::Shape,
            kind: InternalErrorKind::Trap,
            message: Some(truncate_reason(message)),
        });
    }
}

pub(super) fn raw_passthrough_request(
    base_url: Option<&Url>,
    ctx: &RequestContext,
    principal: &Principal,
    upstream: &Upstream,
    openai_oauth: bool,
) -> Result<ShapedRequest, RawPassthroughError> {
    let base_url = base_url.cloned().unwrap_or_else(|| match upstream {
        Upstream::AnthropicDirect { .. } => default_base_url(StorageUpstreamKind::AnthropicApiKey),
        Upstream::OpenaiDirect { .. } => {
            if openai_oauth {
                default_base_url(StorageUpstreamKind::OpenaiOauth)
            } else {
                default_base_url(StorageUpstreamKind::OpenaiApiKey)
            }
        }
        Upstream::GeminiDirect { .. } => default_base_url(StorageUpstreamKind::GeminiApiKey),
    });
    let shape_context = ctx.dialect_shape_context();
    shape_request(
        &RawPassthroughDialect {
            base_url,
            openai_oauth,
        },
        &shape_context,
        upstream,
        principal,
    )
    .map_err(|source| {
        tracing::error!(%source, "raw passthrough request failed");
        RawPassthroughError::Shape(source)
    })
}

pub(super) struct RawPassthroughDialect {
    pub(super) base_url: Url,
    pub(super) openai_oauth: bool,
}

impl UpstreamDialect for RawPassthroughDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let scrubbed_query = context
            .query
            .as_deref()
            .map(cc_lb_upstream::scrub_query_api_key);
        let scrubbed_query = scrubbed_query.as_deref().filter(|query| !query.is_empty());
        let url = if self.openai_oauth && context.path == "/v1/audio/transcriptions" {
            let mut url = crate::openai_compat::transcription::transcribe_url(&self.base_url);
            url.set_query(scrubbed_query);
            url
        } else if let Some(suffix) = context
            .path
            .strip_prefix("/v1/")
            .filter(|_| self.openai_oauth)
        {
            let mut url = self.base_url.clone();
            let path = format!("{}/{suffix}", url.path().trim_end_matches('/'));
            url.set_path(&path);
            url.set_query(scrubbed_query);
            url
        } else {
            cc_lb_upstream::upstream_request_url(&self.base_url, &context.path, scrubbed_query)
        };
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}

pub(super) fn resolve_fanout_call_usage(
    body: &[u8],
    openai_oauth_sent_service_tier: Option<&Option<String>>,
) -> Option<UsageCounts> {
    let policy = if openai_oauth_sent_service_tier.is_some() {
        usage_parser::ServiceTierMergePolicy::OpenaiOauth
    } else {
        usage_parser::ServiceTierMergePolicy::Standard
    };
    let observed = crate::openai_compat::chat::responses_usage_from_sse_with_policy(body, policy)?;
    let mut counts = responses_usage_value_to_counts(&observed);
    if let Some(sent) = openai_oauth_sent_service_tier {
        usage_parser::apply_openai_oauth_service_tier(&mut counts, sent.as_deref());
    }
    Some(counts)
}

fn merge_fanout_evidence(
    current: usage_parser::CacheTokenEvidence,
    incoming: usage_parser::CacheTokenEvidence,
) -> usage_parser::CacheTokenEvidence {
    use usage_parser::CacheTokenEvidence;
    match (current, incoming) {
        (CacheTokenEvidence::Invalid, _) | (_, CacheTokenEvidence::Invalid) => {
            CacheTokenEvidence::Invalid
        }
        (CacheTokenEvidence::Positive, _) | (_, CacheTokenEvidence::Positive) => {
            CacheTokenEvidence::Positive
        }
        (CacheTokenEvidence::Zero, _) | (_, CacheTokenEvidence::Zero) => CacheTokenEvidence::Zero,
        (CacheTokenEvidence::Unreported, CacheTokenEvidence::Unreported) => {
            CacheTokenEvidence::Unreported
        }
    }
}

fn select_fanout_service_tier(calls: &[UsageCounts]) -> Option<String> {
    let tier_rank = |tier: &str| -> u8 {
        let lower = tier.trim().to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "auto" | "default" | "standard" | "standard_only" | "unspecified" | ""
        ) {
            1
        } else {
            0
        }
    };
    calls
        .iter()
        .filter_map(|call| call.service_tier.as_deref())
        .map(str::trim)
        .filter(|tier| !tier.is_empty())
        .min_by(|a, b| {
            tier_rank(a)
                .cmp(&tier_rank(b))
                .then_with(|| a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase()))
        })
        .map(str::to_owned)
}

pub(super) fn usage_counts_to_pricing_slice(u: &UsageCounts) -> cc_lb_lifecycle::UsagePricingSlice {
    cc_lb_lifecycle::UsagePricingSlice {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        input_image_tokens: u.input_image_tokens,
        output_image_tokens: u.output_image_tokens,
        cache_creation_input_tokens: u.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: u.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: u.cache_creation_input_tokens_1h,
        cache_read_input_tokens: u.cache_read_input_tokens,
        cache_write_input_tokens: u.cache_write_input_tokens,
        cache_write_evidence: match u.cache_write_evidence {
            usage_parser::CacheTokenEvidence::Unreported => None,
            evidence => Some(request_cache_evidence(evidence)),
        },
        cache_write_input_tokens_30m: u.cache_write_input_tokens_30m,
        cache_write_input_tokens_12h: u.cache_write_input_tokens_12h,
        service_tier: u.service_tier.clone(),
    }
}

pub(super) fn aggregate_fanout_usage_counts(calls: &[UsageCounts]) -> Option<UsageCounts> {
    let first = calls.first()?;
    if calls.len() == 1 {
        return Some(first.clone());
    }
    let add_opt_u64 = |acc: &mut Option<u64>, add: Option<u64>| {
        if let Some(value) = add {
            *acc = Some(acc.unwrap_or_default().saturating_add(value));
        }
    };
    let mut combined = UsageCounts {
        present: calls.iter().any(|c| c.present),
        billable_present: calls.iter().any(|c| c.billable_present),
        thinking_progress_present: calls.iter().all(|c| c.thinking_progress_present),
        input_includes_cached: first.input_includes_cached,
        service_tier: select_fanout_service_tier(calls),
        pricing_slices: calls.iter().map(usage_counts_to_pricing_slice).collect(),
        ..UsageCounts::default()
    };
    for call in calls {
        combined.input_tokens = combined.input_tokens.saturating_add(call.input_tokens);
        combined.output_tokens = combined.output_tokens.saturating_add(call.output_tokens);
        combined.cache_creation_input_tokens = combined
            .cache_creation_input_tokens
            .saturating_add(call.cache_creation_input_tokens);
        combined.cache_creation_input_tokens_5m = combined
            .cache_creation_input_tokens_5m
            .saturating_add(call.cache_creation_input_tokens_5m);
        combined.cache_creation_input_tokens_1h = combined
            .cache_creation_input_tokens_1h
            .saturating_add(call.cache_creation_input_tokens_1h);
        combined.cache_read_input_tokens = combined
            .cache_read_input_tokens
            .saturating_add(call.cache_read_input_tokens);
        combined.thinking_tokens = combined
            .thinking_tokens
            .saturating_add(call.thinking_tokens);
        add_opt_u64(&mut combined.gross_input_tokens, call.gross_input_tokens);
        add_opt_u64(&mut combined.input_image_tokens, call.input_image_tokens);
        add_opt_u64(&mut combined.output_image_tokens, call.output_image_tokens);
        add_opt_u64(
            &mut combined.tool_use_prompt_tokens,
            call.tool_use_prompt_tokens,
        );
        add_opt_u64(
            &mut combined.cache_write_input_tokens,
            call.cache_write_input_tokens,
        );
        add_opt_u64(
            &mut combined.cache_write_input_tokens_30m,
            call.cache_write_input_tokens_30m,
        );
        add_opt_u64(
            &mut combined.cache_write_input_tokens_12h,
            call.cache_write_input_tokens_12h,
        );
        combined.cache_creation_evidence = merge_fanout_evidence(
            combined.cache_creation_evidence,
            call.cache_creation_evidence,
        );
        combined.cache_read_evidence =
            merge_fanout_evidence(combined.cache_read_evidence, call.cache_read_evidence);
        combined.cache_write_evidence =
            merge_fanout_evidence(combined.cache_write_evidence, call.cache_write_evidence);
    }
    Some(combined)
}

pub(super) fn responses_usage_value_to_counts(
    observed: &crate::openai_compat::chat::ResponsesSseUsage,
) -> UsageCounts {
    let mut envelope = Map::new();
    let mut response = Map::new();
    response.insert("usage".to_owned(), observed.usage.clone());
    if let Some(tier) = observed.service_tier.as_deref() {
        response.insert("service_tier".to_owned(), Value::String(tier.to_owned()));
    }
    envelope.insert("response".to_owned(), Value::Object(response));
    usage_parser::observe_non_stream_json_value_for_shape(
        &Value::Object(envelope),
        crate::provider_capability::UsageShape::OpenaiResponses,
    )
    .usage
}

pub(super) fn body_cap_for_path(config: &LifecycleConfig, path: &str) -> usize {
    const ACCOUNT_OWNED_ROOTS: &[&str] = &[
        "/v1/files",
        "/v1/uploads",
        "/v1/batches",
        "/v1/vector_stores",
        "/v1/conversations",
        "/v1/containers",
        "/v1/videos",
        "/v1/evals",
        "/v1/fine_tuning",
        "/v1/assistants",
        "/v1/threads",
        "/v1/skills",
        "/v1/chatkit",
        "/v1/audio/voices",
    ];
    let account_owned = ACCOUNT_OWNED_ROOTS.iter().any(|root| {
        path.strip_prefix(root)
            .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/'))
    });
    if account_owned || is_multipart_operation(path) {
        config.files_body_cap_bytes
    } else {
        config.messages_body_cap_bytes
    }
}

pub fn is_multipart_operation(path: &str) -> bool {
    matches!(
        path,
        "/v1/audio/transcriptions"
            | "/v1/audio/translations"
            | "/v1/images/edits"
            | "/v1/images/variations"
    )
}

pub(super) async fn multipart_request_model(
    _auth: &crate::authn_rail::Authenticated,
    headers: &HeaderMap,
    body: &Bytes,
) -> Option<String> {
    let content_type = headers.get(http::header::CONTENT_TYPE)?.to_str().ok()?;
    let boundary = multer::parse_boundary(content_type).ok()?;
    let stream = Body::from(body.clone()).into_data_stream();
    let mut multipart = multer::Multipart::new(stream, boundary);
    while let Some(field) = multipart.next_field().await.ok()? {
        if field.name() == Some("model") && field.file_name().is_none() {
            return field.text().await.ok();
        }
    }
    None
}

pub(super) fn unsupported_operation_response(
    scope: Option<cc_lb_domain::Provider>,
    method: &Method,
    path: &str,
) -> Response<Body> {
    let Some(provider) = scope else {
        return anthropic_error_response(
            StatusCode::NOT_FOUND,
            "operation_not_supported",
            "operation is not supported by the consistency-safe proxy boundary",
        );
    };
    crate::error_format::provider_scope_error_response(
        provider,
        StatusCode::NOT_FOUND,
        "operation_not_supported",
        &format!("{method} {path} is not supported by cc-lb yet"),
    )
}

pub(super) fn header_to_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

/// Project the upstream response headers into a `HeaderSnapshot` for the
/// `UpstreamResponseStarted` lifecycle event.
///
/// (we only copy the fields explicitly whitelisted). Provider-specific quota
/// fields are carried in separate maps and parsed by the off-path subscriber.
pub(super) fn header_snapshot_from(
    headers: &HeaderMap,
    upstream_kind: StorageUpstreamKind,
) -> HeaderSnapshot {
    let mut anthropic_headers = std::collections::BTreeMap::new();
    let mut provider_headers = std::collections::BTreeMap::new();
    let is_openai = matches!(
        upstream_kind,
        StorageUpstreamKind::OpenaiApiKey | StorageUpstreamKind::OpenaiOauth
    );
    let is_gemini = matches!(
        upstream_kind,
        StorageUpstreamKind::GeminiApiKey
            | StorageUpstreamKind::GeminiOauth
            | StorageUpstreamKind::VertexApiKey
            | StorageUpstreamKind::VertexCredential
    );
    for (name, value) in headers.iter() {
        let name_lc = name.as_str();
        let Ok(val) = value.to_str() else { continue };
        let is_anthropic = name_lc.starts_with("anthropic-ratelimit-")
            || cc_lb_domain::ANTHROPIC_IDENTITY_HEADERS.contains(&name_lc);
        let is_codex = name_lc.starts_with("x-codex-");
        let is_openai_standard = is_openai && name_lc.starts_with("x-ratelimit-");
        let is_common = matches!(
            name_lc,
            "ratelimit-limit" | "ratelimit-remaining" | "ratelimit-reset"
        );
        if is_anthropic || is_common {
            anthropic_headers.insert(name_lc.to_owned(), val.to_owned());
        }
        if is_codex || is_openai_standard || (is_gemini && name_lc == "x-goog-request-id") {
            provider_headers.insert(name_lc.to_owned(), val.to_owned());
        }
    }
    HeaderSnapshot {
        content_type: header_to_string(headers, "content-type"),
        content_encoding: header_to_string(headers, "content-encoding"),
        request_id: header_to_string(headers, "x-request-id")
            .or_else(|| header_to_string(headers, "request-id"))
            .or_else(|| header_to_string(headers, "x-goog-request-id")),
        retry_after: header_to_string(headers, "retry-after"),
        provider_headers,
        anthropic_headers,
    }
}

pub(super) fn next_request_id() -> String {
    let id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut request_id = String::with_capacity("req_core_".len() + 20);
    request_id.push_str("req_core_");
    let _ = write!(&mut request_id, "{id}");
    request_id
}

pub(super) fn system_time_to_unix_millis(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

pub(super) fn apply_prompt_cache_timings(
    setup: &mut cc_lb_lifecycle::RequestSetupTimings,
    prompt_cache: PromptCacheAnalysisTimings,
) {
    setup.cache_structure_ms = prompt_cache.cache_structure_ms;
    setup.cache_token_key_ms = prompt_cache.cache_token_key_ms;
    setup.cache_count_lookup_ms = prompt_cache.cache_count_lookup_ms;
    setup.cache_tokenizer_queue_ms = prompt_cache.cache_tokenizer_queue_ms;
    setup.cache_serialize_ms = prompt_cache.cache_serialize_ms;
    setup.cache_tokenize_ms = prompt_cache.cache_tokenize_ms;
}

pub(super) fn store_setup_timings(
    observer: Option<&LifecycleContext>,
    timings: cc_lb_lifecycle::RequestSetupTimings,
) {
    if let Some(observer) = observer {
        observer.set_setup_timings(timings);
    }
}

// Shared parse of the request body inside `handle`. Every downstream consumer
// (stream / model / max_tokens / cache metadata) reads through this view so
// the body JSON is parsed exactly once per request instead of up to 7 times.
//
// Parser: `sonic_rs::from_slice` — hot-path SIMD JSON parser. Yields a
// `serde_json::Value` so all downstream consumers stay compatible without
// touching the serialization side (see `docs/adr/0002-json-library-strategy.md`).
pub(super) struct RequestBodyView {
    pub(super) parsed: Result<Arc<Value>, sonic_rs::Error>,
    pub(super) json_parse_ms: f64,
}

impl RequestBodyView {
    pub(super) fn new(body: &Bytes) -> Self {
        let started = Instant::now();
        let parsed = sonic_rs::from_slice::<Value>(body);
        let json_parse_ms = started.elapsed().as_secs_f64() * 1_000.0;
        Self {
            parsed: parsed.map(Arc::new),
            json_parse_ms,
        }
    }

    pub(super) fn value(&self) -> Option<&Value> {
        self.parsed.as_deref().ok()
    }

    pub(super) fn value_arc(&self) -> Option<Arc<Value>> {
        self.parsed.as_ref().ok().cloned()
    }

    pub(super) fn is_valid_json(&self) -> bool {
        self.parsed.is_ok()
    }

    pub(super) fn json_parse_ms(&self) -> f64 {
        self.json_parse_ms
    }

    pub(super) fn stream(&self) -> bool {
        self.value()
            .and_then(|v| v.get("stream"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    pub(super) fn model(&self) -> Option<String> {
        self.value()
            .and_then(|v| v.get("model"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }

    pub(super) fn limit_request(
        &self,
        fallback_model: &str,
        capability: Option<ProviderCapability>,
        breakpoints: &[CacheBreakpoint],
        use_image_rates: bool,
    ) -> LimitRequest {
        LimitRequest::from_value(
            self.value(),
            fallback_model,
            capability,
            breakpoints,
            use_image_rates,
        )
    }
}

#[derive(Clone)]
pub(crate) struct RequestEventContext {
    pub(crate) request_id: String,
    pub(crate) canonical_model_id: String,
    /// Routed upstream name, matching the `RouteCompleted` lifecycle event.
    pub(crate) upstream_name: String,
    /// Model requested in the body, matching the `RouteCompleted` lifecycle event.
    pub(crate) requested_model: Option<String>,
    pub(crate) proxy_setup_ms: Option<u64>,
    pub(crate) stage_timings: AttemptTimings,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct AttemptTimings {
    pub(super) bulkhead_wait_ms: Option<u64>,
    pub(super) dns_ms: Option<u64>,
    pub(super) connect_ms: Option<u64>,
    pub(super) connection_reused: Option<bool>,
    pub(super) shape_ms: Option<u64>,
    pub(super) sign_ms: Option<u64>,
    pub(super) upstream_ttfb_ms: Option<u64>,
}

impl AttemptTimings {
    pub(super) fn reset_attempt_stages(&mut self) {
        self.bulkhead_wait_ms = None;
        self.dns_ms = None;
        self.connect_ms = None;
        self.connection_reused = None;
        self.shape_ms = None;
        self.sign_ms = None;
        self.upstream_ttfb_ms = None;
    }
}

pub(super) fn unix_now_ms(clock: &dyn Clock) -> u64 {
    unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64
}

pub(super) fn response_start_unix_secs(
    dispatch_unix_secs: u64,
    dispatch_started: Instant,
    response_start_at: Option<Instant>,
) -> u64 {
    match response_start_at {
        Some(at) => dispatch_unix_secs
            .saturating_add(at.saturating_duration_since(dispatch_started).as_secs()),
        None => dispatch_unix_secs,
    }
}

pub(super) fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub(super) fn duration_to_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn to_usage_snapshot(u: &UsageCounts) -> cc_lb_lifecycle::UsageSnapshot {
    cc_lb_lifecycle::UsageSnapshot {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        input_image_tokens: u.input_image_tokens,
        output_image_tokens: u.output_image_tokens,
        tool_use_prompt_tokens: u.tool_use_prompt_tokens,
        cache_creation_input_tokens: u.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: u.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: u.cache_creation_input_tokens_1h,
        cache_read_evidence: match u.cache_read_evidence {
            usage_parser::CacheTokenEvidence::Unreported => None,
            usage_parser::CacheTokenEvidence::Zero => {
                Some(cc_lb_request_log::RequestCacheEvidence::Zero)
            }
            usage_parser::CacheTokenEvidence::Positive => {
                Some(cc_lb_request_log::RequestCacheEvidence::Positive)
            }
            usage_parser::CacheTokenEvidence::Invalid => {
                Some(cc_lb_request_log::RequestCacheEvidence::Invalid)
            }
        },
        cache_creation_evidence: match u.cache_creation_evidence {
            usage_parser::CacheTokenEvidence::Unreported => None,
            usage_parser::CacheTokenEvidence::Zero => {
                Some(cc_lb_request_log::RequestCacheEvidence::Zero)
            }
            usage_parser::CacheTokenEvidence::Positive => {
                Some(cc_lb_request_log::RequestCacheEvidence::Positive)
            }
            usage_parser::CacheTokenEvidence::Invalid => {
                Some(cc_lb_request_log::RequestCacheEvidence::Invalid)
            }
        },
        cache_read_input_tokens: u.cache_read_input_tokens,
        cache_write_input_tokens: u.cache_write_input_tokens,
        cache_write_evidence: match u.cache_write_evidence {
            usage_parser::CacheTokenEvidence::Unreported => None,
            evidence => Some(request_cache_evidence(evidence)),
        },
        cache_write_input_tokens_30m: u.cache_write_input_tokens_30m,
        cache_write_input_tokens_12h: u.cache_write_input_tokens_12h,
        thinking_tokens: u.thinking_tokens,
        service_tier: u.service_tier.clone(),
        iterations: u.iterations.clone(),
        pricing_slices: u.pricing_slices.clone(),
    }
}

pub(super) fn principal_kind_lite(kind: DbPrincipalKind) -> PrincipalKindLite {
    match kind {
        DbPrincipalKind::Human => PrincipalKindLite::Human,
        DbPrincipalKind::Machine | DbPrincipalKind::Admin => PrincipalKindLite::Machine,
    }
}

pub(super) fn db_principal_kind_as_str(kind: DbPrincipalKind) -> &'static str {
    match kind {
        DbPrincipalKind::Machine => "machine",
        DbPrincipalKind::Human => "human",
        DbPrincipalKind::Admin => "admin",
    }
}

#[cfg(test)]
mod lifecycle_model_identity_tests {
    use super::{
        ProviderCapability, canonical_model_for_capability, lifecycle_request_model,
        request_cache_metadata_from_value_with_analysis_for_capability,
    };
    use serde_json::json;

    #[test]
    fn canonicalizes_only_gemini_internal_request_identity() {
        assert_eq!(
            lifecycle_request_model(
                Some(ProviderCapability::GeminiGenerateContent),
                "models/gemini-2.5-flash",
            ),
            "gemini-2.5-flash"
        );
        assert_eq!(
            lifecycle_request_model(
                Some(ProviderCapability::GeminiGenerateContent),
                "models/models/gemini-2.5-flash",
            ),
            "models/gemini-2.5-flash"
        );
        assert_eq!(
            lifecycle_request_model(Some(ProviderCapability::OpenaiResponses), "models/gpt-5",),
            "models/gpt-5"
        );
    }

    #[test]
    fn native_gemini_path_model_precedes_body_for_canonical_context() {
        let headers = http::HeaderMap::new();
        let body = json!({"model": "models/body-model"});
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &headers,
            Some(&body),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            Some("models/path-model"),
        );
        assert_eq!(metadata.canonical_model_id, "path-model");
        assert_eq!(
            canonical_model_for_capability(
                Some(ProviderCapability::GeminiCachedContentsCreate),
                "models/gemini-2.5-flash",
            ),
            "gemini-2.5-flash"
        );

        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &headers,
            None,
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            Some("models/path-model"),
        );
        assert_eq!(metadata.canonical_model_id, "path-model");
    }
}
