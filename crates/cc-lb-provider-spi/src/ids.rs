use std::borrow::Cow;
use std::fmt;

use cc_lb_domain::{CredentialKind, Provider, UpstreamKind};
use serde::{Deserialize, Serialize};

/// Canonical identifier for a logical provider adapter in [`crate::ProviderRegistry`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderId(Cow<'static, str>);

impl ProviderId {
    /// Anthropic Claude provider identifier.
    pub const ANTHROPIC: Self = Self::from_static("anthropic");
    /// OpenAI provider identifier.
    pub const OPENAI: Self = Self::from_static("openai");
    /// Google Gemini direct / Code Assist provider identifier.
    pub const GEMINI: Self = Self::from_static("gemini");
    /// Google Cloud Vertex AI provider identifier.
    pub const VERTEX: Self = Self::from_static("vertex");

    /// Constructs a [`ProviderId`] from a static string slice.
    pub const fn from_static(id: &'static str) -> Self {
        Self(Cow::Borrowed(id))
    }

    /// Constructs a [`ProviderId`] from an owned or borrowed string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(Cow::Owned(id.into()))
    }

    /// Returns the string representation of this provider identifier.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for ProviderId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&'static str> for ProviderId {
    fn from(value: &'static str) -> Self {
        Self::from_static(value)
    }
}

impl From<String> for ProviderId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<Provider> for ProviderId {
    fn from(provider: Provider) -> Self {
        match provider {
            Provider::Anthropic => Self::ANTHROPIC,
            Provider::Openai => Self::OPENAI,
            Provider::Gemini => Self::GEMINI,
        }
    }
}

/// Canonical identifier for an authentication / upstream kind supported by a provider adapter.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AuthKindId(Cow<'static, str>);

impl AuthKindId {
    /// Anthropic API-key authentication kind.
    pub const ANTHROPIC_API_KEY: Self = Self::from_static("anthropic_api_key");
    /// Anthropic OAuth authentication kind.
    pub const ANTHROPIC_OAUTH: Self = Self::from_static("anthropic_oauth");
    /// OpenAI API-key authentication kind.
    pub const OPENAI_API_KEY: Self = Self::from_static("openai_api_key");
    /// OpenAI OAuth subscription authentication kind.
    pub const OPENAI_OAUTH: Self = Self::from_static("openai_oauth");
    /// Gemini API-key authentication kind.
    pub const GEMINI_API_KEY: Self = Self::from_static("gemini_api_key");
    /// Gemini OAuth subscription authentication kind.
    pub const GEMINI_OAUTH: Self = Self::from_static("gemini_oauth");
    /// Vertex AI API-key authentication kind.
    pub const VERTEX_API_KEY: Self = Self::from_static("vertex_api_key");
    /// Vertex AI service-account / ADC credential authentication kind.
    pub const VERTEX_CREDENTIAL: Self = Self::from_static("vertex_credential");

    /// Constructs an [`AuthKindId`] from a static string slice.
    pub const fn from_static(id: &'static str) -> Self {
        Self(Cow::Borrowed(id))
    }

    /// Constructs an [`AuthKindId`] from an owned or borrowed string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(Cow::Owned(id.into()))
    }

    /// Returns the string representation of this authentication kind identifier.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl fmt::Display for AuthKindId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for AuthKindId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&'static str> for AuthKindId {
    fn from(value: &'static str) -> Self {
        Self::from_static(value)
    }
}

impl From<String> for AuthKindId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<UpstreamKind> for AuthKindId {
    fn from(kind: UpstreamKind) -> Self {
        match kind {
            UpstreamKind::AnthropicApiKey => Self::ANTHROPIC_API_KEY,
            UpstreamKind::AnthropicOauth => Self::ANTHROPIC_OAUTH,
            UpstreamKind::OpenaiApiKey => Self::OPENAI_API_KEY,
            UpstreamKind::OpenaiOauth => Self::OPENAI_OAUTH,
            UpstreamKind::GeminiApiKey => Self::GEMINI_API_KEY,
            UpstreamKind::GeminiOauth => Self::GEMINI_OAUTH,
            UpstreamKind::VertexApiKey => Self::VERTEX_API_KEY,
            UpstreamKind::VertexCredential => Self::VERTEX_CREDENTIAL,
        }
    }
}

/// High-level authentication mode for an upstream credential kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// Static API key sent via provider auth headers or query params.
    ApiKey,
    /// OAuth access/refresh token bundle.
    Oauth,
    /// Cloud service-account / ADC credential bundle (e.g. Vertex AI).
    ServiceCredential,
}

impl AuthMode {
    /// Maps this authentication mode into the coarse routing credential category.
    pub const fn credential_kind(self) -> CredentialKind {
        match self {
            Self::ApiKey | Self::ServiceCredential => CredentialKind::ApiKey,
            Self::Oauth => CredentialKind::Oauth,
        }
    }
}
