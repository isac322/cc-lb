use std::borrow::Cow;

use bytes::Bytes;
use cc_lb_domain::Provider;
use http::StatusCode;
use url::Url;

use crate::{AuthKindId, AuthMode, ProviderId};

/// Descriptor for one authentication kind owned by a [`ProviderAdapter`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthKindDescriptor {
    /// Canonical authentication kind identifier.
    pub id: AuthKindId,
    /// Human-readable display name.
    pub display_name: Cow<'static, str>,
    /// Authentication mode used by credentials of this kind.
    pub auth_mode: AuthMode,
    /// Stable metric / lifecycle event label for this authentication kind.
    pub metric_label: Cow<'static, str>,
    /// Default proxy data-plane base URL.
    pub default_base_url: Cow<'static, str>,
    /// Optional admin/control fallback base URL when it differs from `default_base_url`.
    pub default_control_base_url: Option<Cow<'static, str>>,
    /// Optional admin preflight fallback base URL when it differs from control/runtime URLs.
    pub default_preflight_base_url: Option<Cow<'static, str>>,
    /// Whether records of this kind carry Vertex project/location configuration.
    pub requires_vertex_config: bool,
}

impl AuthKindDescriptor {
    /// Returns the control-plane fallback URL string, defaulting to [`Self::default_base_url`].
    pub fn control_base_url(&self) -> &str {
        self.default_control_base_url
            .as_deref()
            .unwrap_or(self.default_base_url.as_ref())
    }

    /// Returns the admin preflight fallback URL string, defaulting to [`Self::control_base_url`].
    pub fn preflight_base_url(&self) -> &str {
        self.default_preflight_base_url
            .as_deref()
            .unwrap_or_else(|| self.control_base_url())
    }

    /// Parses [`Self::default_base_url`] into a [`Url`].
    pub fn default_base_url_parsed(&self) -> Result<Url, url::ParseError> {
        Url::parse(self.default_base_url.as_ref())
    }

    /// Parses [`Self::control_base_url`] into a [`Url`].
    pub fn control_base_url_parsed(&self) -> Result<Url, url::ParseError> {
        Url::parse(self.control_base_url())
    }

    /// Parses [`Self::preflight_base_url`] into a [`Url`].
    pub fn preflight_base_url_parsed(&self) -> Result<Url, url::ParseError> {
        Url::parse(self.preflight_base_url())
    }
}

/// Parameters for formatting a provider-native JSON error payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorFormatRequest<'a> {
    /// HTTP status code associated with the rejection.
    pub status: StatusCode,
    /// Provider-style error type / machine code string.
    pub error_type: &'a str,
    /// Redacted human-readable error message.
    pub message: &'a str,
    /// Optional quota or rate-limit dimension associated with a limit rejection.
    pub limit_kind: Option<&'a str>,
}

/// Extension trait implemented by each isolated provider adapter crate.
pub trait ProviderAdapter: Send + Sync {
    /// Unique canonical provider identifier.
    fn provider_id(&self) -> ProviderId;

    /// Human-readable provider display name.
    fn display_name(&self) -> &str;

    /// Optional built-in wire protocol [`Provider`] associated with this adapter.
    fn wire_provider(&self) -> Option<Provider> {
        None
    }

    /// Authentication kinds registered for this provider.
    fn auth_kinds(&self) -> &[AuthKindDescriptor];

    /// Resolves a provider-specific model alias to its canonical form.
    fn canonical_model_id<'a>(&self, requested: &'a str) -> &'a str {
        requested
    }

    /// Returns the provider-specific minimum cacheable prefix threshold in tokens for `canonical`, if known.
    fn cache_threshold_tokens(&self, _canonical: &str) -> Option<usize> {
        None
    }

    /// Formats a provider-native JSON error response body.
    fn format_error_body(&self, req: ErrorFormatRequest<'_>) -> Bytes;
}
