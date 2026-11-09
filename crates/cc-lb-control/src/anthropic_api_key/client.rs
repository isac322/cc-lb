//! Construction-time dependency for Anthropic org Admin API calls.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use url::Url;

use super::host::org_api_applies;

/// Production org Admin API origin.
const PRODUCTION_ORIGIN: &str = "https://api.anthropic.com/";

/// HTTP client for key probes. The legacy hyper client never follows
/// redirects.
pub type ProbeHttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// Bounds applied to every preflight/Re-check and poll probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeLimits {
    /// Deadline for one probe request, including the bounded body read.
    pub per_probe: Duration,
    /// Deadline for a whole preflight.
    pub total: Duration,
    /// Concurrent org probes within one preflight.
    pub max_concurrent_probes: usize,
    /// Largest response body read from a probe.
    pub max_body_bytes: usize,
    /// Pages fetched from a paginated org endpoint.
    pub page_cap: u32,
}

impl ProbeLimits {
    pub const PRODUCTION: Self = Self {
        per_probe: Duration::from_secs(10),
        total: Duration::from_secs(30),
        max_concurrent_probes: 4,
        max_body_bytes: 1024 * 1024,
        page_cap: 3,
    };
}

/// Where org Admin API probes go and how they are bounded.
///
/// Built once at startup with [`AnthropicOrgApi::production`] and shared by
/// the admin API and the scheduler. Tests point it at a local fixture with
/// [`AnthropicOrgApi::with_origin`]; there is no config field or env var.
#[derive(Clone)]
pub struct AnthropicOrgApi {
    origin: Url,
    http: ProbeHttpClient,
    limits: ProbeLimits,
}

impl AnthropicOrgApi {
    /// The `https://api.anthropic.com/` origin with production limits.
    pub fn production() -> Result<Self, url::ParseError> {
        Url::parse(PRODUCTION_ORIGIN).map(Self::with_origin)
    }

    /// A custom origin with production limits.
    pub fn with_origin(origin: Url) -> Self {
        Self {
            origin,
            http: probe_http_client(),
            limits: ProbeLimits::PRODUCTION,
        }
    }

    pub fn origin(&self) -> &Url {
        &self.origin
    }

    /// Whether org probes run for an upstream with this `base_url`.
    pub fn applies_to(&self, base_url: &Url) -> bool {
        org_api_applies(base_url, &self.origin)
    }

    pub fn limits(&self) -> &ProbeLimits {
        &self.limits
    }

    pub fn http(&self) -> &ProbeHttpClient {
        &self.http
    }
}

impl fmt::Debug for AnthropicOrgApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnthropicOrgApi")
            .field("origin", &self.origin.as_str())
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

fn probe_http_client() -> ProbeHttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_targets_the_anthropic_origin() {
        let api = AnthropicOrgApi::production();
        assert!(api.is_ok());
        let Ok(api) = api else { return };
        assert_eq!(api.origin().as_str(), "https://api.anthropic.com/");
        assert_eq!(api.limits(), &ProbeLimits::PRODUCTION);
        let applies = Url::parse("https://api.anthropic.com").map(|url| api.applies_to(&url));
        assert_eq!(applies, Ok(true));
        let custom = Url::parse("https://proxy.example.com/").map(|url| api.applies_to(&url));
        assert_eq!(custom, Ok(false));
    }
}
