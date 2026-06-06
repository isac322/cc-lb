//! First-party Anthropic-compatible passthrough dialects.

#![forbid(unsafe_code)]

pub mod direct;

use bytes::Bytes;
use url::Url;

pub use direct::AnthropicDirectDialect;

pub(crate) const ANTHROPIC_API_BASE_URL: &str = "https://api.anthropic.com";

/// Returns upstream SSE bytes without parsing or rewriting them.
pub fn passthrough_sse_bytes(bytes: Bytes) -> Bytes {
    bytes
}

pub(crate) fn compose_url(base_url: &Url, downstream_path: &str, query: Option<&str>) -> Url {
    let mut url = base_url.clone();
    let base_path = base_url.path().trim_end_matches('/');
    let downstream_path = downstream_path.trim_start_matches('/');

    let composed_path = match (base_path.is_empty(), downstream_path.is_empty()) {
        (true, true) => "/".to_owned(),
        (true, false) => {
            let mut path = String::with_capacity(1 + downstream_path.len());
            path.push('/');
            path.push_str(downstream_path);
            path
        }
        (false, true) => base_path.to_owned(),
        (false, false) => {
            let mut path = String::with_capacity(base_path.len() + 1 + downstream_path.len());
            path.push_str(base_path);
            path.push('/');
            path.push_str(downstream_path);
            path
        }
    };

    url.set_path(&composed_path);
    url.set_query(query);
    url
}
