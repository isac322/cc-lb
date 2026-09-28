//! First-party Anthropic-compatible passthrough dialects.

#![forbid(unsafe_code)]

pub mod direct;

use url::Url;

pub use direct::AnthropicDirectDialect;

pub(crate) const ANTHROPIC_API_BASE_URL: &str = "https://api.anthropic.com";

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

#[cfg(test)]
mod tests {
    use super::*;

    fn base(s: &str) -> Url {
        Url::parse(s).expect("base url parses")
    }

    #[test]
    fn empty_base_path_empty_downstream_path_yields_root() {
        let url = compose_url(&base("https://api.anthropic.com/"), "/", None);
        assert_eq!(url.path(), "/");
    }

    #[test]
    fn empty_base_path_non_empty_downstream_path_prefixes_slash() {
        let url = compose_url(&base("https://api.anthropic.com/"), "/v1/messages", None);
        assert_eq!(url.path(), "/v1/messages");
    }

    #[test]
    fn non_empty_base_path_empty_downstream_path_preserves_base() {
        let url = compose_url(&base("https://gateway.example.com/proxy/"), "/", None);
        assert_eq!(url.path(), "/proxy");
    }

    #[test]
    fn non_empty_base_path_non_empty_downstream_path_concatenates() {
        let url = compose_url(
            &base("https://gateway.example.com/proxy/"),
            "/v1/messages",
            None,
        );
        assert_eq!(url.path(), "/proxy/v1/messages");
    }

    #[test]
    fn query_is_forwarded() {
        let url = compose_url(
            &base("https://api.anthropic.com/"),
            "/v1/messages",
            Some("beta=true"),
        );
        assert_eq!(url.query(), Some("beta=true"));
    }
}
