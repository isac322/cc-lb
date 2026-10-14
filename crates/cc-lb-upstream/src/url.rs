use url::Url;

/// Builds the URL a request for `path` is sent to on an upstream at
/// `base_url`.
///
/// Only the scheme, host and port of `base_url` are kept: `path` replaces
/// any path on `base_url`, trailing slash or not, and `query` replaces its
/// query. The proxy and the API-key preflight both use this, so preflight
/// checks exactly the endpoint live traffic reaches.
#[must_use]
pub fn upstream_request_url(base_url: &Url, path: &str, query: Option<&str>) -> Url {
    let mut url = base_url.clone();
    url.set_path(path.trim_start_matches('/'));
    url.set_query(query);
    url
}

#[cfg(test)]
mod tests {
    use super::upstream_request_url;
    use url::Url;

    fn built(base: &str, path: &str, query: Option<&str>) -> String {
        let base = Url::parse(base).expect("test base URL parses");
        upstream_request_url(&base, path, query).to_string()
    }

    #[test]
    fn origin_only_base_gets_the_request_path() {
        assert_eq!(
            built("https://api.anthropic.com", "/v1/models", None),
            "https://api.anthropic.com/v1/models"
        );
        assert_eq!(
            built("https://api.anthropic.com/", "/v1/models", None),
            "https://api.anthropic.com/v1/models"
        );
    }

    #[test]
    fn base_path_prefix_is_replaced_with_or_without_trailing_slash() {
        for base in [
            "https://proxy.example.com/prefix",
            "https://proxy.example.com/prefix/",
            "https://proxy.example.com/v1/",
        ] {
            assert_eq!(
                built(base, "/v1/messages/count_tokens", None),
                "https://proxy.example.com/v1/messages/count_tokens",
                "{base}"
            );
        }
    }

    #[test]
    fn port_is_kept_and_query_is_replaced() {
        assert_eq!(
            built(
                "http://127.0.0.1:9000/x?stale=1",
                "v1/messages",
                Some("beta=true")
            ),
            "http://127.0.0.1:9000/v1/messages?beta=true"
        );
        assert_eq!(
            built("http://127.0.0.1:9000/?stale=1", "/v1/models", None),
            "http://127.0.0.1:9000/v1/models"
        );
    }
}
