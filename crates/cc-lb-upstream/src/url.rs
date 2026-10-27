use std::borrow::Cow;

use url::Url;

/// Removes case-insensitive `key` query parameters while preserving all other
/// query segments and their order.
#[must_use]
pub fn scrub_query_api_key(query: &str) -> Cow<'_, str> {
    let has_api_key = query.split('&').any(|segment| {
        segment
            .split('=')
            .next()
            .unwrap_or(segment)
            .eq_ignore_ascii_case("key")
    });
    if !has_api_key {
        return Cow::Borrowed(query);
    }

    Cow::Owned(
        query
            .split('&')
            .filter(|segment| {
                !segment
                    .split('=')
                    .next()
                    .unwrap_or(segment)
                    .eq_ignore_ascii_case("key")
            })
            .collect::<Vec<_>>()
            .join("&"),
    )
}

/// Removes case-insensitive `key` query parameters from a URL.
pub fn scrub_url_query_api_key(url: &mut Url) {
    let Some(query) = url.query() else {
        return;
    };
    let scrubbed = scrub_query_api_key(query);
    let (remove, replacement) = match scrubbed {
        Cow::Borrowed(clean) => (clean.is_empty(), None),
        Cow::Owned(clean) => {
            if clean.is_empty() {
                (true, None)
            } else {
                (false, Some(clean))
            }
        }
    };
    if remove {
        url.set_query(None);
    } else if let Some(clean) = replacement {
        url.set_query(Some(&clean));
    }
}

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
    use super::{scrub_query_api_key, scrub_url_query_api_key, upstream_request_url};
    use std::borrow::Cow;
    use url::Url;

    #[test]
    fn scrub_query_api_key_removes_key_segments() {
        assert_eq!(scrub_query_api_key("key=secret"), "");
        assert_eq!(scrub_query_api_key("KEY=secret"), "");
        assert_eq!(scrub_query_api_key("foo=1&key=secret&bar=2"), "foo=1&bar=2");
        assert_eq!(scrub_query_api_key("key"), "");
        assert!(matches!(scrub_query_api_key("foo=1"), Cow::Borrowed(_)));
    }

    #[test]
    fn scrub_url_query_api_key_preserves_non_key_segments() {
        let mut url = Url::parse("https://example.com/path?key=secret&keep=1").expect("url");
        scrub_url_query_api_key(&mut url);
        assert_eq!(url.as_str(), "https://example.com/path?keep=1");

        let mut only_key = Url::parse("https://example.com/path?KEY=secret").expect("url");
        scrub_url_query_api_key(&mut only_key);
        assert_eq!(only_key.as_str(), "https://example.com/path");
    }

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
