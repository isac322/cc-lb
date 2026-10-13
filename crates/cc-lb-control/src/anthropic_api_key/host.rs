//! Host rules for API-key upstreams.

use url::Url;

/// Whether org Admin API probes may run for an upstream whose `base_url` is
/// `base_url`, given the configured org API `origin`.
///
/// Fails closed: true only when the parsed URLs share scheme, host, and
/// effective port, and `base_url` has no userinfo, a root (or empty) path,
/// no query, and no fragment. Inference probes still go to `base_url`; this
/// rule only gates the org probes.
pub fn org_api_applies(base_url: &Url, origin: &Url) -> bool {
    base_url.scheme() == origin.scheme()
        && base_url.username().is_empty()
        && base_url.password().is_none()
        && base_url.host_str().is_some()
        && base_url.host_str() == origin.host_str()
        && base_url.port_or_known_default() == origin.port_or_known_default()
        && matches!(base_url.path(), "" | "/")
        && base_url.query().is_none()
        && base_url.fragment().is_none()
}

/// Write-time validation of any upstream `base_url`: `http`/`https` only and
/// no userinfo. The error is the operator-facing reason.
pub fn validate_base_url(base_url: &Url) -> Result<(), &'static str> {
    if !matches!(base_url.scheme(), "http" | "https") {
        return Err("base_url must use http or https");
    }
    if !base_url.username().is_empty() || base_url.password().is_some() {
        return Err("base_url must not contain credentials");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ORG-HOST-01: comparator table against the production origin.
    #[test]
    fn org_host_01_comparator_table() {
        let origin = Url::parse("https://api.anthropic.com/");
        assert!(origin.is_ok());
        let Ok(origin) = origin else { return };
        let cases = [
            ("https://api.anthropic.com", true),
            ("https://api.anthropic.com/", true),
            ("https://api.anthropic.com:443/", true),
            ("https://API.ANTHROPIC.COM/", true),
            ("http://api.anthropic.com", false),
            ("https://api.anthropic.com:8443", false),
            ("https://api.anthropic.com.evil.com/", false),
            ("https://api.anthropic.com@evil.com/", false),
            ("https://evil.com/?h=api.anthropic.com", false),
            ("https://evil.com/api.anthropic.com/", false),
            ("https://api.anthropic.com./", false),
            ("https://xn--api-anthropic-xyz.com/", false),
            ("https://api.anthropic.com/v1/", false),
            ("https://user:pass@api.anthropic.com/", false),
            ("https://api.anthropic.com/?x=1", false),
            ("https://api.anthropic.com/#frag", false),
        ];
        for (raw, expected) in cases {
            let parsed = Url::parse(raw);
            assert!(parsed.is_ok(), "{raw} parses");
            let applies = parsed.is_ok_and(|url| org_api_applies(&url, &origin));
            assert_eq!(applies, expected, "{raw}");
        }
    }

    #[test]
    fn custom_origin_matches_only_itself() {
        let (Ok(origin), Ok(same), Ok(other_port)) = (
            Url::parse("http://127.0.0.1:4100/"),
            Url::parse("http://127.0.0.1:4100"),
            Url::parse("http://127.0.0.1:4101/"),
        ) else {
            panic!("fixture URLs parse");
        };
        assert!(org_api_applies(&same, &origin));
        assert!(!org_api_applies(&other_port, &origin));
    }

    #[test]
    fn validate_base_url_accepts_http_and_https_without_userinfo() {
        let check = |raw: &str| Url::parse(raw).map(|url| validate_base_url(&url));
        assert_eq!(check("https://proxy.example.com/v1/"), Ok(Ok(())));
        assert_eq!(check("http://127.0.0.1:9000/"), Ok(Ok(())));
        for bad in [
            "file:///etc/passwd",
            "ftp://h/",
            "data:,x",
            "javascript:alert(1)",
            "https://u:p@h/",
            "https://u@h/",
        ] {
            assert!(matches!(check(bad), Ok(Err(_))), "{bad}");
        }
    }
}
