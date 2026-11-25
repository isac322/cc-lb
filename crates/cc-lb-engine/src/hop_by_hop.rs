use std::borrow::Cow;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use http::header::{CONNECTION, COOKIE};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response};
use tower::{Layer, Service};

type StripFuture<T> = Pin<Box<DynStripFuture<T>>>;
type DynStripFuture<T> = dyn Future<Output = T> + Send;

// Hop-by-hop stripping only removes RFC hop-by-hop headers. End-to-end
// request metadata such as x-organization-uuid and X-Trusted-Device-Token must
// survive this layer in the built-in signer flow because the direct/custom
// dialect path clones downstream headers verbatim before signing. A wasmtime
// shape plugin can still replace headers wholesale via headers_mut().clear(),
// so that plugin boundary remains outside this preservation contract.
const HOP_BY_HOP_HEADERS: [HeaderName; 9] = [
    HeaderName::from_static("connection"),
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-authenticate"),
    HeaderName::from_static("proxy-authorization"),
    HeaderName::from_static("te"),
    HeaderName::from_static("trailer"),
    HeaderName::from_static("transfer-encoding"),
    HeaderName::from_static("upgrade"),
    HeaderName::from_static("proxy-connection"),
];

pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let connection_names = connection_listed_headers(headers);
    let mut removal_targets = Vec::new();

    for (name, _) in headers.iter() {
        if is_hop_by_hop(name, &connection_names) {
            removal_targets.push(name.clone());
        }
    }

    for name in removal_targets {
        headers.remove(name);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HopByHopStripLayer;

impl HopByHopStripLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for HopByHopStripLayer {
    type Service = HopByHopStripService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HopByHopStripService::new(inner)
    }
}

#[derive(Clone, Debug)]
pub struct HopByHopStripService<S> {
    inner: S,
}

impl<S> HopByHopStripService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for HopByHopStripService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = StripFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        strip_hop_by_hop(request.headers_mut());
        let future = self.inner.call(request);

        Box::pin(async move {
            let mut response = future.await?;
            strip_hop_by_hop(response.headers_mut());
            Ok(response)
        })
    }
}

fn connection_listed_headers(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .filter_map(|token| HeaderName::from_lowercase(token.to_ascii_lowercase().as_bytes()).ok())
        .collect()
}

fn is_hop_by_hop(name: &HeaderName, connection_names: &HashSet<HeaderName>) -> bool {
    HOP_BY_HOP_HEADERS.contains(name) || connection_names.contains(name)
}

fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value
        .as_bytes()
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
}

/// Returns `true` when `name` is an ingress reverse-proxy, CDN, or tunnel
/// header that must not be forwarded upstream to LLM providers.
pub fn is_ingress_proxy_header_name(name: &str) -> bool {
    starts_with_ignore_ascii_case(name, "cf-")
        || starts_with_ignore_ascii_case(name, "x-forwarded-")
        || starts_with_ignore_ascii_case(name, "x-envoy-")
        || name.eq_ignore_ascii_case("cdn-loop")
        || name.eq_ignore_ascii_case("forwarded")
        || name.eq_ignore_ascii_case("x-real-ip")
        || name.eq_ignore_ascii_case("true-client-ip")
}

/// Typed [`HeaderName`] wrapper for [`is_ingress_proxy_header_name`].
pub fn is_ingress_proxy_header(name: &HeaderName) -> bool {
    is_ingress_proxy_header_name(name.as_str())
}

fn trim_ascii_ows(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && matches!(bytes[start], b' ' | b'\t') {
        start += 1;
    }
    while end > start && matches!(bytes[end - 1], b' ' | b'\t') {
        end -= 1;
    }
    &bytes[start..end]
}

fn is_ingress_auth_cookie_bytes(crumb: &[u8]) -> bool {
    let raw_name = match crumb.iter().position(|&b| b == b'=') {
        Some(eq) => &crumb[..eq],
        None => crumb,
    };
    let cookie_name = trim_ascii_ows(raw_name);
    cookie_name.eq_ignore_ascii_case(b"CF_Authorization")
        || cookie_name.eq_ignore_ascii_case(b"CF_AppSession")
}

/// Removes Cloudflare Access session cookies (`CF_Authorization`,
/// `CF_AppSession`, case-insensitively) from a raw `Cookie` header value
/// without failing on non-UTF-8 sibling cookie crumbs.
///
/// Returns `Some(Cow::Borrowed(raw))` when no Access cookie was present,
/// `None` when no cookie crumbs remain, or `Some(Cow::Owned(cleaned))`
/// joining surviving crumbs with `; `.
pub fn scrub_ingress_auth_cookie_bytes(raw: &[u8]) -> Option<Cow<'_, [u8]>> {
    let mut kept_crumbs: Vec<&[u8]> = Vec::new();
    let mut removed_any = false;
    for part in raw.split(|&b| b == b';') {
        let trimmed = trim_ascii_ows(part);
        if trimmed.is_empty() {
            continue;
        }
        if is_ingress_auth_cookie_bytes(trimmed) {
            removed_any = true;
        } else {
            kept_crumbs.push(trimmed);
        }
    }
    if !removed_any {
        Some(Cow::Borrowed(raw))
    } else if kept_crumbs.is_empty() {
        None
    } else {
        let mut cleaned = Vec::with_capacity(raw.len());
        for (idx, crumb) in kept_crumbs.iter().enumerate() {
            if idx > 0 {
                cleaned.extend_from_slice(b"; ");
            }
            cleaned.extend_from_slice(crumb);
        }
        Some(Cow::Owned(cleaned))
    }
}

/// Removes ingress proxy/CDN/tunnel headers (such as Cloudflare and Envoy
/// forwarding headers) and Cloudflare Access session cookies (`CF_Authorization`,
/// `CF_AppSession`) from downstream-derived headers after authentication.
pub fn strip_ingress_proxy_headers(headers: &mut HeaderMap) {
    let mut removal_targets = Vec::new();
    for (name, _) in headers.iter() {
        if is_ingress_proxy_header(name) {
            removal_targets.push(name.clone());
        }
    }
    for name in removal_targets {
        headers.remove(name);
    }

    if !headers.contains_key(COOKIE) {
        return;
    }

    let mut kept_cookies = Vec::new();
    for value in headers.get_all(COOKIE) {
        match scrub_ingress_auth_cookie_bytes(value.as_bytes()) {
            None => {}
            Some(Cow::Borrowed(_)) => kept_cookies.push(value.clone()),
            Some(Cow::Owned(cleaned)) => {
                if let Ok(cleaned_value) = HeaderValue::from_bytes(&cleaned) {
                    kept_cookies.push(cleaned_value);
                }
            }
        }
    }
    headers.remove(COOKIE);
    for cookie in kept_cookies {
        headers.append(COOKIE, cookie);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_ingress_proxy_headers_removes_cdn_and_proxy_headers_only() {
        let mut headers = HeaderMap::new();
        for (name, val) in [
            ("cf-connecting-ip", "203.0.113.10"),
            ("CF-Connecting-IPv6", "2001:db8::1"),
            ("cf-ray", "1234567890abcdef-ICN"),
            ("cf-visitor", r#"{"scheme":"https"}"#),
            ("cf-ipcountry", "KR"),
            ("cf-warp-tag-id", "warp-tag"),
            ("cf-ew-via", "15"),
            ("cf-access-jwt-assertion", "jwt.payload.sig"),
            ("cf-access-authenticated-user-email", "user@example.com"),
            ("cf-access-client-id", "client-id"),
            ("cf-access-client-secret", "client-secret"),
            ("cdn-loop", "cloudflare; loops=1"),
            ("forwarded", "for=203.0.113.10;proto=https"),
            ("x-forwarded-for", "203.0.113.10"),
            ("X-Forwarded-Host", "cc-lb-proxy.example.com"),
            ("x-forwarded-proto", "https"),
            ("x-real-ip", "203.0.113.10"),
            ("true-client-ip", "203.0.113.10"),
            ("x-envoy-external-address", "203.0.113.10"),
            ("X-Envoy-Upstream-Service-Time", "4"),
            ("user-agent", "codex-tui/1.0"),
            ("originator", "codex_cli_rs"),
            ("anthropic-version", "2023-06-01"),
            ("anthropic-beta", "prompt-caching-2024-07-31"),
            ("openai-organization", "org-123"),
            ("openai-project", "proj-456"),
            ("session_id", "sess-789"),
            ("x-client-request-id", "req-abc"),
            ("x-organization-uuid", "org-uuid"),
            ("x-trusted-device-token", "device-tok"),
            ("accept", "application/json"),
            ("content-type", "application/json"),
            ("traceparent", "00-1234-5678-01"),
        ] {
            headers.insert(
                HeaderName::from_lowercase(name.to_ascii_lowercase().as_bytes()).unwrap(),
                HeaderValue::from_static(val),
            );
        }

        strip_ingress_proxy_headers(&mut headers);

        for removed in [
            "cf-connecting-ip",
            "cf-connecting-ipv6",
            "cf-ray",
            "cf-visitor",
            "cf-ipcountry",
            "cf-warp-tag-id",
            "cf-ew-via",
            "cf-access-jwt-assertion",
            "cf-access-authenticated-user-email",
            "cf-access-client-id",
            "cf-access-client-secret",
            "cdn-loop",
            "forwarded",
            "x-forwarded-for",
            "x-forwarded-host",
            "x-forwarded-proto",
            "x-real-ip",
            "true-client-ip",
            "x-envoy-external-address",
            "x-envoy-upstream-service-time",
        ] {
            assert!(
                !headers.contains_key(removed),
                "{removed} should be stripped"
            );
        }
        for preserved in [
            "user-agent",
            "originator",
            "anthropic-version",
            "anthropic-beta",
            "openai-organization",
            "openai-project",
            "session_id",
            "x-client-request-id",
            "x-organization-uuid",
            "x-trusted-device-token",
            "accept",
            "content-type",
            "traceparent",
        ] {
            assert!(
                headers.contains_key(preserved),
                "{preserved} should be preserved"
            );
        }
    }

    #[test]
    fn strip_ingress_proxy_headers_sanitizes_cf_cookies_and_preserves_others() {
        let mut solo = HeaderMap::new();
        solo.insert(
            COOKIE,
            HeaderValue::from_static("CF_Authorization=jwt.tok.sig"),
        );
        strip_ingress_proxy_headers(&mut solo);
        assert!(!solo.contains_key(COOKIE));

        let mut app_session_only = HeaderMap::new();
        app_session_only.insert(COOKIE, HeaderValue::from_static("CF_AppSession=sess123"));
        strip_ingress_proxy_headers(&mut app_session_only);
        assert!(!app_session_only.contains_key(COOKIE));

        let mut mixed = HeaderMap::new();
        mixed.append(
            COOKIE,
            HeaderValue::from_static("theme=dark; CF_Authorization=secret; foo=bar"),
        );
        mixed.append(COOKIE, HeaderValue::from_static("cf_appsession=abc"));
        mixed.append(COOKIE, HeaderValue::from_static("stay=1"));
        strip_ingress_proxy_headers(&mut mixed);
        let remaining: Vec<&str> = mixed
            .get_all(COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(remaining, vec!["theme=dark; foo=bar", "stay=1"]);
    }

    #[test]
    fn strip_ingress_proxy_headers_sanitizes_cf_cookies_with_non_utf8_sibling() {
        let mut headers = HeaderMap::new();
        headers.append(
            COOKIE,
            HeaderValue::from_bytes(
                b"theme=dark; CF_Authorization=secret; raw=\xff; CF_AppSession=sess; keep=1",
            )
            .unwrap(),
        );
        headers.append(
            COOKIE,
            HeaderValue::from_bytes(b"CF_Authorization=only").unwrap(),
        );
        strip_ingress_proxy_headers(&mut headers);
        let remaining: Vec<&[u8]> = headers
            .get_all(COOKIE)
            .iter()
            .map(HeaderValue::as_bytes)
            .collect();
        assert_eq!(remaining, vec![b"theme=dark; raw=\xff; keep=1".as_slice()]);
    }
}
