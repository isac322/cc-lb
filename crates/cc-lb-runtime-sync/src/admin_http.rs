use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{
    Extensions, HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode, Version, header,
};
use axum::middleware::Next;
use tower::ServiceBuilder;
use tower_http::compression::{
    CompressionLayer,
    predicate::{DefaultPredicate, Predicate},
};
use tower_http::set_header::SetResponseHeaderLayer;

const PERMISSIONS_POLICY: &str = "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()";
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'; img-src 'self' data:; font-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'";

/// Builds the Admin HTTP compression layer while skipping strong-ETag static assets.
pub fn compression_layer() -> CompressionLayer<impl Predicate> {
    CompressionLayer::new()
        .gzip(true)
        .br(true)
        .compress_when(DefaultPredicate::new().and(allows_representation_transform))
}

fn allows_representation_transform(
    _status: StatusCode,
    _version: Version,
    headers: &HeaderMap,
    _extensions: &Extensions,
) -> bool {
    headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|etag| etag.trim_start().starts_with("W/"))
}

/// Applies strict browser security headers to an Admin router.
pub fn with_browser_security_headers(router: Router) -> Router {
    let security_headers = ServiceBuilder::new()
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("x-content-type-options"),
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("strict-origin-when-cross-origin"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("cross-origin-opener-policy"),
            HeaderValue::from_static("same-origin"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("cross-origin-resource-policy"),
            HeaderValue::from_static("same-origin"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("origin-agent-cluster"),
            HeaderValue::from_static("?1"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("permissions-policy"),
            HeaderValue::from_static(PERMISSIONS_POLICY),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static(CONTENT_SECURITY_POLICY),
        ));

    router.layer(security_headers)
}

#[derive(Clone)]
pub struct RequestIdState {
    generator: RequestIdGenerator,
    prefix: &'static str,
}

#[derive(Clone)]
enum RequestIdGenerator {
    UuidV7,
    Counter(Arc<AtomicU64>),
}

impl RequestIdState {
    pub fn uuid_v7(prefix: &'static str) -> Self {
        Self {
            generator: RequestIdGenerator::UuidV7,
            prefix,
        }
    }

    pub fn counter(prefix: &'static str) -> Self {
        Self {
            generator: RequestIdGenerator::Counter(Arc::new(AtomicU64::new(0))),
            prefix,
        }
    }

    pub fn generate(&self) -> String {
        let mut request_id = String::with_capacity(self.prefix.len() + 36);
        request_id.push_str(self.prefix);
        match &self.generator {
            RequestIdGenerator::UuidV7 => {
                let _ = write!(&mut request_id, "{}", uuid::Uuid::now_v7());
            }
            RequestIdGenerator::Counter(counter) => {
                let _ = write!(
                    &mut request_id,
                    "{}",
                    counter.fetch_add(1, Ordering::Relaxed)
                );
            }
        }
        request_id
    }
}

pub async fn request_id_middleware(
    State(state): State<RequestIdState>,
    mut request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let valid_request_id = |value: &&HeaderValue| {
        value
            .to_str()
            .ok()
            .is_some_and(|value| !value.trim().is_empty())
    };
    let request_id = request
        .headers()
        .get("request-id")
        .filter(valid_request_id)
        .or_else(|| {
            request
                .headers()
                .get("x-request-id")
                .filter(valid_request_id)
        })
        .cloned()
        .unwrap_or_else(|| {
            HeaderValue::try_from(state.generate())
                .expect("generated ASCII request id is valid header value")
        });
    request
        .headers_mut()
        .insert(HeaderName::from_static("request-id"), request_id.clone());
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(HeaderName::from_static("request-id"), request_id);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_accepts_weak_etags_but_not_strong_etags() {
        let mut headers = HeaderMap::new();
        assert!(allows_representation_transform(
            StatusCode::OK,
            Version::HTTP_11,
            &headers,
            &Extensions::new(),
        ));

        headers.insert(header::ETAG, HeaderValue::from_static("W/\"rollup:1\""));
        assert!(allows_representation_transform(
            StatusCode::OK,
            Version::HTTP_11,
            &headers,
            &Extensions::new(),
        ));

        headers.insert(header::ETAG, HeaderValue::from_static("\"revision-1\""));
        assert!(!allows_representation_transform(
            StatusCode::OK,
            Version::HTTP_11,
            &headers,
            &Extensions::new(),
        ));
    }
}
