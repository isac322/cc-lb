use axum::http::{Extensions, HeaderMap, StatusCode, Version, header};
use tower_http::compression::{
    CompressionLayer,
    predicate::{DefaultPredicate, Predicate},
};

pub(crate) fn layer() -> CompressionLayer<impl Predicate> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

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
