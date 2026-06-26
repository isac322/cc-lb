use axum::http::{Extensions, HeaderMap, StatusCode, Version, header};
use tower_http::compression::{
    CompressionLayer,
    predicate::{DefaultPredicate, Predicate},
};

pub(crate) fn layer() -> CompressionLayer<impl Predicate> {
    CompressionLayer::new()
        .gzip(true)
        .br(true)
        .compress_when(DefaultPredicate::new().and(skip_etagged_responses))
}

fn skip_etagged_responses(
    _status: StatusCode,
    _version: Version,
    headers: &HeaderMap,
    _extensions: &Extensions,
) -> bool {
    !headers.contains_key(header::ETAG)
}
