use axum::{
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};

pub(crate) fn matches_if_none_match(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(|raw| raw.split(',').any(|tag| tag.trim() == expected))
        .unwrap_or(false)
}

pub(crate) fn not_modified_response(etag: &str) -> Response {
    let mut response = StatusCode::NOT_MODIFIED.into_response();
    apply_private_revalidation_headers(&mut response, Some(etag));
    response
}

pub(crate) fn apply_private_revalidation(mut response: Response, etag: Option<&str>) -> Response {
    apply_private_revalidation_headers(&mut response, etag);
    response
}

fn apply_private_revalidation_headers(response: &mut Response, etag: Option<&str>) {
    let headers = response.headers_mut();
    if let Some(etag) = etag
        && let Ok(value) = HeaderValue::from_str(etag)
    {
        headers.insert(header::ETAG, value);
    }
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
}
