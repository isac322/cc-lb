use axum::body::Body;
use bytes::Bytes;
use http::header::{CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderValue, Response, StatusCode};
use serde_json::json;

pub fn anthropic_error_body(error_type: &str, message: &str) -> Bytes {
    Bytes::from(
        json!({
            "type": "error",
            "error": {
                "type": error_type,
                "message": message,
            }
        })
        .to_string(),
    )
}

pub fn anthropic_error_response(
    status: StatusCode,
    error_type: &str,
    message: &str,
) -> Response<Body> {
    let mut response = Response::new(Body::from(anthropic_error_body(error_type, message)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

pub fn anthropic_error_response_with_retry_after(
    status: StatusCode,
    error_type: &str,
    message: &str,
    retry_after_secs: u64,
) -> Response<Body> {
    let mut response = anthropic_error_response(status, error_type, message);
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}
