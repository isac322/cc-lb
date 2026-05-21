use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_eventstream_codec::{encode_message, encode_message_with_headers};
use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use ring::digest::{digest, SHA256};
use serde_json::{json, Value};

use crate::modes::FakeMode;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Default)]
pub struct AppConfig {}

pub fn app(_config: AppConfig) -> Router {
    Router::new()
        .route("/model/{model_id}/invoke", post(invoke))
        .route(
            "/model/{model_id}/invoke-with-response-stream",
            post(invoke_with_response_stream),
        )
        .fallback(not_found)
}

async fn invoke(Path(model_id): Path<String>, headers: HeaderMap, body: Bytes) -> Response {
    let mode = FakeMode::from_headers(&headers);
    if let Some(response) = mode_response(mode) {
        return response;
    }
    if let Some(response) = auth_failure(&headers, &body, false) {
        return response;
    }

    let _body_json = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    json_response(
        StatusCode::OK,
        json!({
            "id": "msg_bedrock_fake_000000000000000000000000",
            "type": "message",
            "role": "assistant",
            "model": model_id,
            "content": [{
                "type": "text",
                "text": "fake bedrock runtime fixture response HELLO"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50
            }
        }),
    )
}

async fn invoke_with_response_stream(
    Path(_model_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mode = FakeMode::from_headers(&headers);
    if let Some(response) = mode_response(mode) {
        return response;
    }
    let allow_unsigned_exception = mode == FakeMode::ModelStreamErrorException;
    if let Some(response) = auth_failure(&headers, &body, allow_unsigned_exception) {
        return response;
    }

    let bytes = match mode {
        FakeMode::ModelStreamErrorException => exception_stream_bytes(),
        FakeMode::Ok | FakeMode::ValidationException | FakeMode::ClockSkew => ok_stream_bytes(),
    };
    eventstream_response(bytes)
}

async fn not_found() -> Response {
    json_response(
        StatusCode::NOT_FOUND,
        json!({
            "__type": "ResourceNotFoundException",
            "message": "route not found"
        }),
    )
}

fn mode_response(mode: FakeMode) -> Option<Response> {
    match mode {
        FakeMode::ValidationException => Some(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "__type": "ValidationException",
                "message": "forced fake validation error"
            }),
        )),
        FakeMode::ClockSkew => Some(json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "__type": "RequestTimeTooSkewed",
                "message": "forced fake clock skew"
            }),
        )),
        FakeMode::Ok | FakeMode::ModelStreamErrorException => None,
    }
}

fn auth_failure(headers: &HeaderMap, body: &[u8], allow_unsigned: bool) -> Option<Response> {
    let Some(value) = headers.get(AUTHORIZATION) else {
        return if allow_unsigned {
            None
        } else {
            Some(unrecognized_client())
        };
    };
    let Ok(value) = value.to_str() else {
        return Some(unrecognized_client());
    };

    if valid_fixture_authorization(value) && valid_payload_hash(headers, body) {
        eprintln!("fake-bedrock-runtime auth key_label=AKIATEST...");
        None
    } else {
        Some(unrecognized_client())
    }
}

fn valid_fixture_authorization(value: &str) -> bool {
    value.starts_with("AWS4-HMAC-SHA256 ")
        && value.contains("Credential=AKIATEST")
        && value.contains("SignedHeaders=")
        && value.contains("Signature=")
        && !value.contains("Signature=bad")
        && !value.contains("Signature=mismatch")
        && !value
            .contains("Signature=0000000000000000000000000000000000000000000000000000000000000000")
}

fn valid_payload_hash(headers: &HeaderMap, body: &[u8]) -> bool {
    let verify = headers
        .get("x-fake-verify-body-hash")
        .and_then(|value| value.to_str().ok())
        .map(|value| value == "true")
        .unwrap_or(false);
    if !verify {
        return true;
    }

    let Some(expected) = headers
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };

    expected == hex_sha256(body)
}

fn hex_sha256(body: &[u8]) -> String {
    let digest = digest(&SHA256, body);
    let mut output = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        use std::fmt::Write;
        if write!(&mut output, "{byte:02x}").is_err() {
            return String::new();
        }
    }
    output
}

fn unrecognized_client() -> Response {
    json_response(
        StatusCode::UNAUTHORIZED,
        json!({
            "__type": "UnrecognizedClientException",
            "message": "The security token included in the request is invalid."
        }),
    )
}

fn ok_stream_bytes() -> Vec<u8> {
    let mut output = Vec::new();
    for index in 0..50_u64 {
        let payload = json!({
            "type": "content_block_delta",
            "index": index,
            "delta": {
                "type": "text_delta",
                "text": format!("chunk-{index:02} ")
            }
        });
        output.extend_from_slice(&encode_message("chunk", payload.to_string().as_bytes()));
    }
    let stop = json!({
        "type": "message_stop",
        "amazon-bedrock-invocationMetrics": {
            "inputTokenCount": 100,
            "outputTokenCount": 50
        }
    });
    output.extend_from_slice(&encode_message("chunk", stop.to_string().as_bytes()));
    output
}

fn exception_stream_bytes() -> Vec<u8> {
    let payload = json!({
        "message": "forced fake model stream error",
        "originalStatusCode": 500,
        "originalMessage": "forced fake model stream error"
    });
    encode_message_with_headers(
        &[
            (":event-type", "ModelStreamErrorException"),
            (":content-type", "application/json"),
            (":message-type", "exception"),
            (":exception-type", "ModelStreamErrorException"),
        ],
        payload.to_string().as_bytes(),
    )
}

fn eventstream_response(bytes: Vec<u8>) -> Response {
    let mut response = Body::from(bytes).into_response();
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.amazon.eventstream"),
    );
    with_fixture_headers(response)
}

fn json_response(status: StatusCode, body: Value) -> Response {
    let mut response = Json(body).into_response();
    *response.status_mut() = status;
    with_fixture_headers(response)
}

fn with_fixture_headers(mut response: Response) -> Response {
    let request_id = next_request_id();
    let request_id = match HeaderValue::from_str(&request_id) {
        Ok(value) => value,
        Err(_) => HeaderValue::from_static("req_fallback"),
    };
    response
        .headers_mut()
        .insert("x-amzn-requestid", request_id);
    response
}

fn next_request_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };

    format!("req-{nanos:x}-{counter:x}")
}
