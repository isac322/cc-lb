use cc_lb_core::anthropic_error_response;
use http::header::{CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};

#[tokio::test]
async fn snapshot_core_error_responses() {
    let cases = [
        ErrorCase {
            status: StatusCode::UNAUTHORIZED,
            error_type: "authentication_error",
            message: "authentication failed",
            retry_after: None,
        },
        ErrorCase {
            status: StatusCode::FORBIDDEN,
            error_type: "model_not_allowed",
            message: "the requested model is not allowed for this principal",
            retry_after: None,
        },
        ErrorCase {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            error_type: "body_too_large",
            message: "request body exceeds configured cap",
            retry_after: None,
        },
        ErrorCase {
            status: StatusCode::TOO_MANY_REQUESTS,
            error_type: "rate_limit_error",
            message: "request quota exhausted",
            retry_after: Some("42"),
        },
        ErrorCase {
            status: StatusCode::BAD_GATEWAY,
            error_type: "api_error",
            message: "upstream request failed",
            retry_after: None,
        },
        ErrorCase {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error_type: "overloaded_error",
            message: "upstream bulkhead queue is full",
            retry_after: Some("1"),
        },
        ErrorCase {
            status: StatusCode::GATEWAY_TIMEOUT,
            error_type: "api_error",
            message: "request timed out",
            retry_after: None,
        },
    ];

    let mut snapshots = Vec::with_capacity(cases.len());
    for case in cases {
        let mut response = anthropic_error_response(case.status, case.error_type, case.message);
        if let Some(retry_after) = case.retry_after {
            response.headers_mut().insert(
                RETRY_AFTER,
                HeaderValue::from_str(retry_after).expect("retry-after header is valid"),
            );
        }

        let status = response.status();
        let headers = snapshot_headers(response.headers());
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        let body: Value = serde_json::from_slice(&body).expect("error body parses");

        snapshots.push(json!({
            "status": status.as_u16(),
            "reason": status.canonical_reason().unwrap_or(""),
            "headers": headers,
            "body": body,
        }));
    }

    insta::assert_snapshot!(serde_json::to_string_pretty(&snapshots).unwrap());
}

struct ErrorCase {
    status: StatusCode,
    error_type: &'static str,
    message: &'static str,
    retry_after: Option<&'static str>,
}

fn snapshot_headers(headers: &HeaderMap) -> Value {
    json!({
        "content-type": headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or(""),
        "retry-after": headers
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
    })
}
