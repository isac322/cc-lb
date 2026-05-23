use bytes::Bytes;
use cc_lb_dialect_bedrock::BedrockRuntimeDialect;
use cc_lb_plugin_api::UpstreamDialect;
use http::StatusCode;
use serde_json::{Value, json};

#[test]
fn snapshot_normalized_errors() {
    let cases = [
        BedrockErrorCase {
            name: "validation_exception_400",
            status: StatusCode::BAD_REQUEST,
            body: br#"{"__type":"ValidationException","message":"bad request"}"#,
        },
        BedrockErrorCase {
            name: "unrecognized_client_401",
            status: StatusCode::UNAUTHORIZED,
            body: br#"{"__type":"UnrecognizedClientException","message":"bad credentials"}"#,
        },
        BedrockErrorCase {
            name: "request_time_skewed_403",
            status: StatusCode::FORBIDDEN,
            body:
                br#"{"__type":"com.amazonaws.bedrock#RequestTimeTooSkewed","message":"clock skew"}"#,
        },
        BedrockErrorCase {
            name: "throttling_429",
            status: StatusCode::TOO_MANY_REQUESTS,
            body: br#"{"__type":"ThrottlingException","message":"slow down"}"#,
        },
        BedrockErrorCase {
            name: "model_stream_502",
            status: StatusCode::BAD_GATEWAY,
            body: br#"{"__type":"ModelStreamErrorException","message":"stream failed"}"#,
        },
        BedrockErrorCase {
            name: "server_without_type_503",
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: br#"{"message":"service unavailable"}"#,
        },
        BedrockErrorCase {
            name: "timeout_without_type_504",
            status: StatusCode::GATEWAY_TIMEOUT,
            body: br#"{}"#,
        },
    ];

    let snapshots = cases
        .iter()
        .map(|case| {
            let normalized = BedrockRuntimeDialect::default()
                .normalize_error(case.status, &Bytes::from_static(case.body))
                .expect("bedrock error normalizes");
            let body: Value = serde_json::from_slice(&normalized).expect("normalized JSON parses");
            json!({
                "case": case.name,
                "status": case.status.as_u16(),
                "body": body,
            })
        })
        .collect::<Vec<_>>();

    insta::assert_snapshot!(serde_json::to_string_pretty(&snapshots).unwrap());
}

struct BedrockErrorCase {
    name: &'static str,
    status: StatusCode,
    body: &'static [u8],
}
