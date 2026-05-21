use bytes::Bytes;
use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::UpstreamDialect;
use http::StatusCode;

#[test]
fn invalid_argument_normalizes_to_anthropic_invalid_request() {
    let normalized = VertexDialect
        .normalize_error(
            StatusCode::BAD_REQUEST,
            &Bytes::from_static(
                br#"{"error":{"code":400,"message":"bad vertex request","status":"INVALID_ARGUMENT"}}"#,
            ),
        )
        .expect("vertex error normalizes");
    let body: serde_json::Value = serde_json::from_slice(&normalized).expect("json body");

    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["message"], "bad vertex request");
    println!(
        "vertex_error_normalize status=400 gcp_status=INVALID_ARGUMENT anthropic_error_type={}",
        body["error"]["type"].as_str().unwrap_or("missing")
    );
}

#[test]
fn known_vertex_statuses_map_to_anthropic_error_types() {
    let cases = [
        (
            StatusCode::TOO_MANY_REQUESTS,
            "RESOURCE_EXHAUSTED",
            "rate_limit_error",
        ),
        (
            StatusCode::UNAUTHORIZED,
            "UNAUTHENTICATED",
            "authentication_error",
        ),
        (
            StatusCode::FORBIDDEN,
            "PERMISSION_DENIED",
            "permission_error",
        ),
        (StatusCode::NOT_FOUND, "NOT_FOUND", "not_found_error"),
        (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", "api_error"),
        (StatusCode::BAD_GATEWAY, "UNKNOWN", "api_error"),
        (StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE", "api_error"),
    ];

    for (http_status, vertex_status, expected) in cases {
        let body = format!(
            r#"{{"error":{{"code":{},"message":"mapped","status":"{vertex_status}"}}}}"#,
            http_status.as_u16()
        );
        let normalized = VertexDialect
            .normalize_error(http_status, &Bytes::from(body))
            .expect("vertex error normalizes");
        let body: serde_json::Value = serde_json::from_slice(&normalized).expect("json body");

        assert_eq!(body["error"]["type"], expected);
    }
}

#[test]
fn non_vertex_error_body_is_not_normalized() {
    assert!(VertexDialect
        .normalize_error(
            StatusCode::BAD_REQUEST,
            &Bytes::from_static(br#"{"type":"error"}"#)
        )
        .is_none());
}
