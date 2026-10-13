use axum::Router;
use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

const ORG: &str = "sk-ant-api03-ORGKEY0000000000000000AAAA";

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (StatusCode, http::HeaderMap, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    if !body.is_empty() {
        request = request
            .header("content-type", "application/json")
            .header("content-length", body.len().to_string());
    }
    let response = router
        .clone()
        .oneshot(
            request
                .body(Body::from(body.to_owned()))
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, headers, json)
}

#[tokio::test]
async fn keys_without_a_policy_keep_the_default_answers() {
    let router = app(AppConfig::default());
    let auth = [("x-api-key", "sk-ant-api03-nopolicy")];

    let (status, headers, body) = call(&router, "GET", "/v1/models", &auth, "").await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("anthropic-workspace-id").is_none());
    assert_eq!(body["data"][0]["id"], "claude-fable-5");

    let (status, headers, body) = call(
        &router,
        "POST",
        "/v1/messages/count_tokens",
        &auth,
        r#"{"model":"claude-fable-5","messages":[]}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("anthropic-workspace-id").is_none());
    assert_eq!(body["input_tokens"], 100);

    let (status, _, body) = call(&router, "GET", "/v1/organizations/workspaces", &auth, "").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["type"], "permission_error");
}

#[tokio::test]
async fn debug_routes_script_policies_and_expose_the_recorder() {
    let router = app(AppConfig::default());
    let policy = json!({
        "key": ORG,
        "policy": {
            "requires_workspace": true,
            "workspaces": [{"id": "wrkspc_P", "name": "Production"}],
            "count_tokens": {"kind": "sequence", "steps": [
                {"kind": "status", "status": 429, "body": {"type": "error"}, "headers": {"retry-after": "7"}},
                {"kind": "default"}
            ]}
        }
    });
    let (status, _, _) = call(&router, "POST", "/__key_policies", &[], &policy.to_string()).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let key = [("x-api-key", ORG)];
    let (status, _, body) = call(&router, "GET", "/v1/models", &key, "").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["error"]["message"].as_str(),
        Some(fake_anthropic::IDENTITY_LINKED_MESSAGE)
    );

    let with_workspace = [("x-api-key", ORG), ("anthropic-workspace-id", "wrkspc_P")];
    let count = r#"{"model":"m","messages":[]}"#;
    let (status, headers, _) = call(
        &router,
        "POST",
        "/v1/messages/count_tokens",
        &with_workspace,
        count,
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get("retry-after").map(|v| v.as_bytes()),
        Some(&b"7"[..])
    );
    let (status, headers, _) = call(
        &router,
        "POST",
        "/v1/messages/count_tokens",
        &with_workspace,
        count,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("anthropic-workspace-id").map(|v| v.as_bytes()),
        Some(&b"wrkspc_P"[..])
    );

    // api.anthropic.com answers an unknown well-formed id with a 400.
    let unknown = [("x-api-key", ORG), ("anthropic-workspace-id", "wrkspc_X")];
    let (status, _, _) = call(&router, "GET", "/v1/models", &unknown, "").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _, body) = call(&router, "GET", "/v1/organizations/workspaces", &key, "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["id"], "wrkspc_P");

    let (_, _, requests) = call(&router, "GET", "/__requests", &[], "").await;
    let requests = requests.as_array().cloned().unwrap_or_default();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests[0]["method"], "GET");
    assert_eq!(requests[0]["path"], "/v1/models");
    assert_eq!(requests[0]["headers"]["x-api-key"], ORG);
    assert_eq!(requests[1]["body"], count);

    let (status, _, _) = call(&router, "POST", "/__requests/clear", &[], "").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _, _) = call(&router, "DELETE", "/__key_policies", &[], "").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _, _) = call(&router, "GET", "/v1/models", &key, "").await;
    assert_eq!(status, StatusCode::OK);
    let (_, _, requests) = call(&router, "GET", "/__requests", &[], "").await;
    assert_eq!(requests.as_array().map(Vec::len), Some(1));
}
