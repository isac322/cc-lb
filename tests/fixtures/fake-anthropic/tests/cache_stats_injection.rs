use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

#[tokio::test]
async fn t2__inject_cache_stats_overrides_matching_message_usage_only() {
    let app = app(AppConfig::default());
    let matching_body = json!({
        "model": "claude-3-5-sonnet-20241022",
        "messages": [{"role": "user", "content": "cache me"}],
        "max_tokens": 10
    });
    let signature = request_signature(&matching_body);

    let inject_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/__inject_cache_stats")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "match_request_signature": signature,
                        "usage": {
                            "cache_creation_input_tokens": 1234,
                            "cache_read_input_tokens": 5678
                        }
                    })
                    .to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(inject_response.status(), StatusCode::OK);

    let matching = post_messages(app.clone(), &matching_body).await;
    assert_eq!(matching["usage"]["input_tokens"], 100);
    assert_eq!(matching["usage"]["output_tokens"], 50);
    assert_eq!(matching["usage"]["cache_creation_input_tokens"], 1234);
    assert_eq!(matching["usage"]["cache_read_input_tokens"], 5678);

    let non_matching = post_messages(
        app,
        &json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [{"role": "user", "content": "different body"}],
            "max_tokens": 10
        }),
    )
    .await;
    assert_eq!(non_matching["usage"]["input_tokens"], 100);
    assert_eq!(non_matching["usage"]["output_tokens"], 50);
    assert!(
        non_matching["usage"]
            .get("cache_creation_input_tokens")
            .is_none()
    );
    assert!(
        non_matching["usage"]
            .get("cache_read_input_tokens")
            .is_none()
    );
}

async fn post_messages(app: axum::Router, body: &Value) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&body).expect("json body")
}

fn request_signature(body_json: &Value) -> String {
    let canonical = serde_json::to_string(body_json).expect("canonical json");
    let digest = ring::digest::digest(&ring::digest::SHA256, canonical.as_bytes());
    lowercase_hex(digest.as_ref())
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
