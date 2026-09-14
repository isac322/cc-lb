use crate::common;

use serde_json::Value;

#[tokio::test]
async fn api_oauth_usage_returns_anthropic_usage_shape() {
    let server = common::spawn_test_server().await;

    let response = common::proxy_get(
        server.proxy_addr,
        "/api/oauth/usage",
        &server.managed_key.plaintext,
    )
    .await
    .expect("usage route");

    assert_eq!(response.status, 200);
    let json: Value = serde_json::from_str(&response.body).expect("usage json");
    assert!(json.get("5h").is_some());
    assert!(json.get("7d").is_some());
    assert!(json.get("five_hour").is_none());
    assert!(json.get("windows").is_none());
    assert!(json.get("caveats").is_none());
}
