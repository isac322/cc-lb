mod common;

use cc_lb_plugin_api::sign_request;

#[tokio::test]
async fn proactive_refreshes_before_signing() {
    let test_storage = common::storage();
    test_storage
        .put_oauth(
            "alice",
            "anthropic_oauth",
            &common::creds(
                "sk-ant-oat01-old",
                "refresh-old",
                common::now_epoch_secs() + 30,
            ),
        )
        .await
        .expect("seed oauth credentials");
    let http = common::FakeOAuthClient::new(vec![common::success_response(
        "sk-ant-oat01-refreshed",
        Some("refresh-new"),
        600,
    )]);
    let signer = common::signer(
        test_storage.storage.clone(),
        test_storage.aead.clone(),
        http.clone(),
    );

    let signed = sign_request(&signer, common::shaped_request())
        .await
        .expect("sign request");

    assert_eq!(
        signed
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer sk-ant-oat01-refreshed")
    );
    assert_eq!(http.call_count(), 1);
    let stored = test_storage
        .get_oauth("alice", "anthropic_oauth")
        .await
        .expect("load oauth credentials")
        .expect("oauth credentials exist");
    assert_eq!(stored.refresh_token, "refresh-new");
}
