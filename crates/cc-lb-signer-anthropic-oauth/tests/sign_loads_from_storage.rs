mod common;

use cc_lb_plugin_api::sign_request;

#[tokio::test]
async fn sign_loads_from_storage() {
    let test_storage = common::storage();
    test_storage
        .put_oauth(
            "alice",
            "anthropic_oauth",
            &common::creds(
                "sk-ant-oat01-storage-token",
                "refresh-token",
                common::now_epoch_secs() + 600,
            ),
        )
        .await
        .expect("seed oauth credentials");
    let http = common::FakeOAuthClient::new(Vec::new());
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
        Some("Bearer sk-ant-oat01-storage-token")
    );
    assert_eq!(http.call_count(), 0);
}
