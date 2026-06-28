mod common;

#[tokio::test]
async fn debug_does_not_include_raw_token() {
    let clock = common::test_clock();
    let test_storage = common::storage();
    test_storage
        .put_oauth(
            "alice",
            "anthropic_oauth",
            &common::creds(
                "sk-ant-oat01-secret-debug",
                "refresh-secret-debug",
                common::now_epoch_secs(clock.as_ref()) + 600,
            ),
        )
        .await
        .expect("seed oauth credentials");
    let http = common::FakeOAuthClient::new(Vec::new());
    let signer = common::signer(
        test_storage.storage.clone(),
        test_storage.aead.clone(),
        http,
        clock,
    );

    let debug = format!("{signer:?}");

    assert!(!debug.contains("sk-ant-oat01-secret-debug"));
    assert!(!debug.contains("refresh-secret-debug"));
}
