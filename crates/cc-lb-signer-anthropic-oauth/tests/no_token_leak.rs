mod common;

#[tokio::test]
async fn debug_does_not_include_raw_token() {
    let test_storage = common::storage();
    test_storage
        .storage
        .put_oauth(
            "alice",
            "anthropic_oauth",
            &common::creds(
                "sk-ant-oat01-secret-debug",
                "refresh-secret-debug",
                common::now_epoch_secs() + 600,
            ),
        )
        .expect("seed oauth credentials");
    let http = common::FakeOAuthClient::new(Vec::new());
    let signer = common::signer(test_storage.storage.clone(), http);

    let debug = format!("{signer:?}");

    assert!(!debug.contains("sk-ant-oat01-secret-debug"));
    assert!(!debug.contains("refresh-secret-debug"));
}
