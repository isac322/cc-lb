mod common;

use cc_lb_upstream::sign_request;

#[tokio::test]
async fn proactive_refreshes_before_signing() {
    let clock = common::test_clock();
    let test_storage = common::storage();
    test_storage
        .put_oauth(
            "alice",
            "anthropic_oauth",
            &common::creds(
                "sk-ant-oat01-old",
                "refresh-old",
                common::now_epoch_secs(clock.as_ref()) + 30,
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
        clock,
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
    let body = http.bodies().pop().expect("refresh request body recorded");
    let fields = url::form_urlencoded::parse(body.as_bytes()).collect::<Vec<_>>();
    assert_eq!(
        fields,
        vec![
            ("grant_type".into(), "refresh_token".into()),
            ("client_id".into(), "client-test".into()),
            ("refresh_token".into(), "refresh-old".into()),
        ]
    );
    let stored = test_storage
        .get_oauth("alice", "anthropic_oauth")
        .await
        .expect("load oauth credentials")
        .expect("oauth credentials exist");
    assert_eq!(stored.refresh_token, "refresh-new");
}
