mod common;

use cc_lb_signer_anthropic_oauth::{complete_pkce_flow, start_pkce_flow};
use oauth2::{AuthUrl, ClientId, TokenUrl};
use url::Url;

#[tokio::test]
async fn pkce_start_and_complete_flow() {
    let clock = common::test_clock();
    let http = common::FakeOAuthClient::new(vec![common::success_response(
        "sk-ant-oat01-pkce",
        Some("refresh-pkce"),
        600,
    )]);
    let handshake = start_pkce_flow(
        ClientId::new("client-test".to_owned()),
        AuthUrl::new("https://example.test/oauth/authorize".to_owned()).expect("auth url"),
        TokenUrl::new("https://example.test/oauth/token".to_owned()).expect("token url"),
        vec!["messages".to_owned(), "files".to_owned()],
        Url::parse("http://127.0.0.1:1455/callback").expect("redirect url"),
    );

    assert!(
        handshake
            .authorize_url
            .query_pairs()
            .any(|(name, value)| { name == "code_challenge_method" && value == "S256" })
    );

    let creds = complete_pkce_flow(
        handshake,
        "auth-code".to_owned(),
        http.clone(),
        clock.as_ref(),
    )
    .await
    .expect("complete pkce");

    assert_eq!(creds.access_token, "sk-ant-oat01-pkce");
    assert_eq!(creds.refresh_token, "refresh-pkce");
    assert_eq!(http.call_count(), 1);
    let body = http.bodies().pop().expect("request body recorded");
    assert!(body.contains(r#""grant_type":"authorization_code""#));
    assert!(body.contains(r#""code":"auth-code""#));
    assert!(body.contains(r#""code_verifier":"#));
}
