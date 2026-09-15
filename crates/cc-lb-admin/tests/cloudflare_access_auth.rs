use crate::admin_test_common;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use cc_lb_admin::auth::{AdminAuthenticator, build_providers};
use cc_lb_config::{AdminAuthConfig, AdminAuthProviderConfig};
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const ACCESS_HEADER: &str = "cf-access-jwt-assertion";
const ACCESS_AUDIENCE: &str = "admin-audience";
const TEST_RSA_N: &str = "w63MnzLXxLyjN_x5_44pkBeNkbY5kg88mwirAa7lzgrYjSRy36qTl8c7rXSMxhh9Qns9RDBNMQRRxcmAdv-zsBtSrwehh30yDBog01zPJ_hB3vKYDL8OW6t830A0FNzyEHWLxwNXKMpO5vaVvyWLWGZcIXLnnuY_0g9fAvvjk1LoAF8W15YuoT7LagXSxwUwBErlScqEjeHETG0cTc7r5764EiFDcVn7QpIU2OKN-B_ackbzoFSQzRJ4DgOaMJCRHoDrsq3F3q-LmaFE2oJ2NuLqwM-kYt7k-P2kXiD2J39fH6IHmnY1cRrHIQp6jtFaGImArK_mtGyAbPF2lCS9JQ";
const TEST_RSA_E: &str = "AQAB";
const TEST_PRIVATE_KEY_PKCS8_BASE64: &str = concat!(
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDDrcyfMtfEvKM3",
    "/Hn/jimQF42RtjmSDzybCKsBruXOCtiNJHLfqpOXxzutdIzGGH1Cez1EME0xBFHF",
    "yYB2/7OwG1KvB6GHfTIMGiDTXM8n+EHe8pgMvw5bq3zfQDQU3PIQdYvHA1coyk7m",
    "9pW/JYtYZlwhcuee5j/SD18C++OTUugAXxbXli6hPstqBdLHBTAESuVJyoSN4cRM",
    "bRxNzuvnvrgSIUNxWftCkhTY4o34H9pyRvOgVJDNEngOA5owkJEegOuyrcXer4uZ",
    "oUTagnY24urAz6Ri3uT4/aReIPYnf18fogeadjVxGschCnqO0VoYiYCsr+a0bIBs",
    "8XaUJL0lAgMBAAECggEAK9cVgBlpBBfrTZDQWHQmVbUhL6Mu9M1bG7TuczsXG3mM",
    "mNqwrfHOjXPCqBLzszIrZiisSkm0EaZRF8oUcRTK0krc1IAWLX/cJZ/4+MHTW7Yg",
    "M+4QKNLpSZp7KJ1+zanOxedAAL972JSy5sYaOLAVryGHxKq0wAIADHULKWT70Lae",
    "9hKj2p3tadKfFkyvZOa4CdgF9dmB7ZQEjXbT2qGJ6575XapFaih6CZtLkRhVM8cD",
    "uwCtqzFr+dpnMUcF5v0Vy6Ql1DGGb8TDycnNSrbl2zLFpXQxrqC4UQ8nK8po1JpR",
    "6hC04mmsTya2Zuz9UzeCysOs+MDs/5HuO/r0owTloQKBgQDs74yTlWw2F0t5JALv",
    "JfJ8pHrijbgsBaEI61/Q/P/5/dyV3nNsKFxQWBolWYq28+72m2l4SY5CvNAvkklP",
    "VYyvOBD0MQmdwu7W+gPpvEUqfQ8FR2UNxRSfkEOPuBbUZLDcEA0dTs8RUsoaByGC",
    "A22S1yy2NDsv12qxUvFO3pw4/QKBgQDTbG71nGl4FMkqMQX7BFRK7ZOVfQSg1Y+o",
    "k1zzI2INkByYBG+qeCehbgLAQkUFsDPCCETppzx1uUBChqxXVsCjjpC4H688A6Lv",
    "IEsGnlT/ZKnhY/lTztztfNJGz3kGobZSBs0OpUtrctcb2v9NCBI77Sj6ccwn4vib",
    "TahXTrWBSQKBgQDqYTWQsNlw0K5qUYNNix5KynJ9NnAfrBnWtu/7zqpxY/0XjAxl",
    "y682E1EZ7W/Y94lGDgrRYQIHZrwSswUuI5SdqDqtNO0sUK7vnjbMut843qlDMZL3",
    "giOajJ0oyJRc2pZRutceTN1tZ5ZhFPjCoh18irrCKvz5oID8lO38dR3ZCQKBgGCo",
    "cEolyio2Boodg4hxQEBJQXHUiCsnt9fwF0ypXoio3Am77XlYGXY6H1PaeEfTeLY6",
    "pZbU+FUx7mj7vQrpBIVCBnPHOIwNdY4xi1tpQ57HXMtIs5JXPrXsnQ32iHQ5tmrl",
    "5RXPCB4FkMaRZqrHB98R2+wz3oxVvibyaAYSW/TRAoGAQNiF+bFcV9iiWD8GnnXY",
    "Qy8aa+Ql6+Ka0Uu0WbddsRLtRljPehRfY1Pbg0JPX/NFlqo8JRTmHpynkPLUdVyW",
    "u8hBuGVrfkbc6LXy7eZZzYW+4QRn2hfjSBBwXdOZhzMu+MZg+SYnb+jLuHKqhzRi",
    "Ru1/xX6fklokVrNAjgSdXyo="
);

struct JwtFixture {
    _mock_server: MockServer,
    issuer: String,
    signing_key: RsaKeyPair,
}

impl JwtFixture {
    async fn new() -> Self {
        let mock_server = MockServer::start().await;
        let issuer = mock_server.uri();
        let private_key = STANDARD
            .decode(TEST_PRIVATE_KEY_PKCS8_BASE64)
            .expect("embedded PKCS#8 key is valid base64");
        let signing_key =
            RsaKeyPair::from_pkcs8(&private_key).expect("ring accepts embedded PKCS#8 test key");

        Mock::given(method("GET"))
            .and(path("/cdn-cgi/access/certs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "keys": [{
                    "kid": "test",
                    "kty": "RSA",
                    "alg": "RS256",
                    "use": "sig",
                    "n": TEST_RSA_N,
                    "e": TEST_RSA_E,
                }]
            })))
            .mount(&mock_server)
            .await;

        Self {
            _mock_server: mock_server,
            issuer,
            signing_key,
        }
    }

    fn token(
        &self,
        subject: &str,
        email: Option<&str>,
        common_name: Option<&str>,
        audience: Value,
        expires_at: u64,
    ) -> String {
        self.token_with_nbf(subject, email, common_name, audience, expires_at, None)
    }

    fn token_with_nbf(
        &self,
        subject: &str,
        email: Option<&str>,
        common_name: Option<&str>,
        audience: Value,
        expires_at: u64,
        not_before: Option<u64>,
    ) -> String {
        let mut claims = json!({
            "sub": subject,
            "email": email,
            "common_name": common_name,
            "type": if subject.is_empty() { "service_token" } else { "app" },
            "aud": audience,
            "iss": self.issuer.as_str(),
            "iat": unix_secs(),
            "exp": expires_at,
        });
        if let Some(not_before) = not_before {
            claims["nbf"] = json!(not_before);
        }

        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","kid":"test","typ":"JWT"}"#);
        let claims =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("test JWT claims serialize"));
        let signing_input = format!("{header}.{claims}");
        let mut signature = vec![0; self.signing_key.public().modulus_len()];
        self.signing_key
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                signing_input.as_bytes(),
                &mut signature,
            )
            .expect("test JWT signs");
        format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature))
    }

    fn authenticator(&self, include_static_token: bool) -> Arc<AdminAuthenticator> {
        self.authenticator_with_team_domain(self.issuer.clone(), include_static_token)
    }

    fn authenticator_with_team_domain(
        &self,
        team_domain: String,
        include_static_token: bool,
    ) -> Arc<AdminAuthenticator> {
        let mut providers = if include_static_token {
            vec![admin_test_common::static_token_provider("test-token")]
        } else {
            Vec::new()
        };
        providers.extend(
            build_providers(&AdminAuthConfig {
                providers: vec![AdminAuthProviderConfig::CloudflareAccess {
                    id: "cloudflare".to_owned(),
                    team_domain,
                    audiences: vec![ACCESS_AUDIENCE.to_owned()],
                    header: ACCESS_HEADER.to_owned(),
                }],
            })
            .expect("Cloudflare provider builds"),
        );
        Arc::new(AdminAuthenticator::new(providers))
    }
}

#[tokio::test]
async fn cloudflare_access_maps_identities_and_rejects_invalid_claims() {
    let fixture = JwtFixture::new().await;
    let server =
        admin_test_common::spawn_admin_server_with_auth(fixture.authenticator(false)).await;
    let now = unix_secs();

    let human_token = fixture.token(
        "user-subject",
        Some("alice@example.com"),
        None,
        json!(ACCESS_AUDIENCE),
        now + 300,
    );
    let (status, _, human) = session_with_token(&server.client, &human_token).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(human["authority"], fixture.issuer);
    assert_eq!(human["subject"], "user-subject");
    assert_eq!(human["kind"], "human");
    assert_eq!(human["email"], "alice@example.com");
    assert_eq!(human["provider_id"], "cloudflare");
    assert_eq!(human["auth_mode"], "external");

    let array_audience_token = fixture.token(
        "user-subject",
        Some("alice@example.com"),
        None,
        json!(["another-audience", ACCESS_AUDIENCE]),
        now + 300,
    );
    let (status, _, _) = session_with_token(&server.client, &array_audience_token).await;
    assert_eq!(status, axum::http::StatusCode::OK);

    let trailing_slash_server = admin_test_common::spawn_admin_server_with_auth(
        fixture.authenticator_with_team_domain(format!("{}/", fixture.issuer), false),
    )
    .await;
    let (status, _, normalized) =
        session_with_token(&trailing_slash_server.client, &human_token).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(normalized["authority"], fixture.issuer);

    let service_token = fixture.token(
        "",
        None,
        Some("service-client-id.access"),
        json!(ACCESS_AUDIENCE),
        now + 300,
    );
    let (status, _, service) = session_with_token(&server.client, &service_token).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(service["kind"], "service");
    assert_eq!(service["subject"], "service-client-id.access");
    assert_eq!(service["email"], Value::Null);

    let wrong_audience = fixture.token(
        "user-subject",
        Some("alice@example.com"),
        None,
        json!("different-audience"),
        now + 300,
    );
    let (status, _, _) = session_with_token(&server.client, &wrong_audience).await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);

    let expired = fixture.token(
        "user-subject",
        Some("alice@example.com"),
        None,
        json!(ACCESS_AUDIENCE),
        now.saturating_sub(300),
    );
    let (status, _, _) = session_with_token(&server.client, &expired).await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);

    let not_yet_valid = fixture.token_with_nbf(
        "user-subject",
        Some("alice@example.com"),
        None,
        json!(ACCESS_AUDIENCE),
        now + 300,
        Some(now + 300),
    );
    let (status, _, _) = session_with_token(&server.client, &not_yet_valid).await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn missing_cloudflare_header_is_recorded_as_auth_rejected() {
    let fixture = JwtFixture::new().await;
    let server = admin_test_common::spawn_admin_server_with_auth(fixture.authenticator(true)).await;

    let (status, _, _) = server
        .client
        .get_without_auth("/admin/v1/auth/session")
        .await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);

    let (status, _, audit) = server.client.get("/admin/v1/audit").await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let auth_rejected = audit["entries"]
        .as_array()
        .expect("audit entries array")
        .iter()
        .find(|entry| entry["admin_action"] == "auth_rejected")
        .expect("missing credentials audit entry");
    assert_eq!(auth_rejected["route"], "/admin/v1/auth/session");
    assert_eq!(auth_rejected["payload"]["reason"], "missing_credentials");
}

async fn session_with_token(
    client: &admin_test_common::AdminClient,
    token: &str,
) -> (axum::http::StatusCode, axum::http::HeaderMap, Value) {
    client
        .json(
            "GET",
            "/admin/v1/auth/session",
            None,
            &[(ACCESS_HEADER, token)],
        )
        .await
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_secs()
}
