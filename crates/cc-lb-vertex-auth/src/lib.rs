#![forbid(unsafe_code)]

mod credential;
mod token;

pub use credential::{
    AuthorizedUserCredential, CLOUD_PLATFORM_SCOPE, CanonicalCredential, CredentialError,
    GOOGLE_TOKEN_URI, ImpersonatedServiceAccountCredential, MAX_CREDENTIAL_BYTES,
    MAX_RESPONSE_BYTES, ServiceAccountCredential, parse_credential,
};
pub use token::{
    BearerSource, BearerToken, MintError, TokenEndpoint, TokenRegistry, TokenRequest,
    TokenResponse, TokenSource, TokenTransport, TransportError, mint_once,
};
/// Domain-separated AAD for encrypted Vertex credential blobs.
pub fn vertex_credential_aad(upstream_id: &str) -> Vec<u8> {
    let mut aad = b"cc-lb/vertex-credential/v1\0".to_vec();
    aad.extend_from_slice(upstream_id.as_bytes());
    aad
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use cc_lb_clock::{ClockHandle, TestClock};
    use ring::signature::KeyPair;
    use ring::signature::{RSA_PKCS1_2048_8192_SHA256, RsaKeyPair, UnparsedPublicKey};
    use serde_json::Value;
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };
    use tokio::sync::Notify;

    const SA_KEY: &str = include_str!("../../cc-lb-server/tests/fixtures/tls/key-a.pem");
    fn sa_json() -> String {
        format!(
            r#"{{"type":"service_account","client_email":"vertex-test@project.iam.gserviceaccount.com","private_key":{SA_KEY:?},"private_key_id":"kid-1","project_id":"my-project"}}"#
        )
    }
    fn user_json() -> &'static [u8] {
        br#"{"type":"authorized_user","client_id":"client-id","client_secret":"CLIENT-SECRET-VALUE","refresh_token":"REFRESH-TOKEN-VALUE","quota_project_id":"my-project"}"#
    }
    fn impersonated_json() -> String {
        format!(
            r#"{{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target@project.iam.gserviceaccount.com:generateAccessToken","source_credentials":{0},"delegates":["source@project.iam.gserviceaccount.com"],"quota_project_id":"target-project"}}"#,
            String::from_utf8(user_json().to_vec()).expect("utf8")
        )
    }
    fn clock() -> (Arc<TestClock>, ClockHandle) {
        let test = Arc::new(TestClock::new_at_secs(1_700_000_000));
        (test.clone(), test)
    }
    fn response(body: &[u8]) -> TokenResponse {
        TokenResponse {
            status: 200,
            headers: vec![],
            body: body.to_vec(),
        }
    }

    struct FakeTransport {
        calls: Arc<Mutex<Vec<(TokenEndpoint, TokenRequest)>>>,
        responses: Mutex<VecDeque<Result<TokenResponse, TransportError>>>,
        gate: Option<Arc<Notify>>,
        started: Option<Arc<Notify>>,
    }
    type Calls = Arc<Mutex<Vec<(TokenEndpoint, TokenRequest)>>>;
    impl FakeTransport {
        fn one(response: TokenResponse) -> (Arc<Self>, Calls) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            (
                Arc::new(Self {
                    calls: calls.clone(),
                    responses: Mutex::new(VecDeque::from([Ok(response)])),
                    gate: None,
                    started: None,
                }),
                calls,
            )
        }
        fn sequence(responses: Vec<Result<TokenResponse, TransportError>>) -> (Arc<Self>, Calls) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            (
                Arc::new(Self {
                    calls: calls.clone(),
                    responses: Mutex::new(responses.into_iter().collect()),
                    gate: None,
                    started: None,
                }),
                calls,
            )
        }
    }
    #[async_trait]
    impl TokenTransport for FakeTransport {
        async fn post(
            &self,
            endpoint: TokenEndpoint,
            request: TokenRequest,
        ) -> Result<TokenResponse, TransportError> {
            self.calls.lock().expect("calls").push((endpoint, request));
            if let Some(started) = &self.started {
                started.notify_waiters();
            }
            if let Some(gate) = &self.gate {
                gate.notified().await;
            }
            self.responses
                .lock()
                .expect("responses")
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(response(
                        br#"{"access_token":"token","token_type":"Bearer","expires_in":3600}"#,
                    ))
                })
        }
    }

    #[test]
    fn qa_crd_1_valid_credentials() {
        let sa = parse_credential(sa_json().as_bytes()).expect("service account");
        let user = parse_credential(user_json()).expect("authorized user");
        let imp = parse_credential(impersonated_json().as_bytes()).expect("impersonated");
        assert_eq!(sa.credential_type().as_str(), "service_account");
        assert_eq!(user.credential_type().as_str(), "authorized_user");
        assert_eq!(
            imp.credential_type().as_str(),
            "impersonated_service_account"
        );
        assert_eq!(
            sa.principal_hint(),
            "vertex-test@project.iam.gserviceaccount.com"
        );
        assert_eq!(user.quota_project_id(), Some("my-project"));
        assert_eq!(
            imp.principal_hint(),
            "target@project.iam.gserviceaccount.com"
        );
    }

    #[test]
    fn qa_crd_1_canonical_round_trip_all_credential_types() {
        for raw in [
            sa_json(),
            String::from_utf8(user_json().to_vec()).expect("user JSON"),
            impersonated_json(),
        ] {
            let original = parse_credential(raw.as_bytes()).expect("credential");
            let stored = serde_json::to_vec(&original).expect("canonical JSON");
            let restored = parse_credential(&stored).expect("canonical round-trip");
            assert_eq!(restored.credential_type(), original.credential_type());
            assert_eq!(restored.principal_hint(), original.principal_hint());
            assert_eq!(restored.quota_project_id(), original.quota_project_id());
        }
    }

    #[test]
    fn qa_crd_2_static_rejections() {
        let cases = [
            r#"{"type":"external_account","credential_source":{"file":"SECRET-VALUE"}}"#,
            r#"{"type":"authorized_user","client_id":"id","client_secret":"x","refresh_token":"y","credential_source":{"x":"SECRET-VALUE"}}"#,
            r#"{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/a@project.iam.gserviceaccount.com:generateAccessToken","source_credentials":{"type":"impersonated_service_account"}}"#,
            r#"{"type":"authorized_user","client_id":"id","client_secret":"x","refresh_token":"y","universe_domain":"evil.example"}"#,
            r#"{"type":"authorized_user","client_id":"id","client_secret":"x","refresh_token":"y","token_uri":"https://evil.example/token"}"#,
        ];
        for raw in cases {
            let error = parse_credential(raw.as_bytes()).expect_err("must reject");
            assert!(!error.to_string().contains("SECRET-VALUE"));
            assert!(error.code().starts_with("credential_"));
        }
    }

    #[test]
    fn qa_crd_3_input_boundaries_and_formats() {
        let raw = user_json().to_vec();
        let parsed = parse_credential(
            &b"\xef\xbb\xbf\r\n"
                .iter()
                .copied()
                .chain(raw)
                .collect::<Vec<_>>(),
        )
        .expect("BOM CRLF");
        assert_eq!(parsed.principal_hint(), "client-id");
        let duplicate = br#"{"type":"authorized_user","client_id":"first","client_id":"last","client_secret":"x","refresh_token":"y"}"#;
        assert_eq!(
            parse_credential(duplicate)
                .expect("duplicate")
                .principal_hint(),
            "last"
        );
        assert!(matches!(
            parse_credential(b"[]"),
            Err(CredentialError::InvalidJson)
        ));
        assert!(matches!(
            parse_credential(b"not-json"),
            Err(CredentialError::InvalidJson)
        ));
        // Google's client libraries accept PKCS#1 service-account keys, and
        // keys re-exported by other proxies arrive in that form. The same key
        // must yield the same stored PKCS#8 bytes as the Google-issued form.
        let pkcs1_key = include_str!("testdata/key-a-pkcs1.pem");
        assert!(pkcs1_key.starts_with("-----BEGIN RSA PRIVATE KEY-----"));
        let pkcs1 = sa_json().replace(&format!("{SA_KEY:?}"), &format!("{pkcs1_key:?}"));
        let from_pkcs1 = parse_credential(pkcs1.as_bytes()).expect("PKCS#1 key accepted");
        let from_pkcs8 = parse_credential(sa_json().as_bytes()).expect("PKCS#8 key");
        assert_eq!(
            serde_json::to_value(&from_pkcs1).expect("serialize")["private_key"],
            serde_json::to_value(&from_pkcs8).expect("serialize")["private_key"]
        );
        let malformed_pkcs1 = br#"{"type":"service_account","client_email":"x@project.iam.gserviceaccount.com","private_key":"-----BEGIN RSA PRIVATE KEY-----\nAA==\n-----END RSA PRIVATE KEY-----"}"#;
        assert!(matches!(
            parse_credential(malformed_pkcs1),
            Err(CredentialError::InvalidPrivateKey)
        ));
        let mut exact = user_json().to_vec();
        let padding = MAX_CREDENTIAL_BYTES - exact.len();
        exact.extend(std::iter::repeat_n(b' ', padding));
        assert_eq!(exact.len(), MAX_CREDENTIAL_BYTES);
        parse_credential(&exact).expect("exact cap accepted");
        exact.push(b' ');
        assert!(matches!(
            parse_credential(&exact),
            Err(CredentialError::TooLarge)
        ));
    }
    #[test]
    fn qa_crd_4_vertex_ciphertext_requires_domain_separated_aad() {
        use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};

        let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &[7; 32]).expect("test key"));
        let mut ciphertext = b"canonical vertex credential".to_vec();
        key.seal_in_place_append_tag(
            Nonce::assume_unique_for_key([3; 12]),
            Aad::from(vertex_credential_aad("upstream-1")),
            &mut ciphertext,
        )
        .expect("encrypt");

        let open = |aad: Vec<u8>| {
            let mut encrypted = ciphertext.clone();
            key.open_in_place(
                Nonce::assume_unique_for_key([3; 12]),
                Aad::from(aad),
                &mut encrypted,
            )
            .map(|plaintext| plaintext.to_vec())
        };
        assert_eq!(
            open(vertex_credential_aad("upstream-1")).expect("vertex AAD"),
            b"canonical vertex credential"
        );
        assert!(
            open(vertex_credential_aad("upstream-2")).is_err(),
            "wrong upstream binding must fail closed"
        );
        assert!(
            open(b"upstream-1".to_vec()).is_err(),
            "generic API-key AAD must not open Vertex ciphertext"
        );
    }

    #[tokio::test]
    async fn qa_jwt_1_rs256_claims_and_signature() {
        let credential = parse_credential(sa_json().as_bytes()).expect("service account");
        let (transport, calls) = FakeTransport::one(response(
            br#"{"access_token":"token","token_type":"Bearer","expires_in":3600}"#,
        ));
        let (_, now) = clock();
        mint_once(&credential, transport, now).await.expect("mint");
        let calls = calls.lock().expect("calls");
        let body = String::from_utf8(calls[0].1.body.clone()).expect("form");
        let assertion = url::form_urlencoded::parse(body.as_bytes())
            .find(|(k, _)| k == "assertion")
            .map(|(_, v)| v.into_owned())
            .expect("assertion");
        let parts: Vec<_> = assertion.split('.').collect();
        assert_eq!(parts.len(), 3);
        let header: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).expect("header"))
                .expect("header json");
        let claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).expect("claims"))
                .expect("claims json");
        assert_eq!(header["alg"], "RS256");
        assert_eq!(claims["aud"], GOOGLE_TOKEN_URI);
        assert_eq!(claims["scope"], CLOUD_PLATFORM_SCOPE);
        assert_eq!(claims["iss"], "vertex-test@project.iam.gserviceaccount.com");
        assert!(claims["exp"].as_u64().expect("exp") - claims["iat"].as_u64().expect("iat") < 3600);
        let der = credential_private_key(&credential);
        let pair = RsaKeyPair::from_pkcs8(der).expect("key");
        let public = pair.public_key();
        let verifier = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, public);
        let signing = format!("{}.{}", parts[0], parts[1]);
        let signature = URL_SAFE_NO_PAD.decode(parts[2]).expect("signature");
        verifier
            .verify(signing.as_bytes(), &signature)
            .expect("RS256 signature");
    }

    fn credential_private_key(credential: &CanonicalCredential) -> &'static [u8] {
        static DER: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
        DER.get_or_init(|| {
            let value = serde_json::to_value(credential).expect("serialize");
            let encoded = value["private_key"].as_str().expect("key");
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .expect("der")
        })
        .as_slice()
    }

    #[tokio::test]
    async fn qa_mnt_1_fixed_endpoint_once() {
        let credential = parse_credential(sa_json().as_bytes()).expect("credential");
        let (transport, calls) = FakeTransport::one(response(
            br#"{"access_token":"token","token_type":"Bearer","expires_in":3600}"#,
        ));
        let (_, now) = clock();
        mint_once(&credential, transport, now).await.expect("mint");
        let calls = calls.lock().expect("calls");
        assert_eq!(calls.len(), 1);
        assert!(matches!(calls[0].0, TokenEndpoint::OAuth2Token));
    }

    #[tokio::test]
    async fn qa_mnt_2_refresh_grant_no_scope_rotated_ignored() {
        let credential = parse_credential(user_json()).expect("credential");
        let (transport, calls) = FakeTransport::one(response(br#"{"access_token":"token","token_type":"Bearer","expires_in":3600,"refresh_token":"ROTATED"}"#));
        let (_, now) = clock();
        let source = TokenSource::new(credential, transport, now);
        source.bearer().await.expect("mint");
        let body = String::from_utf8(calls.lock().expect("calls")[0].1.body.clone()).expect("form");
        assert!(body.contains("grant_type=refresh_token"));
        assert!(body.contains("client_id=client-id"));
        assert!(body.contains("refresh_token=REFRESH-TOKEN-VALUE"));
        assert!(!body.contains("scope="));
        assert_eq!(
            source
                .bearer()
                .await
                .expect("cached")
                .header
                .to_str()
                .expect("header"),
            "Bearer token"
        );
    }

    #[tokio::test]
    async fn qa_mnt_3_impersonation_order_body_and_quota() {
        let credential = parse_credential(impersonated_json().as_bytes()).expect("credential");
        let responses = vec![
            Ok(response(
                br#"{"access_token":"source","token_type":"Bearer","expires_in":3600}"#,
            )),
            Ok(TokenResponse {
                status: 200,
                headers: vec![("Date".into(), "Tue, 14 Nov 2023 22:13:20 GMT".into())],
                body: br#"{"accessToken":"target","expireTime":"2023-11-14T23:13:20Z"}"#.to_vec(),
            }),
        ];
        let (transport, calls) = FakeTransport::sequence(responses);
        let (_, now) = clock();
        let token = mint_once(&credential, transport, now)
            .await
            .expect("impersonation");
        assert_eq!(token.header.to_str().expect("header"), "Bearer target");
        let calls = calls.lock().expect("calls");
        assert!(matches!(calls[0].0, TokenEndpoint::OAuth2Token));
        assert!(matches!(
            calls[1].0,
            TokenEndpoint::IamGenerateAccessToken { .. }
        ));
        assert!(
            calls[1]
                .1
                .body
                .windows(CLOUD_PLATFORM_SCOPE.len())
                .any(|w| w == CLOUD_PLATFORM_SCOPE.as_bytes())
        );
        assert!(
            calls[1]
                .1
                .headers
                .iter()
                .any(|(k, v)| k == "x-goog-user-project" && v == "my-project")
        );
    }

    #[tokio::test]
    async fn qa_mnt_4_failure_taxonomy() {
        let credential = parse_credential(user_json()).expect("credential");
        for (status, body, terminal) in [
            (400, br#"{"error":"invalid_client"}"#.as_slice(), true),
            (401, br#"{}"#.as_slice(), true),
            (403, br#"{}"#.as_slice(), false),
            (429, br#"{}"#.as_slice(), false),
            (500, br#"{}"#.as_slice(), false),
        ] {
            let (transport, _) = FakeTransport::one(TokenResponse {
                status,
                headers: vec![],
                body: body.to_vec(),
            });
            let (_, now) = clock();
            let error = mint_once(&credential, transport, now)
                .await
                .expect_err("failure");
            assert_eq!(error.terminal_reason().is_some(), terminal);
        }
    }

    #[test]
    fn qa_mnt_5_host_allowlist_is_structural() {
        let hostile = r#"{"type":"impersonated_service_account","service_account_impersonation_url":"https://evil-iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target@project.iam.gserviceaccount.com:generateAccessToken","source_credentials":{"type":"authorized_user","client_id":"id","client_secret":"x","refresh_token":"y"}}"#;
        assert!(matches!(
            parse_credential(hostile.as_bytes()),
            Err(CredentialError::InvalidImpersonationUrl)
        ));
    }

    #[tokio::test]
    async fn qa_mnt_6_invalid_grant_then_success_is_not_terminal() {
        let credential = parse_credential(user_json()).expect("credential");
        let (transport, _) = FakeTransport::sequence(vec![
            Ok(TokenResponse {
                status: 400,
                headers: vec![],
                body: br#"{"error":"invalid_grant"}"#.to_vec(),
            }),
            Ok(response(
                br#"{"access_token":"token","token_type":"Bearer","expires_in":3600}"#,
            )),
        ]);
        let (test_clock, now) = clock();
        let source = TokenSource::new(credential, transport, now);
        assert!(matches!(
            source.bearer().await,
            Err(MintError::InvalidGrant) | Err(MintError::Backoff)
        ));
        test_clock.advance(std::time::Duration::from_secs(31));
        assert!(source.bearer().await.is_ok());
        assert!(!source.is_terminal());
    }

    #[tokio::test]
    async fn qa_mnt_6_invalid_grant_with_response_clock_skew_is_transient() {
        let credential = parse_credential(user_json()).expect("credential");
        let (transport, _) = FakeTransport::sequence(vec![Ok(TokenResponse {
            status: 400,
            headers: vec![("Date".into(), "Wed, 15 Nov 2023 22:13:20 GMT".into())],
            body: br#"{"error":"invalid_grant"}"#.to_vec(),
        })]);
        let (_, now) = clock();
        let source = TokenSource::new(credential, transport, now);
        assert!(matches!(
            source.bearer().await,
            Err(MintError::InvalidGrantClockSkew | MintError::Backoff)
        ));
        assert!(!source.is_terminal());
    }

    #[tokio::test]
    async fn qa_tok_6_waiters_are_woken_after_single_flight_mint() {
        let credential = parse_credential(user_json()).expect("credential");
        let gate = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = Arc::new(FakeTransport {
            calls: calls.clone(),
            responses: Mutex::new(VecDeque::from([Ok(response(
                br#"{"access_token":"token","token_type":"Bearer","expires_in":3600}"#,
            ))])),
            gate: Some(gate.clone()),
            started: Some(started.clone()),
        });
        let (_, now) = clock();
        let source = TokenSource::new(credential, transport, now);
        let first = tokio::spawn({
            let source = source.clone();
            async move { source.bearer().await.expect("first waiter") }
        });
        started.notified().await;
        let mut waiters = Vec::new();
        for _ in 0..32 {
            let source = source.clone();
            waiters.push(tokio::spawn(async move {
                source.bearer().await.expect("follower waiter")
            }));
        }
        tokio::task::yield_now().await;
        gate.notify_waiters();
        first.await.expect("first task");
        for waiter in waiters {
            waiter.await.expect("follower task");
        }
        assert_eq!(calls.lock().expect("calls").len(), 1);
    }

    #[tokio::test]
    async fn qa_tok_2_soft_and_hard_refresh_margins() {
        let credential = parse_credential(user_json()).expect("credential");
        let (transport, calls) = FakeTransport::sequence(vec![
            Ok(TokenResponse {
                status: 200,
                headers: vec![],
                body: br#"{"access_token":"old","token_type":"Bearer","expires_in":180}"#.to_vec(),
            }),
            Ok(response(
                br#"{"access_token":"new","token_type":"Bearer","expires_in":3600}"#,
            )),
        ]);
        let (test_clock, now) = clock();
        let source = TokenSource::new(credential, transport, now);
        let old = source.bearer().await.expect("old");
        test_clock.advance(std::time::Duration::from_secs(60));
        assert_eq!(source.bearer().await.expect("soft").header, old.header);
        test_clock.advance(std::time::Duration::from_secs(70));
        let new = source.bearer().await.expect("hard");
        assert_ne!(new.header, old.header);
        assert_eq!(calls.lock().expect("calls").len(), 2);
    }

    #[tokio::test]
    async fn qa_tok_3_dropped_caller_does_not_cancel_mint() {
        let credential = parse_credential(user_json()).expect("credential");
        let gate = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = Arc::new(FakeTransport {
            calls: calls.clone(),
            responses: Mutex::new(VecDeque::from([Ok(response(
                br#"{"access_token":"token","token_type":"Bearer","expires_in":3600}"#,
            ))])),
            gate: Some(gate.clone()),
            started: Some(started.clone()),
        });
        let (_, now) = clock();
        let source = TokenSource::new(credential, transport, now);
        let first = tokio::spawn({
            let source = source.clone();
            async move { source.bearer().await }
        });
        started.notified().await;
        first.abort();
        gate.notify_waiters();
        assert!(source.bearer().await.is_ok());
        assert_eq!(calls.lock().expect("calls").len(), 1);
    }

    #[test]
    fn qa_sec_4_hostile_urls_never_enter_canonical_form() {
        let hostile = br#"{"type":"authorized_user","client_id":"id","client_secret":"x","refresh_token":"y","token_uri":"https://evil.example/token","service_account_impersonation_url":"https://evil.example"}"#;
        assert!(parse_credential(hostile).is_err());
    }
    #[test]
    fn qa_sec_2_auth_debug_redacts_secrets() {
        let credential = parse_credential(user_json()).expect("credential");
        let debug = format!("{credential:?}");
        assert!(!debug.contains("CLIENT-SECRET-VALUE"));
        assert!(!debug.contains("REFRESH-TOKEN-VALUE"));
        let request = TokenRequest {
            body: b"assertion=PRIVATE-JWT".to_vec(),
            content_type: "application/x-www-form-urlencoded",
            headers: vec![("authorization".into(), "Bearer ACCESS-TOKEN".into())],
        };
        assert!(!format!("{request:?}").contains("PRIVATE-JWT"));
        assert!(!format!("{request:?}").contains("ACCESS-TOKEN"));
    }

    #[test]
    fn qa_tok_4_registry_reuses_only_unchanged_credentials() {
        let credential = parse_credential(user_json()).expect("credential");
        let (transport, _) = FakeTransport::one(response(
            br#"{"access_token":"token","token_type":"Bearer","expires_in":3600}"#,
        ));
        let (_, now) = clock();
        let registry = TokenRegistry::new();
        let first = registry.source_for(
            "upstream",
            b"ciphertext-a",
            credential.clone(),
            transport.clone(),
            now.clone(),
        );
        let same = registry.source_for(
            "upstream",
            b"ciphertext-a",
            credential.clone(),
            transport.clone(),
            now.clone(),
        );
        let replaced = registry.source_for("upstream", b"ciphertext-b", credential, transport, now);
        assert!(Arc::ptr_eq(&first, &same));
        assert!(!Arc::ptr_eq(&first, &replaced));
    }

    #[tokio::test]
    async fn qa_tok_5_terminal_source_fails_closed_without_http() {
        let credential = parse_credential(user_json()).expect("credential");
        let (transport, calls) = FakeTransport::sequence(vec![
            Ok(TokenResponse {
                status: 400,
                headers: vec![],
                body: br#"{"error":"invalid_grant"}"#.to_vec(),
            }),
            Ok(TokenResponse {
                status: 400,
                headers: vec![],
                body: br#"{"error":"invalid_grant"}"#.to_vec(),
            }),
        ]);
        let (test_clock, now) = clock();
        let source = TokenSource::new(credential, transport, now);
        assert!(source.bearer().await.is_err());
        test_clock.advance_secs(31);
        assert!(source.bearer().await.is_err());
        let count = calls.lock().expect("calls").len();
        assert!(matches!(
            source.bearer().await,
            Err(MintError::InvalidGrantTerminal)
        ));
        assert_eq!(calls.lock().expect("calls").len(), count);
    }
}
