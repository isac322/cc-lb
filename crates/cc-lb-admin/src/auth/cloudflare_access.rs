use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::{HeaderMap, HeaderName};
use serde::Deserialize;

use super::jwks::{JwksCache, JwksError};
use super::{AdminActorKind, AdminAuthProvider, AdminIdentity, ProviderOutcome};

const CLOCK_SKEW_LEEWAY_SECS: u64 = 60;
const MAX_COMPACT_JWT_BYTES: usize = 16 * 1024;
const MAX_KID_LEN: usize = 256;

pub(crate) struct CloudflareAccessProvider {
    id: String,
    header: HeaderName,
    issuer: String,
    audiences: Vec<String>,
    jwks: JwksCache,
}

impl CloudflareAccessProvider {
    pub(crate) fn new(
        id: String,
        header: String,
        issuer: String,
        audiences: Vec<String>,
    ) -> Result<Self, ()> {
        let issuer = issuer.trim_end_matches('/').to_owned();
        let jwks_url = format!("{issuer}/cdn-cgi/access/certs");
        Ok(Self {
            id,
            header: HeaderName::from_bytes(header.as_bytes()).map_err(|_| ())?,
            issuer,
            audiences,
            jwks: JwksCache::new(jwks_url),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_jwks_url(mut self, jwks_url: String) -> Self {
        self.jwks = JwksCache::new(jwks_url);
        self
    }

    fn claims_within_time_window(claims: &CfClaims, now: u64) -> bool {
        claims.exp >= now.saturating_sub(CLOCK_SKEW_LEEWAY_SECS)
            && claims
                .nbf
                .is_none_or(|not_before| not_before <= now.saturating_add(CLOCK_SKEW_LEEWAY_SECS))
    }

    async fn authenticate_with_clock(
        &self,
        headers: &HeaderMap,
        mut now_secs_fn: impl FnMut() -> Option<u64>,
    ) -> ProviderOutcome {
        let Some(header_value) = headers.get(&self.header) else {
            return ProviderOutcome::NotPresent;
        };
        let Ok(token) = header_value.to_str() else {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        };
        if token.len() > MAX_COMPACT_JWT_BYTES {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        }

        let jwt = match CompactJwt::parse(token) {
            Ok(jwt)
                if jwt.header.algorithm == "RS256"
                    && !jwt.header.key_id.is_empty()
                    && jwt.header.key_id.len() <= MAX_KID_LEN =>
            {
                jwt
            }
            _ => return ProviderOutcome::Rejected("cf_access_invalid_token"),
        };

        // Validate claim format, issuer, audience, and time window before any JWKS
        // lookup so unauthenticated tokens with invalid metadata cannot trigger
        // JWKS fetches or unknown-kid cache Churn.
        let claims: CfClaims = match sonic_rs::from_slice(&jwt.claims) {
            Ok(claims) => claims,
            Err(_) => return ProviderOutcome::Rejected("cf_access_invalid_token"),
        };
        let Some(before_jwks_now) = now_secs_fn() else {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        };
        if claims.issuer != self.issuer
            || !claims.audience.matches_any(&self.audiences)
            || !Self::claims_within_time_window(&claims, before_jwks_now)
        {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        }

        let key = match self.jwks.get(&jwt.header.key_id).await {
            Ok(key) => key,
            Err(JwksError::UnknownKid) => {
                return ProviderOutcome::Rejected("cf_access_invalid_token");
            }
            Err(
                JwksError::Request(_) | JwksError::InvalidDocument | JwksError::RefreshThrottled,
            ) => {
                return ProviderOutcome::Rejected("cf_access_jwks_unavailable");
            }
        };
        if key.verify_rs256(jwt.signing_input, &jwt.signature).is_err() {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        }
        let Some(verified_now) = now_secs_fn() else {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        };
        if !Self::claims_within_time_window(&claims, verified_now) {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        }

        if !claims.sub.is_empty() {
            let display_name = claims.email.clone();
            return ProviderOutcome::Verified(AdminIdentity {
                authority: self.issuer.clone(),
                subject: claims.sub,
                kind: AdminActorKind::Human,
                provider_id: self.id.clone(),
                email: claims.email,
                display_name,
                groups: Vec::new(),
                expires_at_unix_secs: Some(claims.exp),
            });
        }

        let Some(common_name) = claims.common_name else {
            return ProviderOutcome::Rejected("cf_access_subject_missing");
        };
        ProviderOutcome::Verified(AdminIdentity {
            authority: self.issuer.clone(),
            subject: common_name.clone(),
            kind: AdminActorKind::Service,
            provider_id: self.id.clone(),
            email: None,
            display_name: Some(common_name),
            groups: Vec::new(),
            expires_at_unix_secs: Some(claims.exp),
        })
    }
}

#[async_trait]
impl AdminAuthProvider for CloudflareAccessProvider {
    fn id(&self) -> &str {
        &self.id
    }

    async fn authenticate(&self, headers: &HeaderMap) -> ProviderOutcome {
        self.authenticate_with_clock(headers, unix_secs).await
    }
}

struct CompactJwt<'a> {
    header: JwtHeader,
    claims: Vec<u8>,
    signing_input: &'a [u8],
    signature: Vec<u8>,
}

impl<'a> CompactJwt<'a> {
    fn parse(token: &'a str) -> Result<Self, ()> {
        let mut segments = token.split('.');
        let (Some(header), Some(claims), Some(signature), None) = (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ) else {
            return Err(());
        };
        if header.is_empty() || claims.is_empty() || signature.is_empty() {
            return Err(());
        }

        let signing_input_len = header.len() + 1 + claims.len();
        let header_bytes = URL_SAFE_NO_PAD.decode(header).map_err(|_| ())?;
        let header = sonic_rs::from_slice(&header_bytes).map_err(|_| ())?;
        let claims = URL_SAFE_NO_PAD.decode(claims).map_err(|_| ())?;
        let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| ())?;

        Ok(Self {
            header,
            claims,
            signing_input: &token.as_bytes()[..signing_input_len],
            signature,
        })
    }
}

#[derive(Deserialize)]
struct JwtHeader {
    #[serde(rename = "alg")]
    algorithm: String,
    #[serde(rename = "kid")]
    key_id: String,
}

#[derive(Deserialize)]
struct CfClaims {
    sub: String,
    email: Option<String>,
    common_name: Option<String>,
    exp: u64,
    #[serde(rename = "iss")]
    issuer: String,
    #[serde(rename = "aud")]
    audience: Audience,
    nbf: Option<u64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    fn matches_any(&self, expected: &[String]) -> bool {
        match self {
            Self::One(actual) => expected.iter().any(|candidate| candidate == actual),
            Self::Many(actual) => actual
                .iter()
                .any(|audience| expected.iter().any(|candidate| candidate == audience)),
        }
    }
}

fn unix_secs() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

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

    #[tokio::test]
    async fn jwt_expiring_during_jwks_lookup_is_rejected_after_signature_check() {
        let jwks_body = json!({
            "keys": [{
                "kid": "test",
                "kty": "RSA",
                "alg": "RS256",
                "use": "sig",
                "n": TEST_RSA_N,
                "e": TEST_RSA_E,
            }]
        })
        .to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let body = jwks_body.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let _ = stream.read(&mut buf).await;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                });
            }
        });

        let issuer = format!("http://{addr}");
        let provider = CloudflareAccessProvider::new(
            "cf-test".to_owned(),
            "cf-access-jwt-assertion".to_owned(),
            issuer.clone(),
            vec!["aud-test".to_owned()],
        )
        .expect("provider builds")
        .with_jwks_url(format!("{issuer}/cdn-cgi/access/certs"));

        let private_key = base64::engine::general_purpose::STANDARD
            .decode(TEST_PRIVATE_KEY_PKCS8_BASE64)
            .expect("valid base64");
        let signing_key = RsaKeyPair::from_pkcs8(&private_key).expect("pkcs8 key");

        let exp = 1_700_000_000u64;
        let header_b64 = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","kid":"test","typ":"JWT"}"#);
        let claims_b64 = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "sub": "alice",
                "email": "alice@example.com",
                "iss": issuer,
                "aud": "aud-test",
                "exp": exp,
            }))
            .unwrap(),
        );
        let signing_input = format!("{header_b64}.{claims_b64}");
        let mut sig = vec![0u8; signing_key.public().modulus_len()];
        signing_key
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                signing_input.as_bytes(),
                &mut sig,
            )
            .unwrap();
        let token = format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(sig));

        let mut headers = HeaderMap::new();
        headers.insert("cf-access-jwt-assertion", token.parse().unwrap());

        // Pre-check passes at `exp + CLOCK_SKEW_LEEWAY_SECS`, but clock advances 1s past skew
        // before post-verification check -> must reject as `cf_access_invalid_token`.
        let calls = AtomicUsize::new(0);
        let outcome = provider
            .authenticate_with_clock(&headers, || {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Some(exp + CLOCK_SKEW_LEEWAY_SECS)
                } else {
                    Some(exp + CLOCK_SKEW_LEEWAY_SECS + 1)
                }
            })
            .await;
        assert!(matches!(
            outcome,
            ProviderOutcome::Rejected("cf_access_invalid_token")
        ));

        // Constant valid clock returns Verified.
        let ok = provider
            .authenticate_with_clock(&headers, || Some(exp))
            .await;
        assert!(matches!(ok, ProviderOutcome::Verified(_)));
        server_task.abort();
    }
}
