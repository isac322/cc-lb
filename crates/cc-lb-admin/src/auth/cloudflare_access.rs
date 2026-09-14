use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::{HeaderMap, HeaderName};
use serde::Deserialize;

use super::jwks::{JwksCache, JwksError};
use super::{AdminActorKind, AdminAuthProvider, AdminIdentity, ProviderOutcome};

const CLOCK_SKEW_LEEWAY_SECS: u64 = 60;

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
}

#[async_trait]
impl AdminAuthProvider for CloudflareAccessProvider {
    fn id(&self) -> &str {
        &self.id
    }

    async fn authenticate(&self, headers: &HeaderMap) -> ProviderOutcome {
        let Some(header_value) = headers.get(&self.header) else {
            return ProviderOutcome::NotPresent;
        };
        let Ok(token) = header_value.to_str() else {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        };

        let jwt = match CompactJwt::parse(token) {
            Ok(jwt) if jwt.header.algorithm == "RS256" && !jwt.header.key_id.is_empty() => jwt,
            _ => return ProviderOutcome::Rejected("cf_access_invalid_token"),
        };
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

        let claims: CfClaims = match sonic_rs::from_slice(&jwt.claims) {
            Ok(claims) => claims,
            Err(_) => return ProviderOutcome::Rejected("cf_access_invalid_token"),
        };
        let Some(now) = unix_secs() else {
            return ProviderOutcome::Rejected("cf_access_invalid_token");
        };
        if claims.issuer != self.issuer
            || !claims.audience.matches_any(&self.audiences)
            || claims.exp < now.saturating_sub(CLOCK_SKEW_LEEWAY_SECS)
            || claims
                .nbf
                .is_some_and(|not_before| not_before > now.saturating_add(CLOCK_SKEW_LEEWAY_SECS))
        {
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
