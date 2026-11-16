//! Local stateful bearer codec. Payload metadata is never an authorization proof.
use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Serialize;
use zeroize::Zeroizing;

use super::secret::{compute_index_hash, compute_verify_hash, verify_secret};

const HEADER: &str = "eyJ0eXAiOiJjYy1sYi1zZXNzaW9uLXYxIiwiYWxnIjoiQ0NMQi1TVEFURUZVTCJ9";
const RENEWAL_PREFIX: &str = "cclb-renew-v1.";
const RELAY_PREFIX: &str = "cclb-relay-v1.";
const MAX_PAYLOAD_ENCODED_BYTES: usize = 8192;
const SECRET_ENCODED_BYTES: usize = 43;
const MAX_ACCESS_BYTES: usize = HEADER.len() + 1 + MAX_PAYLOAD_ENCODED_BYTES + 1 + SECRET_ENCODED_BYTES;

/// Caller-supplied local metadata, not vendor identity or granted permissions.
/// `plan` must be verified provider metadata, or `"unknown"`.
pub struct Claims {
    pub issuer: String,
    pub local_user_id: String,
    pub workspace_id: String,
    pub family_id: String,
    pub access_id: String,
    pub issued_at: u64,
    pub expires_at: u64,
    pub plan: String,
}

pub struct RedactedToken(Zeroizing<String>);

impl RedactedToken {
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for RedactedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for RedactedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Debug)]
pub struct MintedToken {
    pub plaintext: RedactedToken,
    pub secret_salt: [u8; 16],
    pub index_hash: [u8; 32],
    pub verify_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Purpose {
    Access,
    Renewal,
    Relay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid local session metadata")]
pub struct MintError;

#[derive(Serialize)]
struct Payload<'a> {
    iss: &'a str,
    aud: &'static str,
    sub: &'a str,
    sid: &'a str,
    jti: &'a str,
    iat: u64,
    exp: u64,
    #[serde(rename = "https://api.openai.com/auth")]
    auth: ConsumerMetadata<'a>,
}

#[derive(Serialize)]
struct ConsumerMetadata<'a> {
    chatgpt_user_id: &'a str,
    chatgpt_account_id: &'a str,
    chatgpt_plan_type: &'a str,
}

pub fn mint_access(claims: &Claims) -> Result<MintedToken, MintError> {
    // Bound input before serialization, including pathological local metadata.
    let strings = [
        claims.issuer.as_str(), claims.local_user_id.as_str(), claims.workspace_id.as_str(),
        claims.family_id.as_str(), claims.access_id.as_str(), claims.plan.as_str(),
    ];
    if strings.iter().any(|s| s.is_empty() || s.len() > MAX_PAYLOAD_ENCODED_BYTES)
        || claims.expires_at <= claims.issued_at
    {
        return Err(MintError);
    }
    let payload = serde_json::to_vec(&Payload {
        iss: &claims.issuer,
        aud: "cc-lb-session-v1",
        sub: &claims.local_user_id,
        sid: &claims.family_id,
        jti: &claims.access_id,
        iat: claims.issued_at,
        exp: claims.expires_at,
        auth: ConsumerMetadata {
            chatgpt_user_id: &claims.local_user_id,
            chatgpt_account_id: &claims.workspace_id,
            chatgpt_plan_type: &claims.plan,
        },
    }).map_err(|_| MintError)?;
    let encoded_len = base64::encoded_len(payload.len(), false).ok_or(MintError)?;
    if encoded_len > MAX_PAYLOAD_ENCODED_BYTES {
        return Err(MintError);
    }
    let mut bearer = Zeroizing::new(String::with_capacity(HEADER.len() + encoded_len + 45));
    bearer.push_str(HEADER);
    bearer.push('.');
    URL_SAFE_NO_PAD.encode_string(payload, &mut bearer);
    bearer.push('.');
    append_secret(&mut bearer);
    Ok(finish(bearer))
}

pub fn mint_renewal() -> MintedToken {
    mint_scoped(RENEWAL_PREFIX)
}

pub fn mint_relay() -> MintedToken {
    mint_scoped(RELAY_PREFIX)
}

fn mint_scoped(prefix: &str) -> MintedToken {
    let mut bearer = Zeroizing::new(String::with_capacity(prefix.len() + SECRET_ENCODED_BYTES));
    bearer.push_str(prefix);
    append_secret(&mut bearer);
    finish(bearer)
}

fn append_secret(bearer: &mut String) {
    let mut bytes = Zeroizing::new([0_u8; 32]);
    rand::fill(&mut bytes[..]);
    URL_SAFE_NO_PAD.encode_string(&bytes[..], bearer);
}

fn finish(bearer: Zeroizing<String>) -> MintedToken {
    let mut secret_salt = [0_u8; 16];
    rand::fill(&mut secret_salt[..]);
    MintedToken {
        index_hash: compute_index_hash(bearer.as_bytes()),
        verify_hash: compute_verify_hash(bearer.as_bytes(), &secret_salt),
        secret_salt,
        plaintext: RedactedToken(bearer),
    }
}

/// Allocation-free bounded syntax validation; deliberately does not decode claims.
pub fn validate(input: &str, purpose: Purpose) -> bool {
    match purpose {
        Purpose::Access => {
            if input.len() > MAX_ACCESS_BYTES {
                return false;
            }
            let Some(body) = input.strip_prefix(HEADER).and_then(|s| s.strip_prefix('.')) else {
                return false;
            };
            let Some((payload, secret)) = body.split_once('.') else {
                return false;
            };
            payload.len() <= MAX_PAYLOAD_ENCODED_BYTES
                && canonical_segment(payload.as_bytes())
                && valid_secret(secret)
        }
        Purpose::Renewal => validate_scoped(input, RENEWAL_PREFIX),
        Purpose::Relay => validate_scoped(input, RELAY_PREFIX),
    }
}

fn validate_scoped(input: &str, prefix: &str) -> bool {
    input.len() == prefix.len() + SECRET_ENCODED_BYTES
        && input.strip_prefix(prefix).is_some_and(valid_secret)
}

fn valid_secret(secret: &str) -> bool {
    secret.len() == SECRET_ENCODED_BYTES && canonical_segment(secret.as_bytes())
}

fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

fn canonical_segment(bytes: &[u8]) -> bool {
    let Some((&last, rest)) = bytes.split_last() else {
        return false;
    };
    let Some(last) = digit(last) else {
        return false;
    };
    let canonical_tail = match bytes.len() % 4 {
        0 => true,
        2 => last & 15 == 0,
        3 => last & 3 == 0,
        _ => false,
    };
    canonical_tail && rest.iter().all(|&b| digit(b).is_some())
}

/// Verifies the entire bearer, not just its entropy segment. A true result only
/// proves possession: callers must still authorize the stored record and lineage.
pub fn verify(
    input: &str,
    purpose: Purpose,
    stored_verify_hash: &[u8; 32],
    stored_salt: &[u8; 16],
) -> bool {
    validate(input, purpose) && verify_secret(input.as_bytes(), stored_verify_hash, stored_salt)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims() -> Claims {
        Claims {
            issuer: "https://proxy.example.com".into(),
            local_user_id: "local-user".into(),
            workspace_id: "local-workspace".into(),
            family_id: "family".into(),
            access_id: "access".into(),
            issued_at: 100,
            expires_at: 200,
            plan: "unknown".into(),
        }
    }

    #[test]
    fn minted_metadata_is_consumer_parseable() {
        let token = mint_access(&claims()).unwrap();
        let parts: Vec<_> = token.plaintext.expose().split('.').collect();
        assert_eq!(parts.len(), 3);
        let header: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        assert_eq!(header, serde_json::json!({"typ":"cc-lb-session-v1","alg":"CCLB-STATEFUL"}));
        let payload: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(payload, serde_json::json!({
            "iss":"https://proxy.example.com", "aud":"cc-lb-session-v1", "sub":"local-user",
            "sid":"family", "jti":"access", "iat":100, "exp":200,
            "https://api.openai.com/auth": {"chatgpt_user_id":"local-user", "chatgpt_account_id":"local-workspace", "chatgpt_plan_type":"unknown"}
        }));
        assert_eq!(URL_SAFE_NO_PAD.decode(parts[2]).unwrap().len(), 32);
        assert_eq!(token.index_hash, compute_index_hash(token.plaintext.expose().as_bytes()));
    }

    #[test]
    fn session_token_full_bytes_are_verified() {
        let token = mint_access(&claims()).unwrap();
        let bearer = token.plaintext.expose();
        assert!(verify(bearer, Purpose::Access, &token.verify_hash, &token.secret_salt));
        for start in [0, bearer.find('.').unwrap() + 1, bearer.rfind('.').unwrap() + 1] {
            let mut changed = bearer.as_bytes().to_vec();
            changed[start] = if changed[start] == b'A' { b'B' } else { b'A' };
            assert!(!verify(std::str::from_utf8(&changed).unwrap(), Purpose::Access, &token.verify_hash, &token.secret_salt));
        }
        for suffix in [".AAAA", " ", "\n"] {
            assert!(!validate(&format!("{bearer}{suffix}"), Purpose::Access));
        }
        let mut noncanonical = bearer.as_bytes().to_vec();
        *noncanonical.last_mut().unwrap() = b'B';
        assert!(!validate(std::str::from_utf8(&noncanonical).unwrap(), Purpose::Access));
        // Valid metadata replacements cannot reuse a verifier, even with the
        // original entropy segment retained.
        let parts: Vec<_> = bearer.split('.').collect();
        let changed_payload = URL_SAFE_NO_PAD.encode(br#"{"sub":"other-user"}"#);
        let changed = format!("{}.{}.{}", parts[0], changed_payload, parts[2]);
        assert!(validate(&changed, Purpose::Access));
        assert!(!verify(&changed, Purpose::Access, &token.verify_hash, &token.secret_salt));
    }

    #[test]
    fn renewal_and_access_are_distinct() {
        let access = mint_access(&claims()).unwrap();
        let renewal = mint_renewal();
        assert!(validate(renewal.plaintext.expose(), Purpose::Renewal));
        assert!(verify(renewal.plaintext.expose(), Purpose::Renewal, &renewal.verify_hash, &renewal.secret_salt));
        assert!(!validate(renewal.plaintext.expose(), Purpose::Access));
        assert!(!validate(access.plaintext.expose(), Purpose::Renewal));
        assert!(!validate("sk-cclb-key_secret", Purpose::Access));
    }

    #[test]
    fn relay_is_opaque_canonical_and_purpose_scoped() {
        let relay = mint_relay();
        let bearer = relay.plaintext.expose();
        let secret = bearer.strip_prefix("cclb-relay-v1.").unwrap();
        assert_eq!(URL_SAFE_NO_PAD.decode(secret).unwrap().len(), 32);
        assert_eq!(relay.index_hash, compute_index_hash(bearer.as_bytes()));
        assert!(verify(bearer, Purpose::Relay, &relay.verify_hash, &relay.secret_salt));
        assert!(!validate(bearer, Purpose::Access));
        assert!(!validate(bearer, Purpose::Renewal));
        assert!(!validate(mint_renewal().plaintext.expose(), Purpose::Relay));
        assert!(!validate(mint_access(&claims()).unwrap().plaintext.expose(), Purpose::Relay));
        for suffix in [".AAAA", " ", "\n"] {
            assert!(!validate(&format!("{bearer}{suffix}"), Purpose::Relay));
        }
        let noncanonical = format!("cclb-relay-v1.{}B", "A".repeat(42));
        assert!(!validate(&noncanonical, Purpose::Relay));
        let replacement = format!("cclb-relay-v1.{}", "A".repeat(43));
        let replacement = if replacement == bearer {
            format!("cclb-relay-v1.B{}", "A".repeat(42))
        } else {
            replacement
        };
        assert!(validate(&replacement, Purpose::Relay));
        assert!(!verify(&replacement, Purpose::Relay, &relay.verify_hash, &relay.secret_salt));
        assert_eq!(format!("{}", relay.plaintext), "[REDACTED]");
        assert_eq!(format!("{:?}", relay.plaintext), "[REDACTED]");
        assert!(!format!("{relay:?}").contains(bearer));
    }

    #[test]
    fn session_token_redacts_secrets() {
        let token = mint_renewal();
        assert_eq!(format!("{}", token.plaintext), "[REDACTED]");
        assert_eq!(format!("{:?}", token.plaintext), "[REDACTED]");
        assert!(!format!("{:?}", token).contains(token.plaintext.expose()));
    }

    #[test]
    fn syntax_work_is_bounded_and_canonical() {
        for payload in ["", "A", "AB", "AAA=", "AA AA"] {
            assert!(!validate(&format!("{HEADER}.{payload}.{}", "A".repeat(43)), Purpose::Access));
        }
        assert!(!validate(&format!("{HEADER}.{}.{}", "A".repeat(MAX_PAYLOAD_ENCODED_BYTES + 1), "A".repeat(43)), Purpose::Access));
        let mut oversized = claims();
        oversized.issuer = "x".repeat(MAX_PAYLOAD_ENCODED_BYTES);
        assert!(mint_access(&oversized).is_err());
    }
}
