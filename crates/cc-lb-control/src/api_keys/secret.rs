use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;
use ulid::Ulid;

const PREFIX: &str = "sk-cclb-";
const KEY_ID_LEN: usize = 26;
const SECRET_B64_LEN: usize = 43;
const SECRET_BYTES_LEN: usize = 32;
const SALT_BYTES_LEN: usize = 16;

pub struct RedactedSecret(Box<str>);

impl RedactedSecret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RedactedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sk-cclb-***")
    }
}

impl fmt::Display for RedactedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sk-cclb-***")
    }
}

pub struct NewKeyOutput {
    pub plaintext: RedactedSecret,
    pub key_id: String,
    pub secret_salt: [u8; 16],
    pub index_hash: [u8; 32],
    pub verify_hash: [u8; 32],
    pub last_4: String,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("invalid API key secret")]
pub struct ParseError;

pub fn generate_new() -> NewKeyOutput {
    let mut secret_bytes = [0_u8; SECRET_BYTES_LEN];
    rand::fill(&mut secret_bytes[..]);

    let mut salt = [0_u8; SALT_BYTES_LEN];
    rand::fill(&mut salt[..]);

    let key_id = Ulid::generate().to_string();
    let secret_b64 = URL_SAFE_NO_PAD.encode(secret_bytes);
    let plaintext = format!("{PREFIX}{key_id}_{secret_b64}");
    let index_hash = compute_index_hash(secret_b64.as_bytes());
    let verify_hash = compute_verify_hash(secret_b64.as_bytes(), &salt);
    let last_4 = secret_b64[secret_b64.len() - 4..].to_owned();

    NewKeyOutput {
        plaintext: RedactedSecret(plaintext.into_boxed_str()),
        key_id,
        secret_salt: salt,
        index_hash,
        verify_hash,
        last_4,
    }
}

pub fn parse(input: &str) -> Result<(String, Vec<u8>), ParseError> {
    let body = input.strip_prefix(PREFIX).ok_or(ParseError)?;
    let (key_id, secret_b64) = body.split_once('_').ok_or(ParseError)?;

    if key_id.len() != KEY_ID_LEN || !key_id.bytes().all(is_crockford_base32) {
        return Err(ParseError);
    }

    if secret_b64.len() != SECRET_B64_LEN || !secret_b64.bytes().all(is_base64_url_no_pad) {
        return Err(ParseError);
    }

    Ok((key_id.to_owned(), secret_b64.as_bytes().to_vec()))
}

pub fn compute_index_hash(secret_b64_bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(secret_b64_bytes).into()
}

pub fn compute_verify_hash(secret_b64_bytes: &[u8], salt: &[u8; 16]) -> [u8; 32] {
    Sha256::new()
        .chain_update(secret_b64_bytes)
        .chain_update(salt)
        .finalize()
        .into()
}

pub fn verify_secret(
    input_secret_b64_bytes: &[u8],
    stored_verify_hash: &[u8; 32],
    stored_salt: &[u8; 16],
) -> bool {
    let input_verify_hash = compute_verify_hash(input_secret_b64_bytes, stored_salt);

    input_verify_hash.ct_eq(stored_verify_hash).into()
}

fn is_crockford_base32(byte: u8) -> bool {
    matches!(byte, b'0'..=b'9' | b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z')
}

fn is_base64_url_no_pad(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_new_parses_same_key_id() {
        let generated = generate_new();

        let (parsed_key_id, secret_b64_bytes) =
            parse(generated.plaintext.expose()).expect("parse generated key");

        assert_eq!(parsed_key_id, generated.key_id);
        assert_eq!(secret_b64_bytes.len(), SECRET_B64_LEN);
        assert_eq!(generated.last_4.len(), 4);
        assert_eq!(generated.index_hash, compute_index_hash(&secret_b64_bytes));
        assert_eq!(
            generated.verify_hash,
            compute_verify_hash(&secret_b64_bytes, &generated.secret_salt)
        );
    }

    #[test]
    fn compute_index_hash_is_deterministic() {
        let secret = b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG";

        assert_eq!(compute_index_hash(secret), compute_index_hash(secret));
    }

    #[test]
    fn compute_verify_hash_depends_on_salt() {
        let secret = b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG";
        let salt_a = [1_u8; 16];
        let salt_b = [2_u8; 16];

        assert_ne!(
            compute_verify_hash(secret, &salt_a),
            compute_verify_hash(secret, &salt_b)
        );
    }

    #[test]
    fn verify_secret_accepts_matching_secret() {
        let secret = b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG";
        let salt = [3_u8; 16];
        let verify_hash = compute_verify_hash(secret, &salt);

        assert!(verify_secret(secret, &verify_hash, &salt));
    }

    #[test]
    fn verify_secret_rejects_tampered() {
        let secret = b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG";
        let salt = [4_u8; 16];
        let verify_hash = compute_verify_hash(secret, &salt);
        let mut mutated = secret.to_vec();
        mutated[0] = b'Z';

        assert!(!verify_secret(&mutated, &verify_hash, &salt));
    }

    #[test]
    fn redacted_debug() {
        let secret = RedactedSecret(
            "sk-cclb-0123456789ABCDEFGHJKMNPQRS_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG".into(),
        );

        assert_eq!(format!("{secret:?}"), "sk-cclb-***");
    }

    #[test]
    fn redacted_display() {
        let secret = RedactedSecret(
            "sk-cclb-0123456789ABCDEFGHJKMNPQRS_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG".into(),
        );

        assert_eq!(format!("{secret}"), "sk-cclb-***");
    }

    #[test]
    fn parse_invalid_returns_error() {
        assert_eq!(parse("invalid"), Err(ParseError));
    }
}
