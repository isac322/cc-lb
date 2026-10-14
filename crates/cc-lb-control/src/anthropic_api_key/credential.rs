//! Decryption of a stored Anthropic API-key credential.

use cc_lb_aead::AeadService;
use cc_lb_storage_api::UpstreamRecord;

use super::normalize::NormalizedApiKey;

/// Why an anthropic api-key credential could not be resolved. Callers map each
/// reason onto their own error type; the variants carry no key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyResolutionError {
    Missing,
    NotDecryptable,
    NotUtf8,
    InvalidHeaderValue,
    /// The plaintext is a JSON string literal: the pre-#848 encoding, which
    /// is no longer decoded. Sending it would put literal quotes on the wire.
    LegacyEncoding,
}

/// Resolve the operator-configured anthropic api key for an upstream record.
///
/// `api_key_ciphertext` is sealed with AAD `upstream.id` over the raw UTF-8
/// key bytes. Resolution fails closed: any error means no credential, never a
/// fallback to the downstream caller's key.
pub fn resolve_api_key(
    aead: &AeadService,
    record: &UpstreamRecord,
) -> Result<zeroize::Zeroizing<String>, ApiKeyResolutionError> {
    let ciphertext = record
        .api_key_ciphertext
        .as_deref()
        .ok_or(ApiKeyResolutionError::Missing)?;
    let plaintext = aead
        .decrypt(ciphertext, record.id.as_bytes())
        .map(zeroize::Zeroizing::new)
        .map_err(|_| ApiKeyResolutionError::NotDecryptable)?;
    // The key must reach the upstream byte-identical to what the operator
    // stored, so it is neither trimmed nor rewritten.
    let key = std::str::from_utf8(&plaintext)
        .map_err(|_| ApiKeyResolutionError::NotUtf8)?
        .to_owned();
    let key = zeroize::Zeroizing::new(key);
    // The signer applies `HeaderValue::from_str` to this value at request time;
    // reject here so a malformed key surfaces as a view-build error instead of
    // a runtime signing failure.
    if key.trim().is_empty() || http::HeaderValue::from_str(&key).is_err() {
        return Err(ApiKeyResolutionError::InvalidHeaderValue);
    }
    if key.len() >= 2 && key.starts_with('"') && key.ends_with('"') {
        return Err(ApiKeyResolutionError::LegacyEncoding);
    }
    Ok(key)
}

/// The stored key for re-use by a probe, exactly as [`resolve_api_key`]
/// hands it to the signer.
///
/// The paste-time rules of
/// [`normalize_api_key`](super::normalize::normalize_api_key) (no internal
/// spaces, at most 512 bytes) are not applied: a key stored before they
/// existed still proxies, so re-checking it or re-probing it after a
/// `base_url` or workspace edit must not refuse it either.
pub fn resolve_stored_api_key(
    aead: &AeadService,
    record: &UpstreamRecord,
) -> Result<NormalizedApiKey, ApiKeyResolutionError> {
    resolve_api_key(aead, record).map(NormalizedApiKey::stored)
}

#[cfg(test)]
mod tests {
    use cc_lb_storage_api::UpstreamKind;
    use uuid::Uuid;

    use super::*;

    fn api_key_record(name: &str, ciphertext: Option<Vec<u8>>) -> UpstreamRecord {
        UpstreamRecord {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            api_key_ciphertext: ciphertext,
            ..UpstreamRecord::default()
        }
    }

    #[test]
    fn resolve_api_key_reads_canonical_raw_id_aad_shape() {
        let aead = AeadService::from_master_key([19; 32]);
        let mut record = api_key_record("canonical", None);
        record.api_key_ciphertext = Some(
            aead.encrypt(b"sk-ant-canonical", record.id.as_bytes())
                .expect("encrypt"),
        );

        let resolved = resolve_api_key(&aead, &record).expect("canonical shape resolves");
        assert_eq!(resolved.as_str(), "sk-ant-canonical");
    }

    #[test]
    fn resolve_api_key_rejects_ciphertext_under_unrelated_aad() {
        let aead = AeadService::from_master_key([19; 32]);
        let record = api_key_record("bound-elsewhere", None);
        let ciphertext = aead
            .encrypt(b"sk-ant-bound-elsewhere", b"some-other-aad")
            .expect("encrypt");
        let record = UpstreamRecord {
            api_key_ciphertext: Some(ciphertext),
            ..record
        };

        assert!(matches!(
            resolve_api_key(&aead, &record),
            Err(ApiKeyResolutionError::NotDecryptable)
        ));
    }

    #[test]
    fn resolve_api_key_rejects_key_with_invalid_header_byte() {
        let aead = AeadService::from_master_key([19; 32]);
        let record = api_key_record("bad-key", None);
        let ciphertext = aead
            .encrypt(b"sk-ant-\nnewline", record.id.as_bytes())
            .expect("encrypt");
        let record = UpstreamRecord {
            api_key_ciphertext: Some(ciphertext),
            ..record
        };

        assert!(matches!(
            resolve_api_key(&aead, &record),
            Err(ApiKeyResolutionError::InvalidHeaderValue)
        ));
    }

    #[test]
    fn resolve_api_key_rejects_legacy_json_string_payload() {
        let aead = AeadService::from_master_key([19; 32]);
        let record = api_key_record("legacy-json", None);
        let legacy = serde_json::to_vec("sk-ant-legacy-id").expect("json encodes");
        let ciphertext = aead
            .encrypt(&legacy, record.id.as_bytes())
            .expect("encrypt");
        let record = UpstreamRecord {
            api_key_ciphertext: Some(ciphertext),
            ..record
        };
        assert!(matches!(
            resolve_api_key(&aead, &record),
            Err(ApiKeyResolutionError::LegacyEncoding)
        ));
    }

    #[test]
    fn resolve_api_key_reports_missing_ciphertext() {
        let aead = AeadService::from_master_key([19; 32]);
        let record = api_key_record("no-credential", None);

        assert!(matches!(
            resolve_api_key(&aead, &record),
            Err(ApiKeyResolutionError::Missing)
        ));
    }
}
