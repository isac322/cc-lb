//! Shared credential validation for stored [`UpstreamRecord`] entries.
//!
//! Used by both [`cc_lb_server`]'s `DynamicView` builder and [`cc_lb_admin`]'s
//! split-mode DB status fallback so an upstream missing credentials or with
//! undecryptable credentials reports the exact same `error` state across modes.

use cc_lb_aead::AeadService;
use cc_lb_storage_api::{UpstreamKind, UpstreamRecord};
use cc_lb_vertex_auth::{parse_credential, vertex_credential_aad};

use crate::anthropic_api_key::credential::{ApiKeyResolutionError, resolve_api_key};

/// Validates that `upstream` has a well-formed, decryptable credential for its kind.
pub fn validate_upstream(upstream: &UpstreamRecord, aead: &AeadService) -> Result<(), String> {
    match upstream.kind {
        UpstreamKind::AnthropicApiKey => {
            if let Err(reason) = resolve_api_key(aead, upstream) {
                return Err(match reason {
                    ApiKeyResolutionError::Missing => {
                        "anthropic api-key upstream missing api_key_ciphertext".to_owned()
                    }
                    ApiKeyResolutionError::NotDecryptable => {
                        "anthropic api-key upstream credential is not decryptable".to_owned()
                    }
                    ApiKeyResolutionError::NotUtf8 => {
                        "anthropic api-key upstream credential is not valid utf-8".to_owned()
                    }
                    ApiKeyResolutionError::InvalidHeaderValue => {
                        "anthropic api-key upstream credential is not a valid header value"
                            .to_owned()
                    }
                    ApiKeyResolutionError::LegacyEncoding => {
                        "anthropic api-key upstream credential uses a retired encoding; re-set api_key_value for this upstream"
                            .to_owned()
                    }
                });
            }
        }
        UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey => {
            if let Err(reason) = resolve_api_key(aead, upstream) {
                let provider = match upstream.kind {
                    UpstreamKind::OpenaiApiKey => "openai",
                    UpstreamKind::GeminiApiKey => "gemini",
                    _ => unreachable!("provider arm restricts upstream kind"),
                };
                return Err(match reason {
                    ApiKeyResolutionError::Missing => {
                        format!("{provider} api-key upstream missing api_key_ciphertext")
                    }
                    ApiKeyResolutionError::NotDecryptable => {
                        format!("{provider} api-key upstream credential is not decryptable")
                    }
                    ApiKeyResolutionError::NotUtf8 => {
                        format!("{provider} api-key upstream credential is not valid utf-8")
                    }
                    ApiKeyResolutionError::InvalidHeaderValue => {
                        format!(
                            "{provider} api-key upstream credential is not a valid header value"
                        )
                    }
                    ApiKeyResolutionError::LegacyEncoding => {
                        format!(
                            "{provider} api-key upstream credential uses a retired encoding; re-set api_key_value for this upstream"
                        )
                    }
                });
            }
        }
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
            let provider = match upstream.kind {
                UpstreamKind::AnthropicOauth => "anthropic",
                UpstreamKind::OpenaiOauth => "openai",
                UpstreamKind::GeminiOauth => "gemini",
                _ => unreachable!("oauth arm restricts upstream kind"),
            };
            let ciphertext = upstream
                .oauth_credentials
                .as_ref()
                .ok_or_else(|| format!("{provider} oauth upstream missing oauth credentials"))?
                .ciphertext();
            if ciphertext.len() < 29 {
                return Err(format!(
                    "{provider} oauth credentials ciphertext is corrupt"
                ));
            }
        }
        UpstreamKind::VertexApiKey => {
            resolve_api_key(aead, upstream).map_err(|reason| {
                format!(
                    "vertex api-key upstream credential invalid: {}",
                    match reason {
                        ApiKeyResolutionError::Missing => "missing",
                        ApiKeyResolutionError::NotDecryptable => "not_decryptable",
                        ApiKeyResolutionError::NotUtf8 => "not_utf8",
                        ApiKeyResolutionError::InvalidHeaderValue => "invalid_header",
                        ApiKeyResolutionError::LegacyEncoding => "legacy_encoding",
                    }
                )
            })?;
        }
        UpstreamKind::VertexCredential => {
            let ciphertext = upstream.api_key_ciphertext.as_deref().ok_or_else(|| {
                "vertex credential upstream missing credential ciphertext".to_owned()
            })?;
            let plaintext = aead
                .decrypt(ciphertext, &vertex_credential_aad(&upstream.id.to_string()))
                .map_err(|_| {
                    "vertex credential upstream credential is not decryptable".to_owned()
                })?;
            parse_credential(&plaintext).map(|_| ()).map_err(|error| {
                format!("vertex credential upstream credential invalid: {error}")
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn rejects_unconfigured_api_key_and_oauth_upstreams() {
        let aead = AeadService::from_master_key([3; 32]);
        let api_key = UpstreamRecord {
            id: Uuid::new_v4(),
            name: "missing-key".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            ..UpstreamRecord::default()
        };
        assert_eq!(
            validate_upstream(&api_key, &aead).unwrap_err(),
            "anthropic api-key upstream missing api_key_ciphertext"
        );

        let oauth = UpstreamRecord {
            id: Uuid::new_v4(),
            name: "missing-oauth".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            ..UpstreamRecord::default()
        };
        assert_eq!(
            validate_upstream(&oauth, &aead).unwrap_err(),
            "anthropic oauth upstream missing oauth credentials"
        );
    }

    #[test]
    fn accepts_encrypted_api_key_upstream() {
        let aead = AeadService::from_master_key([3; 32]);
        let id = Uuid::new_v4();
        let ciphertext = aead.encrypt(b"sk-ant-valid", id.as_bytes()).unwrap();
        let upstream = UpstreamRecord {
            id,
            name: "valid-key".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            api_key_ciphertext: Some(ciphertext),
            ..UpstreamRecord::default()
        };
        assert!(validate_upstream(&upstream, &aead).is_ok());
    }
}
