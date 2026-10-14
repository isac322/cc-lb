//! Server-side normalization of an operator-pasted API key.

use std::fmt;

use zeroize::Zeroizing;

/// Longest accepted key, in bytes.
pub const MAX_API_KEY_BYTES: usize = 512;

/// An API key ready to probe.
///
/// A pasted key comes from [`normalize_api_key`]: outer ASCII trim, then
/// non-empty, printable ASCII (`0x21..=0x7E`), and at most
/// [`MAX_API_KEY_BYTES`] long. A stored key comes from
/// [`resolve_stored_api_key`](super::credential::resolve_stored_api_key)
/// byte-identical to what the signer sends, so a key stored before those
/// input rules existed is probed as live traffic uses it.
///
/// The buffer is zeroed on drop and `Debug` never prints the key.
#[derive(Clone, PartialEq, Eq)]
pub struct NormalizedApiKey(Zeroizing<String>);

impl NormalizedApiKey {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Wraps a stored key that already passed `resolve_api_key`.
    pub(super) fn stored(key: Zeroizing<String>) -> Self {
        Self(key)
    }
}

impl fmt::Debug for NormalizedApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NormalizedApiKey([REDACTED])")
    }
}

/// Why a pasted key was refused before any probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeyInputError {
    #[error("api key is missing")]
    Missing,
    #[error("api key contains characters outside printable ASCII")]
    InvalidCharacters,
    #[error("api key is longer than 512 bytes")]
    TooLong,
}

/// Trims outer ASCII whitespace, then rejects an empty key, any byte outside
/// `0x21..=0x7E`, and keys longer than [`MAX_API_KEY_BYTES`], in that order.
pub fn normalize_api_key(raw: &str) -> Result<NormalizedApiKey, KeyInputError> {
    let trimmed = raw.trim_ascii();
    if trimmed.is_empty() {
        return Err(KeyInputError::Missing);
    }
    if !trimmed.bytes().all(|byte| (0x21..=0x7E).contains(&byte)) {
        return Err(KeyInputError::InvalidCharacters);
    }
    if trimmed.len() > MAX_API_KEY_BYTES {
        return Err(KeyInputError::TooLong);
    }
    Ok(NormalizedApiKey(Zeroizing::new(trimmed.to_owned())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_outer_ascii_whitespace() {
        let key = normalize_api_key("  \tsk-ant-api03-abc\r\n").map(|k| k.as_str().to_owned());
        assert_eq!(key, Ok("sk-ant-api03-abc".to_owned()));
    }

    #[test]
    fn rejects_in_documented_order() {
        assert_eq!(normalize_api_key(" \n "), Err(KeyInputError::Missing));
        assert_eq!(
            normalize_api_key("sk-ant api03"),
            Err(KeyInputError::InvalidCharacters)
        );
        assert_eq!(
            normalize_api_key("sk-ant-é"),
            Err(KeyInputError::InvalidCharacters)
        );
        assert_eq!(
            normalize_api_key(&"a".repeat(MAX_API_KEY_BYTES + 1)),
            Err(KeyInputError::TooLong)
        );
        assert!(normalize_api_key(&"a".repeat(MAX_API_KEY_BYTES)).is_ok());
        // Invalid characters win over length.
        assert_eq!(
            normalize_api_key(&format!("{} x", "a".repeat(600))),
            Err(KeyInputError::InvalidCharacters)
        );
    }

    /// SEC-09: Debug never prints the key.
    #[test]
    fn sec_09_debug_redacts_normalized_key() {
        let key = normalize_api_key("sk-ant-api03-SECRETVALUE1234").ok();
        let rendered = format!("{key:?}");
        assert!(!rendered.contains("SECRETVALUE"), "{rendered}");
        assert!(rendered.contains("[REDACTED]"), "{rendered}");
    }
}
