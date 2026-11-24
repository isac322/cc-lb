use std::sync::Arc;

use cc_lb_aead::AeadService;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AeadKeyError {
    #[error("storage master key env {env} is missing")]
    Missing { env: String },
    #[error("storage master key must be 32 bytes encoded as 64 hex characters")]
    Invalid,
}

/// Decodes a 64-character hex string into a 32-byte AES master key.
pub fn decode_hex_key(value: &str) -> Result<[u8; 32], AeadKeyError> {
    if value.len() != 64 {
        return Err(AeadKeyError::Invalid);
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        key[index] = (high << 4) | low;
    }
    Ok(key)
}

/// Loads and validates an [`AeadService`] master key from the configured environment variable.
pub fn load_aead_from_env(env_var: &str) -> Result<Arc<AeadService>, AeadKeyError> {
    let key_hex = std::env::var(env_var).map_err(|_| AeadKeyError::Missing {
        env: env_var.to_owned(),
    })?;
    let key = decode_hex_key(&key_hex)?;
    Ok(Arc::new(AeadService::from_master_key(key)))
}

fn hex_nibble(byte: u8) -> Result<u8, AeadKeyError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(AeadKeyError::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_hex_key_accepts_64_hex_chars_and_rejects_malformed() {
        let valid = "0123456789abcdef0123456789ABCDEF000000000000000000000000000000ff";
        let key = decode_hex_key(valid).expect("valid 64-char hex key");
        assert_eq!(key[0], 0x01);
        assert_eq!(key[15], 0xef);
        assert_eq!(key[31], 0xff);

        assert!(matches!(decode_hex_key(""), Err(AeadKeyError::Invalid)));
        assert!(matches!(
            decode_hex_key(&valid[..63]),
            Err(AeadKeyError::Invalid)
        ));
        let non_hex = format!("{}zz", &valid[..62]);
        assert!(matches!(
            decode_hex_key(&non_hex),
            Err(AeadKeyError::Invalid)
        ));
    }
}
