use chacha20poly1305::aead::{Aead, Generate, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use zeroize::Zeroize;

use crate::error::{AeadError, AeadResult};

const MASTER_KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

#[derive(Clone)]
pub struct AeadService {
    cipher: ChaCha20Poly1305,
}

impl AeadService {
    pub fn from_master_key(mut master_key: [u8; MASTER_KEY_LEN]) -> Self {
        let cipher = ChaCha20Poly1305::new(<&Key>::from(&master_key));
        master_key.zeroize();
        Self { cipher }
    }

    pub fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> AeadResult<Vec<u8>> {
        let nonce = Nonce::generate();
        let payload = Payload {
            msg: plaintext,
            aad,
        };
        let ciphertext = self
            .cipher
            .encrypt(&nonce, payload)
            .map_err(|_| AeadError::EncryptionFailed)?;

        let mut blob = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&ciphertext);
        Ok(blob)
    }

    pub fn decrypt(&self, blob: &[u8], aad: &[u8]) -> AeadResult<Vec<u8>> {
        if blob.len() < NONCE_LEN {
            return Err(AeadError::CiphertextTooShort);
        }

        let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
        let nonce = <&Nonce>::try_from(nonce_bytes).map_err(|_| AeadError::CiphertextTooShort)?;
        let payload = Payload {
            msg: ciphertext,
            aad,
        };

        self.cipher
            .decrypt(nonce, payload)
            .map_err(|_| AeadError::DecryptionFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_encrypts_and_decrypts_with_aad() {
        let service = AeadService::from_master_key([7; MASTER_KEY_LEN]);
        let plaintext = b"access-secret-token";
        let aad = b"alice";

        let blob = service.encrypt(plaintext, aad).expect("encrypts");

        assert!(blob.len() > NONCE_LEN);
        assert_eq!(service.decrypt(&blob, aad).expect("decrypts"), plaintext);
    }

    #[test]
    fn tampered_ciphertext_is_rejected() {
        let service = AeadService::from_master_key([9; MASTER_KEY_LEN]);
        let mut blob = service
            .encrypt(b"refresh-secret-token", b"alice")
            .expect("encrypts");
        let last = blob.last_mut().expect("nonce plus tag exists");
        *last ^= 0x01;

        assert_eq!(
            service.decrypt(&blob, b"alice"),
            Err(AeadError::DecryptionFailed)
        );
    }

    #[test]
    fn wrong_aad_is_rejected() {
        let service = AeadService::from_master_key([11; MASTER_KEY_LEN]);
        let blob = service
            .encrypt(b"bound-to-alice", b"alice")
            .expect("encrypts");

        assert_eq!(
            service.decrypt(&blob, b"bob"),
            Err(AeadError::DecryptionFailed)
        );
    }

    #[test]
    fn short_blob_is_rejected_before_decryption() {
        let service = AeadService::from_master_key([13; MASTER_KEY_LEN]);

        for len in 0..NONCE_LEN {
            assert_eq!(
                service.decrypt(&vec![0_u8; len], b"alice"),
                Err(AeadError::CiphertextTooShort)
            );
        }
    }

    #[test]
    fn ciphertext_does_not_contain_plaintext_marker() {
        let service = AeadService::from_master_key([15; MASTER_KEY_LEN]);
        let plaintext = b"plain-secret-marker-that-must-not-appear";
        let blob = service.encrypt(plaintext, b"alice").expect("encrypts");

        assert!(!contains_bytes(&blob, plaintext));
    }

    #[test]
    fn decrypts_current_nonce_ciphertext_format() {
        let master_key = [17; MASTER_KEY_LEN];
        let service = AeadService::from_master_key(master_key);
        let plaintext = b"legacy-oauth-json";
        let aad = b"alice";

        let blob = encrypt_like_current_format(master_key, plaintext, aad);

        assert_eq!(&blob[..NONCE_LEN], &[23; NONCE_LEN]);
        assert_eq!(service.decrypt(&blob, aad).expect("decrypts"), plaintext);
    }

    #[test]
    fn encrypts_blob_readable_by_current_format() {
        let master_key = [19; MASTER_KEY_LEN];
        let service = AeadService::from_master_key(master_key);
        let plaintext = b"new-service-oauth-json";
        let aad = b"alice";

        let blob = service.encrypt(plaintext, aad).expect("encrypts");

        assert_eq!(
            decrypt_like_current_format(master_key, &blob, aad),
            plaintext
        );
    }

    fn encrypt_like_current_format(
        master_key: [u8; MASTER_KEY_LEN],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Vec<u8> {
        let cipher = ChaCha20Poly1305::new(<&Key>::from(&master_key));
        let nonce = <&Nonce>::from(&[23; NONCE_LEN]);
        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .expect("current format encrypts");

        let mut blob = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        blob.extend_from_slice(nonce.as_slice());
        blob.extend_from_slice(&ciphertext);
        blob
    }

    fn decrypt_like_current_format(
        master_key: [u8; MASTER_KEY_LEN],
        blob: &[u8],
        aad: &[u8],
    ) -> Vec<u8> {
        let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
        let cipher = ChaCha20Poly1305::new(<&Key>::from(&master_key));
        cipher
            .decrypt(
                <&Nonce>::try_from(nonce_bytes).expect("blob carries a 12-byte nonce prefix"),
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .expect("current format decrypts")
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
