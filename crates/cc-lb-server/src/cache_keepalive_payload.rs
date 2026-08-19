use cc_lb_aead::{AeadError, AeadService};
use cc_lb_engine::cache_keepalive::PersistedRequestSnapshot;
use thiserror::Error;
use uuid::Uuid;

use crate::scheduler_dispatch::cache_keepalive_payload_aad;

const MAGIC: &[u8; 8] = b"CCLBCKA2";
const CODEC_RAW_JSON: u8 = 0;
const CODEC_ZSTD_JSON: u8 = 1;
const HEADER_LEN: usize = MAGIC.len() + 1 + size_of::<u64>() + size_of::<u64>();
const ZSTD_LEVEL: i32 = 1;

pub(crate) struct DecodedCacheKeepalivePayload {
    pub snapshot: PersistedRequestSnapshot,
    pub needs_rewrite: bool,
}

#[derive(Debug, Error)]
pub(crate) enum CacheKeepalivePayloadError {
    #[error("cache keepalive payload encryption failed: {0}")]
    Aead(#[from] AeadError),
    #[error("cache keepalive payload compression failed: {0}")]
    Compression(#[from] std::io::Error),
    #[error("cache keepalive payload header is invalid")]
    InvalidHeader,
    #[error("cache keepalive payload generation is invalid")]
    InvalidGeneration,
    #[error("cache keepalive payload length is invalid")]
    InvalidLength,
    #[error("cache keepalive payload codec {0} is unsupported")]
    UnsupportedCodec(u8),
    #[error("cache keepalive payload JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
}

pub(crate) fn encrypt_cache_keepalive_payload(
    aead: &AeadService,
    principal_id: &str,
    session_key_hash: &str,
    upstream_id: Uuid,
    payload_generation: u64,
    snapshot: &PersistedRequestSnapshot,
) -> Result<Vec<u8>, CacheKeepalivePayloadError> {
    let encoded = serde_json::to_vec(snapshot)?;
    let compressed = zstd::bulk::compress(&encoded, ZSTD_LEVEL)?;
    let (codec, plaintext) = if compressed.len() < encoded.len() {
        (CODEC_ZSTD_JSON, compressed.as_slice())
    } else {
        (CODEC_RAW_JSON, encoded.as_slice())
    };
    let encoded_len =
        u64::try_from(encoded.len()).map_err(|_| CacheKeepalivePayloadError::InvalidLength)?;
    let aad = v2_aad(
        principal_id,
        session_key_hash,
        upstream_id,
        payload_generation,
        codec,
        encoded_len,
    );
    let ciphertext = aead.encrypt(plaintext, &aad)?;
    let mut payload = Vec::with_capacity(HEADER_LEN + ciphertext.len());
    payload.extend_from_slice(MAGIC);
    payload.push(codec);
    payload.extend_from_slice(&payload_generation.to_be_bytes());
    payload.extend_from_slice(&encoded_len.to_be_bytes());
    payload.extend_from_slice(&ciphertext);
    Ok(payload)
}

pub(crate) fn decrypt_cache_keepalive_payload(
    aead: &AeadService,
    principal_id: &str,
    session_key_hash: &str,
    upstream_id: Uuid,
    generation: u64,
    refresh_count: u32,
    payload: &[u8],
) -> Result<DecodedCacheKeepalivePayload, CacheKeepalivePayloadError> {
    if !payload.starts_with(MAGIC) {
        let plaintext = aead.decrypt(
            payload,
            &cache_keepalive_payload_aad(
                principal_id,
                session_key_hash,
                upstream_id,
                generation,
            ),
        )?;
        return Ok(DecodedCacheKeepalivePayload {
            snapshot: serde_json::from_slice(&plaintext)?,
            needs_rewrite: true,
        });
    }
    if payload.len() <= HEADER_LEN {
        return Err(CacheKeepalivePayloadError::InvalidHeader);
    }
    let codec = payload[MAGIC.len()];
    let generation_start = MAGIC.len() + 1;
    let length_start = generation_start + size_of::<u64>();
    let payload_generation = u64::from_be_bytes(
        payload[generation_start..length_start]
            .try_into()
            .map_err(|_| CacheKeepalivePayloadError::InvalidHeader)?,
    );
    let encoded_len = u64::from_be_bytes(
        payload[length_start..HEADER_LEN]
            .try_into()
            .map_err(|_| CacheKeepalivePayloadError::InvalidHeader)?,
    );
    if payload_generation != base_payload_generation(generation, refresh_count)? {
        return Err(CacheKeepalivePayloadError::InvalidGeneration);
    }
    let aad = v2_aad(
        principal_id,
        session_key_hash,
        upstream_id,
        payload_generation,
        codec,
        encoded_len,
    );
    let plaintext = aead.decrypt(&payload[HEADER_LEN..], &aad)?;
    let expected_len =
        usize::try_from(encoded_len).map_err(|_| CacheKeepalivePayloadError::InvalidLength)?;
    let encoded = match codec {
        CODEC_RAW_JSON => {
            if plaintext.len() != expected_len {
                return Err(CacheKeepalivePayloadError::InvalidLength);
            }
            plaintext
        }
        CODEC_ZSTD_JSON => {
            let decoded = zstd::bulk::decompress(&plaintext, expected_len)?;
            if decoded.len() != expected_len {
                return Err(CacheKeepalivePayloadError::InvalidLength);
            }
            decoded
        }
        other => return Err(CacheKeepalivePayloadError::UnsupportedCodec(other)),
    };
    Ok(DecodedCacheKeepalivePayload {
        snapshot: serde_json::from_slice(&encoded)?,
        needs_rewrite: false,
    })
}

pub(crate) fn base_payload_generation(
    generation: u64,
    refresh_count: u32,
) -> Result<u64, CacheKeepalivePayloadError> {
    generation
        .checked_sub(u64::from(refresh_count))
        .filter(|generation| *generation != 0)
        .ok_or(CacheKeepalivePayloadError::InvalidGeneration)
}

fn v2_aad(
    principal_id: &str,
    session_key_hash: &str,
    upstream_id: Uuid,
    payload_generation: u64,
    codec: u8,
    encoded_len: u64,
) -> Vec<u8> {
    format!(
        "cache_keepalive_snapshot:v2:{principal_id}:{session_key_hash}:{upstream_id}:{payload_generation}:{codec}:{encoded_len}"
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use cc_lb_storage_api::CacheTtl;

    use super::*;

    fn snapshot(body: Vec<u8>) -> PersistedRequestSnapshot {
        PersistedRequestSnapshot {
            url: "https://api.anthropic.com/v1/messages".to_owned(),
            method: "POST".to_owned(),
            headers: Vec::new(),
            body,
            upstream_id: Uuid::from_u128(7),
            ttl: CacheTtl::Ttl5m,
        }
    }

    #[test]
    fn v2_roundtrip_compacts_large_json_body_and_survives_refresh_generations() {
        let aead = AeadService::from_master_key([7; 32]);
        let snapshot = snapshot(
            br#"{"model":"claude-test","messages":[{"role":"user","content":"repeated context repeated context repeated context"}]}"#
                .repeat(4_096),
        );
        let legacy_json = serde_json::to_vec(&snapshot).expect("legacy JSON encodes");
        let payload = encrypt_cache_keepalive_payload(
            &aead,
            "principal",
            "session",
            snapshot.upstream_id,
            4,
            &snapshot,
        )
        .expect("v2 payload encrypts");

        assert!(payload.len() < legacy_json.len() / 10);
        let decoded = decrypt_cache_keepalive_payload(
            &aead,
            "principal",
            "session",
            snapshot.upstream_id,
            7,
            3,
            &payload,
        )
        .expect("v2 payload decrypts after refresh generations advance");
        assert_eq!(decoded.snapshot, snapshot);
        assert!(!decoded.needs_rewrite);
    }

    #[test]
    fn legacy_json_payload_remains_readable_and_requests_one_rewrite() {
        let aead = AeadService::from_master_key([9; 32]);
        let snapshot = snapshot(br#"{"model":"claude-test","messages":[]}"#.to_vec());
        let legacy = aead
            .encrypt(
                &serde_json::to_vec(&snapshot).expect("legacy JSON encodes"),
                &cache_keepalive_payload_aad(
                    "principal",
                    "session",
                    snapshot.upstream_id,
                    6,
                ),
            )
            .expect("legacy payload encrypts");

        let decoded = decrypt_cache_keepalive_payload(
            &aead,
            "principal",
            "session",
            snapshot.upstream_id,
            6,
            2,
            &legacy,
        )
        .expect("legacy payload decrypts");
        assert_eq!(decoded.snapshot, snapshot);
        assert!(decoded.needs_rewrite);
    }

    #[test]
    fn v2_payload_rejects_replay_from_an_older_real_request() {
        let aead = AeadService::from_master_key([11; 32]);
        let snapshot = snapshot(br#"{"model":"claude-test","messages":[]}"#.to_vec());
        let payload = encrypt_cache_keepalive_payload(
            &aead,
            "principal",
            "session",
            snapshot.upstream_id,
            4,
            &snapshot,
        )
        .expect("v2 payload encrypts");

        assert!(matches!(
            decrypt_cache_keepalive_payload(
                &aead,
                "principal",
                "session",
                snapshot.upstream_id,
                8,
                1,
                &payload,
            ),
            Err(CacheKeepalivePayloadError::InvalidGeneration)
        ));
    }
}
