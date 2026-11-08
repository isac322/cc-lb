use std::borrow::Cow;

use cc_lb_aead::{AeadError, AeadService};
use cc_lb_engine::cache_keepalive::PersistedRequestSnapshot;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::scheduler_dispatch::cache_keepalive_payload_aad;

const MAGIC: &[u8; 8] = b"CCLBCKA2";
const CODEC_RAW_JSON: u8 = 0;
const CODEC_ZSTD_JSON: u8 = 1;
const CODEC_RAW_FRAMED: u8 = 2;
const CODEC_ZSTD_FRAMED: u8 = 3;
const SNAPSHOT_FRAME_MAGIC: &[u8; 8] = b"CCLBPS01";
const SNAPSHOT_FRAME_HEADER_LEN: usize =
    SNAPSHOT_FRAME_MAGIC.len() + size_of::<u64>() + size_of::<u64>();
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
    #[error("cache keepalive payload frame is invalid")]
    InvalidFrame,
    #[error("cache keepalive payload generation is invalid")]
    InvalidGeneration,
    #[error("cache keepalive payload length is invalid")]
    InvalidLength,
    #[error("cache keepalive payload codec {0} is unsupported")]
    UnsupportedCodec(u8),
    #[error("cache keepalive payload JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Deserialize, Serialize)]
struct SnapshotFrameHeader {
    name: String,
    value: Vec<u8>,
}

#[derive(Deserialize, Serialize)]
struct SnapshotFrameMetadata {
    url: String,
    method: String,
    headers: Vec<SnapshotFrameHeader>,
    upstream_id: Uuid,
    ttl: cc_lb_storage_api::CacheTtl,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix_policy_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix_schema_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    surface: Option<String>,
}

fn encode_snapshot_frame(
    snapshot: &PersistedRequestSnapshot,
) -> Result<Vec<u8>, CacheKeepalivePayloadError> {
    let metadata = serde_json::to_vec(&SnapshotFrameMetadata {
        url: snapshot.url.clone(),
        method: snapshot.method.clone(),
        headers: snapshot
            .headers
            .iter()
            .map(|header| SnapshotFrameHeader {
                name: header.name.clone(),
                value: header.value.clone(),
            })
            .collect(),
        upstream_id: snapshot.upstream_id,
        ttl: snapshot.ttl,
        prefix_fingerprint: snapshot.prefix_fingerprint.clone(),
        prefix_hash: snapshot.prefix_hash.clone(),
        prefix_policy_version: snapshot.prefix_policy_version.clone(),
        prefix_schema_version: snapshot.prefix_schema_version.clone(),
        surface: snapshot.surface.clone(),
    })?;
    let metadata_len =
        u64::try_from(metadata.len()).map_err(|_| CacheKeepalivePayloadError::InvalidLength)?;
    let body_len = u64::try_from(snapshot.body.len())
        .map_err(|_| CacheKeepalivePayloadError::InvalidLength)?;
    let mut frame = Vec::with_capacity(
        SNAPSHOT_FRAME_HEADER_LEN
            .saturating_add(metadata.len())
            .saturating_add(snapshot.body.len()),
    );
    frame.extend_from_slice(SNAPSHOT_FRAME_MAGIC);
    frame.extend_from_slice(&metadata_len.to_be_bytes());
    frame.extend_from_slice(&body_len.to_be_bytes());
    frame.extend_from_slice(&metadata);
    frame.extend_from_slice(&snapshot.body);
    Ok(frame)
}

fn decode_snapshot_frame(
    frame: &[u8],
) -> Result<PersistedRequestSnapshot, CacheKeepalivePayloadError> {
    if frame.len() < SNAPSHOT_FRAME_HEADER_LEN || !frame.starts_with(SNAPSHOT_FRAME_MAGIC) {
        return Err(CacheKeepalivePayloadError::InvalidFrame);
    }
    let metadata_start = SNAPSHOT_FRAME_MAGIC.len();
    let body_len_start = metadata_start + size_of::<u64>();
    let payload_start = body_len_start + size_of::<u64>();
    let metadata_len = u64::from_be_bytes(
        frame[metadata_start..body_len_start]
            .try_into()
            .map_err(|_| CacheKeepalivePayloadError::InvalidFrame)?,
    );
    let body_len = u64::from_be_bytes(
        frame[body_len_start..payload_start]
            .try_into()
            .map_err(|_| CacheKeepalivePayloadError::InvalidFrame)?,
    );
    let metadata_len =
        usize::try_from(metadata_len).map_err(|_| CacheKeepalivePayloadError::InvalidLength)?;
    let body_len =
        usize::try_from(body_len).map_err(|_| CacheKeepalivePayloadError::InvalidLength)?;
    let body_start = payload_start
        .checked_add(metadata_len)
        .ok_or(CacheKeepalivePayloadError::InvalidLength)?;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or(CacheKeepalivePayloadError::InvalidLength)?;
    if body_end != frame.len() {
        return Err(CacheKeepalivePayloadError::InvalidLength);
    }
    let metadata: SnapshotFrameMetadata =
        serde_json::from_slice(&frame[payload_start..body_start])?;
    let snapshot = PersistedRequestSnapshot {
        url: metadata.url,
        method: metadata.method,
        // PersistedHeader is a public struct inside a private module and is
        // not re-exported from the engine cache_keepalive module. Convert only
        // the framed metadata through its existing serde representation rather
        // than widening the public API or parsing the body.
        headers: serde_json::from_value(serde_json::to_value(metadata.headers)?)?,
        body: frame[body_start..body_end].to_vec(),
        upstream_id: metadata.upstream_id,
        ttl: metadata.ttl,
        prefix_fingerprint: metadata.prefix_fingerprint,
        prefix_hash: metadata.prefix_hash,
        prefix_policy_version: metadata.prefix_policy_version,
        prefix_schema_version: metadata.prefix_schema_version,
        surface: metadata.surface,
    };
    Ok(snapshot)
}

pub(crate) fn encrypt_cache_keepalive_payload(
    aead: &AeadService,
    principal_id: &str,
    session_key_hash: &str,
    upstream_id: Uuid,
    payload_generation: u64,
    snapshot: &PersistedRequestSnapshot,
) -> Result<Vec<u8>, CacheKeepalivePayloadError> {
    let encoded = encode_snapshot_frame(snapshot)?;
    let compressed = zstd::bulk::compress(&encoded, ZSTD_LEVEL)?;
    let (codec, plaintext) = if compressed.len() < encoded.len() {
        (CODEC_ZSTD_FRAMED, compressed.as_slice())
    } else {
        (CODEC_RAW_FRAMED, encoded.as_slice())
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
            &cache_keepalive_payload_aad(principal_id, session_key_hash, upstream_id, generation),
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
    let encoded: Cow<'_, [u8]> = match codec {
        CODEC_RAW_JSON | CODEC_RAW_FRAMED => {
            if plaintext.len() != expected_len {
                return Err(CacheKeepalivePayloadError::InvalidLength);
            }
            Cow::Borrowed(plaintext.as_slice())
        }
        CODEC_ZSTD_JSON | CODEC_ZSTD_FRAMED => {
            let decoded = zstd::bulk::decompress(&plaintext, expected_len)?;
            if decoded.len() != expected_len {
                return Err(CacheKeepalivePayloadError::InvalidLength);
            }
            Cow::Owned(decoded)
        }
        other => return Err(CacheKeepalivePayloadError::UnsupportedCodec(other)),
    };
    let snapshot = match codec {
        CODEC_RAW_JSON | CODEC_ZSTD_JSON => serde_json::from_slice(encoded.as_ref())?,
        CODEC_RAW_FRAMED | CODEC_ZSTD_FRAMED => decode_snapshot_frame(encoded.as_ref())?,
        other => return Err(CacheKeepalivePayloadError::UnsupportedCodec(other)),
    };
    Ok(DecodedCacheKeepalivePayload {
        snapshot,
        needs_rewrite: matches!(codec, CODEC_RAW_JSON | CODEC_ZSTD_JSON),
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
            prefix_fingerprint: None,
            prefix_hash: None,
            prefix_policy_version: None,
            prefix_schema_version: None,
        }
    }

    fn encrypt_v2_json_payload(
        aead: &AeadService,
        principal_id: &str,
        session_key_hash: &str,
        snapshot: &PersistedRequestSnapshot,
        payload_generation: u64,
        codec: u8,
    ) -> Result<Vec<u8>, CacheKeepalivePayloadError> {
        let encoded = serde_json::to_vec(snapshot)?;
        let plaintext = match codec {
            CODEC_RAW_JSON => encoded.clone(),
            CODEC_ZSTD_JSON => zstd::bulk::compress(&encoded, ZSTD_LEVEL)?,
            other => return Err(CacheKeepalivePayloadError::UnsupportedCodec(other)),
        };
        let encoded_len =
            u64::try_from(encoded.len()).map_err(|_| CacheKeepalivePayloadError::InvalidLength)?;
        let aad = v2_aad(
            principal_id,
            session_key_hash,
            snapshot.upstream_id,
            payload_generation,
            codec,
            encoded_len,
        );
        let ciphertext = aead.encrypt(&plaintext, &aad)?;
        let mut payload = Vec::with_capacity(HEADER_LEN + ciphertext.len());
        payload.extend_from_slice(MAGIC);
        payload.push(codec);
        payload.extend_from_slice(&payload_generation.to_be_bytes());
        payload.extend_from_slice(&encoded_len.to_be_bytes());
        payload.extend_from_slice(&ciphertext);
        Ok(payload)
    }

    fn encrypt_framed_test_payload(
        aead: &AeadService,
        snapshot: &PersistedRequestSnapshot,
        payload_generation: u64,
        codec: u8,
        frame: &[u8],
    ) -> Result<Vec<u8>, CacheKeepalivePayloadError> {
        let encoded_len =
            u64::try_from(frame.len()).map_err(|_| CacheKeepalivePayloadError::InvalidLength)?;
        let plaintext = match codec {
            CODEC_RAW_FRAMED => Cow::Borrowed(frame),
            CODEC_ZSTD_FRAMED => Cow::Owned(zstd::bulk::compress(frame, ZSTD_LEVEL)?),
            other => return Err(CacheKeepalivePayloadError::UnsupportedCodec(other)),
        };
        let ciphertext = aead.encrypt(
            plaintext.as_ref(),
            &v2_aad(
                "principal",
                "session",
                snapshot.upstream_id,
                payload_generation,
                codec,
                encoded_len,
            ),
        )?;
        let mut payload = Vec::with_capacity(HEADER_LEN + ciphertext.len());
        payload.extend_from_slice(MAGIC);
        payload.push(codec);
        payload.extend_from_slice(&payload_generation.to_be_bytes());
        payload.extend_from_slice(&encoded_len.to_be_bytes());
        payload.extend_from_slice(&ciphertext);
        Ok(payload)
    }

    #[test]
    fn raw_framed_payload_preserves_body_headers_and_refresh_generations()
    -> Result<(), Box<dyn std::error::Error>> {
        let aead = AeadService::from_master_key([19; 32]);
        let mut snapshot = snapshot(b" \n{\"model\":\"claude-test\"}\t\0\xff".to_vec());
        snapshot.headers = serde_json::from_value(serde_json::json!([
            {"name": "x-repeated", "value": [102, 105, 114, 115, 116]},
            {"name": "x-repeated", "value": [115, 101, 99, 111, 110, 100]},
            {"name": "x-binary", "value": [128, 255]}
        ]))?;
        let frame = encode_snapshot_frame(&snapshot)?;
        let payload = encrypt_framed_test_payload(&aead, &snapshot, 5, CODEC_RAW_FRAMED, &frame)?;
        assert_eq!(payload.get(MAGIC.len()), Some(&CODEC_RAW_FRAMED));

        for (generation, refresh_count) in [(5, 0), (8, 3)] {
            let decoded = decrypt_cache_keepalive_payload(
                &aead,
                "principal",
                "session",
                snapshot.upstream_id,
                generation,
                refresh_count,
                &payload,
            )?;
            assert_eq!(decoded.snapshot, snapshot);
            assert!(!decoded.needs_rewrite);
        }
        Ok(())
    }

    #[test]
    fn framed_payload_rejects_mismatched_lengths_overflow_and_trailing_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let aead = AeadService::from_master_key([23; 32]);
        let snapshot = snapshot(br#"{"model":"claude-test","messages":[]}"#.to_vec());
        let frame = encode_snapshot_frame(&snapshot)?;
        let metadata_start = SNAPSHOT_FRAME_MAGIC.len();
        let body_len_start = metadata_start + size_of::<u64>();
        let metadata_len = u64::from_be_bytes(
            frame
                .get(metadata_start..body_len_start)
                .ok_or("frame metadata length is missing")?
                .try_into()?,
        );
        let body_len = u64::try_from(snapshot.body.len())?;
        let mut malformed_frames = Vec::new();
        for (label, start, length) in [
            ("metadata too long", metadata_start, metadata_len + 1),
            ("metadata too short", metadata_start, metadata_len - 1),
            ("body too long", body_len_start, body_len + 1),
            ("body too short", body_len_start, body_len - 1),
            ("metadata length overflow", metadata_start, u64::MAX),
            ("body length overflow", body_len_start, u64::MAX),
        ] {
            let mut malformed = frame.clone();
            malformed
                .get_mut(start..start + size_of::<u64>())
                .ok_or("frame length field is missing")?
                .copy_from_slice(&length.to_be_bytes());
            malformed_frames.push((label, malformed));
        }
        let mut trailing = frame.clone();
        trailing.push(0);
        malformed_frames.push(("trailing byte", trailing));
        let mut truncated = frame;
        truncated.pop();
        malformed_frames.push(("truncated body", truncated));

        for (label, malformed) in malformed_frames {
            for codec in [CODEC_RAW_FRAMED, CODEC_ZSTD_FRAMED] {
                let payload = encrypt_framed_test_payload(&aead, &snapshot, 5, codec, &malformed)?;
                assert!(
                    matches!(
                        decrypt_cache_keepalive_payload(
                            &aead,
                            "principal",
                            "session",
                            snapshot.upstream_id,
                            8,
                            3,
                            &payload,
                        ),
                        Err(CacheKeepalivePayloadError::InvalidLength)
                    ),
                    "{label} must reject for codec {codec}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn framed_payload_rejects_truncated_frame_and_envelope_headers()
    -> Result<(), Box<dyn std::error::Error>> {
        let aead = AeadService::from_master_key([29; 32]);
        let snapshot = snapshot(br#"{"model":"claude-test","messages":[]}"#.to_vec());
        let frame = encode_snapshot_frame(&snapshot)?;
        for header_len in 0..SNAPSHOT_FRAME_HEADER_LEN {
            let truncated = frame.get(..header_len).ok_or("frame header is missing")?;
            for codec in [CODEC_RAW_FRAMED, CODEC_ZSTD_FRAMED] {
                let payload = encrypt_framed_test_payload(&aead, &snapshot, 5, codec, truncated)?;
                assert!(
                    matches!(
                        decrypt_cache_keepalive_payload(
                            &aead,
                            "principal",
                            "session",
                            snapshot.upstream_id,
                            8,
                            3,
                            &payload,
                        ),
                        Err(CacheKeepalivePayloadError::InvalidFrame)
                    ),
                    "frame header length {header_len} must reject for codec {codec}"
                );
            }
        }
        let payload = encrypt_framed_test_payload(&aead, &snapshot, 5, CODEC_RAW_FRAMED, &frame)?;
        for header_len in MAGIC.len()..=HEADER_LEN {
            assert!(
                matches!(
                    decrypt_cache_keepalive_payload(
                        &aead,
                        "principal",
                        "session",
                        snapshot.upstream_id,
                        8,
                        3,
                        payload
                            .get(..header_len)
                            .ok_or("payload header is missing")?,
                    ),
                    Err(CacheKeepalivePayloadError::InvalidHeader)
                ),
                "envelope header length {header_len} must reject"
            );
        }
        Ok(())
    }

    #[test]
    fn v2_roundtrip_compacts_large_json_body_and_survives_refresh_generations() {
        let aead = AeadService::from_master_key([7; 32]);
        let mut snapshot = snapshot(
            br#"{"model":"claude-test","messages":[{"role":"user","content":"repeated context repeated context repeated context"}]}"#
                .repeat(4_096),
        );
        snapshot.headers = serde_json::from_value(serde_json::json!([
            {"name": "content-type", "value": [97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110]}
        ]))
        .expect("snapshot header decodes");
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

        let codec = payload[MAGIC.len()];
        assert_eq!(codec, CODEC_ZSTD_FRAMED);
        let encoded_len = u64::from_be_bytes(
            payload[MAGIC.len() + 1 + size_of::<u64>()..HEADER_LEN]
                .try_into()
                .unwrap(),
        );
        let encoded_len = usize::try_from(encoded_len).unwrap();
        let plaintext = aead
            .decrypt(
                &payload[HEADER_LEN..],
                &v2_aad(
                    "principal",
                    "session",
                    snapshot.upstream_id,
                    4,
                    codec,
                    u64::try_from(encoded_len).unwrap(),
                ),
            )
            .unwrap();
        let framed = if codec == CODEC_ZSTD_FRAMED {
            zstd::bulk::decompress(&plaintext, encoded_len).unwrap()
        } else {
            plaintext
        };
        assert_eq!(framed.len(), encoded_len);
        assert!(framed.len() < legacy_json.len());
        assert_eq!(decode_snapshot_frame(&framed).unwrap(), snapshot);

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
        assert_eq!(decoded.snapshot.headers, snapshot.headers);
        assert!(!decoded.needs_rewrite);
    }

    #[test]
    fn existing_v2_json_codecs_remain_readable_and_request_migration() {
        let aead = AeadService::from_master_key([13; 32]);
        let snapshot = snapshot(
            br#"{"model":"claude-test","messages":[{"role":"user","content":"repeat repeat repeat"}]}"#
                .repeat(64),
        );
        for codec in [CODEC_RAW_JSON, CODEC_ZSTD_JSON] {
            let payload =
                encrypt_v2_json_payload(&aead, "principal", "session", &snapshot, 5, codec)
                    .unwrap();
            let decoded = decrypt_cache_keepalive_payload(
                &aead,
                "principal",
                "session",
                snapshot.upstream_id,
                8,
                3,
                &payload,
            )
            .unwrap();
            assert_eq!(decoded.snapshot, snapshot);
            assert!(decoded.needs_rewrite);
        }
    }

    #[test]
    fn snapshot_frame_rejects_malformed_and_truncated_input() {
        let snapshot = snapshot(br#"{"model":"claude-test","messages":[]}"#.to_vec());
        let frame = encode_snapshot_frame(&snapshot).expect("snapshot frame encodes");

        let mut malformed = frame.clone();
        let Some(first) = malformed.first_mut() else {
            panic!("encoded snapshot frame is empty");
        };
        *first ^= 1;
        assert!(matches!(
            decode_snapshot_frame(&malformed),
            Err(CacheKeepalivePayloadError::InvalidFrame)
        ));

        let truncated = &frame[..frame.len() - 1];
        assert!(matches!(
            decode_snapshot_frame(truncated),
            Err(CacheKeepalivePayloadError::InvalidLength)
        ));
    }

    #[test]
    fn legacy_json_payload_remains_readable_and_requests_one_rewrite() {
        let aead = AeadService::from_master_key([9; 32]);
        let snapshot = snapshot(br#"{"model":"claude-test","messages":[]}"#.to_vec());
        let legacy = aead
            .encrypt(
                &serde_json::to_vec(&snapshot).expect("legacy JSON encodes"),
                &cache_keepalive_payload_aad("principal", "session", snapshot.upstream_id, 6),
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

    #[test]
    fn v2_payload_rejects_aad_mutation() -> Result<(), Box<dyn std::error::Error>> {
        let aead = AeadService::from_master_key([17; 32]);
        let snapshot = snapshot(br#"{"model":"claude-test","messages":[]}"#.to_vec());
        let frame = encode_snapshot_frame(&snapshot)?;
        for codec in [CODEC_RAW_FRAMED, CODEC_ZSTD_FRAMED] {
            let payload = encrypt_framed_test_payload(&aead, &snapshot, 4, codec, &frame)?;
            for (label, principal, session, upstream) in [
                (
                    "principal",
                    "other-principal",
                    "session",
                    snapshot.upstream_id,
                ),
                (
                    "session",
                    "principal",
                    "other-session",
                    snapshot.upstream_id,
                ),
                ("upstream", "principal", "session", Uuid::from_u128(8)),
            ] {
                assert!(
                    matches!(
                        decrypt_cache_keepalive_payload(
                            &aead, principal, session, upstream, 7, 3, &payload,
                        ),
                        Err(CacheKeepalivePayloadError::Aead(_))
                    ),
                    "{label} mutation must reject for codec {codec}"
                );
            }
            for (label, offset) in [
                ("codec", MAGIC.len()),
                ("encoded length", HEADER_LEN - 1),
                ("ciphertext", payload.len() - 1),
            ] {
                let mut mutated = payload.clone();
                *mutated.get_mut(offset).ok_or("mutation byte is missing")? ^= 1;
                assert!(
                    matches!(
                        decrypt_cache_keepalive_payload(
                            &aead,
                            "principal",
                            "session",
                            snapshot.upstream_id,
                            7,
                            3,
                            &mutated,
                        ),
                        Err(CacheKeepalivePayloadError::Aead(_))
                    ),
                    "{label} mutation must reject for codec {codec}"
                );
            }
            let mut mutated_generation = payload;
            let generation_start = MAGIC.len() + 1;
            mutated_generation
                .get_mut(generation_start..generation_start + size_of::<u64>())
                .ok_or("payload generation is missing")?
                .copy_from_slice(&5_u64.to_be_bytes());
            assert!(matches!(
                decrypt_cache_keepalive_payload(
                    &aead,
                    "principal",
                    "session",
                    snapshot.upstream_id,
                    8,
                    3,
                    &mutated_generation,
                ),
                Err(CacheKeepalivePayloadError::Aead(_))
            ));
        }
        Ok(())
    }
}
