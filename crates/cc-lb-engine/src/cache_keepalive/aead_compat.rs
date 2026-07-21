use cc_lb_aead::{AeadError, AeadService};
use cc_lb_storage_api::CacheTtl;
use uuid::Uuid;

use super::{PersistedRequestSnapshot, RequestSnapshot};

const SNAPSHOT_V1_AAD: &[u8] =
    b"cache_keepalive_snapshot:v1:principal-1:session-hash:00000000-0000-0000-0000-000000000007:3";

fn encrypt_v1_snapshot(aead: &AeadService) -> (PersistedRequestSnapshot, Vec<u8>) {
    let expected = PersistedRequestSnapshot {
        url: "https://api.anthropic.com/v1/messages".to_owned(),
        method: "POST".to_owned(),
        headers: Vec::new(),
        body: br#"{"model":"claude-haiku-4-5","max_tokens":32}"#.to_vec(),
        upstream_id: Uuid::from_u128(7),
        ttl: CacheTtl::Ttl5m,
    };
    let plaintext = serde_json::to_vec(&expected).expect("snapshot serializes");
    let encrypted = aead
        .encrypt(&plaintext, SNAPSHOT_V1_AAD)
        .expect("v1 snapshot encrypts");

    (expected, encrypted)
}

#[test]
fn snapshot_v1_decrypts_with_new_column() {
    // Given: a persisted v1 snapshot before a plaintext nullable column is added.
    let aead = AeadService::from_master_key([41; 32]);
    let (expected, encrypted) = encrypt_v1_snapshot(&aead);

    // When: the new-column-compatible path decrypts and rebuilds the snapshot.
    let plaintext = aead
        .decrypt(&encrypted, SNAPSHOT_V1_AAD)
        .expect("v1 AAD decrypts snapshot");
    let persisted: PersistedRequestSnapshot =
        serde_json::from_slice(&plaintext).expect("snapshot payload deserializes");
    let restored = RequestSnapshot::from_persisted(persisted).expect("snapshot rebuilds");

    // Then: the exact persisted snapshot remains usable.
    assert_eq!(restored.to_persisted(), expected);
}

#[test]
fn snapshot_v1_rejects_mutated_aad() {
    // Given: a v1-encrypted persisted snapshot.
    let aead = AeadService::from_master_key([43; 32]);
    let (_, encrypted) = encrypt_v1_snapshot(&aead);
    let mut mutated_aad = SNAPSHOT_V1_AAD.to_vec();
    let last = mutated_aad.last_mut().expect("AAD is non-empty");
    *last ^= 1;

    // When: one AAD byte differs from the authenticated v1 value.
    let result = aead.decrypt(&encrypted, &mutated_aad);

    // Then: authenticated decryption rejects the payload.
    assert_eq!(result, Err(AeadError::DecryptionFailed));
}
