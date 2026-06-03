use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

pub(crate) fn compute_revision_hash(
    upstreams: &[(Uuid, u64)],
    principals: &[(Uuid, u64)],
    chains: &[(Uuid, u64)],
    registry_entries: &[(Uuid, u64, [u8; 32])],
) -> u64 {
    let mut upstreams = upstreams.to_vec();
    let mut principals = principals.to_vec();
    let mut chains = chains.to_vec();
    let mut registry_entries = registry_entries.to_vec();
    upstreams.sort_by_key(|(id, _)| *id);
    principals.sort_by_key(|(id, _)| *id);
    chains.sort_by_key(|(id, _)| *id);
    registry_entries.sort_by_key(|(id, _, _)| *id);

    let mut bytes = Vec::with_capacity(
        (upstreams.len() + principals.len() + chains.len()) * 24 + registry_entries.len() * 88,
    );
    append_triples(&mut bytes, &upstreams);
    append_triples(&mut bytes, &principals);
    append_triples(&mut bytes, &chains);
    append_registry_entries(&mut bytes, &registry_entries);
    xxh3_64(&bytes)
}

fn append_triples(bytes: &mut Vec<u8>, triples: &[(Uuid, u64)]) {
    for (id, revision) in triples {
        bytes.extend_from_slice(&id.as_u128().to_le_bytes());
        bytes.extend_from_slice(&revision.to_le_bytes());
    }
}

fn append_registry_entries(bytes: &mut Vec<u8>, entries: &[(Uuid, u64, [u8; 32])]) {
    for (id, revision, sha256) in entries {
        bytes.extend_from_slice(&id.as_u128().to_le_bytes());
        bytes.extend_from_slice(&revision.to_le_bytes());
        append_sha256_hex(bytes, sha256);
    }
}

fn append_sha256_hex(bytes: &mut Vec<u8>, sha256: &[u8; 32]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in sha256 {
        bytes.push(HEX[(byte >> 4) as usize]);
        bytes.push(HEX[(byte & 0x0f) as usize]);
    }
}
