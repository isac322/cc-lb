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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_hash_is_order_independent_and_detects_mutation() {
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let c = Uuid::from_u128(3);
        let upstreams = vec![(b, 20), (a, 10)];
        let principals = vec![(c, 30), (a, 11)];
        let chains = vec![(b, 21), (c, 31)];
        let registry = vec![(c, 32, [3; 32]), (a, 12, [1; 32])];

        let baseline = compute_revision_hash(&upstreams, &principals, &chains, &registry);
        let reordered = compute_revision_hash(
            &upstreams.iter().copied().rev().collect::<Vec<_>>(),
            &principals.iter().copied().rev().collect::<Vec<_>>(),
            &chains.iter().copied().rev().collect::<Vec<_>>(),
            &registry.iter().copied().rev().collect::<Vec<_>>(),
        );
        assert_eq!(
            reordered, baseline,
            "input ordering must not affect the hash"
        );

        let mut revision_changed = upstreams.clone();
        revision_changed[0].1 ^= 1;
        assert_ne!(
            compute_revision_hash(&revision_changed, &principals, &chains, &registry),
            baseline,
            "a one-bit revision change must invalidate the hash",
        );

        let mut fingerprint_changed = registry.clone();
        fingerprint_changed[0].2[0] ^= 1;
        assert_ne!(
            compute_revision_hash(&upstreams, &principals, &chains, &fingerprint_changed),
            baseline,
            "a one-bit plugin fingerprint change must invalidate the hash",
        );
    }
}
