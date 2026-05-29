use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

pub(crate) fn compute_revision_hash(
    upstreams: &[(Uuid, u64)],
    principals: &[(Uuid, u64)],
    chains: &[(Uuid, u64)],
) -> u64 {
    let mut upstreams = upstreams.to_vec();
    let mut principals = principals.to_vec();
    let mut chains = chains.to_vec();
    upstreams.sort_by_key(|(id, _)| *id);
    principals.sort_by_key(|(id, _)| *id);
    chains.sort_by_key(|(id, _)| *id);

    let mut bytes = Vec::with_capacity((upstreams.len() + principals.len() + chains.len()) * 24);
    append_triples(&mut bytes, &upstreams);
    append_triples(&mut bytes, &principals);
    append_triples(&mut bytes, &chains);
    xxh3_64(&bytes)
}

fn append_triples(bytes: &mut Vec<u8>, triples: &[(Uuid, u64)]) {
    for (id, revision) in triples {
        bytes.extend_from_slice(&id.as_u128().to_le_bytes());
        bytes.extend_from_slice(&revision.to_le_bytes());
    }
}
