pub type CacheNamespace = u64;

pub fn derive_namespace(auth_material: &[u8]) -> CacheNamespace {
    let hash = blake3::hash(auth_material);
    let bytes = hash.as_bytes();
    u64::from_le_bytes(bytes[..8].try_into().expect("blake3 hash is 32 bytes, at least 8 available"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let auth_material = b"test_auth_material";
        let ns1 = derive_namespace(auth_material);
        let ns2 = derive_namespace(auth_material);
        assert_eq!(ns1, ns2, "same auth material should produce same namespace");
    }

    #[test]
    fn distinct_inputs_distinct_outputs() {
        let auth1 = b"first_auth_material";
        let auth2 = b"second_auth_material";
        let ns1 = derive_namespace(auth1);
        let ns2 = derive_namespace(auth2);
        assert_ne!(ns1, ns2, "different auth material should produce different namespaces");
    }

    #[test]
    fn empty_is_valid() {
        let empty = b"";
        let ns1 = derive_namespace(empty);
        let ns2 = derive_namespace(empty);
        assert_eq!(ns1, ns2, "empty auth material should produce deterministic namespace");
    }
}
