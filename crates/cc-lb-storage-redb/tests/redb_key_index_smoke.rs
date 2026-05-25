use cc_lb_storage_redb::{Storage, KEY_INDEX_BY_HASH_V1};

#[test]
fn key_index_roundtrip_is_encrypted_and_removable() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let storage = Storage::open(&path, [13; 32])?;

    let index_hash = [42; 32];
    let composite_row_key = b"principal-1\0key-abc123";

    assert_eq!(storage.get_composite_by_index(&index_hash)?, None);

    let write_txn = storage.begin_write()?;
    storage.put_key_index(&write_txn, &index_hash, composite_row_key)?;
    write_txn.commit()?;

    assert_eq!(
        storage.get_composite_by_index(&index_hash)?,
        Some(composite_row_key.to_vec())
    );

    let raw_value = {
        let read_txn = storage.begin_read()?;
        let table = read_txn.open_table(KEY_INDEX_BY_HASH_V1)?;
        table
            .get(index_hash.as_slice())?
            .expect("stored index value exists")
            .value()
            .to_vec()
    };

    assert!(!contains_bytes(&raw_value, composite_row_key));
    assert!(!contains_bytes(&raw_value, b"principal-1"));
    assert!(!contains_bytes(&raw_value, b"key-abc123"));

    let write_txn = storage.begin_write()?;
    storage.remove_key_index(&write_txn, &index_hash)?;
    write_txn.commit()?;

    assert_eq!(storage.get_composite_by_index(&index_hash)?, None);

    Ok(())
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
