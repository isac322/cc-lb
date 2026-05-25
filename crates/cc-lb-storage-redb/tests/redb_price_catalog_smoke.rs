use cc_lb_storage_redb::{PriceSnapshot, Storage};

#[test]
fn price_catalog_roundtrip_preserves_json_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let storage = Storage::open(&path, [19; 32])?;

    let json_bytes = br#"{"models":[{"id":"claude-sonnet-4-5","price":12345}]}"#;
    storage.put_price_snapshot(json_bytes, 1_765_000_123)?;

    let snapshot = storage
        .get_price_snapshot()?
        .expect("snapshot should exist");
    assert_eq!(
        snapshot,
        PriceSnapshot {
            json_bytes: json_bytes.to_vec(),
            fetched_at_ms: 1_765_000_123,
        }
    );
    assert_eq!(snapshot.json_bytes, json_bytes);

    Ok(())
}

#[test]
fn empty_price_catalog_returns_none() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let storage = Storage::open(&path, [19; 32])?;

    assert_eq!(storage.get_price_snapshot()?, None);

    Ok(())
}
