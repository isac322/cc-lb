use cc_lb_storage_api::PluginBlobRepo;
use cc_lb_storage_conformance::plugin_registry_scenarios::plugin_registry_roundtrip;
use cc_lb_storage_redb::{RedbPluginBlobRepo, RedbPluginRegistryRepo, RedbStorage};

#[tokio::test]
async fn plugin_registry_conformance_redb() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("plugin_registry_conformance.redb");
    let storage = RedbStorage::open(&path, [51; 32])?;
    let registry = RedbPluginRegistryRepo::new(storage.clone())?;
    let blobs = RedbPluginBlobRepo::new(storage)?;

    plugin_registry_roundtrip(&registry, &blobs).await
}

#[tokio::test]
async fn plugin_registry_blob_roundtrip_redb() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("plugin_blob_roundtrip.redb");
    let storage = RedbStorage::open(&path, [52; 32])?;
    let blobs = RedbPluginBlobRepo::new(storage)?;
    let sha256 = [7; 32];
    let bytes = b"wasm bytes";

    blobs.put_blob(&sha256, bytes).await?;
    assert_eq!(blobs.get_blob(&sha256).await?, Some(bytes.to_vec()));
    assert_eq!(blobs.list_blob_keys().await?, vec![sha256]);

    blobs.delete_blob(&sha256).await?;
    assert_eq!(blobs.get_blob(&sha256).await?, None);
    assert!(blobs.list_blob_keys().await?.is_empty());

    Ok(())
}
