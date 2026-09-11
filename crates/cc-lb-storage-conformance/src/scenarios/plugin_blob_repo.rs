use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::PluginBlobRepo;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn roundtrip_delete_and_missing<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let first_key = [0x11; 32];
        let second_key = [0x22; 32];
        let missing_key = [0x33; 32];

        let baseline_keys = storage.list_blob_keys().await?;
        ensure!(
            baseline_keys.windows(2).all(|keys| keys[0] < keys[1]),
            "plugin blob keys must be listed in ascending byte order"
        );
        ensure!(
            !baseline_keys.contains(&first_key)
                && !baseline_keys.contains(&second_key)
                && !baseline_keys.contains(&missing_key),
            "scenario plugin blob keys must not collide with backend seed data"
        );

        ensure!(
            storage.get_blob(&missing_key).await?.is_none(),
            "an unknown plugin blob key must return None"
        );

        storage.put_blob(&first_key, b"first payload").await?;
        ensure!(
            storage.get_blob(&first_key).await? == Some(b"first payload".to_vec()),
            "plugin blob bytes must round-trip exactly"
        );

        storage.put_blob(&first_key, b"replacement payload").await?;
        storage.put_blob(&second_key, b"second payload").await?;
        ensure!(
            storage.get_blob(&first_key).await? == Some(b"replacement payload".to_vec()),
            "putting an existing plugin blob key must replace its bytes"
        );
        let mut expected_keys = baseline_keys.clone();
        expected_keys.extend([first_key, second_key]);
        expected_keys.sort_unstable();
        ensure!(
            storage.list_blob_keys().await? == expected_keys,
            "plugin blob keys must include seeded and inserted rows in ascending byte order"
        );

        storage.delete_blob(&first_key).await?;
        ensure!(
            storage.get_blob(&first_key).await?.is_none(),
            "a deleted plugin blob must become missing"
        );
        expected_keys.retain(|key| key != &first_key);
        ensure!(
            storage.list_blob_keys().await? == expected_keys,
            "deleting one plugin blob must preserve seeded and other inserted blobs"
        );

        storage.delete_blob(&missing_key).await?;
        ensure!(
            storage.get_blob(&missing_key).await?.is_none(),
            "deleting a missing plugin blob must remain an idempotent no-op"
        );
        Ok(())
    })
    .await
}
