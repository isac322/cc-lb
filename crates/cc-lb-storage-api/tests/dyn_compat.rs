use std::sync::Arc;

use cc_lb_storage_api::Storage as StorageTrait;

fn _accepts_arc_dyn_storage(_: Arc<dyn StorageTrait>) {}
fn _accepts_box_dyn_storage(_: Box<dyn StorageTrait>) {}

#[test]
fn redb_storage_is_dyn_compatible() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let storage = cc_lb_storage_redb::Storage::open(&path, [0; 32])?;

    let _: Arc<dyn StorageTrait> = Arc::new(storage);

    Ok(())
}
