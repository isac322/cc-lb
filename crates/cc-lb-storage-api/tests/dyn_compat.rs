use std::sync::Arc;

use cc_lb_storage_api::Storage as StorageTrait;

fn _accepts_arc_dyn_storage(_: Arc<dyn StorageTrait>) {}
fn _accepts_box_dyn_storage(_: Box<dyn StorageTrait>) {}

#[test]
fn t3__sqlite_storage_is_dyn_compatible() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.sqlite");
    let database_url = format!("sqlite://{}", path.display());
    let storage = tokio::runtime::Runtime::new()?.block_on(async {
        cc_lb_storage_sqlite::open_sqlite(
            &database_url,
            Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000)),
        )
        .await
    })?;

    let _: Arc<dyn StorageTrait> = Arc::new(storage);

    Ok(())
}
