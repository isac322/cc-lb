use cc_lb_storage_api::{BackendKind, MetaStore, PriceCatalogCache};

#[tokio::test]
async fn t3__price_catalog_keeps_only_latest_snapshots() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("price-catalog-retention.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");

    for index in 0..5_u64 {
        let payload = format!(r#"{{"models":[{{"id":"model-{index}"}}]}}"#);
        storage
            .put_price_snapshot(payload.as_bytes(), 1_000 + index)
            .await
            .expect("put price snapshot");
    }

    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM price_catalog_snapshots_v1")
        .fetch_one(storage.pool())
        .await
        .expect("count price snapshots");
    assert_eq!(count, 3);

    let oldest_retained =
        sqlx::query_scalar::<_, i64>("SELECT MIN(fetched_at_ms) FROM price_catalog_snapshots_v1")
            .fetch_one(storage.pool())
            .await
            .expect("oldest retained snapshot");
    assert_eq!(oldest_retained, 1_002);
}
