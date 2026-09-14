use anyhow::Result;
use cc_lb_storage_api::PluginBlobRepo;
use cc_lb_storage_postgres::PostgresPluginBlobRepo;

const MIGRATION_0117: &str = include_str!("../migrations/0117_drop_plugin_registry_blobs.sql");
const SHA256: [u8; 32] = [0x17; 32];
const CURRENT_BYTES: &[u8] = b"current-wasm-blob";
const UPDATED_BYTES: &[u8] = b"updated-wasm-blob";

#[tokio::test]
async fn migration_0117_drops_legacy_blobs_without_changing_plugin_blob_repo() -> Result<()> {
    let mut fixture = crate::postgres_fixture::postgres_fixture().await?;

    let result: anyhow::Result<()> = async {
        let repo = PostgresPluginBlobRepo::new(fixture.pool().clone());
        repo.put_blob(&SHA256, CURRENT_BYTES).await?;
        assert_eq!(
            repo.get_blob(&SHA256).await?.as_deref(),
            Some(CURRENT_BYTES)
        );
        assert!(repo.list_blob_keys().await?.contains(&SHA256));

        sqlx::raw_sql(
            "CREATE TABLE plugin_registry_blobs (
                sha256 BYTEA PRIMARY KEY CHECK (length(sha256) = 32),
                bytes BYTEA NOT NULL
            )",
        )
        .execute(fixture.pool())
        .await?;
        sqlx::query("INSERT INTO plugin_registry_blobs (sha256, bytes) VALUES ($1, $2)")
            .bind(SHA256.as_slice())
            .bind(b"legacy-plugin-blob".as_slice())
            .execute(fixture.pool())
            .await?;

        let legacy_rows =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM plugin_registry_blobs")
                .fetch_one(fixture.pool())
                .await?;
        assert_eq!(legacy_rows, 1);

        sqlx::raw_sql(MIGRATION_0117)
            .execute(fixture.pool())
            .await?;

        let legacy_table = sqlx::query_scalar::<_, Option<String>>(
            "SELECT to_regclass('plugin_registry_blobs')::text",
        )
        .fetch_one(fixture.pool())
        .await?;
        assert_eq!(legacy_table, None);

        assert_eq!(
            repo.get_blob(&SHA256).await?.as_deref(),
            Some(CURRENT_BYTES)
        );
        assert!(repo.list_blob_keys().await?.contains(&SHA256));

        repo.put_blob(&SHA256, UPDATED_BYTES).await?;
        assert_eq!(
            repo.get_blob(&SHA256).await?.as_deref(),
            Some(UPDATED_BYTES)
        );
        repo.delete_blob(&SHA256).await?;
        assert_eq!(repo.get_blob(&SHA256).await?, None);

        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;

    result?;
    teardown
}
