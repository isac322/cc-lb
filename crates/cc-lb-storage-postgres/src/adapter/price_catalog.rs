use async_trait::async_trait;
use cc_lb_storage_api::{PriceCatalogCache, PriceCatalogSnapshotRecord, StorageResult};
use serde_json::Value;
use sqlx::types::Json;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl PriceCatalogCache for PostgresStorage {
    async fn put_price_snapshot(&self, json_bytes: &[u8], fetched_at_ms: u64) -> StorageResult<()> {
        let payload = serde_json::from_slice::<Value>(json_bytes)?;
        let fetched_at_ms = u64_to_i64(fetched_at_ms, "price catalog fetched_at_ms")?;

        sqlx::query!(
            "INSERT INTO price_catalog_snapshots_v1 (payload, fetched_at_ms) VALUES ($1, $2)",
            Json(payload) as Json<Value>,
            fetched_at_ms
        )
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_price_snapshot(&self) -> StorageResult<Option<PriceCatalogSnapshotRecord>> {
        let row = sqlx::query!(
            r#"SELECT payload AS "payload: Json<Value>", fetched_at_ms
               FROM price_catalog_snapshots_v1
               ORDER BY fetched_at_ms DESC
               LIMIT 1"#
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        row.map(|row| {
            Ok(PriceCatalogSnapshotRecord {
                json_bytes: serde_json::to_vec(&row.payload.0)?,
                fetched_at_ms: i64_to_u64(row.fetched_at_ms, "price catalog fetched_at_ms")?,
            })
        })
        .transpose()
    }
}
