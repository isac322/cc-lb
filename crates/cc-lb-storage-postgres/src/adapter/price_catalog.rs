use async_trait::async_trait;
use cc_lb_storage_api::{PriceCatalogCache, PriceCatalogSnapshotRecord, StorageResult};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl PriceCatalogCache for PostgresStorage {
    async fn put_price_snapshot(&self, json_bytes: &[u8], fetched_at_ms: u64) -> StorageResult<()> {
        let payload = serde_json::from_slice::<Value>(json_bytes)?;
        let payload_hash = sha256_hex(json_bytes);
        let fetched_at_ms = u64_to_i64(fetched_at_ms, "price catalog fetched_at_ms")?;

        sqlx::query(
            "INSERT INTO price_catalog_snapshots_v1 (payload, payload_hash, fetched_at_ms) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (payload_hash) DO UPDATE SET fetched_at_ms = excluded.fetched_at_ms",
        )
        .bind(payload)
        .bind(&payload_hash)
        .bind(fetched_at_ms)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_price_snapshot(&self) -> StorageResult<Option<PriceCatalogSnapshotRecord>> {
        let row = sqlx::query(
            r#"SELECT payload, fetched_at_ms
               FROM price_catalog_snapshots_v1
               ORDER BY fetched_at_ms DESC
               LIMIT 1"#,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        row.map(|row| {
            let payload = row.try_get::<Value, _>("payload").map_err(map_sqlx_error)?;
            let fetched_at_ms = row
                .try_get::<i64, _>("fetched_at_ms")
                .map_err(map_sqlx_error)?;
            Ok(PriceCatalogSnapshotRecord {
                json_bytes: serde_json::to_vec(&payload)?,
                fetched_at_ms: i64_to_u64(fetched_at_ms, "price catalog fetched_at_ms")?,
            })
        })
        .transpose()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}
