use async_trait::async_trait;
use cc_lb_storage_api::{
    PriceCatalogCache, PriceCatalogSnapshotRecord, StorageError, StorageResult,
};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl PriceCatalogCache for SqliteStorage {
    async fn put_price_snapshot(&self, json_bytes: &[u8], fetched_at_ms: u64) -> StorageResult<()> {
        let _ = serde_json::from_slice::<serde_json::Value>(json_bytes)?;
        let payload =
            std::str::from_utf8(json_bytes).map_err(|error| StorageError::InvalidInput {
                field: "json_bytes".to_owned(),
                reason: error.to_string(),
            })?;
        let payload_hash = sha256_hex(json_bytes);
        let fetched_at_ms = u64_to_i64(fetched_at_ms, "price catalog fetched_at_ms")?;

        sqlx::query(
            "INSERT INTO price_catalog_snapshots_v1 (payload, payload_hash, fetched_at_ms, created_at) \
             VALUES (?, ?, ?, unixepoch()) \
             ON CONFLICT(payload_hash) DO UPDATE SET fetched_at_ms = excluded.fetched_at_ms",
        )
        .bind(payload)
        .bind(&payload_hash)
        .bind(fetched_at_ms)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_price_snapshot(&self) -> StorageResult<Option<PriceCatalogSnapshotRecord>> {
        let row = sqlx::query(
            "SELECT payload, fetched_at_ms FROM price_catalog_snapshots_v1 ORDER BY fetched_at_ms DESC, id DESC LIMIT 1",
        )
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        row.map(|row| {
            let payload = row
                .try_get::<String, _>("payload")
                .map_err(map_sqlx_error)?;
            let fetched_at_ms = row
                .try_get::<i64, _>("fetched_at_ms")
                .map_err(map_sqlx_error)?;

            Ok(PriceCatalogSnapshotRecord {
                json_bytes: payload.into_bytes(),
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

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} does not fit in SQLite INTEGER"),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}
