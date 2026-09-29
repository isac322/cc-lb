use async_trait::async_trait;
use cc_lb_storage_api::{
    PriceCatalogCache, PriceCatalogSnapshotFetch, PriceCatalogSnapshotMetadata,
    PriceCatalogSnapshotRecord, StorageError, StorageResult,
};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

const PRICE_CATALOG_SNAPSHOT_RETENTION: i64 = 3;

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

        sqlx::query(
            "DELETE FROM price_catalog_snapshots_v1 \
             WHERE id NOT IN ( \
                 SELECT id FROM price_catalog_snapshots_v1 \
                 ORDER BY fetched_at_ms DESC, id DESC \
                 LIMIT ? \
             )",
        )
        .bind(PRICE_CATALOG_SNAPSHOT_RETENTION)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_price_snapshot_if_changed(
        &self,
        current_hash: &str,
    ) -> StorageResult<PriceCatalogSnapshotFetch> {
        let row = sqlx::query(
            "SELECT CASE \
                 WHEN payload_hash = ? \
                 THEN NULL ELSE payload END AS payload, \
                 payload_hash, fetched_at_ms \
             FROM price_catalog_snapshots_v1 \
             ORDER BY fetched_at_ms DESC, id DESC \
             LIMIT 1",
        )
        .bind(current_hash)
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        let Some(row) = row else {
            return Ok(PriceCatalogSnapshotFetch::Missing);
        };
        let payload = row
            .try_get::<Option<String>, _>("payload")
            .map_err(map_sqlx_error)?;
        let payload_hash = row
            .try_get::<String, _>("payload_hash")
            .map_err(map_sqlx_error)?;
        let fetched_at_ms = row
            .try_get::<i64, _>("fetched_at_ms")
            .map_err(map_sqlx_error)?;

        snapshot_fetch_from_parts(
            payload,
            payload_hash,
            i64_to_u64(fetched_at_ms, "price catalog fetched_at_ms")?,
        )
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

fn snapshot_fetch_from_parts(
    payload: Option<String>,
    payload_hash: String,
    fetched_at_ms: u64,
) -> StorageResult<PriceCatalogSnapshotFetch> {
    let Some(payload) = payload else {
        return Ok(PriceCatalogSnapshotFetch::Unchanged(
            PriceCatalogSnapshotMetadata {
                payload_hash,
                fetched_at_ms,
            },
        ));
    };

    let json_bytes = payload.into_bytes();
    let actual_hash = sha256_hex(&json_bytes);
    if actual_hash != payload_hash {
        return Err(StorageError::Corrupted {
            message: format!(
                "price catalog payload hash mismatch: expected {payload_hash}, got {actual_hash}"
            ),
        });
    }

    Ok(PriceCatalogSnapshotFetch::Changed(
        PriceCatalogSnapshotRecord {
            json_bytes,
            fetched_at_ms,
        },
    ))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changed_snapshot_rejects_mismatched_payload_hash() {
        let error = snapshot_fetch_from_parts(
            Some(r#"{"models":[]}"#.to_owned()),
            "not-the-payload-hash".to_owned(),
            123,
        )
        .expect_err("mismatched payload hash must be rejected");

        assert!(matches!(error, StorageError::Corrupted { .. }));
    }
}
