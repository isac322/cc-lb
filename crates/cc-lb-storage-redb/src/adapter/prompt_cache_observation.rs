use async_trait::async_trait;
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore, StorageResult, TtlClass,
};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};
use uuid::Uuid;

use crate::adapter::error_map::{map_join_err, map_redb_err};
use crate::{PROMPT_CACHE_OBSERVATIONS, RedbStorage, StorageError};

#[async_trait]
impl PromptCacheObservationStore for RedbStorage {
    async fn upsert_observation(&self, record: &PromptCacheObservationRecord) -> StorageResult<()> {
        let storage = self.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || upsert_observation_sync(&storage, &record))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_active_for_upstream(
        &self,
        upstream_id: Uuid,
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            list_active_for_upstream_sync(&storage, upstream_id, not_expired_at_unix_secs)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn purge_expired_before(&self, ts_unix_secs: u64) -> StorageResult<u64> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || purge_expired_before_sync(&storage, ts_unix_secs))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn count(&self) -> StorageResult<u64> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || count_sync(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

fn upsert_observation_sync(
    storage: &RedbStorage,
    record: &PromptCacheObservationRecord,
) -> Result<(), StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut table = write_txn.open_table(PROMPT_CACHE_OBSERVATIONS)?;
        let key = observation_key(record.upstream_id, &record.prefix_hash, record.ttl_class);
        let bytes = serde_json::to_vec(record)?;
        table.insert(key.as_slice(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn list_active_for_upstream_sync(
    storage: &RedbStorage,
    upstream_id: Uuid,
    not_expired_at_unix_secs: u64,
) -> Result<Vec<PromptCacheObservationRecord>, StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(PROMPT_CACHE_OBSERVATIONS)?;
    let mut records = Vec::new();
    for row in table.iter()? {
        let (key, value) = row?;
        if !key.value().starts_with(upstream_id.as_bytes()) {
            continue;
        }
        let record: PromptCacheObservationRecord = serde_json::from_slice(value.value())?;
        if record.upstream_id == upstream_id
            && record.expires_at_unix_secs > not_expired_at_unix_secs
        {
            records.push(record);
        }
    }
    records.sort_by(|left, right| {
        left.prefix_hash
            .cmp(&right.prefix_hash)
            .then_with(|| ttl_class_code(left.ttl_class).cmp(&ttl_class_code(right.ttl_class)))
    });
    Ok(records)
}

fn purge_expired_before_sync(
    storage: &RedbStorage,
    ts_unix_secs: u64,
) -> Result<u64, StorageError> {
    let write_txn = storage.db.begin_write()?;
    let victims = {
        let table = write_txn.open_table(PROMPT_CACHE_OBSERVATIONS)?;
        let mut victims = Vec::new();
        for row in table.iter()? {
            let (key, value) = row?;
            let record: PromptCacheObservationRecord = serde_json::from_slice(value.value())?;
            if record.expires_at_unix_secs < ts_unix_secs {
                victims.push(key.value().to_vec());
            }
        }
        victims
    };
    {
        let mut table = write_txn.open_table(PROMPT_CACHE_OBSERVATIONS)?;
        for key in &victims {
            table.remove(key.as_slice())?;
        }
    }
    let deleted = victims.len() as u64;
    write_txn.commit()?;
    Ok(deleted)
}

fn count_sync(storage: &RedbStorage) -> Result<u64, StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(PROMPT_CACHE_OBSERVATIONS)?;
    Ok(table.len()?)
}

fn observation_key(upstream_id: Uuid, prefix_hash: &str, ttl_class: TtlClass) -> Vec<u8> {
    let prefix_bytes = prefix_hash.as_bytes();
    let prefix_len = u32::try_from(prefix_bytes.len()).unwrap_or(u32::MAX);
    let mut key = Vec::with_capacity(16 + 4 + prefix_bytes.len() + 1);
    key.extend_from_slice(upstream_id.as_bytes());
    key.extend_from_slice(&prefix_len.to_be_bytes());
    key.extend_from_slice(prefix_bytes);
    key.push(ttl_class_code(ttl_class));
    key
}

fn ttl_class_code(ttl_class: TtlClass) -> u8 {
    match ttl_class {
        TtlClass::Ephemeral5m => 0,
        TtlClass::Ephemeral1h => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn temp_storage() -> TestResult<(tempfile::TempDir, RedbStorage)> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("storage.redb");
        let storage = RedbStorage::open(&path, [13; 32])?;
        Ok((dir, storage))
    }

    fn observation(
        upstream_id: Uuid,
        prefix_hash: &str,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
    ) -> PromptCacheObservationRecord {
        PromptCacheObservationRecord {
            upstream_id,
            canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
            prefix_hash: prefix_hash.to_owned(),
            ttl_class,
            expires_at_unix_secs,
            last_observed_at_unix_secs: expires_at_unix_secs.saturating_sub(60),
            hash_schema_version: 1,
        }
    }

    #[tokio::test]
    async fn redb_prompt_cache_upsert_then_list_returns_active() -> TestResult {
        let (_dir, storage) = temp_storage()?;
        let upstream_id = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        let first = observation(upstream_id, "sha256:upsert", TtlClass::Ephemeral5m, 1_500);
        let replacement = PromptCacheObservationRecord {
            expires_at_unix_secs: 2_500,
            last_observed_at_unix_secs: 2_000,
            ..first.clone()
        };

        storage.upsert_observation(&first).await?;
        storage.upsert_observation(&replacement).await?;

        let records = storage.list_active_for_upstream(upstream_id, 2_000).await?;
        assert_eq!(records, vec![replacement]);
        assert_eq!(storage.count().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn redb_prompt_cache_list_filters_expired() -> TestResult {
        let (_dir, storage) = temp_storage()?;
        let upstream_id = Uuid::from_u128(0x2222_3333_4444_5555_6666_7777_8888_9999);
        let other_upstream_id = Uuid::from_u128(0x3333_4444_5555_6666_7777_8888_9999_aaaa);
        let active = observation(upstream_id, "sha256:active", TtlClass::Ephemeral5m, 2_500);
        let expired = observation(upstream_id, "sha256:expired", TtlClass::Ephemeral1h, 900);
        let other = observation(
            other_upstream_id,
            "sha256:other",
            TtlClass::Ephemeral5m,
            2_500,
        );

        storage.upsert_observation(&active).await?;
        storage.upsert_observation(&expired).await?;
        storage.upsert_observation(&other).await?;

        let records = storage.list_active_for_upstream(upstream_id, 1_000).await?;
        assert_eq!(records, vec![active]);
        Ok(())
    }

    #[tokio::test]
    async fn redb_prompt_cache_purge_removes_expired() -> TestResult {
        let (_dir, storage) = temp_storage()?;
        let upstream_id = Uuid::from_u128(0x4444_5555_6666_7777_8888_9999_aaaa_bbbb);
        let expired = observation(upstream_id, "sha256:expired", TtlClass::Ephemeral5m, 999);
        let boundary = observation(upstream_id, "sha256:boundary", TtlClass::Ephemeral1h, 1_000);
        let active = observation(upstream_id, "sha256:active", TtlClass::Ephemeral5m, 1_001);

        storage.upsert_observation(&expired).await?;
        storage.upsert_observation(&boundary).await?;
        storage.upsert_observation(&active).await?;

        assert_eq!(storage.purge_expired_before(1_000).await?, 1);
        assert_eq!(storage.count().await?, 2);

        let records = storage.list_active_for_upstream(upstream_id, 0).await?;
        let prefixes = records
            .into_iter()
            .map(|record| record.prefix_hash)
            .collect::<Vec<_>>();
        assert_eq!(prefixes, vec!["sha256:active", "sha256:boundary"]);
        Ok(())
    }

    #[tokio::test]
    async fn redb_prompt_cache_count_after_inserts() -> TestResult {
        let (_dir, storage) = temp_storage()?;
        let upstream_id = Uuid::from_u128(0x5555_6666_7777_8888_9999_aaaa_bbbb_cccc);
        let one = observation(upstream_id, "sha256:one", TtlClass::Ephemeral5m, 1_500);
        let two = observation(upstream_id, "sha256:two", TtlClass::Ephemeral1h, 1_800);

        storage.upsert_observation(&one).await?;
        storage.upsert_observation(&two).await?;

        assert_eq!(storage.count().await?, 2);
        Ok(())
    }
}
