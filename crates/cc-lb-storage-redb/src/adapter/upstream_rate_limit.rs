use std::collections::HashSet;

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult,
    upstream_rate_limit::{UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore},
};
use redb::{ReadableDatabase, ReadableTable};

use crate::error_map::{map_join_err, map_redb_err};
use crate::{RedbStorage, StorageError, UPSTREAM_RATE_LIMIT_STATE_V1};

#[async_trait]
impl UpstreamRateLimitStateStore for RedbStorage {
    async fn put_observation(
        &self,
        record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        let storage = self.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || put_observation_sync(&storage, &record))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_for_upstream_ids(
        &self,
        upstream_ids: &[uuid::Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        let storage = self.clone();
        let upstream_ids = upstream_ids.to_vec();
        tokio::task::spawn_blocking(move || list_for_upstream_ids_sync(&storage, &upstream_ids))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

fn put_observation_sync(
    storage: &RedbStorage,
    record: &UpstreamRateLimitObservationRecord,
) -> Result<(), StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut table = write_txn.open_table(UPSTREAM_RATE_LIMIT_STATE_V1)?;
        let key = observation_key(record.upstream_id, &record.window, record.kind.as_str());
        if let Some(existing) = table.get(key.as_str())? {
            let existing_record: UpstreamRateLimitObservationRecord =
                serde_json::from_slice(existing.value())?;
            if existing_record.observed_at_unix_secs > record.observed_at_unix_secs {
                return Ok(());
            }
        }
        let payload = serde_json::to_vec(record)?;
        table.insert(key.as_str(), payload.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn list_for_upstream_ids_sync(
    storage: &RedbStorage,
    upstream_ids: &[uuid::Uuid],
) -> Result<Vec<UpstreamRateLimitObservationRecord>, StorageError> {
    if upstream_ids.is_empty() {
        return Ok(Vec::new());
    }
    let requested = upstream_ids
        .iter()
        .cloned()
        .collect::<HashSet<uuid::Uuid>>();
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(UPSTREAM_RATE_LIMIT_STATE_V1)?;
    let mut records = Vec::new();
    for row in table.iter()? {
        let (_, value) = row?;
        let record: UpstreamRateLimitObservationRecord = serde_json::from_slice(value.value())?;
        if requested.contains(&record.upstream_id) {
            records.push(record);
        }
    }
    records.sort_by(|left, right| {
        left.upstream_id
            .cmp(&right.upstream_id)
            .then_with(|| left.window.cmp(&right.window))
            .then_with(|| left.kind.as_str().cmp(right.kind.as_str()))
    });
    Ok(records)
}

fn observation_key(upstream_id: uuid::Uuid, window: &str, kind: &str) -> String {
    format!("{}:{upstream_id}:{window}:{kind}", window.len())
}
