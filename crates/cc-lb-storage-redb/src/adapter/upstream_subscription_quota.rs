use std::collections::{BTreeMap, BTreeSet, HashSet};

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, SubscriptionQuotaBucket, SubscriptionQuotaLatestRecord,
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaStore,
};
use redb::{ReadableDatabase, ReadableTable};
use uuid::Uuid;

use crate::adapter::error_map::{map_join_err, map_redb_err};
use crate::{
    RedbStorage, StorageError, UPSTREAM_SUBSCRIPTION_QUOTA_LATEST_V1,
    UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_BY_TIME_V1,
    UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_V1,
};

#[async_trait]
impl UpstreamSubscriptionQuotaStore for RedbStorage {
    async fn put_subscription_quota_batch(
        &self,
        records: &[SubscriptionQuotaObservationRecord],
    ) -> StorageResult<()> {
        let storage = self.clone();
        let records = records.to_vec();
        tokio::task::spawn_blocking(move || put_subscription_quota_batch_sync(&storage, &records))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaLatestRecord>> {
        let storage = self.clone();
        let upstream_ids = upstream_ids.to_vec();
        tokio::task::spawn_blocking(move || {
            list_latest_subscription_quota_for_upstreams_sync(&storage, &upstream_ids)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn list_subscription_quota_series(
        &self,
        query: SubscriptionQuotaSeriesQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSeries>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || list_subscription_quota_series_sync(&storage, &query))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn delete_subscription_quota_before(
        &self,
        cutoff_unix_millis: u64,
        batch_size: u32,
    ) -> StorageResult<u64> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            delete_subscription_quota_before_sync(&storage, cutoff_unix_millis, batch_size)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}

fn put_subscription_quota_batch_sync(
    storage: &RedbStorage,
    records: &[SubscriptionQuotaObservationRecord],
) -> Result<(), StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut observations = write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_V1)?;
        let mut observations_by_time =
            write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_BY_TIME_V1)?;
        let mut latest = write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_LATEST_V1)?;
        for record in records {
            let bytes = serde_json::to_vec(record)?;
            let observation_key = obs_key(
                record.upstream_id,
                record.window,
                record.source,
                record.observed_at_unix_millis,
                record.sample_id,
            );
            observations.insert(observation_key.as_slice(), bytes.as_slice())?;
            let by_time_key = obs_by_time_key(
                record.observed_at_unix_millis,
                record.upstream_id,
                record.window,
                record.source,
                record.sample_id,
            );
            observations_by_time.insert(by_time_key.as_slice(), &[][..])?;
            let latest_key = latest_key(record.upstream_id, record.window, record.source);
            let should_replace = latest
                .get(latest_key.as_slice())?
                .map(|value| {
                    serde_json::from_slice::<SubscriptionQuotaObservationRecord>(value.value()).map(
                        |existing| {
                            existing.observed_at_unix_millis <= record.observed_at_unix_millis
                        },
                    )
                })
                .transpose()?
                .unwrap_or(true);
            if should_replace {
                latest.insert(latest_key.as_slice(), bytes.as_slice())?;
            }
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn list_latest_subscription_quota_for_upstreams_sync(
    storage: &RedbStorage,
    upstream_ids: &[Uuid],
) -> Result<Vec<SubscriptionQuotaLatestRecord>, StorageError> {
    if upstream_ids.is_empty() {
        return Ok(Vec::new());
    }
    let requested = upstream_ids.iter().copied().collect::<HashSet<_>>();
    let read_txn = storage.db.begin_read()?;
    let latest = read_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_LATEST_V1)?;
    let mut records = Vec::new();
    for row in latest.iter()? {
        let (_, value) = row?;
        let record: SubscriptionQuotaObservationRecord = serde_json::from_slice(value.value())?;
        if requested.contains(&record.upstream_id) {
            records.push(record);
        }
    }
    records.sort_by(|left, right| {
        left.upstream_id
            .cmp(&right.upstream_id)
            .then_with(|| left.window.cmp(&right.window))
            .then_with(|| left.source.cmp(&right.source))
    });
    Ok(records)
}

fn list_subscription_quota_series_sync(
    storage: &RedbStorage,
    query: &SubscriptionQuotaSeriesQuery,
) -> Result<Vec<SubscriptionQuotaSeries>, StorageError> {
    if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
        return Ok(Vec::new());
    }
    let requested_upstreams = query.upstream_ids.iter().copied().collect::<HashSet<_>>();
    let requested_windows = query.windows.iter().copied().collect::<HashSet<_>>();
    let requested_sources = filtered_sources(query).into_iter().collect::<HashSet<_>>();
    if requested_sources.is_empty() {
        return Ok(Vec::new());
    }
    let read_txn = storage.db.begin_read()?;
    let observations = read_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_V1)?;
    let mut records = Vec::new();
    for row in observations.iter()? {
        let (_, value) = row?;
        let record: SubscriptionQuotaObservationRecord = serde_json::from_slice(value.value())?;
        if requested_upstreams.contains(&record.upstream_id)
            && requested_windows.contains(&record.window)
            && requested_sources.contains(&record.source)
            && record.observed_at_unix_millis >= query.since_unix_millis
            && record.observed_at_unix_millis <= query.until_unix_millis
        {
            records.push(record);
        }
    }
    Ok(build_series(records, query))
}

fn delete_subscription_quota_before_sync(
    storage: &RedbStorage,
    cutoff_millis: u64,
    batch_size: u32,
) -> Result<u64, StorageError> {
    if batch_size == 0 {
        return Ok(0);
    }
    let write_txn = storage.db.begin_write()?;
    let mut victims = Vec::new();
    {
        let observations_by_time =
            write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_BY_TIME_V1)?;
        for row in observations_by_time.iter()? {
            let (key, _) = row?;
            let key_bytes = key.value().to_vec();
            let parsed = parse_obs_by_time_key(&key_bytes)?;
            if parsed.observed_at_millis >= cutoff_millis {
                break;
            }
            victims.push((key_bytes, parsed));
            if victims.len() >= batch_size as usize {
                break;
            }
        }
    }
    {
        let mut observations = write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_V1)?;
        let mut observations_by_time =
            write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_BY_TIME_V1)?;
        for (by_time_key, parsed) in &victims {
            let observation_key = obs_key(
                parsed.upstream_id,
                parsed.window,
                parsed.source,
                parsed.observed_at_millis,
                parsed.sample_id,
            );
            observations.remove(observation_key.as_slice())?;
            observations_by_time.remove(by_time_key.as_slice())?;
        }
    }
    let deleted = victims.len() as u64;
    write_txn.commit()?;
    Ok(deleted)
}

fn obs_key(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    source: SubscriptionQuotaSource,
    observed_at_millis: u64,
    sample_id: Uuid,
) -> Vec<u8> {
    let desc = u64::MAX - observed_at_millis;
    let mut key = Vec::with_capacity(42);
    key.extend_from_slice(upstream_id.as_bytes());
    key.push(window.code());
    key.push(source.code());
    key.extend_from_slice(&desc.to_be_bytes());
    key.extend_from_slice(sample_id.as_bytes());
    key
}

fn obs_by_time_key(
    observed_at_millis: u64,
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    source: SubscriptionQuotaSource,
    sample_id: Uuid,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(42);
    key.extend_from_slice(&observed_at_millis.to_be_bytes());
    key.extend_from_slice(upstream_id.as_bytes());
    key.push(window.code());
    key.push(source.code());
    key.extend_from_slice(sample_id.as_bytes());
    key
}

fn latest_key(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    source: SubscriptionQuotaSource,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(18);
    key.extend_from_slice(upstream_id.as_bytes());
    key.push(window.code());
    key.push(source.code());
    key
}

struct ParsedByTimeKey {
    observed_at_millis: u64,
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    source: SubscriptionQuotaSource,
    sample_id: Uuid,
}

fn parse_obs_by_time_key(key: &[u8]) -> Result<ParsedByTimeKey, StorageError> {
    if key.len() != 42 {
        return Err(StorageError::InvalidInput {
            field: "subscription_quota_by_time_key".to_owned(),
            reason: "invalid length".to_owned(),
        });
    }
    let observed_at_millis =
        u64::from_be_bytes(
            key[0..8]
                .try_into()
                .map_err(|_| StorageError::InvalidInput {
                    field: "subscription_quota_by_time_key".to_owned(),
                    reason: "invalid observed_at".to_owned(),
                })?,
        );
    let upstream_id =
        Uuid::from_slice(&key[8..24]).map_err(|error| StorageError::InvalidInput {
            field: "subscription_quota_by_time_key".to_owned(),
            reason: error.to_string(),
        })?;
    let window =
        SubscriptionQuotaWindow::from_code(key[24]).ok_or_else(|| StorageError::InvalidInput {
            field: "subscription_quota_by_time_key".to_owned(),
            reason: "invalid window".to_owned(),
        })?;
    let source =
        SubscriptionQuotaSource::from_code(key[25]).ok_or_else(|| StorageError::InvalidInput {
            field: "subscription_quota_by_time_key".to_owned(),
            reason: "invalid source".to_owned(),
        })?;
    let sample_id = Uuid::from_slice(&key[26..42]).map_err(|error| StorageError::InvalidInput {
        field: "subscription_quota_by_time_key".to_owned(),
        reason: error.to_string(),
    })?;
    Ok(ParsedByTimeKey {
        observed_at_millis,
        upstream_id,
        window,
        source,
        sample_id,
    })
}

fn build_series(
    records: Vec<SubscriptionQuotaObservationRecord>,
    query: &SubscriptionQuotaSeriesQuery,
) -> Vec<SubscriptionQuotaSeries> {
    let bucket_secs = query.bucket_secs.max(1);
    let mut groups: BTreeMap<
        (Uuid, SubscriptionQuotaWindow, u8),
        Vec<SubscriptionQuotaObservationRecord>,
    > = BTreeMap::new();
    for record in records {
        groups
            .entry((
                record.upstream_id,
                record.window,
                source_merge_code(query.source_merge),
            ))
            .or_default()
            .push(record);
    }
    let mut series = Vec::new();
    for ((upstream_id, window, source_code), group_records) in groups {
        let mut buckets: BTreeMap<u64, Vec<SubscriptionQuotaObservationRecord>> = BTreeMap::new();
        for record in group_records {
            let bucket = (record.observed_at_unix_millis / 1000) / bucket_secs * bucket_secs;
            buckets.entry(bucket).or_default().push(record);
        }
        let mut bucket_records = buckets
            .into_iter()
            .map(|(bucket_start_unix_secs, records)| {
                bucket_from_records(bucket_start_unix_secs, &records)
            })
            .collect::<Vec<_>>();
        downsample(&mut bucket_records, query.max_points_per_series as usize);
        series.push(SubscriptionQuotaSeries {
            upstream_id,
            window,
            source: source_merge_from_code(source_code),
            buckets: bucket_records,
        });
    }
    series
}

fn source_merge_code(source: SubscriptionQuotaSourceMerge) -> u8 {
    match source {
        SubscriptionQuotaSourceMerge::Header => 1,
        SubscriptionQuotaSourceMerge::Api => 2,
        SubscriptionQuotaSourceMerge::Merged => 3,
    }
}

fn source_merge_from_code(code: u8) -> SubscriptionQuotaSourceMerge {
    match code {
        1 => SubscriptionQuotaSourceMerge::Header,
        2 => SubscriptionQuotaSourceMerge::Api,
        _ => SubscriptionQuotaSourceMerge::Merged,
    }
}

fn bucket_from_records(
    bucket_start_unix_secs: u64,
    records: &[SubscriptionQuotaObservationRecord],
) -> SubscriptionQuotaBucket {
    let mut util_count = 0u32;
    let mut util_sum = 0.0;
    let mut util_min: Option<f64> = None;
    let mut util_max: Option<f64> = None;
    let mut last = &records[0];
    let mut sources_seen = BTreeSet::new();
    for record in records {
        sources_seen.insert(record.source);
        if record.observed_at_unix_millis >= last.observed_at_unix_millis {
            last = record;
        }
        if let Some(utilization) = record.utilization {
            util_count += 1;
            util_sum += utilization;
            util_min = Some(util_min.map_or(utilization, |value| value.min(utilization)));
            util_max = Some(util_max.map_or(utilization, |value| value.max(utilization)));
        }
    }
    SubscriptionQuotaBucket {
        bucket_start_unix_secs,
        observed: true,
        sample_count: u32::try_from(records.len()).unwrap_or(u32::MAX),
        utilization_min: util_min,
        utilization_avg: (util_count > 0).then_some(util_sum / f64::from(util_count)),
        utilization_max: util_max,
        utilization_last: last.utilization,
        status_last: last.status,
        resets_at_unix_secs_last: last.resets_at_unix_secs,
        observed_at_unix_millis_last: Some(last.observed_at_unix_millis),
        sources_seen: sources_seen.into_iter().collect(),
    }
}

fn downsample<T: Clone>(items: &mut Vec<T>, max_points: usize) {
    if max_points == 0 {
        items.clear();
        return;
    }
    if items.len() <= max_points {
        return;
    }
    if max_points == 1 {
        items.truncate(1);
        return;
    }
    let original = items.clone();
    let last_index = original.len() - 1;
    let mut sampled = Vec::with_capacity(max_points);
    for point in 0..max_points {
        sampled.push(original[point * last_index / (max_points - 1)].clone());
    }
    *items = sampled;
}

fn filtered_sources(query: &SubscriptionQuotaSeriesQuery) -> Vec<SubscriptionQuotaSource> {
    query
        .sources
        .iter()
        .copied()
        .filter(|source| match query.source_merge {
            SubscriptionQuotaSourceMerge::Merged => true,
            SubscriptionQuotaSourceMerge::Header => *source == SubscriptionQuotaSource::Header,
            SubscriptionQuotaSourceMerge::Api => *source == SubscriptionQuotaSource::Api,
        })
        .collect()
}
