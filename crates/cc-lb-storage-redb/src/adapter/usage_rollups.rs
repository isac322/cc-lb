use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UsageRollupStore,
    types::{
        UsageRollup as ApiUsageRollup, UsageRollupKey as ApiUsageRollupKey,
        UsageRollupResolution as ApiUsageRollupResolution, UsageRollupRun as ApiUsageRollupRun,
    },
};

use crate::{
    Storage, UsageRollup as RedbUsageRollup, UsageRollupKey as RedbUsageRollupKey,
    UsageRollupResolution as RedbUsageRollupResolution, UsageRollupRun as RedbUsageRollupRun,
};

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl UsageRollupStore for Storage {
    async fn rollup_usage_once(&self) -> StorageResult<ApiUsageRollupRun> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || Storage::rollup_usage_once(&storage))
            .await
            .map_err(map_join_err)?
            .map(to_api_usage_rollup_run)
            .map_err(map_redb_err)
    }

    async fn query_usage_rollups(&self) -> StorageResult<Vec<ApiUsageRollup>> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || Storage::query_usage_rollups(&storage))
            .await
            .map_err(map_join_err)?
            .map(|rollups| rollups.into_iter().map(to_api_usage_rollup).collect())
            .map_err(map_redb_err)
    }

    async fn query_usage_rollups_in_range(
        &self,
        resolution: ApiUsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<ApiUsageRollup>> {
        let storage = self.clone();
        let resolution = to_redb_usage_rollup_resolution(resolution);

        tokio::task::spawn_blocking(move || {
            Storage::query_usage_rollups_in_range(
                &storage,
                resolution,
                window_start_unix_secs,
                window_end_unix_secs,
            )
        })
        .await
        .map_err(map_join_err)?
        .map(|rollups| rollups.into_iter().map(to_api_usage_rollup).collect())
        .map_err(map_redb_err)
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || Storage::usage_rollup_checkpoint(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        run: &ApiUsageRollupRun,
    ) -> StorageResult<()> {
        let storage = self.clone();
        let _run = to_redb_usage_rollup_run(run);

        tokio::task::spawn_blocking(move || Storage::rollup_usage_once(&storage))
            .await
            .map_err(map_join_err)?
            .map(|_| ())
            .map_err(map_redb_err)
    }
}

fn to_redb_usage_rollup_resolution(
    resolution: ApiUsageRollupResolution,
) -> RedbUsageRollupResolution {
    match resolution {
        ApiUsageRollupResolution::Minute => RedbUsageRollupResolution::Minute,
        ApiUsageRollupResolution::Hour => RedbUsageRollupResolution::Hour,
    }
}

fn to_api_usage_rollup_resolution(
    resolution: RedbUsageRollupResolution,
) -> ApiUsageRollupResolution {
    match resolution {
        RedbUsageRollupResolution::Minute => ApiUsageRollupResolution::Minute,
        RedbUsageRollupResolution::Hour => ApiUsageRollupResolution::Hour,
    }
}

fn to_api_usage_rollup_key(key: RedbUsageRollupKey) -> ApiUsageRollupKey {
    ApiUsageRollupKey {
        resolution: to_api_usage_rollup_resolution(key.resolution),
        bucket_start: key.bucket_start,
        principal: key.principal,
        upstream: key.upstream,
        model: key.model,
    }
}

fn to_api_usage_rollup(rollup: RedbUsageRollup) -> ApiUsageRollup {
    let key = to_api_usage_rollup_key(RedbUsageRollupKey {
        resolution: rollup.resolution,
        bucket_start: rollup.bucket_start,
        principal: rollup.principal,
        upstream: rollup.upstream,
        model: rollup.model,
    });

    ApiUsageRollup {
        resolution: key.resolution,
        bucket_start: key.bucket_start,
        principal: key.principal,
        upstream: key.upstream,
        model: key.model,
        request_count: rollup.request_count,
        input_tokens: rollup.input_tokens,
        output_tokens: rollup.output_tokens,
        error_count: rollup.error_count,
        latency_count: rollup.latency_count,
        latency_ms_sum: rollup.latency_ms_sum,
        latency_ms_min: rollup.latency_ms_min,
        latency_ms_max: rollup.latency_ms_max,
        virtual_cost_micros: rollup.virtual_cost_micros,
    }
}

fn to_redb_usage_rollup_run(run: &ApiUsageRollupRun) -> RedbUsageRollupRun {
    RedbUsageRollupRun {
        processed_events: run.processed_events,
        updated_rollups: run.updated_rollups,
        checkpoint: run.checkpoint,
    }
}

fn to_api_usage_rollup_run(run: RedbUsageRollupRun) -> ApiUsageRollupRun {
    ApiUsageRollupRun {
        processed_events: run.processed_events,
        updated_rollups: run.updated_rollups,
        checkpoint: run.checkpoint,
    }
}
