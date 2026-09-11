use std::sync::Arc;

use anyhow::Result;
use cc_lb_storage_api::{
    ApiKeyUsage, ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey, ApiKeyUsageBucketQuery,
    ApiKeyUsageBucketStore, ApiKeyUsageFlush, ApiKeyUsageFlushResult,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn flush_idempotency_aggregation_and_range_boundary<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let first_writer = Uuid::from_u128(1);
        let second_writer = Uuid::from_u128(2);
        let bucket_key = ApiKeyUsageBucketKey {
            key_id: "key-a".to_owned(),
            bucket_width_secs: 60,
            bucket_start_unix_secs: 1_699_999_980,
        };
        let first_flush = ApiKeyUsageFlush {
            writer_epoch: first_writer,
            flush_id: Uuid::from_u128(11),
            lease_until_unix_secs: 1_700_000_100,
            deltas: vec![ApiKeyUsageBucketDelta {
                key: bucket_key.clone(),
                usage: ApiKeyUsage {
                    requests: 1,
                    input_tokens: 10,
                    output_tokens: 20,
                    cost_usd_micros: 30,
                },
            }],
        };

        anyhow::ensure!(
            storage.flush_api_key_usage(&first_flush).await?
                == ApiKeyUsageFlushResult::LeaseLost,
            "an unregistered writer must not flush usage"
        );

        storage
            .register_api_key_usage_writer(first_writer, 1_700_000_100)
            .await?;
        storage
            .register_api_key_usage_writer(second_writer, 1_700_000_100)
            .await?;
        anyhow::ensure!(
            storage.flush_api_key_usage(&first_flush).await?
                == ApiKeyUsageFlushResult::Applied,
            "the first registered flush must apply"
        );
        anyhow::ensure!(
            storage.flush_api_key_usage(&first_flush).await?
                == ApiKeyUsageFlushResult::AlreadyApplied,
            "replaying a flush id must be idempotent"
        );

        let second_flush = ApiKeyUsageFlush {
            writer_epoch: second_writer,
            flush_id: Uuid::from_u128(12),
            lease_until_unix_secs: 1_700_000_100,
            deltas: vec![ApiKeyUsageBucketDelta {
                key: bucket_key.clone(),
                usage: ApiKeyUsage {
                    requests: 2,
                    input_tokens: 20,
                    output_tokens: 40,
                    cost_usd_micros: 60,
                },
            }],
        };
        anyhow::ensure!(
            storage.flush_api_key_usage(&second_flush).await?
                == ApiKeyUsageFlushResult::Applied,
            "the second writer flush must apply"
        );

        let excluding_first = storage
            .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                key_ids: vec![bucket_key.key_id.clone()],
                since_unix_secs: 1_700_000_039,
                until_unix_secs: 1_700_000_040,
                exclude_writer_epoch: first_writer,
            })
            .await?;
        anyhow::ensure!(
            excluding_first.len() == 1 && excluding_first[0].usage == second_flush.deltas[0].usage,
            "writer exclusion must leave only the other writer's usage: {excluding_first:?}"
        );

        let aggregated = storage
            .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                key_ids: vec![bucket_key.key_id.clone()],
                since_unix_secs: 1_700_000_039,
                until_unix_secs: 1_700_000_040,
                exclude_writer_epoch: Uuid::nil(),
            })
            .await?;
        anyhow::ensure!(
            aggregated
                == vec![cc_lb_storage_api::ApiKeyUsageBucket {
                    key: bucket_key.clone(),
                    usage: ApiKeyUsage {
                        requests: 3,
                        input_tokens: 30,
                        output_tokens: 60,
                        cost_usd_micros: 90,
                    },
                }],
            "queries must aggregate matching buckets across writers without replaying a duplicate flush: {aggregated:?}"
        );

        let boundary = storage
            .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                key_ids: vec![bucket_key.key_id],
                since_unix_secs: 1_700_000_040,
                until_unix_secs: 1_700_000_040,
                exclude_writer_epoch: Uuid::nil(),
            })
            .await?;
        anyhow::ensure!(
            boundary.is_empty(),
            "a bucket ending exactly at since_unix_secs must be excluded"
        );

        Ok(())
    })
    .await
}
