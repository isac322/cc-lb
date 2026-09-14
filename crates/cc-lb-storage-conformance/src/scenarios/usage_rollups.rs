use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, RequestEventUpstream, UpstreamCreate, UpstreamStore,
    UsageRollupResolution, UsageRollupStore, upstream::UpstreamKind,
};
use uuid::Uuid;

use super::super::{ConformanceBackend, ConformanceFixture};

const BUCKET_START: u64 = 1_800_020_040;
const RANGE_END: u64 = BUCKET_START + 120;

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let wait_backend = Arc::clone(&backend);
    let mut fixture = ConformanceFixture::new(backend).await?;
    let scenario_result =
        filtered_usage_rollups_refresh_after_upsert(wait_backend.as_ref(), &fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;
    Ok(())
}

pub async fn checkpoint_is_absent_before_first_rollup<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result = async {
        ensure!(
            fixture.storage().usage_rollup_checkpoint().await?.is_none(),
            "a fresh storage must not fabricate a usage-rollup checkpoint"
        );
        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn empty_queries_preserve_range_boundaries<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result = async {
        let storage = fixture.storage();
        ensure!(
            storage.query_usage_rollups().await?.is_empty(),
            "a fresh storage must not fabricate usage rollups"
        );
        ensure!(
            storage
                .query_usage_rollups_in_range(
                    UsageRollupResolution::Minute,
                    BUCKET_START,
                    RANGE_END,
                )
                .await?
                .is_empty(),
            "an empty matching range must return no usage rollups"
        );
        ensure!(
            storage
                .query_usage_rollups_in_range(UsageRollupResolution::Hour, RANGE_END, BUCKET_START,)
                .await?
                .is_empty(),
            "an inverted usage-rollup range must return no rows"
        );
        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn filtered_usage_rollups_refresh_after_upsert<B>(
    backend: &B,
    fixture: &ConformanceFixture<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    let storage = fixture.storage();
    let selected = create_upstream(storage.as_ref(), "usage-rollup-selected").await?;
    let excluded = create_upstream(storage.as_ref(), "usage-rollup-excluded").await?;

    storage
        .append_request_event(&usage_event(
            BUCKET_START + 5,
            "usage-rollup-selected-initial",
            selected,
            "usage-rollup-selected",
            10,
        ))
        .await?;
    storage
        .append_request_event(&usage_event(
            BUCKET_START + 5,
            "usage-rollup-excluded-initial",
            excluded,
            "usage-rollup-excluded",
            1_000,
        ))
        .await?;
    backend
        .wait_for_events_visible_for_rollup(storage.as_ref())
        .await?;
    storage.rollup_usage_once().await?;

    let all_rollups = storage
        .query_usage_rollups_in_range(UsageRollupResolution::Minute, BUCKET_START, RANGE_END)
        .await?;
    ensure!(
        all_rollups
            .iter()
            .any(|rollup| rollup.upstream_id == excluded),
        "fixture must contain the excluded upstream's minute rollup"
    );

    let initial = storage
        .query_usage_rollups_for_upstreams_in_range(
            &[selected],
            UsageRollupResolution::Minute,
            BUCKET_START,
            RANGE_END,
        )
        .await?;
    ensure!(
        initial.len() == 1,
        "filtered point-in-time query should return one selected row, got {}",
        initial.len()
    );
    ensure!(
        initial[0].upstream_id == selected
            && initial[0].bucket_start == BUCKET_START
            && initial[0].request_count == 1
            && initial[0].input_tokens == 10,
        "filtered point-in-time row did not preserve the selected upstream values: {:?}",
        initial[0]
    );

    storage
        .append_request_event(&usage_event(
            BUCKET_START + 25,
            "usage-rollup-selected-upsert",
            selected,
            "usage-rollup-selected",
            20,
        ))
        .await?;
    storage
        .append_request_event(&usage_event(
            BUCKET_START + 65,
            "usage-rollup-selected-next-bucket",
            selected,
            "usage-rollup-selected",
            30,
        ))
        .await?;
    backend
        .wait_for_events_visible_for_rollup(storage.as_ref())
        .await?;
    storage.rollup_usage_once().await?;

    let refreshed = storage
        .query_usage_rollups_for_upstreams_in_range(
            &[selected],
            UsageRollupResolution::Minute,
            BUCKET_START,
            RANGE_END,
        )
        .await?;
    ensure!(
        refreshed.len() == 2,
        "filtered transition query should return the updated and new selected rows, got {}",
        refreshed.len()
    );
    ensure!(
        refreshed
            .iter()
            .all(|rollup| rollup.upstream_id == selected),
        "filtered transition query leaked an excluded upstream: {refreshed:?}"
    );
    let updated_bucket = refreshed
        .iter()
        .find(|rollup| rollup.bucket_start == BUCKET_START)
        .ok_or_else(|| anyhow::anyhow!("updated selected bucket is missing"))?;
    ensure!(
        updated_bucket.request_count == 2 && updated_bucket.input_tokens == 30,
        "existing selected bucket did not reflect the upsert: {updated_bucket:?}"
    );
    let new_bucket = refreshed
        .iter()
        .find(|rollup| rollup.bucket_start == BUCKET_START + 60)
        .ok_or_else(|| anyhow::anyhow!("new selected bucket is missing"))?;
    ensure!(
        new_bucket.request_count == 1 && new_bucket.input_tokens == 30,
        "new selected bucket did not appear after refresh: {new_bucket:?}"
    );

    ensure!(
        storage
            .query_usage_rollups_for_upstreams_in_range(
                &[],
                UsageRollupResolution::Minute,
                BUCKET_START,
                RANGE_END,
            )
            .await?
            .is_empty(),
        "empty upstream ids must return no rows"
    );
    ensure!(
        storage
            .query_usage_rollups_for_upstreams_in_range(
                &[selected],
                UsageRollupResolution::Minute,
                RANGE_END,
                BUCKET_START,
            )
            .await?
            .is_empty(),
        "an inverted range must return no rows"
    );

    Ok(())
}

pub async fn overview_excluded_error_buckets_preserve_boundaries<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let wait_backend = Arc::clone(&backend);
    let mut fixture = ConformanceFixture::new(backend).await?;
    let scenario_result = async {
        let storage = fixture.storage();
        let upstream_id =
            create_upstream(storage.as_ref(), "overview-excluded-error-upstream").await?;
        let statuses = [401, 403, 404, 400, 500, 200];
        for (index, status) in statuses.into_iter().enumerate() {
            let mut event = usage_event(
                BUCKET_START + 5,
                &format!("overview-excluded-error-{status}-{index}"),
                upstream_id,
                "overview-excluded-error-upstream",
                0,
            );
            event.status = status;
            storage.append_request_event(&event).await?;
        }
        let mut next_bucket = usage_event(
            BUCKET_START + 65,
            "overview-excluded-error-next-bucket",
            upstream_id,
            "overview-excluded-error-upstream",
            0,
        );
        next_bucket.status = 404;
        storage.append_request_event(&next_bucket).await?;

        wait_backend
            .wait_for_events_visible_for_rollup(storage.as_ref())
            .await?;
        storage.rollup_usage_once().await?;

        let buckets = storage
            .query_overview_excluded_error_buckets_in_range(
                UsageRollupResolution::Minute,
                BUCKET_START,
                RANGE_END,
            )
            .await?;
        ensure!(
            buckets.len() == 2
                && buckets[0].bucket_start == BUCKET_START
                && buckets[0].error_count == 3
                && buckets[1].bucket_start == BUCKET_START + 60
                && buckets[1].error_count == 1,
            "overview excluded-error rollups must count only 401/403/404 and order minute buckets ascending: {buckets:?}"
        );

        let first_bucket_only = storage
            .query_overview_excluded_error_buckets_in_range(
                UsageRollupResolution::Minute,
                BUCKET_START,
                BUCKET_START + 60,
            )
            .await?;
        ensure!(
            first_bucket_only.len() == 1
                && first_bucket_only[0].bucket_start == BUCKET_START
                && first_bucket_only[0].error_count == 3,
            "overview excluded-error range end must be exclusive"
        );
        ensure!(
            storage
                .query_overview_excluded_error_buckets_in_range(
                    UsageRollupResolution::Minute,
                    RANGE_END,
                    BUCKET_START,
                )
                .await?
                .is_empty(),
            "an inverted overview excluded-error range must return empty"
        );
        ensure!(
            storage
                .query_overview_excluded_error_buckets_in_range(
                    UsageRollupResolution::Hour,
                    RANGE_END,
                    RANGE_END + 3_600,
                )
                .await?
                .is_empty(),
            "an overview excluded-error range without matching buckets must return empty"
        );
        Ok(())
    }
    .await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result
}

async fn create_upstream<S>(storage: &S, name: &str) -> Result<Uuid>
where
    S: UpstreamStore + ?Sized,
{
    Ok(storage
        .create(UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        })
        .await?
        .id)
}

fn usage_event(
    ts: u64,
    request_id: &str,
    upstream_id: Uuid,
    upstream_name: &str,
    input_tokens: u64,
) -> RequestEvent {
    RequestEvent {
        ts,
        ts_ms: Some(ts.saturating_mul(1_000)),
        request_id: request_id.to_owned(),
        principal_id: Some("usage-rollup-principal".to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(input_tokens),
        output_tokens: Some(0),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(0),
        duration_ms: 10,
        error_code: None,
        ..Default::default()
    }
}
