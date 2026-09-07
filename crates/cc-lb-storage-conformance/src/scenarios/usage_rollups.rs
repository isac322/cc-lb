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
