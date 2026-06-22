//! Atomicity conformance scenarios.

use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RequestEventStore, UpstreamCreate, UpstreamStore, UpstreamUpdate, UsageRollupStore,
    types::{RequestEvent, RequestEventUpstream, UsageRollup, UsageRollupResolution},
    upstream::UpstreamKind,
};
use uuid::Uuid;

use super::super::{ConformanceBackend, ConformanceFixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let scenario_result = usage_rollup_idempotent(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    let mut fixture = ConformanceFixture::new(backend).await?;
    let scenario_result = usage_rollup_v2_preserves_upstream_id_across_renames(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

pub async fn usage_rollup_idempotent<B>(fixture: &ConformanceFixture<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let storage = fixture.storage();
    for event in usage_events() {
        storage.append_request_event(&event).await?;
    }

    let first_run = storage.rollup_usage_once().await?;
    ensure!(
        first_run.processed_events == 4,
        "first rollup should process 4 events, got {}",
        first_run.processed_events
    );
    ensure!(
        first_run.updated_rollups == 5,
        "first rollup should update 5 rollup rows, got {}",
        first_run.updated_rollups
    );
    ensure!(
        first_run.checkpoint.is_some(),
        "first rollup should persist a checkpoint"
    );

    let rollups = storage.query_usage_rollups().await?;
    ensure!(
        rollups.len() == 5,
        "expected 5 rollup rows, got {}",
        rollups.len()
    );
    assert_rollup(
        find_rollup(
            &rollups,
            UsageRollupResolution::Minute,
            1_800_000_000,
            "usage-principal-a",
            Uuid::nil(),
            "anthropic_direct",
            "claude-sonnet-4-5",
        )?,
        ExpectedRollup {
            request_count: 2,
            input_tokens: 15,
            output_tokens: 20,
            error_count: 1,
            latency_count: 2,
            latency_ms_sum: 400,
            latency_ms_min: Some(100),
            latency_ms_max: Some(300),
        },
    )?;
    assert_rollup(
        find_rollup(
            &rollups,
            UsageRollupResolution::Hour,
            1_800_000_000,
            "usage-principal-a",
            Uuid::nil(),
            "anthropic_direct",
            "claude-sonnet-4-5",
        )?,
        ExpectedRollup {
            request_count: 3,
            input_tokens: 22,
            output_tokens: 28,
            error_count: 1,
            latency_count: 3,
            latency_ms_sum: 450,
            latency_ms_min: Some(50),
            latency_ms_max: Some(300),
        },
    )?;

    let second_run = storage.rollup_usage_once().await?;
    ensure!(
        second_run.processed_events == 0,
        "second rollup should process no events"
    );
    ensure!(
        second_run.updated_rollups == 0,
        "second rollup should update no rows"
    );
    ensure!(
        second_run.checkpoint == first_run.checkpoint,
        "idempotent rerun should keep the same checkpoint"
    );
    ensure!(
        storage.query_usage_rollups().await? == rollups,
        "idempotent rerun should not mutate existing rollups"
    );
    ensure!(
        storage.usage_rollup_checkpoint().await? == first_run.checkpoint,
        "stored checkpoint should match first rollup checkpoint"
    );

    Ok(())
}

pub async fn usage_rollup_v2_preserves_upstream_id_across_renames<B>(
    fixture: &ConformanceFixture<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    let storage = fixture.storage();
    let upstream = storage
        .create(UpstreamCreate {
            name: "alpha".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        })
        .await?;
    let upstream_id = upstream.id;

    storage
        .append_request_event(&request_event_with_upstream_identity(
            1_800_010_005,
            "usage-rename-alpha",
            "usage-rename-principal",
            upstream_id,
            "alpha",
        ))
        .await?;
    storage.rollup_usage_once().await?;

    let updated = storage
        .update(
            upstream_id,
            upstream.revision,
            UpstreamUpdate {
                name: Some("beta".to_owned()),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    ensure!(
        updated.id == upstream_id,
        "rename must keep upstream id stable"
    );

    storage
        .append_request_event(&request_event_with_upstream_identity(
            1_800_010_065,
            "usage-rename-beta",
            "usage-rename-principal",
            upstream_id,
            "beta",
        ))
        .await?;
    storage.rollup_usage_once().await?;

    let rollups = storage.query_usage_rollups().await?;
    let by_upstream_id = rollups
        .iter()
        .filter(|rollup| rollup.upstream_id == upstream_id)
        .collect::<Vec<_>>();
    let observed = by_upstream_id
        .iter()
        .map(|rollup| {
            (
                rollup.resolution,
                rollup.bucket_start,
                rollup.upstream_name.clone(),
            )
        })
        .collect::<Vec<_>>();
    ensure!(
        by_upstream_id.len() == 3,
        "expected two minute rows plus one merged hour row for renamed events, got {}",
        by_upstream_id.len()
    );
    ensure!(
        by_upstream_id
            .iter()
            .any(|rollup| rollup.bucket_start == 1_800_009_960 && rollup.upstream_name == "alpha"),
        "alpha rollup should keep stable id with alpha display name; observed {observed:?}"
    );
    ensure!(
        by_upstream_id
            .iter()
            .any(|rollup| rollup.bucket_start == 1_800_010_020 && rollup.upstream_name == "beta"),
        "beta rollup should keep stable id with beta display name"
    );

    Ok(())
}

#[derive(Clone, Copy)]
struct ExpectedRollup {
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    error_count: u64,
    latency_count: u64,
    latency_ms_sum: u64,
    latency_ms_min: Option<u64>,
    latency_ms_max: Option<u64>,
}

fn assert_rollup(rollup: &UsageRollup, expected: ExpectedRollup) -> Result<()> {
    ensure!(
        rollup.request_count == expected.request_count,
        "request_count mismatch"
    );
    ensure!(
        rollup.input_tokens == expected.input_tokens,
        "input_tokens mismatch"
    );
    ensure!(
        rollup.output_tokens == expected.output_tokens,
        "output_tokens mismatch"
    );
    ensure!(
        rollup.error_count == expected.error_count,
        "error_count mismatch"
    );
    ensure!(
        rollup.latency_count == expected.latency_count,
        "latency_count mismatch"
    );
    ensure!(
        rollup.latency_ms_sum == expected.latency_ms_sum,
        "latency_ms_sum mismatch"
    );
    ensure!(
        rollup.latency_ms_min == expected.latency_ms_min,
        "latency_ms_min mismatch"
    );
    ensure!(
        rollup.latency_ms_max == expected.latency_ms_max,
        "latency_ms_max mismatch"
    );

    Ok(())
}

fn find_rollup<'a>(
    rollups: &'a [UsageRollup],
    resolution: UsageRollupResolution,
    bucket_start: u64,
    principal: &str,
    upstream_id: Uuid,
    upstream: &str,
    model: &str,
) -> Result<&'a UsageRollup> {
    rollups
        .iter()
        .find(|rollup| {
            rollup.resolution == resolution
                && rollup.bucket_start == bucket_start
                && rollup.principal == principal
                && rollup.upstream_id == upstream_id
                && rollup.upstream_name == upstream
                && rollup.model == model
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing rollup for {:?}/{bucket_start}/{principal}/{upstream}/{model}",
                resolution
            )
        })
}

fn usage_events() -> Vec<RequestEvent> {
    vec![
        request_event(
            1_800_000_005,
            "usage-req-a-1",
            "usage-principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            200,
            Some(10),
            Some(20),
            100,
        ),
        request_event(
            1_800_000_030,
            "usage-req-a-2",
            "usage-principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            429,
            Some(5),
            None,
            300,
        ),
        request_event(
            1_800_000_065,
            "usage-req-a-3",
            "usage-principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            200,
            Some(7),
            Some(8),
            50,
        ),
        request_event(
            1_800_000_045,
            "usage-req-b-1",
            "usage-principal-b",
            RequestEventUpstream::AnthropicDirect,
            "claude-opus-4-1",
            500,
            None,
            Some(1),
            700,
        ),
    ]
}

#[allow(clippy::too_many_arguments)]
fn request_event(
    ts: u64,
    request_id: &str,
    principal: &str,
    upstream: RequestEventUpstream,
    model: &str,
    status: u16,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    duration_ms: u64,
) -> RequestEvent {
    RequestEvent {
        ts,
        request_id: request_id.to_owned(),
        principal_id: Some(principal.to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(upstream),
        model: Some(model.to_owned()),
        status,
        input_tokens,
        output_tokens,
        duration_ms,
        error_code: None,
        ..Default::default()
    }
}

fn request_event_with_upstream_identity(
    ts: u64,
    request_id: &str,
    principal: &str,
    upstream_id: Uuid,
    upstream_name: &str,
) -> RequestEvent {
    RequestEvent {
        ts,
        request_id: request_id.to_owned(),
        principal_id: Some(principal.to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(2),
        duration_ms: 10,
        error_code: None,
        ..Default::default()
    }
}
