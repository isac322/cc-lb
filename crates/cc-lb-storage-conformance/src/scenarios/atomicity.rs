//! Atomicity conformance scenarios.

use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    LimitStateStore, QuotaStore, RequestEventStore, UsageRollupStore,
    types::{
        BucketKind, PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState,
        RequestEvent, RequestEventUpstream, UsageRollup, UsageRollupResolution,
    },
};
use futures::future::try_join_all;

use super::super::{ConformanceBackend, ConformanceFixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let scenario_result = quota_atomic_rmw(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let scenario_result = quota_try_incr_capacity(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let scenario_result = quota_adjust_signed(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let scenario_result = quota_sweep_range(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let scenario_result = limit_state_upsert_idempotent(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let scenario_result = limit_state_list_by_principal(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    let mut fixture = ConformanceFixture::new(backend).await?;
    let scenario_result = usage_rollup_idempotent(&fixture).await;
    let teardown_result = fixture.teardown().await;
    scenario_result?;
    teardown_result?;

    Ok(())
}

pub async fn quota_atomic_rmw<B>(fixture: &ConformanceFixture<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    const PRINCIPAL_ID: &str = "quota-atomic-rmw-principal";
    const WINDOW_START: u64 = 1_800_100_000;
    const INCREMENTS: u64 = 100;

    let futures = (0..INCREMENTS).map(|_| {
        let storage = fixture.storage();

        async move {
            storage
                .incr_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::Requests, 1)
                .await
        }
    });

    let observed_totals = try_join_all(futures).await?;
    ensure!(
        observed_totals.len() == INCREMENTS as usize,
        "expected {INCREMENTS} quota increments, observed {}",
        observed_totals.len()
    );
    ensure!(
        observed_totals.iter().copied().max() == Some(INCREMENTS),
        "largest observed counter should equal final sum {INCREMENTS}"
    );

    let final_total = fixture
        .storage()
        .get_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::Requests)
        .await?;
    ensure!(
        final_total == INCREMENTS,
        "final quota should equal sum {INCREMENTS}, got {final_total}"
    );

    Ok(())
}

pub async fn quota_try_incr_capacity<B>(fixture: &ConformanceFixture<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    const PRINCIPAL_ID: &str = "quota-capacity-principal";
    const WINDOW_START: u64 = 1_800_100_060;

    let storage = fixture.storage();
    ensure!(
        storage
            .try_incr_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::InputTokens, 4, 10)
            .await?
            == Some(4),
        "first capacity increment should store 4"
    );
    ensure!(
        storage
            .try_incr_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::InputTokens, 5, 10)
            .await?
            == Some(9),
        "second capacity increment should store 9"
    );
    ensure!(
        storage
            .try_incr_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::InputTokens, 1, 10)
            .await?
            == Some(10),
        "boundary capacity increment should store 10"
    );
    ensure!(
        storage
            .try_incr_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::InputTokens, 1, 10)
            .await?
            .is_none(),
        "increment after capacity boundary should return None"
    );
    ensure!(
        storage
            .get_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::InputTokens)
            .await?
            == 10,
        "failed capacity increment must not change stored total"
    );

    Ok(())
}

pub async fn quota_adjust_signed<B>(fixture: &ConformanceFixture<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    const PRINCIPAL_ID: &str = "quota-adjust-principal";
    const WINDOW_START: u64 = 1_800_100_120;

    let storage = fixture.storage();
    ensure!(
        storage
            .incr_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::OutputTokens, 10)
            .await?
            == 10,
        "initial quota increment should store 10"
    );
    ensure!(
        storage
            .adjust_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::OutputTokens, 5)
            .await?
            == 15,
        "positive quota adjustment should store 15"
    );
    ensure!(
        storage
            .adjust_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::OutputTokens, -4)
            .await?
            == 11,
        "negative quota adjustment should store 11"
    );
    ensure!(
        storage
            .adjust_quota(PRINCIPAL_ID, WINDOW_START, BucketKind::OutputTokens, -11)
            .await?
            == 0,
        "negative quota adjustment to zero should store 0"
    );

    Ok(())
}

pub async fn quota_sweep_range<B>(fixture: &ConformanceFixture<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let storage = fixture.storage();
    storage
        .incr_quota("quota-sweep-a", 100, BucketKind::Requests, 3)
        .await?;
    storage
        .incr_quota("quota-sweep-a", 200, BucketKind::InputTokens, 5)
        .await?;
    storage
        .incr_quota("quota-sweep-a", 300, BucketKind::OutputTokens, 7)
        .await?;
    storage
        .incr_quota("quota-sweep-b", 400, BucketKind::Requests, 11)
        .await?;

    let deleted = storage.sweep_old_quotas(300).await?;
    ensure!(
        deleted == 2,
        "sweep should delete 2 old quota rows, got {deleted}"
    );
    ensure!(
        storage
            .get_quota("quota-sweep-a", 100, BucketKind::Requests)
            .await?
            == 0,
        "window below sweep boundary should be removed"
    );
    ensure!(
        storage
            .get_quota("quota-sweep-a", 200, BucketKind::InputTokens)
            .await?
            == 0,
        "second window below sweep boundary should be removed"
    );
    ensure!(
        storage
            .get_quota("quota-sweep-a", 300, BucketKind::OutputTokens)
            .await?
            == 7,
        "window equal to sweep boundary should remain"
    );
    ensure!(
        storage
            .get_quota("quota-sweep-b", 400, BucketKind::Requests)
            .await?
            == 11,
        "window above sweep boundary should remain"
    );

    Ok(())
}

pub async fn limit_state_upsert_idempotent<B>(fixture: &ConformanceFixture<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let storage = fixture.storage();
    let original = limit_state(
        "limit-upsert-principal",
        PrincipalLimitIdentityKind::Credential,
        Some("credential-a"),
        false,
        "weekly",
        PrincipalLimitKind::Tokens,
        Some(100_000),
        Some(99_990),
        Some("2026-05-24T00:00:01Z"),
        1_800_200_000,
        1_800_200_001,
    );
    let updated = limit_state(
        "limit-upsert-principal",
        PrincipalLimitIdentityKind::Credential,
        Some("credential-a"),
        false,
        "weekly",
        PrincipalLimitKind::Tokens,
        Some(100_000),
        Some(99_980),
        Some("2026-05-24T00:00:02Z"),
        1_800_200_010,
        1_800_200_011,
    );

    storage.put_principal_limit_state(&original).await?;
    storage.put_principal_limit_state(&original).await?;
    storage.put_principal_limit_state(&updated).await?;

    let stored = storage
        .get_principal_limit_state(
            "limit-upsert-principal",
            PrincipalLimitIdentityKind::Credential,
            Some("credential-a"),
            "weekly",
            PrincipalLimitKind::Tokens,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("updated principal limit state should exist"))?;
    ensure!(
        stored == updated,
        "upsert should keep latest state snapshot"
    );

    let listed = storage
        .list_principal_limit_states("limit-upsert-principal")
        .await?;
    ensure!(
        listed.len() == 1,
        "duplicate upserts should leave one state row"
    );
    ensure!(
        listed.first() == Some(&updated),
        "listed upsert row should match latest state"
    );

    Ok(())
}

pub async fn limit_state_list_by_principal<B>(fixture: &ConformanceFixture<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let storage = fixture.storage();
    let principal_account = limit_state(
        "limit-list-principal-a",
        PrincipalLimitIdentityKind::Account,
        Some("org-a"),
        true,
        "5h",
        PrincipalLimitKind::Requests,
        Some(1_000),
        Some(900),
        None,
        1_800_201_000,
        1_800_201_001,
    );
    let principal_credential = limit_state(
        "limit-list-principal-a",
        PrincipalLimitIdentityKind::Credential,
        Some("credential-a"),
        false,
        "weekly",
        PrincipalLimitKind::Tokens,
        Some(3_000),
        Some(2_900),
        None,
        1_800_201_010,
        1_800_201_011,
    );
    let other_principal = limit_state(
        "limit-list-principal-b",
        PrincipalLimitIdentityKind::Account,
        Some("org-b"),
        true,
        "5h",
        PrincipalLimitKind::Requests,
        Some(2_000),
        Some(1_900),
        None,
        1_800_201_020,
        1_800_201_021,
    );

    storage
        .put_principal_limit_state(&principal_account)
        .await?;
    storage
        .put_principal_limit_state(&principal_credential)
        .await?;
    storage.put_principal_limit_state(&other_principal).await?;

    let listed = storage
        .list_principal_limit_states("limit-list-principal-a")
        .await?;
    ensure!(
        listed.len() == 2,
        "principal-a should have exactly 2 states"
    );
    ensure!(
        listed
            .iter()
            .all(|state| state.principal_id == "limit-list-principal-a"),
        "listed states should all belong to the requested principal"
    );
    ensure!(
        listed.iter().any(|state| state == &principal_account),
        "listed states should include account state"
    );
    ensure!(
        listed.iter().any(|state| state == &principal_credential),
        "listed states should include credential state"
    );

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

#[allow(clippy::too_many_arguments)]
fn limit_state(
    principal_id: &str,
    identity_kind: PrincipalLimitIdentityKind,
    identity_value: Option<&str>,
    account_observed: bool,
    window: &str,
    kind: PrincipalLimitKind,
    limit: Option<u64>,
    remaining: Option<u64>,
    reset: Option<&str>,
    observed_at_unix_secs: u64,
    stored_at_unix_secs: u64,
) -> PrincipalLimitState {
    PrincipalLimitState {
        principal_id: principal_id.to_owned(),
        identity_kind,
        identity_value: identity_value.map(ToOwned::to_owned),
        account_observed,
        window: window.to_owned(),
        kind,
        limit,
        remaining,
        reset: reset.map(ToOwned::to_owned),
        observed_at_unix_secs,
        stored_at_unix_secs,
    }
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
    upstream: &str,
    model: &str,
) -> Result<&'a UsageRollup> {
    rollups
        .iter()
        .find(|rollup| {
            rollup.resolution == resolution
                && rollup.bucket_start == bucket_start
                && rollup.principal == principal
                && rollup.upstream == upstream
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
            RequestEventUpstream::CustomAnthropicSpec,
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
    }
}
