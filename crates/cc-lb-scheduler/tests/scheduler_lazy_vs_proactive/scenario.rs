#![allow(deprecated, clippy::manual_async_fn, clippy::too_many_arguments)]

use std::future::Future;

use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{EntityJob, SchedulerBackend};
use cc_lb_server::refresh::LazyRefresher;
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use uuid::Uuid;

use crate::common::{
    LAZY_REQUEST_DELAY, LOSER_SETTLE_DELAY, POLL_INTERVAL, TestResult, WAIT_TIMEOUT,
    assert_generation_incremented_by_one,
};
use crate::fake::FakeAnthropic;

pub async fn run_race_scenario<
    ReadClaim,
    ReadClaimFuture,
    ReadUpstream,
    ReadUpstreamFuture,
    CountMetadata,
    CountMetadataFuture,
    OAuthRunning,
    OAuthRunningFuture,
>(
    fake: &FakeAnthropic,
    backend: SchedulerBackend,
    lazy: LazyRefresher,
    upstream_id: Uuid,
    mut read_claim_generation: ReadClaim,
    mut read_upstream_generation: ReadUpstream,
    mut metadata_count: CountMetadata,
    mut oauth_job_running: OAuthRunning,
) -> TestResult<()>
where
    ReadClaim: FnMut() -> ReadClaimFuture,
    ReadClaimFuture: Future<Output = TestResult<u64>>,
    ReadUpstream: FnMut() -> ReadUpstreamFuture,
    ReadUpstreamFuture: Future<Output = TestResult<u64>>,
    CountMetadata: FnMut() -> CountMetadataFuture,
    CountMetadataFuture: Future<Output = TestResult<i64>>,
    OAuthRunning: FnMut() -> OAuthRunningFuture,
    OAuthRunningFuture: Future<Output = TestResult<bool>>,
{
    let claim_before = read_claim_generation().await?;
    let upstream_before = read_upstream_generation().await?;

    backend
        .push_job(EntityJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)))
        .await?;
    tokio::time::sleep(LAZY_REQUEST_DELAY).await;
    let lazy_task = tokio::spawn(async move { lazy.refresh_one(upstream_id).await });

    fake.wait_for_refresh_request().await?;
    if let Err(error) = wait_for_oauth_job_running(&mut oauth_job_running).await {
        fake.release_refresh_response();
        return Err(error);
    }
    tokio::time::sleep(LOSER_SETTLE_DELAY).await;
    fake.release_refresh_response();

    lazy_task
        .await
        .map_err(|error| format!("lazy refresh task join failed: {error}"))?
        .map_err(|error| format!("lazy refresh failed: {error}"))?;
    wait_for_metadata_count(&mut metadata_count).await?;

    let refresh_count = fake.refresh_history_len().await?;
    let claim_after = read_claim_generation().await?;
    let upstream_after = read_upstream_generation().await?;
    let metadata_after = metadata_count().await?;

    assert_eq!(refresh_count, 1, "fake refresh HTTP count");
    assert_generation_incremented_by_one(
        "oauth_refresh_claims.generation",
        claim_before,
        claim_after,
    )?;
    assert_generation_incremented_by_one(
        "upstreams_v1.oauth_token_generation",
        upstream_before,
        upstream_after,
    )?;
    assert_eq!(metadata_after, 1, "metadata refresh enqueue count");
    Ok(())
}

async fn wait_for_oauth_job_running<Check, CheckFuture>(check: &mut Check) -> TestResult<()>
where
    Check: FnMut() -> CheckFuture,
    CheckFuture: Future<Output = TestResult<bool>>,
{
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        if check().await? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for OAuthRefreshJob to run".into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_metadata_count<Count, CountFuture>(count: &mut Count) -> TestResult<()>
where
    Count: FnMut() -> CountFuture,
    CountFuture: Future<Output = TestResult<i64>>,
{
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let current = count().await?;
        if current == 1 {
            return Ok(());
        }
        if current > 1 {
            return Err(format!("metadata refresh enqueued {current} rows, expected 1").into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for MetadataRefreshJob enqueue".into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
