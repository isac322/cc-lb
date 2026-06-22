#![allow(deprecated, clippy::manual_async_fn, clippy::too_many_arguments)]

use std::future::Future;

use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerBackend};
use cc_lb_server::refresh::LazyRefresher;
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use uuid::Uuid;

use crate::common::{
    LAZY_REQUEST_DELAY, LOSER_SETTLE_DELAY, POLL_INTERVAL, TestResult, WAIT_TIMEOUT,
};
use crate::fake::FakeAnthropic;

pub async fn run_race_scenario<
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
    mut read_upstream_generation: ReadUpstream,
    mut metadata_count: CountMetadata,
    mut oauth_job_running: OAuthRunning,
) -> TestResult<()>
where
    ReadUpstream: FnMut() -> ReadUpstreamFuture,
    ReadUpstreamFuture: Future<Output = TestResult<u64>>,
    CountMetadata: FnMut() -> CountMetadataFuture,
    CountMetadataFuture: Future<Output = TestResult<i64>>,
    OAuthRunning: FnMut() -> OAuthRunningFuture,
    OAuthRunningFuture: Future<Output = TestResult<bool>>,
{
    let upstream_before = read_upstream_generation().await?;

    backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)))
        .await?;
    fake.wait_for_refresh_request().await?;
    if let Err(error) = wait_for_oauth_job_running(&mut oauth_job_running).await {
        fake.release_refresh_response();
        return Err(error);
    }
    fake.release_refresh_response();
    wait_for_metadata_count(&mut metadata_count, 1).await?;
    tokio::time::sleep(LAZY_REQUEST_DELAY + LOSER_SETTLE_DELAY).await;
    lazy.refresh_one(upstream_id)
        .await
        .map_err(|error| format!("lazy refresh failed: {error}"))?;
    wait_for_metadata_count(&mut metadata_count, 2).await?;

    let refresh_count = fake.refresh_history_len().await?;
    let upstream_after = read_upstream_generation().await?;
    let metadata_after = metadata_count().await?;

    assert_eq!(refresh_count, 2, "fake refresh HTTP count");
    assert_generation_incremented_by(
        "upstreams_v1.oauth_token_generation",
        upstream_before,
        upstream_after,
        2,
    )?;
    assert_eq!(metadata_after, 2, "metadata refresh enqueue count");
    Ok(())
}

fn assert_generation_incremented_by(
    label: &str,
    before: u64,
    after: u64,
    delta: u64,
) -> TestResult<()> {
    let expected = before.saturating_add(delta);
    if after != expected {
        return Err(
            format!("{label} changed from {before} to {after}, expected {expected}").into(),
        );
    }
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

async fn wait_for_metadata_count<Count, CountFuture>(
    count: &mut Count,
    expected: i64,
) -> TestResult<()>
where
    Count: FnMut() -> CountFuture,
    CountFuture: Future<Output = TestResult<i64>>,
{
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let current = count().await?;
        if current == expected {
            return Ok(());
        }
        if current > expected {
            return Err(
                format!("metadata refresh enqueued {current} rows, expected {expected}").into(),
            );
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for MetadataRefreshJob enqueue".into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
