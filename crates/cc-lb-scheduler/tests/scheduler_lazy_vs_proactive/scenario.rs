#![allow(clippy::manual_async_fn, clippy::too_many_arguments)]

use std::future::Future;

use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerBackend};
use cc_lb_server::refresh::LazyRefresher;
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use uuid::Uuid;

use super::common::{TestResult, WAIT_TIMEOUT};
use super::fake::FakeAnthropic;
use super::worker::OAuthWorkerProbe;

pub async fn run_race_scenario<
    ReadUpstream,
    ReadUpstreamFuture,
    CountMetadata,
    CountMetadataFuture,
>(
    fake: &FakeAnthropic,
    backend: SchedulerBackend,
    lazy: LazyRefresher,
    upstream_id: Uuid,
    mut read_upstream_generation: ReadUpstream,
    mut metadata_count: CountMetadata,
    oauth_probe: &OAuthWorkerProbe,
) -> TestResult<()>
where
    ReadUpstream: FnMut() -> ReadUpstreamFuture,
    ReadUpstreamFuture: Future<Output = TestResult<u64>>,
    CountMetadata: FnMut() -> CountMetadataFuture,
    CountMetadataFuture: Future<Output = TestResult<i64>>,
{
    let upstream_before = read_upstream_generation().await?;

    backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)))
        .await?;
    fake.wait_for_refresh_request().await?;
    if let Err(error) = wait_for_oauth_job_state(oauth_probe, true).await {
        fake.release_refresh_response();
        return Err(error);
    }
    fake.release_refresh_response();
    wait_for_metadata_enqueue(oauth_probe, 1).await?;
    wait_for_oauth_job_state(oauth_probe, false).await?;
    lazy.refresh_one(upstream_id)
        .await
        .map_err(|error| format!("lazy refresh failed: {error}"))?;
    wait_for_metadata_enqueue(oauth_probe, 2).await?;

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

async fn wait_for_oauth_job_state(
    probe: &OAuthWorkerProbe,
    expected_running: bool,
) -> TestResult<()> {
    if probe.wait_for_running(expected_running, WAIT_TIMEOUT).await {
        return Ok(());
    }

    Err(format!(
        "timed out waiting for OAuthRefreshJob running={expected_running}; last state: {:?}",
        probe.state()
    )
    .into())
}

async fn wait_for_metadata_enqueue(probe: &OAuthWorkerProbe, expected: usize) -> TestResult<()> {
    if probe
        .wait_for_metadata_enqueued(expected, WAIT_TIMEOUT)
        .await
    {
        return Ok(());
    }

    Err(format!(
        "timed out waiting for MetadataRefreshJob enqueue count={expected}; last state: {:?}",
        probe.state()
    )
    .into())
}
