#![allow(clippy::manual_async_fn, clippy::too_many_arguments)]

use std::future::Future;
use std::sync::Arc;

use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerBackend, SchedulerPushTask};
use cc_lb_server::refresh::{LazyRefreshClaimGuard, LazyRefreshTaskState, LazyRefresher};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::StorageResult;
use tokio::sync::watch;
use uuid::Uuid;

use super::common::{POLL_INTERVAL, TestResult, WAIT_TIMEOUT};
use super::fake::FakeAnthropic;

pub struct ClaimContentionObserver {
    inner: Arc<dyn LazyRefreshClaimGuard>,
    observed: watch::Sender<Option<String>>,
}

impl ClaimContentionObserver {
    pub fn new(inner: Arc<dyn LazyRefreshClaimGuard>) -> Arc<Self> {
        let (observed, _) = watch::channel(None);
        Arc::new(Self { inner, observed })
    }

    pub async fn wait(&self) -> String {
        let mut observed = self.observed.subscribe();
        loop {
            let idempotency_key = observed.borrow().clone();
            if let Some(idempotency_key) = idempotency_key {
                return idempotency_key;
            }
            observed
                .changed()
                .await
                .expect("claim contention observer remains alive");
        }
    }
}

#[async_trait::async_trait]
impl LazyRefreshClaimGuard for ClaimContentionObserver {
    async fn begin_refresh(
        &self,
        upstream_id: Uuid,
        expected_generation: u64,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<String> {
        let idempotency_key = self
            .inner
            .begin_refresh(
                upstream_id,
                expected_generation,
                expires_at_unix_secs,
                now_unix_secs,
            )
            .await?;
        self.observed.send_replace(Some(idempotency_key.clone()));
        Ok(idempotency_key)
    }

    async fn task_state(&self, idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        self.inner.task_state(idempotency_key).await
    }
}

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
    contention: Arc<ClaimContentionObserver>,
    upstream_id: Uuid,
    expires_at_unix_secs: u64,
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

    let job = OAuthRefreshJob::for_generation(upstream_id, upstream_before);
    backend
        .push_adaptive_task(SchedulerPushTask {
            args: AdaptiveJob::OAuthRefresh(job.clone()),
            idempotency_key: Some(format!(
                "adaptive:oauth_refresh:{upstream_id}:proactive-race"
            )),
            run_at_unix_secs: Some(OAuthRefreshJob::run_at_for_expires_at(expires_at_unix_secs)),
            max_attempts: None,
        })
        .await?;
    fake.wait_for_refresh_request().await?;
    if let Err(error) = wait_for_oauth_job_running(&mut oauth_job_running).await {
        fake.release_refresh_response();
        return Err(error);
    }

    let lazy_refresh = tokio::spawn(async move { lazy.refresh_one(upstream_id).await });
    let lazy_idempotency_key = contention.wait().await;
    assert_eq!(
        fake.refresh_history_len().await?,
        1,
        "only the scheduled lease holder may call the provider while the response is held",
    );
    fake.release_refresh_response();

    lazy_refresh
        .await
        .map_err(|error| format!("lazy refresh task failed to join: {error}"))?
        .map_err(|error| format!("lazy refresh failed: {error}"))?;
    wait_for_oauth_task_done(contention.as_ref(), &lazy_idempotency_key).await?;
    wait_for_metadata_count(&mut metadata_count, 1).await?;

    let refresh_count = fake.refresh_history_len().await?;
    let upstream_after = read_upstream_generation().await?;
    let metadata_after = metadata_count().await?;

    assert_eq!(refresh_count, 1, "fake refresh HTTP count");
    assert_generation_incremented_by(
        "upstreams_v1.oauth_token_generation",
        upstream_before,
        upstream_after,
        1,
    )?;
    assert_eq!(metadata_after, 1, "metadata refresh enqueue count");
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

async fn wait_for_oauth_task_done(
    claim_guard: &dyn LazyRefreshClaimGuard,
    idempotency_key: &str,
) -> TestResult<()> {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        match claim_guard.task_state(idempotency_key).await? {
            LazyRefreshTaskState::Done => return Ok(()),
            LazyRefreshTaskState::TerminalFailure { reason } => {
                return Err(
                    format!("lazy-seeded OAuthRefreshJob failed permanently: {reason}").into(),
                );
            }
            LazyRefreshTaskState::Active => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for lazy-seeded OAuthRefreshJob to finish".into());
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
