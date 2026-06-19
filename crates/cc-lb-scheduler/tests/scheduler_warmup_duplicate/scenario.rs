use std::future::Future;
use std::time::Instant;

use cc_lb_scheduler::scheduler_metrics;
use cc_lb_scheduler::worker::EntityJob;
use metrics_exporter_prometheus::PrometheusHandle;

use crate::common::{
    FakeWarmupHttp, POLL_INTERVAL, SharedOutcomes, TestResult, WAIT_TIMEOUT,
    duplicate_metric_value, install_crash_hook_once, recorded_duplicate_effects,
};

pub async fn run_duplicate_scenario<Push, PushFuture, Count, CountFuture>(
    handle: &PrometheusHandle,
    job: EntityJob,
    http: &FakeWarmupHttp,
    outcomes: &SharedOutcomes,
    mut push_job: Push,
    mut effect_count: Count,
) -> TestResult<()>
where
    Push: FnMut(EntityJob) -> PushFuture,
    PushFuture: Future<Output = TestResult<()>>,
    Count: FnMut() -> CountFuture,
    CountFuture: Future<Output = TestResult<i64>>,
{
    scheduler_metrics::touch_scheduler_metric_handles();
    let duplicate_before = duplicate_metric_value(handle);
    install_crash_hook_once();

    push_job(job.clone()).await?;
    http.wait_for_count(1).await?;
    push_job(job.clone()).await?;
    http.wait_for_count(2).await?;
    wait_for_effect_count(&mut effect_count, 1).await?;
    push_job(job).await?;
    wait_for_duplicate_metric(handle, duplicate_before + 1.0).await?;

    assert_eq!(http.count(), 2);
    assert_eq!(effect_count().await?, 1);
    assert_eq!(recorded_duplicate_effects(outcomes)?, 1);
    Ok(())
}

async fn wait_for_effect_count<Count, CountFuture>(
    effect_count: &mut Count,
    expected: i64,
) -> TestResult<()>
where
    Count: FnMut() -> CountFuture,
    CountFuture: Future<Output = TestResult<i64>>,
{
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        if effect_count().await? == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {expected} warmup_effects rows").into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_duplicate_metric(handle: &PrometheusHandle, expected: f64) -> TestResult<()> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        if duplicate_metric_value(handle) == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for duplicate_effect metric {expected}; metrics:\n{}",
                handle.render()
            )
            .into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
