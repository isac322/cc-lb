use cc_lb_scheduler::jobs::oauth_usage_poll::{OAuthUsagePollJob, compute_next_run_at};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::state_stores::OAuthUsagePollCursor;
use cc_lb_scheduler::worker::EntityJob;
use sqlx::Database;
use std::future::Future;
use uuid::Uuid;

use crate::common::{
    FIRST_SUCCESS_AT, RESTART_NOW, SECOND_SUCCESS_AT, SUCCESS_INTERVAL_SECS, THROTTLE_AT,
    TestResult,
};
use crate::state::UsagePollWorkerState;

pub async fn drive_complete_cycle<Db, Push, PushFuture, Read, ReadFuture>(
    state: &UsagePollWorkerState<Db>,
    upstream_id: Uuid,
    mut push_job: Push,
    mut read_cursor: Read,
) -> TestResult<OAuthUsagePollCursor>
where
    Db: Database,
    Push: FnMut(EntityJob) -> PushFuture,
    PushFuture: Future<Output = TestResult<()>>,
    Read: FnMut() -> ReadFuture,
    ReadFuture: Future<Output = TestResult<Option<OAuthUsagePollCursor>>>,
{
    state.set_now(FIRST_SUCCESS_AT);
    state.enqueue_success(FIRST_SUCCESS_AT)?;
    push_job(oauth_usage_poll_job(upstream_id)).await?;
    assert_eq!(state.wait_for_outcome_count(1).await?, JobOutcome::Done);
    let first = require_cursor(read_cursor().await?, "first success")?;
    assert_success_cursor(&first, &[FIRST_SUCCESS_AT], &[], FIRST_SUCCESS_AT, 1);

    state.set_now(THROTTLE_AT);
    state.enqueue_throttle(THROTTLE_AT)?;
    push_job(oauth_usage_poll_job(upstream_id)).await?;
    assert_eq!(state.wait_for_outcome_count(2).await?, JobOutcome::Done);
    let throttled = require_cursor(read_cursor().await?, "throttle")?;
    assert_throttle_cursor(&throttled, &[FIRST_SUCCESS_AT], &[THROTTLE_AT], 2);

    state.set_now(SECOND_SUCCESS_AT);
    state.enqueue_success(SECOND_SUCCESS_AT)?;
    push_job(oauth_usage_poll_job(upstream_id)).await?;
    assert_eq!(state.wait_for_outcome_count(3).await?, JobOutcome::Done);
    let final_cursor = require_cursor(read_cursor().await?, "second success")?;
    assert_success_cursor(
        &final_cursor,
        &[FIRST_SUCCESS_AT, SECOND_SUCCESS_AT],
        &[THROTTLE_AT],
        SECOND_SUCCESS_AT,
        3,
    );
    assert_eq!(state.http_call_count(), 3);
    Ok(final_cursor)
}

pub async fn assert_restart_uses_persisted_cursor<Db, Push, PushFuture, Read, ReadFuture>(
    state: &UsagePollWorkerState<Db>,
    upstream_id: Uuid,
    snapshot: &OAuthUsagePollCursor,
    mut push_job: Push,
    mut read_cursor: Read,
) -> TestResult<()>
where
    Db: Database,
    Push: FnMut(EntityJob) -> PushFuture,
    PushFuture: Future<Output = TestResult<()>>,
    Read: FnMut() -> ReadFuture,
    ReadFuture: Future<Output = TestResult<Option<OAuthUsagePollCursor>>>,
{
    let calls_before = state.http_call_count();
    state.set_now(RESTART_NOW);
    push_job(oauth_usage_poll_job(upstream_id)).await?;
    assert_eq!(state.wait_for_outcome_count(1).await?, JobOutcome::Done);
    let oracle = compute_next_run_at(RESTART_NOW, state.config(), Some(snapshot));
    assert_eq!(
        oracle,
        SECOND_SUCCESS_AT.saturating_add(SUCCESS_INTERVAL_SECS)
    );
    assert!(oracle > RESTART_NOW);
    assert_eq!(state.http_call_count(), calls_before);
    let after_restart = require_cursor(read_cursor().await?, "restart")?;
    assert_eq!(after_restart, *snapshot);
    Ok(())
}

fn oauth_usage_poll_job(upstream_id: Uuid) -> EntityJob {
    EntityJob::OAuthUsagePoll(OAuthUsagePollJob {
        upstream_id,
        traceparent: None,
    })
}

fn require_cursor(
    cursor: Option<OAuthUsagePollCursor>,
    label: &str,
) -> TestResult<OAuthUsagePollCursor> {
    cursor.ok_or_else(|| format!("missing cursor after {label}").into())
}

fn assert_success_cursor(
    cursor: &OAuthUsagePollCursor,
    successes: &[u64],
    throttles: &[u64],
    observed_at: u64,
    attempt_count: u32,
) {
    assert_eq!(cursor.last_status, Some(200));
    assert_eq!(cursor.attempt_count, attempt_count);
    assert_eq!(cursor.last_observed_at_unix_secs, Some(observed_at));
    assert_eq!(
        cursor.last_window_start_unix_millis,
        Some(observed_at.saturating_mul(1_000))
    );
    assert_eq!(
        cursor.last_window_end_unix_millis,
        Some(observed_at.saturating_add(60).saturating_mul(1_000))
    );
    assert_eq!(cursor.recent_successes_unix_secs, successes);
    assert_eq!(cursor.recent_throttles_unix_secs, throttles);
}

fn assert_throttle_cursor(
    cursor: &OAuthUsagePollCursor,
    successes: &[u64],
    throttles: &[u64],
    attempt_count: u32,
) {
    assert_eq!(cursor.last_status, Some(429));
    assert_eq!(cursor.attempt_count, attempt_count);
    assert_eq!(cursor.last_observed_at_unix_secs, Some(THROTTLE_AT));
    assert_eq!(cursor.last_throttle_at_unix_secs, Some(THROTTLE_AT));
    assert_eq!(cursor.last_throttle_count, 1);
    assert_eq!(cursor.recent_successes_unix_secs, successes);
    assert_eq!(cursor.recent_throttles_unix_secs, throttles);
}
