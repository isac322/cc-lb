//! #988 keepalive pre-deploy E2E scenarios.
//!
//! These tests intentionally use `SystemClock` and the production scheduler/worker
//! path. The 45-second idle is deliberate: `PgPollFetcher` backs off for 2, 4, 8,
//! 16, and 32 seconds using `futures_timer`, so this wait cannot be virtualized.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_lb_storage_api::cache_keepalive_job_key;

use super::keepalive_support::harness::{
    Backend, ClockMode, Harness, HarnessOptions, KeepaliveOptions, request_body,
};

const IDLE_AFTER_WORKER_START: Duration = Duration::from_secs(45);
const WORKER_START_WAIT: Duration = Duration::from_secs(5);
const JOB_LOCK_WAIT: Duration = Duration::from_secs(30);
const RENEWAL_WAIT: Duration = Duration::from_secs(10);
const SESSION_ID: &str = "00000000-0000-7000-8000-000000000988";

fn options(backend: Backend) -> HarnessOptions {
    HarnessOptions {
        backend,
        clock: ClockMode::System,
        keepalive: KeepaliveOptions {
            // run_at is approximately one second after the live request: the 5m
            // cache TTL is 300s and the lead time is 299s.
            refresh_lead_time_5m_secs: 299,
            ..KeepaliveOptions::default()
        },
    }
}

async fn run_wall_clock(backend: Backend, lock_delay_bound_secs: i64) {
    let Some(harness) = Harness::optional(options(backend))
        .await
        .expect("wall-clock harness setup")
    else {
        eprintln!("SKIP: CI_POSTGRES_URL is unset");
        return;
    };

    let worker = harness
        .wait_for_keepalive_worker(WORKER_START_WAIT)
        .await
        .expect("cache keepalive worker did not register");
    let (worker_anchor_secs, anchor_source) = worker
        .started_at
        .map(|value| (value, "started_at"))
        .or_else(|| worker.last_heartbeat.map(|value| (value, "last_heartbeat")))
        .expect("cache keepalive worker has started_at or last_heartbeat");
    let worker_anchor_secs =
        u64::try_from(worker_anchor_secs).expect("worker anchor timestamp fits u64");
    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_secs();
    let worker_age = now_unix_secs.saturating_sub(worker_anchor_secs);
    assert!(
        worker_age <= WORKER_START_WAIT.as_secs(),
        "keepalive worker heartbeat is stale: age={worker_age}s, \
         started_at={:?}, last_heartbeat={:?}",
        worker.started_at,
        worker.last_heartbeat
    );
    // `build_app_with_storage` starts consumers before it returns, so an
    // anchor captured after construction can skip part of the fetcher's idle
    // backoff. Anchor to the observed worker registration/heartbeat instead.
    let worker_anchor = Instant::now()
        .checked_sub(Duration::from_secs(worker_age))
        .unwrap_or_else(Instant::now);
    eprintln!(
        "wall-clock {backend:?}: keepalive worker id={} started_at={:?} \
         last_heartbeat={:?} anchor={}={} age={}s",
        worker.id,
        worker.started_at,
        worker.last_heartbeat,
        anchor_source,
        worker_anchor_secs,
        worker_age
    );
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        worker_anchor + IDLE_AFTER_WORKER_START,
    ))
    .await;
    let worker_after_idle = harness
        .read_keepalive_worker()
        .await
        .expect("cache keepalive worker disappeared during idle");
    assert_eq!(
        worker_after_idle.id, worker.id,
        "cache keepalive worker restarted during the anchored idle"
    );

    let live = harness
        .post(
            request_body(100_000, "wall-clock", 90_000, "bash", "tool_use", false),
            SESSION_ID,
            None,
        )
        .await;
    assert_eq!(live["stop_reason"], "tool_use");
    let session_key_hash = harness.session_key_hash(SESSION_ID, None);
    harness
        .wait_for_enqueued_generation(&session_key_hash, 1)
        .await;
    let job_key = cache_keepalive_job_key(&session_key_hash, 1);

    // Measure scheduler latency independently of upstream request validation.
    // If the worker never claims the job, this is the #987 failure signal.
    let timing = harness.wait_for_job_locked(&job_key, JOB_LOCK_WAIT).await;
    let timing = timing.unwrap_or_else(|| {
        panic!(
            "cache keepalive job was not locked within {JOB_LOCK_WAIT:?}; \
             lock_at is NULL at the deadline for {job_key}"
        )
    });
    let run_at = timing.run_at.expect("cache keepalive job has run_at");
    let lock_at = timing
        .lock_at
        .expect("locked cache keepalive job has lock_at");
    if backend == Backend::Postgres {
        let lock_delay_ms = lock_at - run_at;
        eprintln!(
            "wall-clock {backend:?}: lock_at-run_at={lock_delay_ms}ms \
             (run_at={run_at}, lock_at={lock_at})"
        );
        assert!(
            (0..=lock_delay_bound_secs * 1_000).contains(&lock_delay_ms),
            "lock_at - run_at must be between 0 and {}ms, got {lock_delay_ms}ms",
            lock_delay_bound_secs * 1_000
        );
    } else {
        let lock_delay = lock_at - run_at;
        eprintln!(
            "wall-clock {backend:?}: lock_at-run_at={lock_delay}s \
             (run_at={run_at}, lock_at={lock_at})"
        );
        assert!(
            (0..=lock_delay_bound_secs).contains(&lock_delay),
            "lock_at - run_at must be between 0 and {lock_delay_bound_secs}s, got {lock_delay}s"
        );
    }

    // This is the sole bounded wait for the renewal attempt after scheduler
    // latency has been measured. Rejected attempts are still observable.
    let renewal = harness
        .mock
        .wait_for_event_kind("renewal", RENEWAL_WAIT)
        .await
        .expect("renewal attempt did not reach the mock within the bounded wait");
    assert!(
        renewal.accepted(),
        "renewal rejected: {:?}",
        renewal.rejection
    );
    assert!(renewal.synthetic, "renewal must use max_tokens=0");
    assert!(renewal.hit, "renewal must be a cache HIT");

    let session = harness
        .read_session(&session_key_hash)
        .await
        .expect("keepalive session exists");
    assert_eq!(
        session.status, "active",
        "renewed session must remain active"
    );
    assert_ne!(session.terminal_reason.as_deref(), Some("expired"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn d2_sqlite_wall_clock_scheduler_dispatches_before_expiry() {
    // Three measured local runs recorded 2s, 3s, and 2s for lock_at-run_at;
    // <=5s leaves margin for normal SQLite scheduling jitter while remaining
    // substantially tighter than the 300s expiry window.
    run_wall_clock(Backend::Sqlite, 5).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn d1_postgres_wall_clock_scheduler_claims_near_run_at() {
    run_wall_clock(Backend::Postgres, 2).await;
}
