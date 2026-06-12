mod upstream_warmup_multireplica_common;

use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use cc_lb_admin::v1::upstreams;
use cc_lb_server::upstream_warmup_loop::WARMUP_LEASE_TTL_SECS;
use cc_lb_storage_api::UpstreamStore;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

use upstream_warmup_multireplica_common::{
    FIVE_HOURS_SECS, PostgresWarmupFixture, RunningWarmupServer, TestResult, assert_warmup_posts,
    error, low_jitter_cycle_key, low_jitter_old_cycle_key, response_json,
};

static SERIAL_TESTS: Mutex<()> = Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resilience_reclaims_after_forced_lease_expiry() -> TestResult {
    let _serial = SERIAL_TESTS.lock().await;
    let Some(fixture) = PostgresWarmupFixture::create("warmup_multi_restart").await? else {
        return Ok(());
    };

    let server = RunningWarmupServer::spawn(Duration::from_secs(2)).await?;
    let upstream = fixture
        .create_due_oauth_upstream(server.base_url()?)
        .await?;
    let db_start = fixture.storage.warmup_now_unix_secs().await?;
    let cycle_key = low_jitter_cycle_key(upstream.id, db_start);
    fixture
        .seed_latest_observation(upstream.id, cycle_key, 0)
        .await?;

    let test_start = Instant::now();
    let first_replica = Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0001);
    let first_loop = fixture.warmup_loop(first_replica);
    let first_cancel = CancellationToken::new();
    let first_task = tokio::spawn({
        let first_loop = first_loop.clone();
        let first_cancel = first_cancel.clone();
        async move { first_loop.scan_and_fire_once(&first_cancel).await }
    });

    let first_calls = server
        .wait_for_call_count(1, Duration::from_secs(5))
        .await?;
    assert_warmup_posts(&first_calls);
    first_task.abort();
    let _ = first_task.await;

    let lease_until = fixture
        .warmup_lease_until(upstream.id)
        .await?
        .ok_or_else(|| error("warmup lease was not recorded"))?;
    assert!(
        lease_until >= db_start + WARMUP_LEASE_TTL_SECS - 1,
        "lease_until={lease_until} db_start={db_start} ttl={WARMUP_LEASE_TTL_SECS}"
    );

    let second_replica = Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0002);
    let pre_expiry_loop = fixture.warmup_loop(second_replica);
    pre_expiry_loop
        .scan_and_fire_once(&CancellationToken::new())
        .await?;
    assert_eq!(
        server.call_count().await,
        1,
        "new replica fired before lease expiry"
    );

    fixture.expire_warmup_lease(upstream.id).await?;
    let forced_expiry_at = Instant::now();
    let second_loop = fixture.warmup_loop(second_replica);
    second_loop
        .scan_and_fire_once(&CancellationToken::new())
        .await?;

    let calls = server
        .wait_for_call_count(2, Duration::from_secs(5))
        .await?;
    assert_eq!(calls.len(), 2, "expected exactly two warmup fires");
    assert_warmup_posts(&calls);
    assert!(
        calls[1].received_at >= forced_expiry_at,
        "second fire happened before forced lease expiry"
    );
    assert!(calls[1].received_at > calls[0].received_at);

    let write_count = fixture.cycle_key_write_count(upstream.id).await?;
    assert_eq!(
        write_count, 1,
        "only recovered replica should write cycle key"
    );
    let stored = fixture
        .storage
        .get_by_id(upstream.id)
        .await?
        .expect("upstream remains readable");
    assert_eq!(stored.last_warmup_cycle_key, Some(cycle_key));

    tracing::info!(
        target: "warmup_multireplica_test",
        second_fire_elapsed_secs = calls[1].received_at.duration_since(test_start).as_secs(),
        lease_ttl_secs = WARMUP_LEASE_TTL_SECS,
        forced_expiry = true,
        "second warmup fire observed after lease expiry"
    );

    fixture.drop_schema().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fire_now_returns_accepted_with_loop_holder_while_loop_dispatches() -> TestResult {
    let _serial = SERIAL_TESTS.lock().await;
    let Some(fixture) = PostgresWarmupFixture::create("warmup_multi_fire_now").await? else {
        return Ok(());
    };

    let server = RunningWarmupServer::spawn(Duration::from_secs(2)).await?;
    let upstream = fixture
        .create_due_oauth_upstream(server.base_url()?)
        .await?;
    let now = fixture.storage.warmup_now_unix_secs().await?;
    let cycle_key = low_jitter_cycle_key(upstream.id, now);
    fixture
        .seed_latest_observation(upstream.id, cycle_key, 0)
        .await?;

    let loop_replica = Uuid::from_u128(0x3000_0000_0000_0000_0000_0000_0000_0001);
    let loop_holder = loop_replica.to_string();
    let warmup_loop = fixture.warmup_loop(loop_replica);
    let cancel = CancellationToken::new();
    let loop_task = tokio::spawn({
        let warmup_loop = warmup_loop.clone();
        let cancel = cancel.clone();
        async move { warmup_loop.scan_and_fire_once(&cancel).await }
    });

    let calls = server
        .wait_for_call_count(1, Duration::from_secs(5))
        .await?;
    assert_warmup_posts(&calls);

    let response = upstreams::router()
        .with_state(fixture.admin_state())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "/admin/v1/upstreams/{}/warmup/fire-now",
                    upstream.id
                ))
                .body(Body::empty())?,
        )
        .await?;
    let (status, body) = response_json(response).await?;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["fired"], false);
    assert_eq!(body["reason"], "lease_held");
    assert_eq!(body["held_by"], loop_holder);

    loop_task.abort();
    let _ = loop_task.await;
    fixture.drop_schema().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drift_mid_cycle_writes_original_key_then_refires_with_latest_observation() -> TestResult {
    let _serial = SERIAL_TESTS.lock().await;
    let Some(fixture) = PostgresWarmupFixture::create("warmup_multi_drift").await? else {
        return Ok(());
    };

    let server = RunningWarmupServer::spawn(Duration::from_millis(500)).await?;
    let upstream = fixture
        .create_due_oauth_upstream(server.base_url()?)
        .await?;
    let now = fixture.storage.warmup_now_unix_secs().await?;
    let first_cycle_key = low_jitter_old_cycle_key(upstream.id, now);
    let second_cycle_key = low_jitter_cycle_key(upstream.id, now);
    assert!(first_cycle_key + FIVE_HOURS_SECS < now);
    assert!(second_cycle_key > first_cycle_key);
    fixture
        .seed_latest_observation(upstream.id, first_cycle_key, 0)
        .await?;

    let replica = Uuid::from_u128(0x4000_0000_0000_0000_0000_0000_0000_0001);
    let warmup_loop = fixture.warmup_loop(replica);
    let first_scan = tokio::spawn({
        let warmup_loop = warmup_loop.clone();
        async move {
            warmup_loop
                .scan_and_fire_once(&CancellationToken::new())
                .await
        }
    });

    let first_calls = server
        .wait_for_call_count(1, Duration::from_secs(5))
        .await?;
    assert_warmup_posts(&first_calls);
    fixture
        .seed_latest_observation(upstream.id, second_cycle_key, 1)
        .await?;
    first_scan.await??;

    let first_writes = fixture.cycle_key_writes(upstream.id).await?;
    assert_eq!(first_writes, vec![first_cycle_key]);
    let stored_after_first = fixture
        .storage
        .get_by_id(upstream.id)
        .await?
        .expect("upstream remains readable");
    assert_eq!(
        stored_after_first.last_warmup_cycle_key,
        Some(first_cycle_key)
    );

    warmup_loop
        .scan_and_fire_once(&CancellationToken::new())
        .await?;
    let calls = server
        .wait_for_call_count(2, Duration::from_secs(5))
        .await?;
    assert_eq!(
        calls.len(),
        2,
        "expected refire after latest observation drift"
    );
    assert_warmup_posts(&calls);

    let writes = fixture.cycle_key_writes(upstream.id).await?;
    assert_eq!(writes, vec![first_cycle_key, second_cycle_key]);
    let stored_after_second = fixture
        .storage
        .get_by_id(upstream.id)
        .await?
        .expect("upstream remains readable");
    assert_eq!(
        stored_after_second.last_warmup_cycle_key,
        Some(second_cycle_key)
    );

    tracing::info!(
        target: "warmup_multireplica_test",
        fires = calls.len(),
        first_cycle_key,
        second_cycle_key,
        "drift mid-cycle refired with latest observation"
    );

    fixture.drop_schema().await
}
