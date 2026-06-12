mod upstream_warmup_multireplica_common;

use std::time::Duration;

use cc_lb_storage_api::UpstreamStore;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use upstream_warmup_multireplica_common::{
    PostgresWarmupFixture, RunningWarmupServer, TestResult, assert_warmup_posts,
    low_jitter_cycle_key,
};

static SERIAL_TESTS: Mutex<()> = Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ten_trial_lease_race_converges_to_one_fire() -> TestResult {
    let _serial = SERIAL_TESTS.lock().await;
    let Some(fixture) = PostgresWarmupFixture::create("warmup_multi_race").await? else {
        return Ok(());
    };

    for trial in 0..10 {
        run_trial(&fixture, trial).await?;
    }

    fixture.drop_schema().await
}

async fn run_trial(fixture: &PostgresWarmupFixture, trial: usize) -> TestResult {
    let server = RunningWarmupServer::spawn(Duration::ZERO).await?;
    let upstream = fixture
        .create_due_oauth_upstream(server.base_url()?)
        .await?;
    let now = fixture.storage.warmup_now_unix_secs().await?;
    let cycle_key = low_jitter_cycle_key(upstream.id, now);
    fixture
        .seed_latest_observation(upstream.id, cycle_key, trial as u64)
        .await?;

    let replica_a = Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 + trial as u128 * 2);
    let replica_b =
        Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 + trial as u128 * 2 + 1);
    let loop_a = fixture.warmup_loop(replica_a);
    let loop_b = fixture.warmup_loop(replica_b);
    let cancel = CancellationToken::new();

    let (result_a, result_b) = tokio::join!(
        loop_a.scan_and_fire_once(&cancel),
        loop_b.scan_and_fire_once(&cancel),
    );
    result_a?;
    result_b?;

    let calls = server
        .wait_for_call_count(1, Duration::from_secs(5))
        .await?;
    assert_eq!(
        calls.len(),
        1,
        "trial {trial}: expected exactly one warmup fire"
    );
    assert_warmup_posts(&calls);

    let write_count = fixture.cycle_key_write_count(upstream.id).await?;
    assert_eq!(
        write_count, 1,
        "trial {trial}: expected exactly one cycle-key write"
    );
    let stored = fixture
        .storage
        .get_by_id(upstream.id)
        .await?
        .expect("upstream remains readable");
    assert_eq!(stored.last_warmup_cycle_key, Some(cycle_key));
    assert_eq!(stored.warmup_lease_holder, None);
    assert_eq!(stored.warmup_lease_until_unix_secs, None);

    tracing::info!(
        target: "warmup_multireplica_test",
        trial,
        fires = calls.len(),
        cycle_key_writes = write_count,
        lease_released = stored.warmup_lease_holder.is_none(),
        "lease race trial complete"
    );

    fixture.storage.hard_delete(upstream.id).await?;
    Ok(())
}
