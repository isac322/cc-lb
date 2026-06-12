mod upstream_warmup_harness;

use std::time::Duration;

use upstream_warmup_harness::{
    WarmupFixture, assert_locked_warmup_request, delayed_ok_response,
    drive_time_until_message_count, ok_response,
};

#[tokio::test(start_paused = true)]
async fn drift_observation_reschedules_and_fires_after_new_reset() {
    let fixture = WarmupFixture::new().await;
    let initial_cycle_key = fixture.now_unix_secs() - 60;
    let drifted_cycle_key = fixture.now_unix_secs() + 600;
    let upstream_id = fixture.create_due_oauth_upstream("drift").await;
    fixture
        .put_five_hour_observation(upstream_id, initial_cycle_key)
        .await;
    fixture.fake.messages.push_response(ok_response());

    fixture.scan_once().await;
    assert_eq!(fixture.fake.messages.request_count(), 1);

    fixture
        .put_five_hour_observation(upstream_id, drifted_cycle_key)
        .await;
    fixture
        .set_next_warmup_at(upstream_id, fixture.now_unix_secs())
        .await;
    fixture.scan_once().await;

    let scheduled = fixture
        .upstream_record(upstream_id)
        .await
        .next_warmup_at
        .expect("future drift should schedule next_warmup_at")
        .timestamp();
    assert_eq!(fixture.fake.messages.request_count(), 1);
    assert!(
        scheduled >= drifted_cycle_key + 30,
        "scheduled={scheduled}, drifted_cycle_key={drifted_cycle_key}"
    );
    assert!(
        scheduled <= drifted_cycle_key + 90,
        "scheduled={scheduled}, drifted_cycle_key={drifted_cycle_key}"
    );

    fixture.set_now_unix_secs(scheduled + 1);
    fixture.fake.messages.push_response(ok_response());
    fixture.scan_once().await;

    let requests = fixture.fake.messages.requests();
    assert_eq!(
        requests.len(),
        2,
        "drifted cycle should produce second POST"
    );
    assert_locked_warmup_request(&requests[1]);
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, Some(drifted_cycle_key));
}

#[tokio::test(start_paused = true)]
async fn container_restart_mid_fire_double_fires_same_cycle() {
    let fixture = WarmupFixture::new().await;
    let cycle_key = fixture.now_unix_secs() - 60;
    let upstream_id = fixture.create_due_oauth_upstream("restart-mid-fire").await;
    fixture
        .put_five_hour_observation(upstream_id, cycle_key)
        .await;
    fixture
        .fake
        .messages
        .push_response(delayed_ok_response(Duration::from_secs(5)));
    let replica_id = fixture.replica_id;
    let cancel = tokio_util::sync::CancellationToken::new();
    let warmup_loop = fixture.warmup_loop(replica_id, cancel.clone());
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move { warmup_loop.scan_and_fire_once(&task_cancel).await });

    assert!(
        drive_time_until_message_count(&fixture.fake.messages, 1).await,
        "first request did not reach fake Anthropic before cancellation"
    );
    cancel.cancel();
    task.await
        .expect("first warmup scan joins")
        .expect("cancelled first scan is handled");

    let after_abort = fixture.upstream_record(upstream_id).await;
    assert_eq!(after_abort.last_warmup_cycle_key, None);

    fixture.fake.messages.push_response(ok_response());
    fixture.scan_once_with_replica(replica_id).await;

    let requests = fixture.fake.messages.requests();
    assert_eq!(
        requests.len(),
        2,
        "restart with same replica identity should re-fire the same cycle"
    );
    assert_locked_warmup_request(&requests[0]);
    assert_locked_warmup_request(&requests[1]);
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, Some(cycle_key));
}
