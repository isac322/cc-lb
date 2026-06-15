mod upstream_warmup_harness;

use upstream_warmup_harness::{WarmupFixture, assert_locked_warmup_request, ok_response};

#[tokio::test]
async fn bootstrap_fire_when_no_observation_and_no_prior_cycle_key() {
    let fixture = WarmupFixture::new().await;
    let upstream_id = fixture
        .create_due_oauth_upstream("registration-bootstrap")
        .await;
    let now_before = fixture.now_unix_secs();
    fixture.fake.messages.push_response(ok_response());

    fixture.scan_once().await;

    let requests = fixture.fake.messages.requests();
    assert_eq!(
        requests.len(),
        1,
        "no observation + no prior cycle key must bootstrap-fire on the first scan"
    );
    assert_locked_warmup_request(&requests[0]);
    let record = fixture.upstream_record(upstream_id).await;
    assert!(
        record
            .last_warmup_cycle_key
            .is_some_and(|ck| ck >= now_before),
        "bootstrap fire should write a synthetic cycle key >= db_now (got {:?})",
        record.last_warmup_cycle_key,
    );
}

#[tokio::test]
async fn no_observation_with_prior_cycle_key_short_backs_off() {
    let fixture = WarmupFixture::new().await;
    let upstream_id = fixture
        .create_due_oauth_upstream("registration-stale-prior")
        .await;
    let prior_cycle_key = fixture.now_unix_secs() - 7200;
    fixture
        .set_last_warmup_cycle_key(upstream_id, prior_cycle_key)
        .await;
    fixture
        .set_next_warmup_at(upstream_id, fixture.now_unix_secs() - 1)
        .await;

    fixture.scan_once().await;

    assert_eq!(
        fixture.fake.messages.request_count(),
        0,
        "without an observation but with a prior cycle key, the loop must NOT bootstrap-fire"
    );
    let after = fixture.upstream_record(upstream_id).await;
    assert_eq!(after.last_warmup_cycle_key, Some(prior_cycle_key));
}

#[tokio::test]
async fn future_first_observation_schedules_without_bootstrap_fire() {
    let fixture = WarmupFixture::new().await;
    let upstream_id = fixture
        .create_oauth_upstream("registration-future", Some(fixture.now_unix_secs()))
        .await;
    let future_cycle_key = fixture.now_unix_secs() + 300;
    fixture
        .put_five_hour_observation(upstream_id, future_cycle_key)
        .await;

    fixture.scan_once().await;

    assert_eq!(
        fixture.fake.messages.request_count(),
        0,
        "future first observation should schedule, not bootstrap immediately"
    );
    let scheduled = fixture
        .upstream_record(upstream_id)
        .await
        .next_warmup_at
        .expect("future observation schedules next_warmup_at")
        .timestamp();
    assert!(scheduled >= future_cycle_key + 30);
    assert!(scheduled <= future_cycle_key + 90);
}

#[tokio::test]
async fn null_next_warmup_auto_heals_on_scan() {
    let fixture = WarmupFixture::new().await;
    let upstream_id = fixture
        .create_oauth_upstream("registration-null-heal", None)
        .await;
    assert!(
        fixture
            .upstream_record(upstream_id)
            .await
            .next_warmup_at
            .is_none()
    );

    fixture.scan_once().await;

    let after_scan = fixture.upstream_record(upstream_id).await;
    assert!(
        after_scan.next_warmup_at.is_some(),
        "scan must auto-heal null next_warmup_at into a scheduled retry"
    );
}
