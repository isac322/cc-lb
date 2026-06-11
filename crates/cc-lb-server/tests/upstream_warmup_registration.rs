mod upstream_warmup_harness;

use upstream_warmup_harness::{WarmupFixture, assert_locked_warmup_request, ok_response};

#[tokio::test(start_paused = true)]
async fn wait_for_observation_branch_does_not_bootstrap_before_quota_observation() {
    let fixture = WarmupFixture::new().await;
    let upstream_id = fixture.create_due_oauth_upstream("registration-wait").await;

    fixture.scan_once().await;

    assert_eq!(
        fixture.fake.messages.request_count(),
        0,
        "wait-for-observation branch must not fire before latest quota exists"
    );
    assert_eq!(
        fixture
            .upstream_record(upstream_id)
            .await
            .last_warmup_cycle_key,
        None
    );

    let cycle_key = fixture.now_unix_secs() - 60;
    fixture
        .put_five_hour_observation(upstream_id, cycle_key)
        .await;
    fixture.fake.messages.push_response(ok_response());
    fixture.scan_once().await;

    let requests = fixture.fake.messages.requests();
    assert_eq!(
        requests.len(),
        1,
        "first observation should arm exactly one warmup request"
    );
    assert_locked_warmup_request(&requests[0]);
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, Some(cycle_key));
}

#[tokio::test(start_paused = true)]
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
