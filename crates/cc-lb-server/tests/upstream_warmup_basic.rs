mod upstream_warmup_harness;

use upstream_warmup_harness::{
    WarmupFixture, assert_locked_warmup_request, assert_next_warmup_after_cycle, ok_response,
};

#[tokio::test]
async fn basic_posts_once_and_writes_matching_cycle_key() {
    let fixture = WarmupFixture::new().await;
    let cycle_key = fixture.now_unix_secs() - 60;
    let upstream_id = fixture.create_due_oauth_upstream("basic").await;
    fixture
        .put_five_hour_observation(upstream_id, cycle_key)
        .await;
    fixture.fake.messages.push_response(ok_response());

    fixture.scan_once().await;

    let requests = fixture.fake.messages.requests();
    assert_eq!(requests.len(), 1, "expected exactly one warmup POST");
    assert_locked_warmup_request(&requests[0]);

    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, Some(cycle_key));
    assert_next_warmup_after_cycle(&record, cycle_key);
}

#[tokio::test]
async fn same_observation_cycle_does_not_fire_again() {
    let fixture = WarmupFixture::new().await;
    let cycle_key = fixture.now_unix_secs() - 60;
    let upstream_id = fixture.create_due_oauth_upstream("basic-idempotent").await;
    fixture
        .put_five_hour_observation(upstream_id, cycle_key)
        .await;
    fixture.fake.messages.push_response(ok_response());

    fixture.scan_once().await;
    fixture
        .set_next_warmup_at(upstream_id, fixture.now_unix_secs())
        .await;
    fixture.scan_once().await;

    assert_eq!(
        fixture.fake.messages.request_count(),
        1,
        "same cycle_key must not double-fire"
    );
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, Some(cycle_key));
}
