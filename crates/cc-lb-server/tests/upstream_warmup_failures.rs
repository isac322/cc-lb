mod upstream_warmup_harness;

use http::StatusCode;
use upstream_warmup_harness::{
    WarmupFixture, active_rate_limit_response, assert_next_warmup_after_cycle, capture_logs,
    error_response, ok_response,
};

#[tokio::test]
async fn unauthorized_refreshes_once_then_success_writes_cycle_key() {
    let fixture = WarmupFixture::new().await;
    let cycle_key = fixture.now_unix_secs() - 60;
    let upstream_id = fixture
        .create_due_oauth_upstream("401-refresh-success")
        .await;
    fixture
        .put_five_hour_observation(upstream_id, cycle_key)
        .await;
    fixture
        .fake
        .messages
        .push_response(error_response(StatusCode::UNAUTHORIZED));
    fixture.fake.messages.push_response(ok_response());

    fixture.scan_once().await;

    assert_eq!(fixture.fake.messages.request_count(), 2);
    assert_eq!(fixture.refresh_history_len().await, 1);
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, Some(cycle_key));
}

#[tokio::test]
async fn persistent_unauthorized_refreshes_once_then_abandons_without_cycle_key() {
    let logs = capture_logs();
    let fixture = WarmupFixture::new().await;
    let cycle_key = fixture.now_unix_secs() - 60;
    let upstream_id = fixture.create_due_oauth_upstream("401-persistent").await;
    fixture
        .put_five_hour_observation(upstream_id, cycle_key)
        .await;
    fixture
        .fake
        .messages
        .push_response(error_response(StatusCode::UNAUTHORIZED));
    fixture
        .fake
        .messages
        .push_response(error_response(StatusCode::UNAUTHORIZED));

    fixture.scan_once().await;

    assert_eq!(fixture.fake.messages.request_count(), 2);
    assert_eq!(fixture.refresh_history_len().await, 1);
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, None);
    assert_next_warmup_after_cycle(&record, cycle_key);
    let captured = logs.contents();
    assert!(
        captured.contains("action=\"cycle_abandoned\"")
            && captured.contains("reason=\"auth_failed\""),
        "expected abandon-after-refresh log, captured logs:\n{captured}"
    );
}

#[tokio::test]
async fn forbidden_abandons_without_retry_or_cycle_key() {
    permanent_failure_abandons_without_retry(StatusCode::FORBIDDEN, "forbidden").await;
}

#[tokio::test]
async fn bad_request_abandons_without_retry_or_cycle_key() {
    permanent_failure_abandons_without_retry(StatusCode::BAD_REQUEST, "bad_request").await;
}

#[tokio::test]
async fn not_found_abandons_without_retry_or_cycle_key() {
    permanent_failure_abandons_without_retry(StatusCode::NOT_FOUND, "not_found").await;
}

#[tokio::test]
async fn rate_limit_with_active_five_hour_header_writes_cycle_key_without_retry() {
    let fixture = WarmupFixture::new().await;
    let candidate_cycle_key = fixture.now_unix_secs() - 60;
    let active_cycle_key = fixture.now_unix_secs() + 300;
    let upstream_id = fixture.create_due_oauth_upstream("429-active").await;
    fixture
        .put_five_hour_observation(upstream_id, candidate_cycle_key)
        .await;
    fixture
        .fake
        .messages
        .push_response(active_rate_limit_response(active_cycle_key));

    fixture.scan_once().await;

    assert_eq!(fixture.fake.messages.request_count(), 1);
    assert_eq!(fixture.refresh_history_len().await, 0);
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, Some(active_cycle_key));
}

#[tokio::test]
async fn transient_rate_limit_backs_off_then_eventually_writes_cycle_key() {
    let fixture = WarmupFixture::new().await;
    let cycle_key = fixture.now_unix_secs() - 60;
    let upstream_id = fixture.create_due_oauth_upstream("429-transient").await;
    fixture
        .put_five_hour_observation(upstream_id, cycle_key)
        .await;
    fixture
        .fake
        .messages
        .push_response(error_response(StatusCode::TOO_MANY_REQUESTS));

    fixture.scan_once().await;

    let backed_off = fixture.upstream_record(upstream_id).await;
    assert_eq!(backed_off.last_warmup_cycle_key, None);
    let next = backed_off
        .next_warmup_at
        .expect("transient failure should schedule backoff")
        .timestamp();
    assert!(next >= fixture.now_unix_secs() + 60);
    assert!(next <= fixture.now_unix_secs() + 61);
    assert_eq!(fixture.fake.messages.request_count(), 1);

    fixture.set_now_unix_secs(next);
    fixture.fake.messages.push_response(ok_response());
    fixture.scan_once().await;

    assert_eq!(fixture.fake.messages.request_count(), 2);
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, Some(cycle_key));
}

async fn permanent_failure_abandons_without_retry(status: StatusCode, reason: &str) {
    let logs = capture_logs();
    let fixture = WarmupFixture::new().await;
    let cycle_key = fixture.now_unix_secs() - 60;
    let upstream_id = fixture
        .create_due_oauth_upstream(&format!("permanent-{}", status.as_u16()))
        .await;
    fixture
        .put_five_hour_observation(upstream_id, cycle_key)
        .await;
    fixture.fake.messages.push_response(error_response(status));

    fixture.scan_once().await;

    assert_eq!(fixture.fake.messages.request_count(), 1);
    assert_eq!(fixture.refresh_history_len().await, 0);
    let record = fixture.upstream_record(upstream_id).await;
    assert_eq!(record.last_warmup_cycle_key, None);
    assert_next_warmup_after_cycle(&record, cycle_key);
    let captured = logs.contents();
    assert!(
        captured.contains("action=\"cycle_abandoned\"")
            && captured.contains(&format!("reason=\"{reason}\"")),
        "expected warmup abandon log for {reason}, captured logs:\n{captured}"
    );
}
