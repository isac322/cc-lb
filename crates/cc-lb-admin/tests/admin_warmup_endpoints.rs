#[path = "admin_warmup_endpoints/scheduler.rs"]
mod scheduler_support;
#[path = "admin_warmup_endpoints/support.rs"]
mod support;

use axum::http::StatusCode;
use cc_lb_storage_api::warmup_attempts::{WarmupAttemptCursor, WarmupAttemptOutcome};
use serde_json::Value;
use uuid::Uuid;

#[tokio::test]
async fn t3__test_summary_returns_expected_shape() {
    let fixture = support::new_fixture().await;

    let (status, body) = support::get_json(
        fixture.app,
        &format!("/admin/v1/upstreams/{}/warmup", fixture.upstream_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        support::object_keys(&body),
        support::keys(&[
            "upstream_id",
            "last_attempt",
            "recent_attempts",
            "next_scheduled_at_unix_secs",
            "recent_summary_7d",
            "dialect_plugin",
        ])
    );
    assert_eq!(body["upstream_id"], fixture.upstream_id.to_string());
    assert_eq!(
        body["last_attempt"]["id"],
        fixture.attempts[0].id.to_string()
    );
    assert_eq!(
        support::attempt_ids(&body, "recent_attempts"),
        support::expected_ids(&fixture.attempts[..10])
    );
    assert_eq!(
        body["next_scheduled_at_unix_secs"],
        support::NEXT_SCHEDULED_AT
    );
    assert_eq!(
        body["dialect_plugin"],
        serde_json::to_value(&fixture.dialect_plugin).unwrap()
    );
    assert_eq!(
        body["recent_summary_7d"]["success"],
        support::RECENT_7D_COUNTS.0
    );
    assert_eq!(
        body["recent_summary_7d"]["skipped"],
        support::RECENT_7D_COUNTS.1
    );
    assert_eq!(
        body["recent_summary_7d"]["transient_failure"],
        support::RECENT_7D_COUNTS.2
    );
    assert_eq!(
        body["recent_summary_7d"]["permanent_failure"],
        support::RECENT_7D_COUNTS.3
    );
}

#[tokio::test]
async fn t3__test_summary_404_unknown_upstream() {
    let fixture = support::new_fixture().await;
    let unknown = Uuid::from_u128(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF);

    let (status, body) = support::get_json(
        fixture.app,
        &format!("/admin/v1/upstreams/{unknown}/warmup"),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "upstream_not_found");
}

#[tokio::test]
async fn t3__attempts_pagination_returns_next_cursor() {
    let fixture = support::new_fixture().await;

    let (status, body) = support::get_json(
        fixture.app,
        &format!(
            "/admin/v1/upstreams/{}/warmup/attempts?limit=5",
            fixture.upstream_id
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        support::attempt_ids(&body, "attempts"),
        support::expected_ids(&fixture.attempts[..5])
    );
    let next_cursor = body["next_cursor"].as_str().expect("next cursor");
    assert_eq!(
        WarmupAttemptCursor::decode(next_cursor),
        Ok(support::cursor_for(&fixture.attempts[4]))
    );
}

#[tokio::test]
async fn t3__attempts_outcome_filter() {
    let fixture = support::new_fixture().await;
    let expected = fixture
        .attempts
        .iter()
        .filter(|attempt| matches!(attempt.outcome, WarmupAttemptOutcome::PermanentFailure(_)))
        .cloned()
        .collect::<Vec<_>>();

    let (status, body) = support::get_json(
        fixture.app,
        &format!(
            "/admin/v1/upstreams/{}/warmup/attempts?limit=20&outcome=permanent_failure",
            fixture.upstream_id
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        support::attempt_ids(&body, "attempts"),
        support::expected_ids(&expected)
    );
    assert!(
        body["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|attempt| attempt["status"] == "permanent_failure")
    );
    assert_eq!(body["next_cursor"], Value::Null);
}

#[tokio::test]
async fn t3__attempts_before_cursor_excludes_rows_at_or_before() {
    let fixture = support::new_fixture().await;
    let before = support::cursor_for(&fixture.attempts[6]).encode();

    let (status, body) = support::get_json(
        fixture.app,
        &format!(
            "/admin/v1/upstreams/{}/warmup/attempts?limit=5&before={}",
            fixture.upstream_id, before
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        support::attempt_ids(&body, "attempts"),
        support::expected_ids(&fixture.attempts[7..12])
    );
}

#[tokio::test]
async fn t3__test_attempts_404_unknown_upstream() {
    let fixture = support::new_fixture().await;
    let unknown = Uuid::from_u128(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF);

    let (status, body) = support::get_json(
        fixture.app,
        &format!("/admin/v1/upstreams/{unknown}/warmup/attempts"),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "upstream_not_found");
}

#[tokio::test]
async fn t3__test_upstream_detail_shape_unchanged() {
    let fixture = support::new_fixture().await;

    let (status, body) = support::get_json(
        fixture.app,
        &format!("/admin/v1/upstreams/{}", fixture.upstream_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        support::object_keys(&body),
        support::keys(&[
            "id",
            "name",
            "kind",
            "enabled",
            "warmup_enabled",
            "warmup_dialect_plugin",
            "spec_revision",
            "status",
        ])
    );
    assert!(body.get("dialect_plugin").is_none());
    assert!(body.get("last_attempt").is_none());
    assert!(body.get("recent_attempts").is_none());
    assert!(body.get("recent_summary_7d").is_none());
}
