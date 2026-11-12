#[path = "admin_warmup_endpoints/scheduler.rs"]
mod scheduler_support;
#[path = "admin_warmup_endpoints/support.rs"]
mod support;

use axum::http::StatusCode;
use cc_lb_oauth_protocol::GeminiOAuthProfile;
use cc_lb_storage_api::SubscriptionQuotaWindow;
use cc_lb_storage_api::warmup_attempts::{WarmupAttemptCursor, WarmupAttemptOutcome, WarmupLane};
use serde_json::{Value, json};
use uuid::Uuid;

const LANE_KEYS: &[&str] = &[
    "lane",
    "eligible",
    "ineligible_reason",
    "target_windows",
    "next_scheduled_at_unix_secs",
    "last_attempt",
    "recent_attempts",
];

fn lanes(body: &Value) -> &Vec<Value> {
    body["lanes"].as_array().expect("lanes array")
}

fn lane<'a>(body: &'a Value, name: &str) -> &'a Value {
    lanes(body)
        .iter()
        .find(|lane| lane["lane"] == name)
        .unwrap_or_else(|| panic!("lane {name} present in {body}"))
}

#[tokio::test]
async fn test_summary_returns_expected_shape() {
    let fixture = support::new_fixture().await;

    let (status, body) = support::get_json(
        fixture.app,
        &format!("/admin/v1/upstreams/{}/warmup", fixture.upstream_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        support::object_keys(&body),
        support::keys(&["upstream_id", "dialect_plugin", "lanes"])
    );
    assert_eq!(body["upstream_id"], fixture.upstream_id.to_string());
    assert_eq!(
        body["dialect_plugin"],
        serde_json::to_value(&fixture.dialect_plugin).unwrap()
    );
    assert_eq!(lanes(&body).len(), 1, "anthropic has only the primary lane");
    let primary = lane(&body, "primary");
    assert_eq!(support::object_keys(primary), support::keys(LANE_KEYS));
    assert_eq!(primary["eligible"], true);
    assert_eq!(primary["ineligible_reason"], Value::Null);
    assert_eq!(primary["target_windows"], json!(["5h", "7d"]));
    assert_eq!(
        primary["last_attempt"]["id"],
        fixture.attempts[0].id.to_string()
    );
    assert_eq!(primary["last_attempt"]["lane"], "primary");
    assert_eq!(
        support::attempt_ids(primary, "recent_attempts"),
        support::expected_ids(&fixture.attempts[..10])
    );
    assert_eq!(
        primary["next_scheduled_at_unix_secs"],
        support::NEXT_SCHEDULED_AT
    );
}

#[tokio::test]
async fn test_summary_gemini_antigravity_returns_both_lanes() {
    let fixture = support::new_gemini_fixture(GeminiOAuthProfile::Antigravity).await;
    let primary_attempts = fixture.attempts_for(WarmupLane::Primary);
    let third_party_attempts = fixture.attempts_for(WarmupLane::ThirdParty);

    let (status, body) = support::get_json(
        fixture.app.clone(),
        &format!("/admin/v1/upstreams/{}/warmup", fixture.upstream_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        lanes(&body)
            .iter()
            .map(|lane| lane["lane"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["primary", "third_party"]
    );

    let primary = lane(&body, "primary");
    assert_eq!(support::object_keys(primary), support::keys(LANE_KEYS));
    assert_eq!(primary["eligible"], true);
    assert_eq!(primary["target_windows"], json!(["gemini_5h", "gemini_7d"]));
    assert_eq!(
        primary["next_scheduled_at_unix_secs"],
        support::NEXT_SCHEDULED_AT
    );
    assert_eq!(
        primary["last_attempt"]["id"],
        primary_attempts[0].id.to_string()
    );
    assert_eq!(
        support::attempt_ids(primary, "recent_attempts"),
        support::expected_ids(&primary_attempts[..10])
    );

    let third_party = lane(&body, "third_party");
    assert_eq!(support::object_keys(third_party), support::keys(LANE_KEYS));
    assert_eq!(third_party["eligible"], true);
    assert_eq!(third_party["ineligible_reason"], Value::Null);
    assert_eq!(
        third_party["target_windows"],
        json!(["third_party_5h", "third_party_7d"])
    );
    assert_eq!(
        third_party["next_scheduled_at_unix_secs"],
        support::NEXT_THIRD_PARTY_SCHEDULED_AT
    );
    assert_eq!(
        third_party["last_attempt"]["id"],
        third_party_attempts[0].id.to_string()
    );
    assert_eq!(
        support::attempt_ids(third_party, "recent_attempts"),
        support::expected_ids(&third_party_attempts[..10])
    );
    assert!(
        third_party["recent_attempts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|attempt| attempt["lane"] == "third_party")
    );
}

#[tokio::test]
async fn test_summary_gemini_cli_third_party_lane_profile_unsupported() {
    let fixture = support::new_gemini_fixture(GeminiOAuthProfile::GeminiCli).await;

    let (status, body) = support::get_json(
        fixture.app.clone(),
        &format!("/admin/v1/upstreams/{}/warmup", fixture.upstream_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(lanes(&body).len(), 2);
    assert_eq!(lane(&body, "primary")["eligible"], true);
    let third_party = lane(&body, "third_party");
    assert_eq!(third_party["eligible"], false);
    assert_eq!(third_party["ineligible_reason"], "profile_unsupported");
}

#[tokio::test]
async fn test_summary_anthropic_primary_stays_eligible_when_windows_absent() {
    let fixture = support::new_fixture().await;
    support::record_absent_windows(
        fixture.storage.as_ref(),
        fixture.upstream_id,
        &[
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ],
    )
    .await;

    let (status, body) = support::get_json(
        fixture.app,
        &format!("/admin/v1/upstreams/{}/warmup", fixture.upstream_id),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let primary = lane(&body, "primary");
    assert_eq!(primary["eligible"], true);
    assert_eq!(primary["ineligible_reason"], Value::Null);
    assert_eq!(primary["target_windows"], json!([]));
}

#[tokio::test]
async fn test_summary_third_party_lane_absent_windows() {
    let fixture = support::new_gemini_fixture(GeminiOAuthProfile::Antigravity).await;
    let uri = format!("/admin/v1/upstreams/{}/warmup", fixture.upstream_id);

    // One absent window narrows the targets but keeps the lane eligible.
    support::record_absent_windows(
        fixture.storage.as_ref(),
        fixture.upstream_id,
        &[SubscriptionQuotaWindow::ThirdPartyFiveHour],
    )
    .await;
    let (status, body) = support::get_json(fixture.app.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK);
    let third_party = lane(&body, "third_party");
    assert_eq!(third_party["eligible"], true);
    assert_eq!(third_party["target_windows"], json!(["third_party_7d"]));

    // Every target window absent makes the lane ineligible; primary unaffected.
    support::record_absent_windows(
        fixture.storage.as_ref(),
        fixture.upstream_id,
        &[SubscriptionQuotaWindow::ThirdPartySevenDay],
    )
    .await;
    let (status, body) = support::get_json(fixture.app.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK);
    let third_party = lane(&body, "third_party");
    assert_eq!(third_party["eligible"], false);
    assert_eq!(third_party["ineligible_reason"], "quota_group_absent");
    assert_eq!(third_party["target_windows"], json!([]));
    let primary = lane(&body, "primary");
    assert_eq!(primary["eligible"], true);
    assert_eq!(primary["target_windows"], json!(["gemini_5h", "gemini_7d"]));
}

#[tokio::test]
async fn test_attempts_lane_filter() {
    let fixture = support::new_gemini_fixture(GeminiOAuthProfile::Antigravity).await;

    for (lane_name, lane) in [
        ("primary", WarmupLane::Primary),
        ("third_party", WarmupLane::ThirdParty),
    ] {
        let (status, body) = support::get_json(
            fixture.app.clone(),
            &format!(
                "/admin/v1/upstreams/{}/warmup/attempts?limit=50&lane={lane_name}",
                fixture.upstream_id
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            support::attempt_ids(&body, "attempts"),
            support::expected_ids(&fixture.attempts_for(lane))
        );
        assert!(
            body["attempts"]
                .as_array()
                .unwrap()
                .iter()
                .all(|attempt| attempt["lane"] == lane_name)
        );
    }

    let (status, body) = support::get_json(
        fixture.app.clone(),
        &format!(
            "/admin/v1/upstreams/{}/warmup/attempts?limit=50",
            fixture.upstream_id
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        support::attempt_ids(&body, "attempts"),
        support::expected_ids(&fixture.attempts),
        "no lane filter returns every lane"
    );
}

#[tokio::test]
async fn test_attempts_unknown_lane_400() {
    let fixture = support::new_fixture().await;

    let (status, body) = support::get_json(
        fixture.app,
        &format!(
            "/admin/v1/upstreams/{}/warmup/attempts?lane=claude",
            fixture.upstream_id
        ),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_warmup_lane");
}

#[tokio::test]
async fn test_summary_404_unknown_upstream() {
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
async fn test_attempts_pagination_returns_next_cursor() {
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
async fn test_attempts_status_filter() {
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
            "/admin/v1/upstreams/{}/warmup/attempts?limit=20&status=permanent_failure",
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
async fn test_attempts_before_cursor_excludes_rows_at_or_before() {
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
async fn test_attempts_404_unknown_upstream() {
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
async fn test_upstream_detail_shape_unchanged() {
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
            "base_url",
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
    assert!(body.get("lanes").is_none());
}
