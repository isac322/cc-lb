use std::sync::Arc;

use crate::config_admin_common;

#[path = "cache_keepalive_contracts/empty.rs"]
mod empty;
#[path = "cache_keepalive_contracts/fixtures.rs"]
mod fixtures;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use cc_lb_storage_api::upstream::UpstreamKind as StorageUpstreamKind;
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveReplaceRequest, CacheKeepaliveSessionStore,
    CacheTtl, UpstreamCreate, UpstreamStore,
};
use config_admin_common::{app, authed_bytes, authed_json, temp_storage};
use fixtures::{principal_create_body, seed_cache_keepalive_contract_rows};
use serde_json::{Value, json};

const NOW_UNIX_SECS: u64 = 1_730_000_100;

async fn create_principal_id() -> (
    tempfile::TempDir,
    std::sync::Arc<cc_lb_storage_sqlite::SqliteStorage>,
    cc_lb_admin::AdminState,
    String,
) {
    let (directory, storage) = temp_storage().await;
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS));
    let state =
        config_admin_common::test_state_with_clock(Config::default(), Some(storage.clone()), clock);
    let (status, _headers, body, _raw) = authed_json(
        app(state.clone()),
        "POST",
        "/admin/v1/principals",
        Some(principal_create_body()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "principal create failed: {body:?}"
    );
    let principal_id = body["id"]
        .as_str()
        .expect("created principal id")
        .to_owned();
    (directory, storage, state, principal_id)
}

#[tokio::test]
async fn cache_keepalive_list_returns_card_summary_without_rows_when_limit_is_zero() {
    // Given: a principal with persisted sessions and a deterministic current time.
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_cache_keepalive_contract_rows(&storage, &principal_id).await;

    // When: the card asks for its aggregate metrics only.
    let uri = format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=0");
    let (status, _headers, raw) = authed_bytes(app(state), "GET", &uri, None).await;

    // Then: the typed endpoint responds with the summary-only contract.
    assert_eq!(
        status,
        StatusCode::OK,
        "Cache keepalive list endpoint is not implemented: {raw:?}"
    );
    let body: Value = serde_json::from_slice(&raw).expect("list response JSON");
    assert!(body["summary"]["renewing_now"].is_number());
    assert!(body["summary"]["sessions_last_5m"].is_number());
    assert!(body["summary"]["renewals_fired"].is_number());
    assert!(body["summary"]["cost_saved"].is_number());
    assert_eq!(body["rows"], serde_json::json!([]));
    assert!(body["next_cursor"].is_null());
}

#[tokio::test]
async fn cache_keepalive_list_preserves_soft_deleted_upstream_names() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let upstream = storage
        .create(UpstreamCreate {
            name: "retired-upstream".to_owned(),
            kind: StorageUpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        })
        .await
        .expect("create upstream");
    let session_key_hash = "soft-deleted-upstream-session";
    storage
        .replace_from_real_request(&CacheKeepaliveReplaceRequest {
            session_key_hash: session_key_hash.to_owned(),
            principal_id: principal_id.clone(),
            accounting_key_id: None,
            upstream_id: upstream.id,
            cache_anchor_at_unix_secs: NOW_UNIX_SECS,
            ttl: CacheTtl::Ttl5m,
            run_at_unix_secs: NOW_UNIX_SECS + 270,
            expires_at_unix_secs: NOW_UNIX_SECS + 300,
            encrypted_payload: vec![1],
            display_reason: "scheduled".to_owned(),
            config_snapshot: CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            },
            now_unix_secs: NOW_UNIX_SECS,
        })
        .await
        .expect("seed keepalive session");
    storage
        .soft_delete(upstream.id, upstream.revision)
        .await
        .expect("soft delete upstream");

    let list_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=10");
    let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &list_uri, None).await;
    assert_eq!(status, StatusCode::OK, "list response: {raw:?}");
    let list: Value = serde_json::from_slice(&raw).expect("list response JSON");
    let row = list["rows"]
        .as_array()
        .expect("list rows")
        .iter()
        .find(|row| row["id"] == session_key_hash)
        .expect("seeded keepalive row");
    assert_eq!(row["upstream"], "retired-upstream");

    let detail_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive/{session_key_hash}");
    let (status, _headers, raw) = authed_bytes(app(state), "GET", &detail_uri, None).await;
    assert_eq!(status, StatusCode::OK, "detail response: {raw:?}");
    let detail: Value = serde_json::from_slice(&raw).expect("detail response JSON");
    assert_eq!(detail["upstream"], row["upstream"]);
}

#[tokio::test]
async fn cache_keepalive_detail_returns_frozen_multi_turn_fields() {
    // Given: a principal with a terminal session and its persisted renewal turns.
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_cache_keepalive_contract_rows(&storage, &principal_id).await;

    // When: the drawer loads the selected session.
    let uri = format!("/admin/v1/principals/{principal_id}/cache-keepalive/f4b71a0c9d52");
    let (status, _headers, raw) = authed_bytes(app(state), "GET", &uri, None).await;

    // Then: every frozen detail field is present and turns remain newest first.
    assert_eq!(
        status,
        StatusCode::OK,
        "Cache keepalive detail endpoint is not implemented: {raw:?}"
    );
    let body: Value = serde_json::from_slice(&raw).expect("detail response JSON");
    for field in [
        "id",
        "last_message_at_ms",
        "state",
        "ttl",
        "attempts",
        "max_attempts",
        "reason",
        "generation",
        "upstream",
        "error",
        "net_pnl",
        "session_key_hash",
        "renewal_tokens",
        "total_avoided",
        "total_spent",
        "total_renewals",
        "is_last_pending",
        "turns",
        "config_snapshot",
        "raw_record",
    ] {
        assert!(body.get(field).is_some(), "missing detail field {field}");
    }
    let turns = body["turns"].as_array().expect("detail turns array");
    assert_eq!(turns.len(), 3);
    assert!(turns.windows(2).all(|pair| {
        pair[0]["time_ms"].as_u64().unwrap_or_default()
            >= pair[1]["time_ms"].as_u64().unwrap_or_default()
    }));
}

#[tokio::test]
async fn cache_keepalive_list_scopes_rows_by_horizon_and_paginates_with_a_cursor() {
    // Given: one terminal session outside 24 hours and several recent rows.
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_cache_keepalive_contract_rows(&storage, &principal_id).await;

    // When: the drawer reads the all-time list in two pages and the default horizon.
    let all_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=2");
    let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &all_uri, None).await;
    assert_eq!(status, StatusCode::OK, "all-time list: {raw:?}");
    let first: Value = serde_json::from_slice(&raw).expect("first page JSON");
    let cursor = first["next_cursor"].as_str().expect("next cursor");
    let next_uri = format!("{all_uri}&cursor={cursor}");
    let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &next_uri, None).await;
    assert_eq!(status, StatusCode::OK, "second page: {raw:?}");
    let second: Value = serde_json::from_slice(&raw).expect("second page JSON");
    let first_ids = first["rows"]
        .as_array()
        .expect("first rows")
        .iter()
        .map(|row| row["id"].as_str().expect("row id"))
        .collect::<Vec<_>>();
    assert!(
        second["rows"]
            .as_array()
            .expect("second rows")
            .iter()
            .all(|row| !first_ids.contains(&row["id"].as_str().expect("row id")))
    );
    let default_uri = format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=100");
    let (status, _headers, raw) = authed_bytes(app(state), "GET", &default_uri, None).await;

    // Then: the horizon changes only list rows, never fixed card metrics.
    assert_eq!(status, StatusCode::OK, "default horizon list: {raw:?}");
    let default_page: Value = serde_json::from_slice(&raw).expect("default page JSON");
    assert_eq!(first["summary"], default_page["summary"]);
    assert!(
        default_page["rows"]
            .as_array()
            .expect("default rows")
            .iter()
            .all(|row| row["id"] != "capped-max-duration-4h")
    );
}

#[tokio::test]
async fn cache_keepalive_list_filters_base_states_and_orthogonal_errors() {
    // Given: persisted Renewed, Scheduled, Capped, Expired, Not tracked, and Error rows.
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_cache_keepalive_contract_rows(&storage, &principal_id).await;

    // When: the drawer selects the frozen state and error filters.
    for (status_filter, expected_id, expected_reason) in [
        (
            "renewed",
            "a1f39c2b7e04",
            "agent-in-turn (tool_use: `bash`)",
        ),
        (
            "scheduled",
            "7b204de1c83f",
            "agent-in-turn (tool_use: `edit_file`) — first renewal in 4m 30s",
        ),
        ("capped", "f4b71a0c9d52", "max renewals reached"),
        ("expired", "0b33e9f71a2c", "TTL expired before follow-up"),
        (
            "not_tracked",
            "decision-2d8077e9a1c4",
            "user turn (stop_reason=end_turn)",
        ),
    ] {
        let uri = format!(
            "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&status={status_filter}"
        );
        let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &uri, None).await;
        assert_eq!(status, StatusCode::OK, "{status_filter} filter: {raw:?}");
        let body: Value = serde_json::from_slice(&raw).expect("filtered response JSON");
        assert!(
            body["rows"]
                .as_array()
                .expect("filtered rows")
                .iter()
                .any(|row| { row["id"] == expected_id && row["reason"] == expected_reason })
        );
    }
    let error_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&status=error");
    let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &error_uri, None).await;
    assert_eq!(status, StatusCode::OK, "error status filter: {raw:?}");
    let status_error: Value = serde_json::from_slice(&raw).expect("error response JSON");
    let boolean_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&error=true");
    let (status, _headers, raw) = authed_bytes(app(state), "GET", &boolean_uri, None).await;

    // Then: Error selects every row with an error regardless of its base state.
    assert_eq!(status, StatusCode::OK, "error=true filter: {raw:?}");
    let boolean_error: Value = serde_json::from_slice(&raw).expect("boolean error JSON");
    assert_eq!(status_error["rows"], boolean_error["rows"]);
    assert!(
        status_error["rows"]
            .as_array()
            .expect("error rows")
            .iter()
            .all(|row| row["error"].is_string())
    );
}

#[tokio::test]
async fn cache_keepalive_detail_resolves_not_tracked_decision_and_rejects_malformed_queries() {
    // Given: a decision-only Not tracked entry.
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_cache_keepalive_contract_rows(&storage, &principal_id).await;

    // When: the detail route and malformed query boundaries are requested.
    let detail_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive/decision-2d8077e9a1c4");
    let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &detail_uri, None).await;
    assert_eq!(status, StatusCode::OK, "not tracked detail: {raw:?}");
    let detail: Value = serde_json::from_slice(&raw).expect("decision detail JSON");
    assert_eq!(detail["state"], "not_tracked");
    assert_eq!(detail["attempts"], Value::Null);
    assert_eq!(detail["turns"], json!([]));
    for query in ["horizon=2h", "status=warm", "cursor=not-a-cursor"] {
        let uri = format!("/admin/v1/principals/{principal_id}/cache-keepalive?{query}");
        let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &uri, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {raw:?}");
    }

    // Then: detail exposes its raw record without fabricating renewal activity.
    assert_eq!(detail["raw_record"]["principal_id"], principal_id);
    assert_eq!(detail["total_renewals"], 0);
}
