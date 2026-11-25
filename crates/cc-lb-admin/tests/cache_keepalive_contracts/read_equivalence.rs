use std::collections::{BTreeMap, HashMap};
use std::process::Command;

use axum::{
    extract::Query,
    http::{StatusCode, Uri, header},
};
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion};
use cc_lb_storage_api::{
    CacheKeepaliveHitRefreshRequest, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    PrincipalStore,
};
use serde_json::{Value, json};
use sqlx::Row as _;
use uuid::Uuid;

use crate::config_admin_common::{app, authed_bytes, authed_json};

use super::fixtures::{
    cache_keepalive_replace_request, principal_create_body_named, seed_cache_keepalive_decision,
    seed_cache_keepalive_turn,
};
use super::{
    NOW_UNIX_SECS, create_principal_id, keepalive_detail_uri, seed_cache_keepalive_contract_rows,
};

const FIVE_MINUTES_MS: u64 = 5 * 60 * 1_000;

#[derive(serde::Deserialize)]
struct DetailId {
    id: String,
}

async fn get_json(state: &cc_lb_admin::AdminState, uri: &str) -> (StatusCode, Value) {
    let (status, _headers, body, _raw) = authed_json(app(state.clone()), "GET", uri, None).await;
    (status, body)
}

async fn create_principal(state: &cc_lb_admin::AdminState, name: &str, enabled: bool) -> String {
    let (status, _headers, body, _raw) = authed_json(
        app(state.clone()),
        "POST",
        "/admin/v1/principals",
        Some(principal_create_body_named(name, enabled)),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "principal create: {body:?}");
    body["id"]
        .as_str()
        .expect("created principal id")
        .to_owned()
}

async fn seed_session(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    id: &str,
    last_message_at_ms: u64,
    expires_at_unix_secs: u64,
) {
    storage
        .replace_from_real_request_fenced(
            &cache_keepalive_replace_request(
                principal_id,
                id,
                last_message_at_ms / 1_000,
                expires_at_unix_secs,
            ),
            None,
        )
        .await
        .expect("seed cache keepalive session");
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET last_message_at_ms = ? WHERE session_key_hash = ?",
    )
    .bind(i64::try_from(last_message_at_ms).expect("fixture milliseconds fit i64"))
    .bind(id)
    .execute(storage.pool())
    .await
    .expect("set exact session timestamp");
}

async fn legacy_summary_oracle(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    now_ms: u64,
) -> Value {
    let rows = sqlx::query(
        "WITH entries AS (
            SELECT 'session:' || session_key_hash AS entry_id,
                   last_message_at_ms, status, refresh_count
            FROM cache_keepalive_sessions
            WHERE principal_id = ?
            UNION ALL
            SELECT 'decision:' || source_ref_id AS entry_id,
                   COALESCE(last_message_at_ms, ts * 1000), NULL, NULL
            FROM cache_keepalive_decisions
            WHERE principal_id = ?
              AND NOT EXISTS (
                  SELECT 1 FROM cache_keepalive_turns turn_row
                  WHERE turn_row.source_ref_id = cache_keepalive_decisions.source_ref_id
              )
         )
         SELECT entry_id, last_message_at_ms, status, refresh_count
         FROM entries
         ORDER BY last_message_at_ms DESC, entry_id ASC",
    )
    .bind(principal_id)
    .bind(principal_id)
    .fetch_all(storage.pool())
    .await
    .expect("run legacy full-list oracle");

    let cutoff = now_ms.saturating_sub(FIVE_MINUTES_MS);
    let mut renewing_now = 0_u64;
    let mut sessions_last_5m = 0_u64;
    for row in rows {
        let last_message_at_ms: i64 = row.get("last_message_at_ms");
        if u64::try_from(last_message_at_ms).expect("fixture timestamp is nonnegative") >= cutoff {
            sessions_last_5m = sessions_last_5m.saturating_add(1);
        }
        let status: Option<String> = row.get("status");
        if status.as_deref() == Some("active") {
            renewing_now = renewing_now.saturating_add(1);
        }
    }

    json!({
        "summary": {
            "renewing_now": renewing_now,
            "sessions_last_5m": sessions_last_5m,
            "retained_successful_hits": 0,
            "unlisted_sessions": 0,
            "unlisted_renewals": 0,
            "unlisted_spent": 0.0,
            "unlisted_net": 0.0,
            "total_spent": 0.0,
            "total_avoided": null,
            "evidence_status": "none",
            "unknown_cycles": 0,
            "not_needed_cycles": 0,
            "retained_from_ms": null
        },
        "rows": [],
        "next_cursor": null
    })
}

#[tokio::test]
async fn invalid_principal_id_is_400() {
    let (_directory, _storage, state, _principal_id) = create_principal_id().await;
    let (status, body) = get_json(&state, "/admin/v1/principals/not-a-uuid/cache-keepalive").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({ "error": "invalid_principal_id" }));
}

#[tokio::test]
async fn unknown_or_deleted_principal_is_404() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let absent_id = Uuid::new_v4();
    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{absent_id}/cache-keepalive"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({ "error": "unknown_principal" }));

    let principal_uuid = principal_id.parse().expect("principal UUID");
    let principal = PrincipalStore::get_by_id(storage.as_ref(), principal_uuid)
        .await
        .expect("read principal")
        .expect("principal exists");
    PrincipalStore::soft_delete(
        storage.as_ref(),
        principal_uuid,
        principal.revision,
        NOW_UNIX_SECS,
    )
    .await
    .expect("soft delete principal")
    .expect("deleted principal exists");
    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({ "error": "unknown_principal" }));
}

#[tokio::test]
async fn invalid_keepalive_query_values_preserve_codes() {
    let (_directory, _storage, state, principal_id) = create_principal_id().await;
    let cases = [
        ("limit=101", "invalid_cache_keepalive_limit"),
        ("limit=abc", "invalid_cache_keepalive_limit"),
        ("horizon=2h", "invalid_cache_keepalive_horizon"),
        ("status=warm", "invalid_cache_keepalive_status"),
        (
            "status=renewed&error=true",
            "invalid_cache_keepalive_filter",
        ),
        ("error=maybe", "invalid_cache_keepalive_error"),
        ("cursor=not-base64", "invalid_cache_keepalive_cursor"),
    ];
    for (query, expected_error) in cases {
        let (status, body) = get_json(
            &state,
            &format!("/admin/v1/principals/{principal_id}/cache-keepalive?{query}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "query {query}: {body:?}");
        assert_eq!(body, json!({ "error": expected_error }), "query {query}");
    }
}

#[tokio::test]
async fn mismatched_cursor_is_400_invalid_input() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    seed_cache_keepalive_decision(&storage, &principal_id, "cursor-new", now_ms).await;
    seed_cache_keepalive_decision(&storage, &principal_id, "cursor-old", now_ms - 1).await;
    let other_id = create_principal(&state, "cache-keepalive-cursor-other", true).await;

    let (status, first) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first page: {first:?}");
    let cursor = first["next_cursor"].as_str().expect("next cursor");
    for uri in [
        format!(
            "/admin/v1/principals/{other_id}/cache-keepalive?horizon=all&limit=1&cursor={cursor}"
        ),
        format!(
            "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=7d&limit=1&cursor={cursor}"
        ),
        format!(
            "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&status=not_tracked&limit=1&cursor={cursor}"
        ),
    ] {
        let (status, body) = get_json(&state, &uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body:?}");
        assert_eq!(body["error"], "invalid_input");
        assert_eq!(body["field"], "cache_keepalive_session_cursor");
        assert_eq!(
            body["reason"],
            "cursor does not match principal, horizon, or filter"
        );
    }
}

#[tokio::test]
async fn detail_query_transport_round_trips_structural_and_escaped_keys() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    let ids = [
        format!("{principal_id}/thread:thread-a"),
        format!("{principal_id}/thread:thread-a/agent:agent-a"),
        format!("{principal_id}/thread:a%252Fagent:b"),
        format!("{principal_id}/thread:a%25-ready"),
    ];

    for id in ids {
        seed_session(&storage, &principal_id, &id, now_ms, NOW_UNIX_SECS + 300).await;
        let uri = keepalive_detail_uri(&principal_id, &id);
        let (status, body) = get_json(&state, &uri).await;
        assert_eq!(status, StatusCode::OK, "{id}: {body:?}");
        assert_eq!(body["id"], id);
        assert_eq!(body["session_key_hash"], id);

        let stored: String = sqlx::query(
            "SELECT session_key_hash FROM cache_keepalive_sessions WHERE session_key_hash = ?",
        )
        .bind(&id)
        .fetch_one(storage.pool())
        .await
        .expect("stored session key")
        .get("session_key_hash");
        assert_eq!(stored, id);
    }
}

#[tokio::test]
async fn detail_query_transport_round_trips_special_characters() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    let ids = [
        format!("{principal_id}/thread:+%&=?# space/ユニコード"),
        format!("{principal_id}/thread:literal%2F/%25"),
    ];

    for id in ids {
        seed_session(&storage, &principal_id, &id, now_ms, NOW_UNIX_SECS + 300).await;
        let uri = keepalive_detail_uri(&principal_id, &id);
        let (status, body) = get_json(&state, &uri).await;
        assert_eq!(status, StatusCode::OK, "{id}: {body:?}");
        assert_eq!(body["id"], id);
        assert_eq!(body["session_key_hash"], id);
        let stored = storage
            .get_cache_keepalive_session(&id)
            .await
            .expect("read stored session")
            .expect("stored session exists");
        assert_eq!(stored.session_key_hash, id);
    }
}

#[tokio::test]
async fn detail_query_transport_preserves_malformed_percent_behavior() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    for (index, suffix) in ["%", "%2", "%GG", "%FF"].into_iter().enumerate() {
        let uri = format!(
            "/admin/v1/principals/{principal_id}/cache-keepalive/detail?id=malformed-{index}-{suffix}"
        );
        let reference = Query::<DetailId>::try_from_uri(&uri.parse::<Uri>().unwrap());
        match reference {
            Ok(reference) => {
                seed_session(
                    &storage,
                    &principal_id,
                    &reference.id,
                    NOW_UNIX_SECS * 1_000,
                    NOW_UNIX_SECS + 300,
                )
                .await;
                let (status, body) = get_json(&state, &uri).await;
                assert_eq!(status, StatusCode::OK, "{uri}: {body:?}");
                assert_eq!(body["id"], reference.id);
            }
            Err(_) => {
                let (status, headers, raw) =
                    authed_bytes(app(state.clone()), "GET", &uri, None).await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {raw:?}");
                assert_eq!(headers[header::CONTENT_TYPE], "application/json");
                let body: Value = serde_json::from_slice(&raw).expect("validation response JSON");
                assert_eq!(body["error"], "validation_failed");
                assert_eq!(body["field"], "id");
            }
        }
    }
}

#[tokio::test]
async fn detail_query_transport_preserves_principal_scope() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let other_id = create_principal(&state, "cache-keepalive-query-other", true).await;
    let session_id = format!("{principal_id}/thread:scoped/agent:agent-a");
    seed_session(
        &storage,
        &principal_id,
        &session_id,
        NOW_UNIX_SECS * 1_000,
        NOW_UNIX_SECS + 300,
    )
    .await;

    let (status, body) = get_json(&state, &keepalive_detail_uri(&principal_id, &session_id)).await;
    assert_eq!(status, StatusCode::OK, "owner lookup: {body:?}");
    assert_eq!(body["id"], session_id);

    let (status, body) = get_json(&state, &keepalive_detail_uri(&other_id, &session_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({ "error": "unknown_cache_keepalive_entry" }));

    for (principal, expected) in [
        (
            "not-a-uuid".to_owned(),
            json!({ "error": "invalid_principal_id" }),
        ),
        (
            Uuid::new_v4().to_string(),
            json!({ "error": "unknown_principal" }),
        ),
    ] {
        let (status, body) = get_json(&state, &keepalive_detail_uri(&principal, &session_id)).await;
        assert_eq!(
            status,
            if expected["error"] == "invalid_principal_id" {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::NOT_FOUND
            }
        );
        assert_eq!(body, expected);
    }

    let principal_uuid = principal_id.parse().expect("principal UUID");
    let principal = PrincipalStore::get_by_id(storage.as_ref(), principal_uuid)
        .await
        .expect("read principal")
        .expect("principal exists");
    PrincipalStore::soft_delete(
        storage.as_ref(),
        principal_uuid,
        principal.revision,
        NOW_UNIX_SECS,
    )
    .await
    .expect("soft delete principal")
    .expect("deleted principal exists");
    let (status, body) = get_json(&state, &keepalive_detail_uri(&principal_id, &session_id)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({ "error": "unknown_principal" }));
}

#[tokio::test]
async fn detail_query_transport_preserves_validation_lookup_and_fallback_boundaries() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_cache_keepalive_contract_rows(&storage, &principal_id).await;

    let (status, body) = get_json(&state, &keepalive_detail_uri(&principal_id, "missing-id")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({ "error": "unknown_cache_keepalive_entry" }));

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive/detail?id="),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({ "error": "unknown_cache_keepalive_entry" }));

    let known_uri = keepalive_detail_uri(&principal_id, "f4b71a0c9d52");
    let (status, known) = get_json(&state, &known_uri).await;
    assert_eq!(status, StatusCode::OK, "known detail: {known:?}");
    let known_with_extra = format!("{known_uri}&limit=1");
    let (status, extra) = get_json(&state, &known_with_extra).await;
    assert_eq!(status, StatusCode::OK, "extra detail query: {extra:?}");
    assert_eq!(extra["id"], known["id"]);

    for uri in [
        format!("/admin/v1/principals/{principal_id}/cache-keepalive/detail"),
        format!("/admin/v1/principals/{principal_id}/cache-keepalive/detail?id=a&id=b"),
    ] {
        let (status, headers, raw) = authed_bytes(app(state.clone()), "GET", &uri, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {raw:?}");
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        let body: Value = serde_json::from_slice(&raw).expect("validation response JSON");
        assert_eq!(body["error"], "validation_failed");
        assert_eq!(body["field"], "id");
    }

    let (status, _headers, raw) = authed_bytes(
        app(state.clone()),
        "GET",
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive/decision:1"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(raw.is_empty(), "legacy path must use unmatched fallback");

    let (status, _headers, raw) =
        authed_bytes(app(state), "GET", "/admin/v1/not-a-real-route", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        raw.is_empty(),
        "unknown admin path must use unmatched fallback"
    );
}

#[tokio::test]
async fn detail_direct_lookup_preserves_session_and_decision_wire() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    seed_session(
        &storage,
        &principal_id,
        "detail-session",
        now_ms,
        NOW_UNIX_SECS + 300,
    )
    .await;
    seed_cache_keepalive_turn(
        &storage,
        &principal_id,
        "detail-session",
        "detail-session-turn",
        NOW_UNIX_SECS,
        "unknown-model",
        20_000,
    )
    .await;
    seed_cache_keepalive_decision(&storage, &principal_id, "detail-decision", now_ms - 1).await;

    let session_uri = keepalive_detail_uri(&principal_id, "detail-session");
    let (session_status, session) = get_json(&state, &session_uri).await;
    assert_eq!(
        session_status,
        StatusCode::OK,
        "session detail: {session:?}"
    );
    assert_eq!(session["id"], "detail-session");
    assert_eq!(session["raw_record"]["status"], "active");
    assert_eq!(session["turns"].as_array().expect("session turns").len(), 1);
    assert!(session["turns"][0]["pnl"].is_null());

    let decision_uri = keepalive_detail_uri(&principal_id, "detail-decision");
    let (decision_status, decision) = get_json(&state, &decision_uri).await;
    assert_eq!(
        decision_status,
        StatusCode::OK,
        "decision detail: {decision:?}"
    );
    assert_eq!(decision["state"], "not_tracked");
    assert!(decision["attempts"].is_null());
    assert_eq!(decision["turns"], json!([]));
    assert_eq!(decision["raw_record"]["principal_id"], principal_id);
}

#[tokio::test]
async fn detail_collision_matches_legacy_first_match() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    seed_session(
        &storage,
        &principal_id,
        "collision",
        now_ms,
        NOW_UNIX_SECS + 300,
    )
    .await;
    seed_cache_keepalive_decision(&storage, &principal_id, "collision", now_ms).await;

    let collision_uri = keepalive_detail_uri(&principal_id, "collision");
    let (status, tied) = get_json(&state, &collision_uri).await;
    assert_eq!(status, StatusCode::OK, "tie detail: {tied:?}");
    assert_eq!(
        tied["state"], "not_tracked",
        "decision namespace sorts first"
    );

    sqlx::query("UPDATE cache_keepalive_sessions SET last_message_at_ms = ? WHERE session_key_hash = 'collision'")
        .bind(i64::try_from(now_ms + 1).expect("fixture timestamp fits"))
        .execute(storage.pool())
        .await
        .expect("make session newer");
    let (status, session_newer) = get_json(&state, &collision_uri).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "session-newer detail: {session_newer:?}"
    );
    assert_eq!(session_newer["raw_record"]["status"], "active");

    sqlx::query("UPDATE cache_keepalive_decisions SET last_message_at_ms = ? WHERE source_ref_id = 'collision'")
        .bind(i64::try_from(now_ms + 2).expect("fixture timestamp fits"))
        .execute(storage.pool())
        .await
        .expect("make decision newer");
    let (status, decision_newer) = get_json(&state, &collision_uri).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "decision-newer detail: {decision_newer:?}"
    );
    assert_eq!(decision_newer["state"], "not_tracked");
}

#[tokio::test]
async fn detail_hides_late_joined_decision() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    let uri = keepalive_detail_uri(&principal_id, "late-decision");
    seed_cache_keepalive_decision(&storage, &principal_id, "late-decision", now_ms).await;
    let (before_status, before) = get_json(&state, &uri).await;
    assert_eq!(
        before_status,
        StatusCode::OK,
        "before late turn: {before:?}"
    );
    assert_eq!(before["state"], "not_tracked");

    seed_cache_keepalive_turn(
        &storage,
        &principal_id,
        "late-session",
        "late-decision",
        NOW_UNIX_SECS,
        "unknown-model",
        1,
    )
    .await;
    let (after_status, after) = get_json(&state, &uri).await;
    assert_eq!(
        after_status,
        StatusCode::NOT_FOUND,
        "after late turn: {after:?}"
    );
    assert_eq!(after, json!({ "error": "unknown_cache_keepalive_entry" }));
}

#[tokio::test]
async fn empty_and_disabled_summary_are_zero() {
    let (_directory, _storage, state, enabled_id) = create_principal_id().await;
    let disabled_id = create_principal(&state, "cache-keepalive-disabled-equivalence", false).await;
    let expected = json!({
        "summary": {
            "renewing_now": 0,
            "sessions_last_5m": 0,
            "retained_successful_hits": 0,
            "unlisted_sessions": 0,
            "unlisted_renewals": 0,
            "unlisted_spent": 0.0,
            "unlisted_net": 0.0,
            "cost_saved": 0.0,
            "total_spent": 0.0,
            "total_avoided": 0.0,
            "evidence_status": "none",
            "unknown_cycles": 0,
            "not_needed_cycles": 0,
            "retained_from_ms": null
        },
        "groups": null,
        "rows": [],
        "next_cursor": null
    });
    for principal_id in [enabled_id, disabled_id] {
        let (status, body) = get_json(
            &state,
            &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=0"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "empty summary: {body:?}");
        assert_eq!(body, expected);
    }
}

#[tokio::test]
async fn summary_five_minute_cutoff_is_inclusive() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    let cutoff = now_ms - FIVE_MINUTES_MS;
    seed_session(
        &storage,
        &principal_id,
        "session-at-cutoff",
        cutoff,
        NOW_UNIX_SECS + 300,
    )
    .await;
    seed_session(
        &storage,
        &principal_id,
        "session-before-cutoff",
        cutoff - 1,
        NOW_UNIX_SECS + 300,
    )
    .await;
    seed_cache_keepalive_decision(&storage, &principal_id, "decision-at-cutoff", cutoff).await;
    seed_cache_keepalive_decision(
        &storage,
        &principal_id,
        "decision-before-cutoff",
        cutoff - 1,
    )
    .await;
    seed_cache_keepalive_decision(&storage, &principal_id, "joined-at-cutoff", cutoff).await;
    seed_cache_keepalive_turn(
        &storage,
        &principal_id,
        "joined-session",
        "joined-at-cutoff",
        cutoff / 1_000,
        "unknown-model",
        1,
    )
    .await;

    let mut expected = legacy_summary_oracle(&storage, &principal_id, now_ms).await;
    expected["summary"]["retained_successful_hits"] = json!(1);
    expected["summary"]["evidence_status"] = json!("unknown");
    expected["summary"]["cost_saved"] = json!(0.0);
    expected["summary"]["total_avoided"] = json!(0.0);
    expected["summary"]["unknown_cycles"] = json!(1);
    // The joined turn has neither a session row nor renewal evidence, so it is
    // only visible through the deleted-sessions aggregate.
    expected["summary"]["unlisted_sessions"] = json!(1);
    expected["summary"]["unlisted_renewals"] = json!(1);
    expected["groups"] = Value::Null;
    let (status, candidate) = get_json(
        &state,
        &format!(
            "/admin/v1/principals/{principal_id}/cache-keepalive?limit=0&horizon=24h&status=expired"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "candidate summary: {candidate:?}");
    assert_eq!(
        candidate, expected,
        "candidate must equal legacy full-list summary"
    );
    assert_eq!(candidate["summary"]["sessions_last_5m"], 2);
}

#[tokio::test]
async fn mixed_read_paths_preserve_sort_horizon_limit_and_tenant_scope() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let other_id = create_principal(&state, "cache-keepalive-tenant-other", true).await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    seed_session(
        &storage,
        &principal_id,
        "z-session",
        now_ms,
        NOW_UNIX_SECS + 300,
    )
    .await;
    seed_cache_keepalive_decision(&storage, &principal_id, "a-decision", now_ms).await;
    seed_cache_keepalive_decision(
        &storage,
        &principal_id,
        "old-decision",
        now_ms - 8 * 24 * 60 * 60 * 1_000,
    )
    .await;
    seed_cache_keepalive_decision(&storage, &other_id, "tenant-secret", now_ms + 1).await;

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=2"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "mixed list: {body:?}");
    let ids = body["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["id"].as_str().expect("row id"))
        .collect::<Vec<_>>();
    assert_eq!(ids, ["a-decision", "z-session"]);
    assert!(body["next_cursor"].is_null());
    assert!(!body.to_string().contains("old-decision"));
    assert!(!body.to_string().contains("tenant-secret"));

    let missing_uri = keepalive_detail_uri(&principal_id, "tenant-secret");
    let (status, missing) = get_json(&state, &missing_uri).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing, json!({ "error": "unknown_cache_keepalive_entry" }));
}

#[tokio::test]
async fn corruption_mapping_tracks_narrow_read_scope() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    seed_session(
        &storage,
        &principal_id,
        "corrupt-session",
        now_ms,
        NOW_UNIX_SECS + 300,
    )
    .await;
    sqlx::query("UPDATE cache_keepalive_sessions SET config_snapshot = '{' WHERE session_key_hash = 'corrupt-session'")
        .execute(storage.pool())
        .await
        .expect("corrupt selected summary session");
    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "selected summary corruption: {body:?}"
    );
    assert_eq!(body, json!({ "error": "storage_error" }));

    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let old_ms = now_ms - 8 * 24 * 60 * 60 * 1_000;
    seed_cache_keepalive_decision(&storage, &principal_id, "unrelated-old-corrupt", old_ms).await;
    sqlx::query("UPDATE cache_keepalive_decisions SET config_snapshot = '{' WHERE source_ref_id = 'unrelated-old-corrupt'")
        .execute(storage.pool())
        .await
        .expect("corrupt unrelated old decision");
    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unrelated old corruption: {body:?}");
    assert_eq!(body["rows"], json!([]));

    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_cache_keepalive_decision(&storage, &principal_id, "selected-corrupt-decision", now_ms)
        .await;
    sqlx::query("UPDATE cache_keepalive_decisions SET config_snapshot = '{' WHERE source_ref_id = 'selected-corrupt-decision'")
        .execute(storage.pool())
        .await
        .expect("corrupt selected detail decision");
    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "selected list corruption: {body:?}"
    );
    assert_eq!(body, json!({ "error": "storage_error" }));
    let corrupt_decision_uri = keepalive_detail_uri(&principal_id, "selected-corrupt-decision");
    let (status, body) = get_json(&state, &corrupt_decision_uri).await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "selected detail corruption: {body:?}"
    );
    assert_eq!(body, json!({ "error": "storage_error" }));

    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_session(
        &storage,
        &principal_id,
        "selected-corrupt-turn-session",
        now_ms,
        NOW_UNIX_SECS + 300,
    )
    .await;
    seed_cache_keepalive_turn(
        &storage,
        &principal_id,
        "selected-corrupt-turn-session",
        "selected-corrupt-turn",
        NOW_UNIX_SECS,
        "unknown-model",
        1,
    )
    .await;
    sqlx::query(
        "UPDATE cache_keepalive_turns SET cache_read_input_tokens = -1
         WHERE source_ref_id = 'selected-corrupt-turn'",
    )
    .execute(storage.pool())
    .await
    .expect("corrupt selected turn");
    let corrupt_turn_uri = keepalive_detail_uri(&principal_id, "selected-corrupt-turn-session");
    let (status, body) = get_json(&state, &corrupt_turn_uri).await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "selected turn corruption: {body:?}"
    );
    assert_eq!(body, json!({ "error": "storage_error" }));
}

#[tokio::test]
async fn reactivation_increments_generation_and_resets_summary_inputs() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let request = cache_keepalive_replace_request(
        &principal_id,
        "reactivated-session",
        NOW_UNIX_SECS - 10,
        NOW_UNIX_SECS + 300,
    );
    let first = storage
        .replace_from_real_request_fenced(&request, None)
        .await
        .expect("create session")
        .expect("created session record");
    let renewed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: first.session_key_hash.clone(),
            generation: first.generation,
            cache_anchor_at_unix_secs: NOW_UNIX_SECS - 9,
            run_at_unix_secs: NOW_UNIX_SECS + 261,
            expires_at_unix_secs: NOW_UNIX_SECS + 291,
            encrypted_payload: None,
            now_unix_secs: NOW_UNIX_SECS - 9,
        })
        .await
        .expect("renew session")
        .expect("renewed generation");
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                &renewed.session_key_hash,
                renewed.generation,
                CacheKeepaliveTerminalReason::DispatchError,
                Some("old failure"),
                NOW_UNIX_SECS - 8,
            )
            .await
            .expect("terminalize session")
    );

    let detail_uri = keepalive_detail_uri(&principal_id, "reactivated-session");
    let (before_status, before) = get_json(&state, &detail_uri).await;
    assert_eq!(
        before_status,
        StatusCode::OK,
        "before replacement: {before:?}"
    );
    assert_eq!(before["error"], "old failure");
    assert_eq!(before["attempts"], 1);

    let replacement = storage
        .replace_from_real_request_fenced(
            &cache_keepalive_replace_request(
                &principal_id,
                "reactivated-session",
                NOW_UNIX_SECS,
                NOW_UNIX_SECS + 300,
            ),
            None,
        )
        .await
        .expect("reactivate session")
        .expect("reactivated session record");
    assert_eq!(replacement.generation, renewed.generation + 1);
    assert_eq!(replacement.refresh_count, 0);
    assert!(replacement.error.is_none());
    assert!(replacement.terminal_reason.is_none());

    let (after_status, after) = get_json(&state, &detail_uri).await;
    assert_eq!(after_status, StatusCode::OK, "after replacement: {after:?}");
    assert_eq!(after["generation"], replacement.generation);
    assert_eq!(after["attempts"], 0);
    assert!(after["error"].is_null());
    assert_eq!(after["raw_record"]["status"], "active");
    let (summary_status, summary) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=0"),
    )
    .await;
    assert_eq!(
        summary_status,
        StatusCode::OK,
        "replacement summary: {summary:?}"
    );
    assert_eq!(summary["summary"]["renewing_now"], 1);
    assert_eq!(summary["summary"]["retained_successful_hits"], 0);
    let (list_status, list) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK, "replacement list: {list:?}");
    assert_eq!(list["rows"][0]["id"], "reactivated-session");
    assert_eq!(list["rows"][0]["state"], "scheduled");
    assert_eq!(list["rows"][0]["attempts"], 0);
}

#[tokio::test]
async fn late_turn_transition_converges_without_duplicate_visible_entry() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    seed_cache_keepalive_decision(&storage, &principal_id, "transition-source", now_ms).await;
    let list_uri = format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all");
    let (before_status, before) = get_json(&state, &list_uri).await;
    assert_eq!(
        before_status,
        StatusCode::OK,
        "before transition: {before:?}"
    );
    assert_eq!(before["rows"][0]["id"], "transition-source");

    seed_session(
        &storage,
        &principal_id,
        "transition-session",
        now_ms + 1,
        NOW_UNIX_SECS + 300,
    )
    .await;
    seed_cache_keepalive_turn(
        &storage,
        &principal_id,
        "transition-session",
        "transition-source",
        NOW_UNIX_SECS,
        "unknown-model",
        1,
    )
    .await;
    let (after_status, after) = get_json(&state, &list_uri).await;
    assert_eq!(after_status, StatusCode::OK, "after transition: {after:?}");
    let ids = after["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["id"].as_str().expect("row id"))
        .collect::<Vec<_>>();
    assert_eq!(ids, ["transition-session"]);
}

#[tokio::test]
async fn cleanup_removes_only_expired_sessions_from_read_surfaces() {
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let now_ms = NOW_UNIX_SECS * 1_000;
    seed_session(
        &storage,
        &principal_id,
        "cleanup-expired",
        now_ms - 1,
        NOW_UNIX_SECS - 1,
    )
    .await;
    seed_session(
        &storage,
        &principal_id,
        "cleanup-retained",
        now_ms,
        NOW_UNIX_SECS + 300,
    )
    .await;
    let summary_uri = format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=0");
    let (before_status, before) = get_json(&state, &summary_uri).await;
    assert_eq!(before_status, StatusCode::OK, "before cleanup: {before:?}");
    assert_eq!(before["summary"]["renewing_now"], 2);
    let list_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=10");
    let (before_list_status, before_list) = get_json(&state, &list_uri).await;
    assert_eq!(
        before_list_status,
        StatusCode::OK,
        "before cleanup list: {before_list:?}"
    );
    let before_ids = before_list["rows"]
        .as_array()
        .expect("before cleanup rows")
        .iter()
        .map(|row| row["id"].as_str().expect("row id"))
        .collect::<Vec<_>>();
    assert_eq!(before_ids, ["cleanup-retained", "cleanup-expired"]);

    let removed = storage
        .purge_cache_keepalive_expired(NOW_UNIX_SECS)
        .await
        .expect("purge expired keepalive sessions");
    assert_eq!(removed, 1);

    let (after_status, after) = get_json(&state, &summary_uri).await;
    assert_eq!(after_status, StatusCode::OK, "after cleanup: {after:?}");
    assert_eq!(after["summary"]["renewing_now"], 1);
    let (after_list_status, after_list) = get_json(&state, &list_uri).await;
    assert_eq!(
        after_list_status,
        StatusCode::OK,
        "after cleanup list: {after_list:?}"
    );
    let after_ids = after_list["rows"]
        .as_array()
        .expect("after cleanup rows")
        .iter()
        .map(|row| row["id"].as_str().expect("row id"))
        .collect::<Vec<_>>();
    assert_eq!(after_ids, ["cleanup-retained"]);
    let expired_uri = keepalive_detail_uri(&principal_id, "cleanup-expired");
    let (expired_status, expired) = get_json(&state, &expired_uri).await;
    assert_eq!(
        expired_status,
        StatusCode::NOT_FOUND,
        "expired detail: {expired:?}"
    );
    let retained_uri = keepalive_detail_uri(&principal_id, "cleanup-retained");
    let (retained_status, retained) = get_json(&state, &retained_uri).await;
    assert_eq!(
        retained_status,
        StatusCode::OK,
        "retained detail: {retained:?}"
    );
}

#[test]
fn haiku_5_5_threshold_band_summary_matches_activity_and_missing_count_stays_null() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_equivalence::haiku_5_5_threshold_band_summary_matches_activity_and_missing_count_stays_null";

    if std::env::var("CC_LB_KEEPALIVE_HAIKU_BAND_CHILD").as_deref() == Ok("1") {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated Haiku threshold band runtime")
            .block_on(assert_haiku_5_5_threshold_band_summary_matches_activity());
        return;
    }

    let output =
        Command::new(std::env::current_exe().expect("current integration test executable"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env("CC_LB_KEEPALIVE_HAIKU_BAND_CHILD", "1")
            .output()
            .expect("run isolated Haiku threshold band test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains(TEST_NAME) && stdout.contains("1 passed"),
        "isolated Haiku threshold band test did not execute successfully\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

async fn assert_haiku_5_5_threshold_band_summary_matches_activity() {
    use cc_lb_storage_api::{
        CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow, CacheKeepaliveEvidenceRow,
        CacheKeepaliveTurnRow, CacheTtl, RequestEvent, RequestEventProjections, RequestEventStore,
    };

    const MODEL: &str = "claude-haiku-5-5";
    let mut models = HashMap::new();
    models.insert(
        MODEL.to_owned(),
        Pricing {
            native: None,
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_micros_usd(100_000),
            output_per_million_usd: UsdPerMillion::from_micros_usd(500_000),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: BTreeMap::new(),
        },
    );
    let mut cache_creation_per_million_usd = HashMap::new();
    cache_creation_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(125_000));
    let mut cache_read_per_million_usd = HashMap::new();
    cache_read_per_million_usd.insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(10_000));
    let mut bands = BTreeMap::new();
    bands.insert(
        100_000,
        cc_lb_pricing::ThresholdBand {
            default: cc_lb_pricing::ThresholdRate {
                input_per_million_usd: Some(UsdPerMillion::from_micros_usd(500_000)),
                output_per_million_usd: Some(UsdPerMillion::from_micros_usd(2_500_000)),
                cache_creation_per_million_usd: Some(UsdPerMillion::from_micros_usd(625_000)),
                cache_read_per_million_usd: Some(UsdPerMillion::from_micros_usd(50_000)),
            },
            by_tier: BTreeMap::new(),
        },
    );
    let mut above_tokens_rates = HashMap::new();
    above_tokens_rates.insert(MODEL.to_owned(), bands);
    cc_lb_pricing::global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "haiku-contract-threshold".to_owned(),
        fetched_at_ms: 1,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates,
        status: CatalogStatus::Ok,
    });

    let seed_cycle = |storage: std::sync::Arc<cc_lb_storage_sqlite::SqliteStorage>,
                      principal_id: &str,
                      session_id: &str,
                      cycle_id: Uuid,
                      gross_prompt_tokens: Option<u64>| {
        let principal_id = principal_id.to_owned();
        let session_id = session_id.to_owned();
        async move {
            let upstream_id = Uuid::from_u128(0xa11ce);
            let ts = NOW_UNIX_SECS - 100;
            let source_ref_id = format!("{session_id}:1");
            let prefix_hash = format!("prefix-{session_id}");
            let config = CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            };
            storage
                .replace_from_real_request_fenced(
                    &cc_lb_storage_api::CacheKeepaliveReplaceRequest {
                        session_key_hash: session_id.clone(),
                        principal_id: principal_id.clone(),
                        accounting_key_id: None,
                        upstream_id,
                        cycle_id,
                        cache_anchor_at_unix_secs: ts - 270,
                        ttl: CacheTtl::Ttl5m,
                        run_at_unix_secs: ts + 270,
                        expires_at_unix_secs: ts + 300,
                        encrypted_payload: vec![1],
                        display_reason: "agent-in-turn".to_owned(),
                        config_snapshot: config.clone(),
                        now_unix_secs: ts,
                    },
                    None,
                )
                .await
                .expect("seed haiku session");
            let event = RequestEvent {
                ts,
                ts_ms: Some(ts * 1_000),
                request_id: source_ref_id.clone(),
                source_kind: Some("renewal".to_owned()),
                source_ref_id: Some(source_ref_id.clone()),
                event_id: Some(format!("event-{source_ref_id}")),
                principal_id: Some(principal_id.clone()),
                upstream_id: Some(upstream_id),
                model: Some(MODEL.to_owned()),
                status: 200,
                duration_ms: 10,
                ..RequestEvent::default()
            };
            let renewal = CacheKeepaliveEvidenceRow {
                evidence_id: format!("renewal-{session_id}"),
                cycle_id: Some(cycle_id),
                evidence_kind: "renewal".to_owned(),
                request_start_ms: ts * 1_000,
                prefix_fingerprint: None,
                prefix_hash: Some(prefix_hash.clone()),
                prefix_policy_version: Some("policy".to_owned()),
                prefix_schema_version: Some("schema".to_owned()),
                principal_id: principal_id.clone(),
                upstream_id,
                model: MODEL.to_owned(),
                ttl: CacheTtl::Ttl5m,
                hit_miss: Some("hit".to_owned()),
                cache_read_input_tokens: Some(20_000),
                gross_prompt_tokens: None,
                cost_micros: Some(1_000),
                request_ref: None,
                event_ref: Some(source_ref_id.clone()),
                session_key_hash: Some(session_id.clone()),
                cycle_anchor_ms: Some((ts - 270) * 1_000),
            };
            let follow_up = CacheKeepaliveEvidenceRow {
                evidence_id: format!("followup-{session_id}"),
                cycle_id: None,
                evidence_kind: "follow_up".to_owned(),
                request_start_ms: (ts + 60) * 1_000,
                gross_prompt_tokens,
                cost_micros: None,
                event_ref: None,
                cycle_anchor_ms: None,
                ..renewal.clone()
            };
            let projections = RequestEventProjections {
                turn: Some(CacheKeepaliveTurnRow {
                    source_ref_id: source_ref_id.clone(),
                    session_key_hash: session_id.clone(),
                    principal_id: principal_id.clone(),
                    accounting_key_id: None,
                    upstream_id,
                    cycle_id: Some(cycle_id),
                    model: MODEL.to_owned(),
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cache_creation_input_tokens_5m: 0,
                    cache_creation_input_tokens_1h: 0,
                    cache_read_input_tokens: 20_000,
                    cost_micros: Some(1_000),
                    hit_miss: "hit".to_owned(),
                    ts,
                }),
                decision: CacheKeepaliveDecisionRow {
                    source_ref_id,
                    principal_id: principal_id.clone(),
                    session_key_hash: Some(session_id),
                    upstream_id,
                    decision: "tracked".to_owned(),
                    reason: "renewal".to_owned(),
                    error: None,
                    generation: 1,
                    ttl: CacheTtl::Ttl5m,
                    config_snapshot: Some(config),
                    last_message_at_ms: ts * 1_000,
                    ts,
                },
                evidence: vec![renewal, follow_up],
            };
            storage
                .append_request_event_with_projections(&event, &projections)
                .await
                .expect("append haiku keepalive turn");
        }
    };

    for (session_id, cycle_num, gross_prompt_tokens, expected_avoided, expected_saved) in [
        (
            "haiku-above-100k",
            94_001_u128,
            Some(120_000_u64),
            Some(0.0115),
            Some(0.0105),
        ),
        (
            "haiku-below-100k",
            94_002_u128,
            Some(80_000_u64),
            Some(0.0023),
            Some(0.0013),
        ),
        ("haiku-missing-prompt", 94_003_u128, None, None, None),
    ] {
        let (_directory, storage, state, principal_id) = create_principal_id().await;
        seed_cycle(
            storage.clone(),
            &principal_id,
            session_id,
            Uuid::from_u128(cycle_num),
            gross_prompt_tokens,
        )
        .await;
        let (list_status, list) = get_json(
            &state,
            &format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=10"),
        )
        .await;
        assert_eq!(list_status, StatusCode::OK, "list {session_id}: {list:?}");
        assert_eq!(
            list["summary"]["total_avoided"],
            serde_json::json!(expected_avoided),
            "summary total_avoided for {session_id}"
        );
        assert_eq!(
            list["summary"]["cost_saved"],
            serde_json::json!(expected_saved),
            "summary cost_saved for {session_id}"
        );
        assert_eq!(
            list["rows"][0]["total_avoided"],
            serde_json::json!(expected_avoided),
            "row total_avoided for {session_id}"
        );
        assert_eq!(
            list["rows"][0]["net_pnl"],
            serde_json::json!(expected_saved),
            "row net_pnl for {session_id}"
        );
        let (detail_status, detail) =
            get_json(&state, &keepalive_detail_uri(&principal_id, session_id)).await;
        assert_eq!(
            detail_status,
            StatusCode::OK,
            "detail {session_id}: {detail:?}"
        );
        assert_eq!(
            detail["total_avoided"],
            serde_json::json!(expected_avoided),
            "detail total_avoided for {session_id}"
        );
        assert_eq!(
            detail["net_pnl"],
            serde_json::json!(expected_saved),
            "detail net_pnl for {session_id}"
        );
        assert_eq!(
            detail["turns"][0]["pnl"],
            serde_json::json!(expected_saved),
            "detail turn pnl for {session_id}"
        );
        assert_eq!(detail["gaps"][0]["ending_kind"], "followup_hit");
        assert_eq!(
            detail["gaps"][0]["avoided_usd"],
            serde_json::json!(expected_avoided),
            "detail gap avoided_usd for {session_id}"
        );
    }
}

#[test]
fn unknown_evidence_does_not_fabricate_pnl() {
    const TEST_NAME: &str =
        "cache_keepalive_contracts::read_equivalence::unknown_evidence_does_not_fabricate_pnl";

    if std::env::var("CC_LB_KEEPALIVE_OVERFLOW_CHILD").as_deref() == Ok("1") {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated P&L overflow runtime")
            .block_on(assert_unknown_evidence_does_not_fabricate_pnl());
        return;
    }

    let output =
        Command::new(std::env::current_exe().expect("current integration test executable"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env("CC_LB_KEEPALIVE_OVERFLOW_CHILD", "1")
            .output()
            .expect("run isolated P&L overflow route test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains(TEST_NAME) && stdout.contains("1 passed"),
        "isolated overflow test did not execute successfully\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

async fn assert_unknown_evidence_does_not_fabricate_pnl() {
    let mut models = HashMap::new();
    models.insert(
        "overflow-route-model".to_owned(),
        Pricing {
            native: None,
            model: "overflow-route-model".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(0),
            output_per_million_usd: UsdPerMillion::from_whole_usd(0),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: BTreeMap::new(),
        },
    );
    let mut cache_creation_per_million_usd = HashMap::new();
    cache_creation_per_million_usd.insert(
        "overflow-route-model".to_owned(),
        UsdPerMillion::from_micros_usd(u64::MAX),
    );
    let mut cache_read_per_million_usd = HashMap::new();
    cache_read_per_million_usd.insert(
        "overflow-route-model".to_owned(),
        UsdPerMillion::from_micros_usd(u64::MAX),
    );
    cc_lb_pricing::global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "isolated-overflow".to_owned(),
        fetched_at_ms: 1,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates: HashMap::new(),
        status: CatalogStatus::Ok,
    });

    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_session(
        &storage,
        &principal_id,
        "overflow-route-session",
        NOW_UNIX_SECS * 1_000,
        NOW_UNIX_SECS + 300,
    )
    .await;
    seed_cache_keepalive_turn(
        &storage,
        &principal_id,
        "overflow-route-session",
        "overflow-route-turn",
        NOW_UNIX_SECS,
        "overflow-route-model",
        i64::MAX as u64,
    )
    .await;
    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "overflow response: {body:?}");
    assert_eq!(body["rows"][0]["evidence_status"], "unknown");
    // Missing cycle evidence leaves avoided/net unknown even with zero spend;
    // the huge token count must not become an invented saving.
    assert!(body["rows"][0]["net_pnl"].is_null());
    assert!(body["rows"][0]["total_avoided"].is_null());
    assert_eq!(body["rows"][0]["total_spent"], 0.0);
    assert_eq!(body["summary"]["evidence_status"], "unknown");
    assert!(
        body["summary"]["unknown_cycles"]
            .as_u64()
            .is_some_and(|count| count > 0)
    );
    assert_eq!(body["summary"]["cost_saved"], 0.0);
    assert_eq!(body["summary"]["total_avoided"], 0.0);
}
