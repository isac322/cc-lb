use std::collections::{BTreeMap, HashMap};

use axum::http::StatusCode;
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion};
use cc_lb_storage_api::{CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason};
use serde_json::Value;
use uuid::Uuid;

use crate::config_admin_common::{app, authed_json};

use super::fixtures::{cache_keepalive_replace_request, seed_cache_keepalive_turn};
use super::{NOW_UNIX_SECS, create_principal_id, keepalive_detail_uri};

const SESSION_ID: &str = "evidence-scope-boundary";
const TURN_SOURCE_REF: &str = "evidence-scope-boundary-turn";
const RENEWAL_EVIDENCE_ID: &str = "evidence-scope-boundary-renewal";
const FOLLOW_UP_EVIDENCE_ID: &str = "evidence-scope-boundary-follow-up";
const CYCLE_ID: Uuid = Uuid::from_u128(0x1101);
const PREFIX_HASH: &str = "evidence-scope-boundary-prefix";

async fn get_json(state: &cc_lb_admin::AdminState, uri: &str) -> (StatusCode, Value) {
    let (status, _headers, body, _raw) = authed_json(app(state.clone()), "GET", uri, None).await;
    (status, body)
}

async fn seed_boundary_evidence(storage: &cc_lb_storage_sqlite::SqliteStorage, principal_id: &str) {
    let turn_start_ms = NOW_UNIX_SECS * 1_000;
    let session = cache_keepalive_replace_request(
        principal_id,
        SESSION_ID,
        NOW_UNIX_SECS,
        NOW_UNIX_SECS + 300,
    );
    let record = storage
        .replace_from_real_request(&session)
        .await
        .expect("seed evidence-scope session");
    sqlx::query("UPDATE cache_keepalive_sessions SET cycle_id = ? WHERE session_key_hash = ?")
        .bind(CYCLE_ID.to_string())
        .bind(SESSION_ID)
        .execute(storage.pool())
        .await
        .expect("link evidence-scope session cycle");
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                &record.session_key_hash,
                record.generation,
                CacheKeepaliveTerminalReason::MaxRefreshes,
                None,
                NOW_UNIX_SECS + 1,
            )
            .await
            .expect("terminalize evidence-scope session")
    );
    sqlx::query("UPDATE cache_keepalive_sessions SET cycle_id = ? WHERE session_key_hash = ?")
        .bind(CYCLE_ID.to_string())
        .bind(SESSION_ID)
        .execute(storage.pool())
        .await
        .expect("link evidence-scope session cycle");
    seed_cache_keepalive_turn(
        storage,
        principal_id,
        SESSION_ID,
        TURN_SOURCE_REF,
        NOW_UNIX_SECS,
        "claude-sonnet-4-5",
        8_000,
    )
    .await;
    sqlx::query("UPDATE cache_keepalive_turns SET cycle_id = ? WHERE source_ref_id = ?")
        .bind(CYCLE_ID.to_string())
        .bind(TURN_SOURCE_REF)
        .execute(storage.pool())
        .await
        .expect("link evidence-scope turn cycle");

    for (
        evidence_id,
        evidence_kind,
        request_start_ms,
        cycle_id,
        prefix_hash,
        event_ref,
        cache_read_input_tokens,
        cost_micros,
    ) in [
        (
            RENEWAL_EVIDENCE_ID,
            "renewal",
            turn_start_ms - 1_000,
            Some(CYCLE_ID.to_string()),
            Some(PREFIX_HASH),
            Some(TURN_SOURCE_REF),
            Some(8_000_i64),
            Some(14_400_i64),
        ),
        (
            FOLLOW_UP_EVIDENCE_ID,
            "follow_up",
            turn_start_ms,
            None,
            Some(PREFIX_HASH),
            Some("evidence-scope-follow-up"),
            Some(8_000_i64),
            Some(14_400_i64),
        ),
    ] {
        sqlx::query(
            "INSERT INTO cache_keepalive_evidence
             (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
              prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
              upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
              request_ref, event_ref, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(evidence_id)
        .bind(cycle_id)
        .bind(evidence_kind)
        .bind(i64::try_from(request_start_ms).expect("evidence timestamp fits i64"))
        .bind("boundary-fingerprint")
        .bind(prefix_hash)
        .bind("policy-v1")
        .bind("schema-v1")
        .bind(principal_id)
        .bind(Uuid::from_u128(0xa11ce).to_string())
        .bind("claude-sonnet-4-5")
        .bind("5m")
        .bind("hit")
        .bind(cache_read_input_tokens)
        .bind(cost_micros)
        .bind(format!("request-{evidence_id}"))
        .bind(event_ref)
        .bind(i64::try_from(request_start_ms).expect("evidence timestamp fits i64"))
        .execute(storage.pool())
        .await
        .expect("seed evidence-scope evidence");
    }
}

#[tokio::test]
async fn list_and_detail_include_follow_up_at_turn_boundary_like_summary() {
    cc_lb_pricing::global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "evidence-scope-boundary".to_owned(),
        fetched_at_ms: 1,
        models: HashMap::from([(
            "claude-sonnet-4-5".to_owned(),
            Pricing {
                model: "claude-sonnet-4-5".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: UsdPerMillion::from_whole_usd(15),
                by_tier: BTreeMap::new(),
            },
        )]),
        raw_json: Vec::new(),
        cache_creation_per_million_usd: HashMap::from([(
            "claude-sonnet-4-5".to_owned(),
            UsdPerMillion::from_micros_usd(3_750_000),
        )]),
        cache_read_per_million_usd: HashMap::from([(
            "claude-sonnet-4-5".to_owned(),
            UsdPerMillion::from_micros_usd(300_000),
        )]),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates: HashMap::new(),
        status: CatalogStatus::Ok,
    });

    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_boundary_evidence(&storage, &principal_id).await;

    let list_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=10");
    let (list_status, list) = get_json(&state, &list_uri).await;
    assert_eq!(list_status, StatusCode::OK, "list response: {list:?}");
    assert_eq!(list["summary"]["evidence_status"], "observed");
    assert_eq!(
        list["summary"]["retained_from_ms"],
        NOW_UNIX_SECS * 1_000 - 1_000
    );
    assert_eq!(list["rows"].as_array().expect("list rows").len(), 1);
    assert_eq!(list["rows"][0]["id"], SESSION_ID);
    assert_eq!(list["rows"][0]["evidence_status"], "observed");
    assert!(
        list["rows"][0]["total_avoided"]
            .as_f64()
            .is_some_and(|value| value > 0.0),
        "follow-up should contribute positive avoided value: {:?}",
        list["rows"][0]["total_avoided"]
    );

    let detail_uri = keepalive_detail_uri(&principal_id, SESSION_ID);
    let (detail_status, detail) = get_json(&state, &detail_uri).await;
    assert_eq!(detail_status, StatusCode::OK, "detail response: {detail:?}");
    assert_eq!(detail["evidence_status"], "observed");
    assert_eq!(detail["turns"][0]["evidence_status"], "observed");
    assert_eq!(detail["turns"][0]["followed_up"], true);

    let (summary_status, summary) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=0"),
    )
    .await;
    assert_eq!(
        summary_status,
        StatusCode::OK,
        "summary response: {summary:?}"
    );
    assert_eq!(summary["summary"]["evidence_status"], "observed");
    assert_eq!(
        summary["summary"]["retained_from_ms"],
        NOW_UNIX_SECS * 1_000 - 1_000
    );
    assert_eq!(summary["summary"]["renewals_fired"], 1);
    assert!(
        summary["summary"]["total_avoided"]
            .as_f64()
            .is_some_and(|value| value > 0.0),
        "follow-up should contribute positive avoided value: {:?}",
        summary["summary"]["total_avoided"]
    );
}
