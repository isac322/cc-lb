use std::collections::{BTreeMap, HashMap};
use std::process::Command;

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

#[test]
fn list_and_detail_include_follow_up_at_turn_boundary_like_summary() {
    const TEST_NAME: &str = "cache_keepalive_contracts::evidence_scope::list_and_detail_include_follow_up_at_turn_boundary_like_summary";

    if std::env::var("CC_LB_KEEPALIVE_EVIDENCE_SCOPE_CHILD").as_deref() == Ok("1") {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated evidence-scope runtime")
            .block_on(list_and_detail_include_follow_up_at_turn_boundary_like_summary_async());
        return;
    }

    let output =
        Command::new(std::env::current_exe().expect("current integration test executable"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env("CC_LB_KEEPALIVE_EVIDENCE_SCOPE_CHILD", "1")
            .output()
            .expect("run isolated evidence-scope route test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains(TEST_NAME) && stdout.contains("1 passed"),
        "isolated evidence-scope test did not execute successfully\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

async fn list_and_detail_include_follow_up_at_turn_boundary_like_summary_async() {
    cc_lb_pricing::global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "evidence-scope-boundary".to_owned(),
        fetched_at_ms: 1,
        models: HashMap::from([(
            "claude-sonnet-4-5".to_owned(),
            Pricing {
                native: None,
                model: "claude-sonnet-4-5".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: UsdPerMillion::from_whole_usd(15),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
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
    assert_eq!(summary["summary"]["retained_successful_hits"], 1);
    assert!(
        summary["summary"]["total_avoided"]
            .as_f64()
            .is_some_and(|value| value > 0.0),
        "follow-up should contribute positive avoided value: {:?}",
        summary["summary"]["total_avoided"]
    );
}

#[test]
fn list_and_detail_include_follow_up_at_subsecond_ttl_boundary() {
    const TEST_NAME: &str = "cache_keepalive_contracts::evidence_scope::list_and_detail_include_follow_up_at_subsecond_ttl_boundary";

    if std::env::var("CC_LB_KEEPALIVE_EVIDENCE_SCOPE_UPPER_CHILD").as_deref() == Ok("1") {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated evidence-scope upper runtime")
            .block_on(list_and_detail_include_follow_up_at_subsecond_ttl_boundary_async());
        return;
    }

    let output =
        Command::new(std::env::current_exe().expect("current integration test executable"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env("CC_LB_KEEPALIVE_EVIDENCE_SCOPE_UPPER_CHILD", "1")
            .output()
            .expect("run isolated evidence-scope upper-bound test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains(TEST_NAME) && stdout.contains("1 passed"),
        "isolated evidence-scope upper-bound test did not execute successfully\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

async fn list_and_detail_include_follow_up_at_subsecond_ttl_boundary_async() {
    cc_lb_pricing::global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "evidence-scope-upper-boundary".to_owned(),
        fetched_at_ms: 1,
        models: HashMap::from([(
            "claude-sonnet-4-5".to_owned(),
            Pricing {
                native: None,
                model: "claude-sonnet-4-5".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: UsdPerMillion::from_whole_usd(15),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
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
    seed_subsecond_upper_boundary_evidence(&storage, &principal_id).await;

    let list_uri =
        format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=10");
    let (list_status, list) = get_json(&state, &list_uri).await;
    assert_eq!(list_status, StatusCode::OK, "list response: {list:?}");
    assert_eq!(list["rows"].as_array().expect("list rows").len(), 1);
    assert!(
        list["rows"][0]["total_avoided"]
            .as_f64()
            .is_some_and(|value| value > 0.0),
        "list must retain the follow-up in the renewal TTL window: {:?}",
        list["rows"][0]["total_avoided"]
    );

    let detail_uri = keepalive_detail_uri(&principal_id, UPPER_SESSION_ID);
    let (detail_status, detail) = get_json(&state, &detail_uri).await;
    assert_eq!(detail_status, StatusCode::OK, "detail response: {detail:?}");
    assert_eq!(detail["evidence_status"], "observed");
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
    assert!(
        summary["summary"]["total_avoided"]
            .as_f64()
            .is_some_and(|value| value > 0.0),
        "summary must retain the follow-up in the renewal TTL window: {:?}",
        summary["summary"]["total_avoided"]
    );
}

const UPPER_SESSION_ID: &str = "evidence-scope-upper-boundary";
const UPPER_TURN_SOURCE_REF: &str = "evidence-scope-upper-boundary-turn";
const UPPER_RENEWAL_EVIDENCE_ID: &str = "evidence-scope-upper-boundary-renewal";
const UPPER_FOLLOW_UP_EVIDENCE_ID: &str = "evidence-scope-upper-boundary-follow-up";
const UPPER_CYCLE_ID: Uuid = Uuid::from_u128(0x1102);
const UPPER_PREFIX_HASH: &str = "evidence-scope-upper-boundary-prefix";

async fn seed_subsecond_upper_boundary_evidence(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
) {
    let session = cache_keepalive_replace_request(
        principal_id,
        UPPER_SESSION_ID,
        NOW_UNIX_SECS - 3_700,
        NOW_UNIX_SECS + 240,
    );
    let record = storage
        .replace_from_real_request(&session)
        .await
        .expect("seed upper-bound evidence session");
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                &record.session_key_hash,
                record.generation,
                CacheKeepaliveTerminalReason::MaxRefreshes,
                None,
                NOW_UNIX_SECS - 10,
            )
            .await
            .expect("terminalize upper-bound evidence session")
    );
    sqlx::query("UPDATE cache_keepalive_sessions SET cycle_id = ? WHERE session_key_hash = ?")
        .bind(UPPER_CYCLE_ID.to_string())
        .bind(UPPER_SESSION_ID)
        .execute(storage.pool())
        .await
        .expect("link upper-bound evidence session cycle");

    let turn_ts = NOW_UNIX_SECS - 3_605;
    let renewal_start_ms = turn_ts * 1_000 + 900;
    let follow_up_start_ms = (turn_ts + 3_600) * 1_000 + 500;
    seed_cache_keepalive_turn(
        storage,
        principal_id,
        UPPER_SESSION_ID,
        UPPER_TURN_SOURCE_REF,
        turn_ts,
        "claude-sonnet-4-5",
        20_000,
    )
    .await;
    sqlx::query("UPDATE cache_keepalive_turns SET cycle_id = ? WHERE source_ref_id = ?")
        .bind(UPPER_CYCLE_ID.to_string())
        .bind(UPPER_TURN_SOURCE_REF)
        .execute(storage.pool())
        .await
        .expect("link upper-bound evidence turn cycle");

    for (
        evidence_id,
        evidence_kind,
        request_start_ms,
        cycle_id,
        event_ref,
        cache_read_input_tokens,
        cost_micros,
    ) in [
        (
            UPPER_RENEWAL_EVIDENCE_ID,
            "renewal",
            renewal_start_ms,
            Some(UPPER_CYCLE_ID.to_string()),
            Some(UPPER_TURN_SOURCE_REF),
            Some(20_000_i64),
            Some(6_000_i64),
        ),
        (
            UPPER_FOLLOW_UP_EVIDENCE_ID,
            "follow_up",
            follow_up_start_ms,
            None,
            Some("evidence-scope-upper-boundary-follow-up"),
            Some(20_000_i64),
            None,
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
        .bind("upper-boundary-fingerprint")
        .bind(UPPER_PREFIX_HASH)
        .bind("policy-v1")
        .bind("schema-v1")
        .bind(principal_id)
        .bind(Uuid::from_u128(0xa11ce).to_string())
        .bind("claude-sonnet-4-5")
        .bind("1h")
        .bind("hit")
        .bind(cache_read_input_tokens)
        .bind(cost_micros)
        .bind(format!("request-{evidence_id}"))
        .bind(event_ref)
        .bind(i64::try_from(request_start_ms).expect("evidence timestamp fits i64"))
        .execute(storage.pool())
        .await
        .expect("seed upper-bound evidence");
    }
}
