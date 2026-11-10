use std::{
    collections::{BTreeMap, HashMap},
    process::Command,
};

use axum::http::StatusCode;
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion};
use cc_lb_storage_api::{
    CacheKeepaliveEvidenceRow, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    CacheKeepaliveTurnRow, CacheTtl, RequestEvent, RequestEventProjections, RequestEventStore,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::config_admin_common::{app, authed_json};

use super::fixtures::{cache_keepalive_decision, cache_keepalive_replace_request};
use super::{NOW_UNIX_SECS, create_principal_id, keepalive_detail_uri};

const UPSTREAM_ID: Uuid = Uuid::from_u128(0xa11ce);
const MODEL: &str = "claude-sonnet-4-5";
const CYCLE_ONE: Uuid = Uuid::from_u128(1);
const CYCLE_TWO: Uuid = Uuid::from_u128(2);
const CHILD_ENV: &str = "CC_LB_ADMIN_READ_MODEL_REGRESSION";

fn install_regression_catalog() {
    let mut models = HashMap::new();
    models.insert(
        MODEL.to_owned(),
        Pricing {
            native: None,
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: BTreeMap::new(),
        },
    );
    let mut cache_creation_per_million_usd = HashMap::new();
    cache_creation_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(3_750_000));
    let mut cache_read_per_million_usd = HashMap::new();
    cache_read_per_million_usd.insert(MODEL.to_owned(), UsdPerMillion::from_micros_usd(300_000));
    cc_lb_pricing::global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: "cache-keepalive-read-model-regressions".to_owned(),
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
}

async fn get_json(state: &cc_lb_admin::AdminState, uri: &str) -> (StatusCode, Value) {
    let (status, _headers, body, _raw) = authed_json(app(state.clone()), "GET", uri, None).await;
    (status, body)
}

/// Compare JSON dollar amounts at the persisted microdollar precision.
///
/// JSON numbers are parsed as binary floating point, so values such as
/// `-0.021` can differ by a few representational bits at the HTTP boundary.
fn assert_json_dollars(value: &Value, expected_micros: i64) {
    let actual_dollars = value.as_f64().expect("JSON dollar amount");
    let actual_micros = (actual_dollars * 1_000_000.0).round() as i64;
    assert_eq!(
        actual_micros, expected_micros,
        "expected {expected_micros} microdollars, got {actual_dollars} dollars ({actual_micros} microdollars)"
    );
}

async fn seed_session(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    session_key_hash: &str,
    cycle_id: Uuid,
    terminal: bool,
) {
    let mut request = cache_keepalive_replace_request(
        principal_id,
        session_key_hash,
        NOW_UNIX_SECS,
        NOW_UNIX_SECS + 300,
    );
    request.cycle_id = cycle_id;
    storage
        .replace_from_real_request(&request)
        .await
        .expect("seed cache keepalive regression session");
    if terminal {
        assert!(
            storage
                .mark_cache_keepalive_terminal(
                    session_key_hash,
                    1,
                    CacheKeepaliveTerminalReason::MaxRefreshes,
                    None,
                    NOW_UNIX_SECS + 1,
                )
                .await
                .expect("terminalize cache keepalive regression session")
        );
    }
}

struct AppendTurnParams<'a> {
    principal_id: &'a str,
    session_key_hash: &'a str,
    cycle_id: Uuid,
    source_ref_id: &'a str,
    ts: u64,
    cache_read_input_tokens: u64,
    cost_micros: i64,
    hit_miss: &'a str,
    with_follow_up: bool,
}

async fn append_turn(storage: &cc_lb_storage_sqlite::SqliteStorage, params: AppendTurnParams<'_>) {
    let AppendTurnParams {
        principal_id,
        session_key_hash,
        cycle_id,
        source_ref_id,
        ts,
        cache_read_input_tokens,
        cost_micros,
        hit_miss,
        with_follow_up,
    } = params;
    let event_id = format!("event-{source_ref_id}");
    let mut decision =
        cache_keepalive_decision(principal_id, source_ref_id, ts.saturating_mul(1_000));
    decision.session_key_hash = Some(session_key_hash.to_owned());
    decision.upstream_id = UPSTREAM_ID;
    decision.decision = "reschedule".to_owned();
    decision.reason = "cache_keepalive_turn".to_owned();
    decision.generation = 1;
    decision.ttl = CacheTtl::Ttl5m;

    let turn = CacheKeepaliveTurnRow {
        source_ref_id: source_ref_id.to_owned(),
        session_key_hash: session_key_hash.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: None,
        upstream_id: UPSTREAM_ID,
        cycle_id: Some(cycle_id),
        model: MODEL.to_owned(),
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_creation_input_tokens_5m: 0,
        cache_creation_input_tokens_1h: 0,
        cache_read_input_tokens,
        cost_micros: Some(cost_micros),
        hit_miss: hit_miss.to_owned(),
        ts,
    };
    let mut evidence = vec![CacheKeepaliveEvidenceRow {
        evidence_id: format!("renewal-{source_ref_id}"),
        cycle_id: Some(cycle_id),
        evidence_kind: "renewal".to_owned(),
        request_start_ms: ts.saturating_mul(1_000),
        prefix_fingerprint: Some("fingerprint".to_owned()),
        prefix_hash: Some("prefix".to_owned()),
        prefix_policy_version: Some("policy".to_owned()),
        prefix_schema_version: Some("schema".to_owned()),
        principal_id: principal_id.to_owned(),
        upstream_id: UPSTREAM_ID,
        model: MODEL.to_owned(),
        ttl: CacheTtl::Ttl5m,
        hit_miss: Some(hit_miss.to_owned()),
        cache_read_input_tokens: Some(cache_read_input_tokens),
        cost_micros: Some(cost_micros),
        request_ref: Some(format!("request-{source_ref_id}")),
        event_ref: Some(source_ref_id.to_owned()),
    }];
    if with_follow_up {
        evidence.push(CacheKeepaliveEvidenceRow {
            evidence_id: format!("follow-up-{source_ref_id}"),
            cycle_id: Some(cycle_id),
            evidence_kind: "follow_up".to_owned(),
            request_start_ms: ts.saturating_mul(1_000).saturating_add(1_000),
            prefix_fingerprint: Some("fingerprint".to_owned()),
            prefix_hash: Some("prefix".to_owned()),
            prefix_policy_version: Some("policy".to_owned()),
            prefix_schema_version: Some("schema".to_owned()),
            principal_id: principal_id.to_owned(),
            upstream_id: UPSTREAM_ID,
            model: MODEL.to_owned(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some("hit".to_owned()),
            cache_read_input_tokens: Some(cache_read_input_tokens),
            cost_micros: None,
            request_ref: Some(format!("follow-up-request-{source_ref_id}")),
            event_ref: None,
        });
    }
    storage
        .append_request_event_with_projections(
            &RequestEvent {
                ts,
                ts_ms: Some(ts.saturating_mul(1_000)),
                request_id: format!("request-{source_ref_id}"),
                source_kind: Some("renewal".to_owned()),
                source_ref_id: Some(source_ref_id.to_owned()),
                event_id: Some(event_id),
                principal_id: Some(principal_id.to_owned()),
                upstream_id: Some(UPSTREAM_ID),
                model: Some(MODEL.to_owned()),
                status: 200,
                duration_ms: 1,
                ..RequestEvent::default()
            },
            &RequestEventProjections {
                turn: Some(turn),
                decision,
                evidence,
            },
        )
        .await
        .expect("append cache keepalive regression turn");
}

fn run_isolated(test_name: &str) {
    let output =
        Command::new(std::env::current_exe().expect("current integration test executable"))
            .args(["--exact", test_name, "--nocapture"])
            .env(CHILD_ENV, test_name)
            .output()
            .expect("run isolated cache keepalive regression");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains(test_name) && stdout.contains("1 passed"),
        "isolated regression did not execute successfully\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn terminal_unfollowed_hit_and_miss_preserve_observed_loss() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::terminal_unfollowed_hit_and_miss_preserve_observed_loss";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(terminal_unfollowed_hit_and_miss_preserve_observed_loss_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn terminal_unfollowed_hit_and_miss_preserve_observed_loss_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_session(
        &storage,
        &principal_id,
        "terminal-hit-miss",
        CYCLE_ONE,
        true,
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "terminal-hit-miss",
            cycle_id: CYCLE_ONE,
            source_ref_id: "terminal-hit",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "terminal-hit-miss",
            cycle_id: CYCLE_ONE,
            source_ref_id: "terminal-miss",
            ts: NOW_UNIX_SECS + 1,
            cache_read_input_tokens: 20_000,
            cost_micros: 15_000,
            hit_miss: "miss",
            with_follow_up: false,
        },
    )
    .await;

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "terminal list: {body:?}");
    assert_eq!(body["summary"]["total_spent"], json!(0.021));
    assert_eq!(body["summary"]["total_avoided"], json!(0.0));
    assert_json_dollars(&body["summary"]["cost_saved"], -21_000);
    assert_eq!(body["summary"]["evidence_status"], "observed");
    assert_eq!(body["summary"]["retained_successful_hits"], 1);

    let (detail_status, detail) = get_json(
        &state,
        &keepalive_detail_uri(&principal_id, "terminal-hit-miss"),
    )
    .await;
    assert_eq!(detail_status, StatusCode::OK, "terminal detail: {detail:?}");
    assert_eq!(detail["total_spent"], json!(0.021));
    assert_eq!(detail["total_avoided"], json!(0.0));
    assert_json_dollars(&detail["net_pnl"], -21_000);
    assert_eq!(detail["evidence_status"], "observed");
    let turns = detail["turns"].as_array().expect("terminal detail turns");
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["followed_up"], false);
    assert_eq!(turns[1]["followed_up"], false);
}

#[test]
fn terminal_unfollowed_hit_only_preserves_observed_loss() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::terminal_unfollowed_hit_only_preserves_observed_loss";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(terminal_unfollowed_hit_only_preserves_observed_loss_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn terminal_unfollowed_hit_only_preserves_observed_loss_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_session(
        &storage,
        &principal_id,
        "terminal-hit-only",
        CYCLE_ONE,
        true,
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "terminal-hit-only",
            cycle_id: CYCLE_ONE,
            source_ref_id: "terminal-hit-only",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "terminal hit-only list: {body:?}");
    assert_eq!(body["summary"]["total_spent"], json!(0.006));
    assert_eq!(body["summary"]["total_avoided"], json!(0.0));
    assert_json_dollars(&body["summary"]["cost_saved"], -6_000);
    assert_eq!(body["summary"]["evidence_status"], "observed");
    assert_eq!(body["summary"]["retained_successful_hits"], 1);

    let (detail_status, detail) = get_json(
        &state,
        &keepalive_detail_uri(&principal_id, "terminal-hit-only"),
    )
    .await;
    assert_eq!(
        detail_status,
        StatusCode::OK,
        "terminal hit-only detail: {detail:?}"
    );
    assert_eq!(detail["total_spent"], json!(0.006));
    assert_eq!(detail["total_avoided"], json!(0.0));
    assert_json_dollars(&detail["net_pnl"], -6_000);
    assert_eq!(detail["evidence_status"], "observed");
    let turns = detail["turns"].as_array().expect("terminal hit-only turns");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0]["followed_up"], false);
    assert_eq!(turns[0]["pending"], false);
    assert_json_dollars(&turns[0]["pnl"], -6_000);
}

#[test]
fn replacement_retains_c1_economics_without_inheriting_c2_timeline() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::replacement_retains_c1_economics_without_inheriting_c2_timeline";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(replacement_retains_c1_economics_without_inheriting_c2_timeline_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn replacement_retains_c1_economics_without_inheriting_c2_timeline_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_session(&storage, &principal_id, "replaced-c1-c2", CYCLE_ONE, false).await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "replaced-c1-c2",
            cycle_id: CYCLE_ONE,
            source_ref_id: "c1-turn",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 21_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;
    let mut replacement = cache_keepalive_replace_request(
        &principal_id,
        "replaced-c1-c2",
        NOW_UNIX_SECS + 2,
        NOW_UNIX_SECS + 302,
    );
    replacement.cycle_id = CYCLE_TWO;
    storage
        .replace_from_real_request(&replacement)
        .await
        .expect("replace c1 with c2");

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "replacement list: {body:?}");
    assert_eq!(body["summary"]["total_spent"], json!(0.021));
    assert_eq!(body["summary"]["total_avoided"], json!(0.075));
    assert_eq!(body["summary"]["cost_saved"], json!(0.054));
    assert_eq!(body["summary"]["retained_successful_hits"], 1);
    assert_eq!(body["summary"]["evidence_status"], "observed");

    let (detail_status, detail) = get_json(
        &state,
        &keepalive_detail_uri(&principal_id, "replaced-c1-c2"),
    )
    .await;
    assert_eq!(
        detail_status,
        StatusCode::OK,
        "replacement detail: {detail:?}"
    );
    assert!(detail["is_active"].as_bool().expect("replacement active"));
    assert_eq!(detail["total_spent"], json!(0.021));
    assert_eq!(detail["total_avoided"], json!(0.075));
    assert_eq!(detail["net_pnl"], json!(0.054));
    assert_eq!(detail["total_renewals_current_cycle"], 0);
    assert_eq!(detail["is_last_pending"], false);
    assert_eq!(detail["turns"], json!([]));
}

#[test]
fn persisted_zero_cost_is_real_zero_spend_in_http_pnl() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::persisted_zero_cost_is_real_zero_spend_in_http_pnl";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(persisted_zero_cost_is_real_zero_spend_in_http_pnl_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn persisted_zero_cost_is_real_zero_spend_in_http_pnl_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_session(
        &storage,
        &principal_id,
        "persisted-zero-cost",
        CYCLE_ONE,
        true,
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "persisted-zero-cost",
            cycle_id: CYCLE_ONE,
            source_ref_id: "zero-cost-turn",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 0,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "zero-cost list: {body:?}");
    assert_eq!(body["summary"]["total_spent"], json!(0.0));
    assert_eq!(body["summary"]["total_avoided"], json!(0.075));
    assert_eq!(body["summary"]["cost_saved"], json!(0.075));

    let (detail_status, detail) = get_json(
        &state,
        &keepalive_detail_uri(&principal_id, "persisted-zero-cost"),
    )
    .await;
    assert_eq!(
        detail_status,
        StatusCode::OK,
        "zero-cost detail: {detail:?}"
    );
    assert_eq!(detail["total_spent"], json!(0.0));
    assert_eq!(detail["net_pnl"], json!(0.075));
    assert_eq!(detail["turns"][0]["pnl"], json!(0.075));
}

#[test]
fn miss_attempt_is_not_counted_as_successful_renewal() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::miss_attempt_is_not_counted_as_successful_renewal";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(miss_attempt_is_not_counted_as_successful_renewal_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn miss_attempt_is_not_counted_as_successful_renewal_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_session(
        &storage,
        &principal_id,
        "miss-only-session",
        CYCLE_ONE,
        true,
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "miss-only-session",
            cycle_id: CYCLE_ONE,
            source_ref_id: "miss-only-turn",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 15_000,
            hit_miss: "miss",
            with_follow_up: false,
        },
    )
    .await;

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "miss list: {body:?}");
    assert_eq!(body["summary"]["retained_successful_hits"], 0);
    assert_eq!(body["summary"]["total_spent"], json!(0.015));
    assert_eq!(body["summary"]["total_avoided"], json!(0.0));

    let (detail_status, detail) = get_json(
        &state,
        &keepalive_detail_uri(&principal_id, "miss-only-session"),
    )
    .await;
    assert_eq!(detail_status, StatusCode::OK, "miss detail: {detail:?}");
    assert_eq!(detail["total_renewals_current_cycle"], 0);
    assert_eq!(detail["turns"][0]["renewals"], 0);
    assert_eq!(detail["turns"][0]["followed_up"], false);
    assert_json_dollars(&detail["turns"][0]["pnl"], -15_000);
}

#[test]
fn active_unfollowed_turns_are_pending_but_active_followup_is_observed() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::active_unfollowed_turns_are_pending_but_active_followup_is_observed";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(active_unfollowed_turns_are_pending_but_active_followup_is_observed_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn active_unfollowed_turns_are_pending_but_active_followup_is_observed_case() {
    install_regression_catalog();

    let (_pending_directory, pending_storage, pending_state, pending_principal_id) =
        create_principal_id().await;
    seed_session(
        &pending_storage,
        &pending_principal_id,
        "active-unfollowed-hit",
        CYCLE_ONE,
        false,
    )
    .await;
    append_turn(
        &pending_storage,
        AppendTurnParams {
            principal_id: &pending_principal_id,
            session_key_hash: "active-unfollowed-hit",
            cycle_id: CYCLE_ONE,
            source_ref_id: "active-unfollowed-hit",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    seed_session(
        &pending_storage,
        &pending_principal_id,
        "active-unfollowed-miss",
        CYCLE_TWO,
        false,
    )
    .await;
    append_turn(
        &pending_storage,
        AppendTurnParams {
            principal_id: &pending_principal_id,
            session_key_hash: "active-unfollowed-miss",
            cycle_id: CYCLE_TWO,
            source_ref_id: "active-unfollowed-miss",
            ts: NOW_UNIX_SECS + 1,
            cache_read_input_tokens: 20_000,
            cost_micros: 15_000,
            hit_miss: "miss",
            with_follow_up: false,
        },
    )
    .await;

    let (pending_status, pending_body) = get_json(
        &pending_state,
        &format!("/admin/v1/principals/{pending_principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(
        pending_status,
        StatusCode::OK,
        "active unfollowed list: {pending_body:?}"
    );
    assert_eq!(pending_body["summary"]["evidence_status"], "pending");
    assert_json_dollars(&pending_body["summary"]["cost_saved"], -21_000);
    assert_json_dollars(&pending_body["summary"]["total_avoided"], 0);

    let (_observed_directory, observed_storage, observed_state, observed_principal_id) =
        create_principal_id().await;
    seed_session(
        &observed_storage,
        &observed_principal_id,
        "active-followup",
        CYCLE_ONE,
        false,
    )
    .await;
    append_turn(
        &observed_storage,
        AppendTurnParams {
            principal_id: &observed_principal_id,
            session_key_hash: "active-followup",
            cycle_id: CYCLE_ONE,
            source_ref_id: "active-followup",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;
    seed_session(
        &observed_storage,
        &observed_principal_id,
        "active-followup-miss",
        CYCLE_TWO,
        false,
    )
    .await;
    append_turn(
        &observed_storage,
        AppendTurnParams {
            principal_id: &observed_principal_id,
            session_key_hash: "active-followup-miss",
            cycle_id: CYCLE_TWO,
            source_ref_id: "active-followup-miss",
            ts: NOW_UNIX_SECS + 1,
            cache_read_input_tokens: 20_000,
            cost_micros: 15_000,
            hit_miss: "miss",
            with_follow_up: true,
        },
    )
    .await;

    let (observed_status, observed_body) = get_json(
        &observed_state,
        &format!("/admin/v1/principals/{observed_principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(
        observed_status,
        StatusCode::OK,
        "active follow-up list: {observed_body:?}"
    );
    assert_eq!(observed_body["summary"]["evidence_status"], "observed");
    assert_json_dollars(&observed_body["summary"]["total_spent"], 21_000);
    assert_json_dollars(&observed_body["summary"]["total_avoided"], 75_000);
    assert_json_dollars(&observed_body["summary"]["cost_saved"], 54_000);
    assert_eq!(observed_body["summary"]["retained_successful_hits"], 1);
    let (miss_detail_status, miss_detail) = get_json(
        &observed_state,
        &keepalive_detail_uri(&observed_principal_id, "active-followup-miss"),
    )
    .await;
    assert_eq!(
        miss_detail_status,
        StatusCode::OK,
        "active miss follow-up detail: {miss_detail:?}"
    );
    assert_json_dollars(&miss_detail["total_spent"], 15_000);
    assert_json_dollars(&miss_detail["total_avoided"], 0);
    assert_json_dollars(&miss_detail["net_pnl"], -15_000);
    assert_eq!(miss_detail["turns"][0]["followed_up"], false);
}

#[test]
fn active_same_cycle_hit_then_miss_shared_follow_up_has_no_credit() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::active_same_cycle_hit_then_miss_shared_follow_up_has_no_credit";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(active_same_cycle_hit_then_miss_shared_follow_up_has_no_credit_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn active_same_cycle_hit_then_miss_shared_follow_up_has_no_credit_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let session_key_hash = "active-same-cycle-hit-miss";
    seed_session(&storage, &principal_id, session_key_hash, CYCLE_ONE, false).await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash,
            cycle_id: CYCLE_ONE,
            source_ref_id: "active-same-cycle-hit",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash,
            cycle_id: CYCLE_ONE,
            source_ref_id: "active-same-cycle-miss",
            ts: NOW_UNIX_SECS + 1,
            cache_read_input_tokens: 20_000,
            cost_micros: 15_000,
            hit_miss: "miss",
            with_follow_up: true,
        },
    )
    .await;

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "same-cycle hit then miss list: {body:?}"
    );
    assert_eq!(body["summary"]["evidence_status"], "observed");
    assert_json_dollars(&body["summary"]["total_spent"], 21_000);
    assert_json_dollars(&body["summary"]["total_avoided"], 0);
    assert_json_dollars(&body["summary"]["cost_saved"], -21_000);
    assert_eq!(body["summary"]["retained_successful_hits"], 1);

    let row = body["rows"]
        .as_array()
        .expect("same-cycle rows")
        .iter()
        .find(|row| row["id"] == session_key_hash)
        .expect("same-cycle session row");
    assert_eq!(row["evidence_status"], "observed");
    assert_json_dollars(&row["total_spent"], 21_000);
    assert_json_dollars(&row["total_avoided"], 0);
    assert_json_dollars(&row["net_pnl"], -21_000);

    let (detail_status, detail) = get_json(
        &state,
        &keepalive_detail_uri(&principal_id, session_key_hash),
    )
    .await;
    assert_eq!(
        detail_status,
        StatusCode::OK,
        "same-cycle hit then miss detail: {detail:?}"
    );
    assert_eq!(detail["evidence_status"], "observed");
    assert_json_dollars(&detail["total_spent"], 21_000);
    assert_json_dollars(&detail["total_avoided"], 0);
    assert_json_dollars(&detail["net_pnl"], -21_000);
    assert_eq!(detail["is_last_pending"], false);
    let turns = detail["turns"].as_array().expect("same-cycle detail turns");
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["turn_number"], 2);
    assert_eq!(turns[0]["label"], "no follow-up (loss)");
    assert_eq!(turns[0]["renewals"], 0);
    assert_eq!(turns[0]["pending"], false);
    assert_eq!(turns[0]["followed_up"], false);
    assert_json_dollars(&turns[0]["pnl"], -15_000);
    assert_eq!(turns[1]["turn_number"], 1);
    assert_eq!(turns[1]["label"], "no follow-up (loss)");
    assert_eq!(turns[1]["renewals"], 1);
    assert_eq!(turns[1]["pending"], false);
    assert_eq!(turns[1]["followed_up"], false);
    assert_json_dollars(&turns[1]["pnl"], -6_000);
}

#[test]
fn active_two_follow_ups_select_earliest_follow_up_renewal() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::active_two_follow_ups_select_earliest_follow_up_renewal";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(active_two_follow_ups_select_earliest_follow_up_renewal_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn active_two_follow_ups_select_earliest_follow_up_renewal_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    let session_key_hash = "active-two-follow-ups";
    seed_session(&storage, &principal_id, session_key_hash, CYCLE_ONE, false).await;

    // F1 matches only the hit R1; F2 matches only the miss R2. The HTTP
    // read model must credit the earliest qualifying follow-up, not the miss.
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash,
            cycle_id: CYCLE_ONE,
            source_ref_id: "active-two-follow-ups-r1",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash,
            cycle_id: CYCLE_ONE,
            source_ref_id: "active-two-follow-ups-r2",
            ts: NOW_UNIX_SECS + 1,
            cache_read_input_tokens: 20_000,
            cost_micros: 15_000,
            hit_miss: "miss",
            with_follow_up: true,
        },
    )
    .await;

    let (status, body) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "two-follow-up list: {body:?}");
    assert_eq!(body["summary"]["evidence_status"], "pending");
    assert_json_dollars(&body["summary"]["total_spent"], 21_000);
    assert_json_dollars(&body["summary"]["total_avoided"], 75_000);
    assert_json_dollars(&body["summary"]["cost_saved"], 54_000);
    assert_eq!(body["summary"]["retained_successful_hits"], 1);
    let row = body["rows"]
        .as_array()
        .expect("two-follow-up rows")
        .iter()
        .find(|row| row["id"] == session_key_hash)
        .expect("two-follow-up row");
    assert_eq!(row["evidence_status"], "pending");
    assert_json_dollars(&row["total_spent"], 21_000);
    assert_json_dollars(&row["total_avoided"], 75_000);
    assert_json_dollars(&row["net_pnl"], 54_000);

    let (detail_status, detail) = get_json(
        &state,
        &keepalive_detail_uri(&principal_id, session_key_hash),
    )
    .await;
    assert_eq!(
        detail_status,
        StatusCode::OK,
        "two-follow-up detail: {detail:?}"
    );
    assert_eq!(detail["evidence_status"], "pending");
    assert_json_dollars(&detail["total_spent"], 21_000);
    assert_json_dollars(&detail["total_avoided"], 75_000);
    assert_json_dollars(&detail["net_pnl"], 54_000);
    assert_eq!(detail["is_last_pending"], true);
    let turns = detail["turns"]
        .as_array()
        .expect("two-follow-up detail turns");
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["time_ms"], json!((NOW_UNIX_SECS + 1) * 1_000));
    assert_eq!(turns[0]["label"], "waiting for follow-up");
    assert_eq!(turns[0]["evidence_status"], "pending");
    assert_eq!(turns[0]["pending"], true);
    assert_eq!(turns[0]["followed_up"], false);
    assert_json_dollars(&turns[0]["pnl"], -15_000);
    assert_eq!(turns[1]["time_ms"], json!(NOW_UNIX_SECS * 1_000));
    assert_eq!(turns[1]["label"], "cache used by follow-up");
    assert_eq!(turns[1]["followed_up"], true);
    assert_eq!(turns[1]["evidence_status"], "observed");
    assert_eq!(turns[1]["pending"], false);
    assert_json_dollars(&turns[1]["pnl"], 69_000);
}

#[test]
fn active_multi_turn_and_cycle_history_keep_pending_parity() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::active_multi_turn_and_cycle_history_keep_pending_parity";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(active_multi_turn_and_cycle_history_keep_pending_parity_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn active_multi_turn_and_cycle_history_keep_pending_parity_case() {
    install_regression_catalog();

    let (_multi_turn_directory, multi_turn_storage, multi_turn_state, multi_turn_principal_id) =
        create_principal_id().await;
    seed_session(
        &multi_turn_storage,
        &multi_turn_principal_id,
        "active-multi-turn",
        CYCLE_ONE,
        false,
    )
    .await;
    for (source_ref_id, ts, hit_miss, cost_micros) in [
        ("active-multi-turn-old", NOW_UNIX_SECS, "hit", 6_000),
        (
            "active-multi-turn-latest",
            NOW_UNIX_SECS + 1,
            "miss",
            15_000,
        ),
    ] {
        append_turn(
            &multi_turn_storage,
            AppendTurnParams {
                principal_id: &multi_turn_principal_id,
                session_key_hash: "active-multi-turn",
                cycle_id: CYCLE_ONE,
                source_ref_id,
                ts,
                cache_read_input_tokens: 20_000,
                cost_micros,
                hit_miss,
                with_follow_up: false,
            },
        )
        .await;
    }
    let (multi_turn_status, multi_turn_body) = get_json(
        &multi_turn_state,
        &format!("/admin/v1/principals/{multi_turn_principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(
        multi_turn_status,
        StatusCode::OK,
        "active multi-turn list: {multi_turn_body:?}"
    );
    assert_eq!(multi_turn_body["summary"]["evidence_status"], "pending");
    assert_json_dollars(&multi_turn_body["summary"]["total_spent"], 21_000);
    assert_eq!(multi_turn_body["summary"]["retained_successful_hits"], 1);
    assert_json_dollars(&multi_turn_body["summary"]["cost_saved"], -21_000);
    assert_json_dollars(&multi_turn_body["summary"]["total_avoided"], 0);
    let multi_turn_row = multi_turn_body["rows"]
        .as_array()
        .expect("active multi-turn rows")
        .iter()
        .find(|row| row["id"] == "active-multi-turn")
        .expect("active multi-turn row");
    assert_eq!(multi_turn_row["evidence_status"], "pending");
    assert_json_dollars(&multi_turn_row["total_spent"], 21_000);
    assert_json_dollars(&multi_turn_row["net_pnl"], -21_000);
    assert_json_dollars(&multi_turn_row["total_avoided"], 0);

    let (multi_turn_detail_status, multi_turn_detail) = get_json(
        &multi_turn_state,
        &keepalive_detail_uri(&multi_turn_principal_id, "active-multi-turn"),
    )
    .await;
    assert_eq!(
        multi_turn_detail_status,
        StatusCode::OK,
        "active multi-turn detail: {multi_turn_detail:?}"
    );
    assert_eq!(multi_turn_detail["evidence_status"], "pending");
    assert_json_dollars(&multi_turn_detail["total_spent"], 21_000);
    assert_json_dollars(&multi_turn_detail["net_pnl"], -21_000);
    assert_json_dollars(&multi_turn_detail["total_avoided"], 0);
    assert_eq!(multi_turn_detail["is_last_pending"], true);
    let multi_turns = multi_turn_detail["turns"]
        .as_array()
        .expect("active multi-turn detail turns");
    assert_eq!(multi_turns.len(), 2);
    assert_eq!(multi_turns[0]["pending"], true);
    assert_eq!(multi_turns[1]["pending"], false);

    let (_historical_directory, historical_storage, historical_state, historical_principal_id) =
        create_principal_id().await;
    seed_session(
        &historical_storage,
        &historical_principal_id,
        "historical-active-pending",
        CYCLE_ONE,
        false,
    )
    .await;
    append_turn(
        &historical_storage,
        AppendTurnParams {
            principal_id: &historical_principal_id,
            session_key_hash: "historical-active-pending",
            cycle_id: CYCLE_ONE,
            source_ref_id: "historical-active-pending-old",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    let mut replacement = cache_keepalive_replace_request(
        &historical_principal_id,
        "historical-active-pending",
        NOW_UNIX_SECS + 2,
        NOW_UNIX_SECS + 302,
    );
    replacement.cycle_id = CYCLE_TWO;
    historical_storage
        .replace_from_real_request(&replacement)
        .await
        .expect("replace with current cycle");
    append_turn(
        &historical_storage,
        AppendTurnParams {
            principal_id: &historical_principal_id,
            session_key_hash: "historical-active-pending",
            cycle_id: CYCLE_TWO,
            source_ref_id: "historical-active-pending-current",
            ts: NOW_UNIX_SECS + 3,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    let (historical_status, historical_body) = get_json(
        &historical_state,
        &format!("/admin/v1/principals/{historical_principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(
        historical_status,
        StatusCode::OK,
        "historical active list: {historical_body:?}"
    );
    assert_eq!(historical_body["summary"]["evidence_status"], "pending");
    assert_json_dollars(&historical_body["summary"]["total_spent"], 12_000);
    assert_json_dollars(&historical_body["summary"]["cost_saved"], -12_000);
    assert_json_dollars(&historical_body["summary"]["total_avoided"], 0);
    let (historical_detail_status, historical_detail) = get_json(
        &historical_state,
        &keepalive_detail_uri(&historical_principal_id, "historical-active-pending"),
    )
    .await;
    assert_eq!(
        historical_detail_status,
        StatusCode::OK,
        "historical active detail: {historical_detail:?}"
    );
    assert_eq!(historical_detail["evidence_status"], "pending");
    assert_json_dollars(&historical_detail["total_spent"], 12_000);
    assert_json_dollars(&historical_detail["net_pnl"], -12_000);
    assert_json_dollars(&historical_detail["total_avoided"], 0);
    assert_eq!(historical_detail["turns"].as_array().unwrap().len(), 1);
    assert_eq!(historical_detail["turns"][0]["pending"], true);

    let (_follow_ups_directory, follow_ups_storage, follow_ups_state, follow_ups_principal_id) =
        create_principal_id().await;
    seed_session(
        &follow_ups_storage,
        &follow_ups_principal_id,
        "active-multiple-follow-ups",
        CYCLE_ONE,
        false,
    )
    .await;
    for (source_ref_id, ts) in [
        ("active-multiple-follow-ups-first", NOW_UNIX_SECS),
        ("active-multiple-follow-ups-latest", NOW_UNIX_SECS + 1),
    ] {
        append_turn(
            &follow_ups_storage,
            AppendTurnParams {
                principal_id: &follow_ups_principal_id,
                session_key_hash: "active-multiple-follow-ups",
                cycle_id: CYCLE_ONE,
                source_ref_id,
                ts,
                cache_read_input_tokens: 20_000,
                cost_micros: 6_000,
                hit_miss: "hit",
                with_follow_up: true,
            },
        )
        .await;
    }
    let (follow_ups_status, follow_ups_body) = get_json(
        &follow_ups_state,
        &format!("/admin/v1/principals/{follow_ups_principal_id}/cache-keepalive?limit=10"),
    )
    .await;
    assert_eq!(
        follow_ups_status,
        StatusCode::OK,
        "multiple follow-up list: {follow_ups_body:?}"
    );
    assert_eq!(follow_ups_body["summary"]["evidence_status"], "pending");
    assert_json_dollars(&follow_ups_body["summary"]["total_spent"], 12_000);
    assert_json_dollars(&follow_ups_body["summary"]["cost_saved"], 63_000);
    assert_json_dollars(&follow_ups_body["summary"]["total_avoided"], 75_000);
    let (follow_ups_detail_status, follow_ups_detail) = get_json(
        &follow_ups_state,
        &keepalive_detail_uri(&follow_ups_principal_id, "active-multiple-follow-ups"),
    )
    .await;
    assert_eq!(
        follow_ups_detail_status,
        StatusCode::OK,
        "multiple follow-up detail: {follow_ups_detail:?}"
    );
    let follow_up_turns = follow_ups_detail["turns"]
        .as_array()
        .expect("multiple follow-up detail turns");
    assert_eq!(follow_up_turns.len(), 2);
    assert_eq!(follow_up_turns[0]["pending"], true);
    assert_eq!(follow_up_turns[0]["followed_up"], false);
    assert_eq!(follow_up_turns[1]["pending"], false);
    assert_eq!(follow_up_turns[1]["followed_up"], true);
}

#[test]
fn summary_respects_horizon_and_ignores_follow_up_only_evidence() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::summary_respects_horizon_and_ignores_follow_up_only_evidence";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated regression runtime")
            .block_on(summary_respects_horizon_and_ignores_follow_up_only_evidence_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn summary_respects_horizon_and_ignores_follow_up_only_evidence_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;

    sqlx::query(
        "INSERT INTO cache_keepalive_evidence
         (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint,
          prefix_hash, prefix_policy_version, prefix_schema_version, principal_id,
          upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros,
          request_ref, event_ref, created_at_ms)
         VALUES ('follow-up-only', NULL, 'follow_up', ?, 'fp', 'prefix', 'policy', 'schema',
                 ?, ?, ?, '5m', 'hit', 20000, 6000, 'req-1', 'ev-1', ?)",
    )
    .bind(i64::try_from(NOW_UNIX_SECS * 1_000).unwrap())
    .bind(&principal_id)
    .bind(UPSTREAM_ID.to_string())
    .bind(MODEL)
    .bind(i64::try_from(NOW_UNIX_SECS * 1_000).unwrap())
    .execute(storage.pool())
    .await
    .expect("insert follow-up-only evidence");

    let (status, empty_summary) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        empty_summary["summary"]["retained_from_ms"].is_null(),
        "follow_up-only evidence must not populate retained_from_ms: {empty_summary:?}"
    );

    let old_ts = NOW_UNIX_SECS - 3 * 24 * 60 * 60;
    let mut old_decision =
        cache_keepalive_decision(&principal_id, "legacy-old-turn", old_ts * 1_000);
    old_decision.session_key_hash = Some("legacy-session".to_owned());
    old_decision.upstream_id = UPSTREAM_ID;
    storage
        .append_request_event_with_projections(
            &RequestEvent {
                ts: old_ts,
                ts_ms: Some(old_ts * 1_000),
                request_id: "req-legacy-old".to_owned(),
                source_kind: Some("renewal".to_owned()),
                source_ref_id: Some("legacy-old-turn".to_owned()),
                event_id: Some("ev-legacy-old".to_owned()),
                principal_id: Some(principal_id.clone()),
                upstream_id: Some(UPSTREAM_ID),
                model: Some(MODEL.to_owned()),
                status: 200,
                duration_ms: 1,
                ..RequestEvent::default()
            },
            &RequestEventProjections {
                turn: Some(CacheKeepaliveTurnRow {
                    source_ref_id: "legacy-old-turn".to_owned(),
                    session_key_hash: "legacy-session".to_owned(),
                    principal_id: principal_id.clone(),
                    accounting_key_id: None,
                    upstream_id: UPSTREAM_ID,
                    cycle_id: None,
                    model: MODEL.to_owned(),
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cache_creation_input_tokens_5m: 0,
                    cache_creation_input_tokens_1h: 0,
                    cache_read_input_tokens: 20_000,
                    cost_micros: Some(15_000),
                    hit_miss: "hit".to_owned(),
                    ts: old_ts,
                }),
                decision: old_decision,
                evidence: Vec::new(),
            },
        )
        .await
        .expect("seed legacy old turn");

    seed_session(&storage, &principal_id, "recent-observed", CYCLE_ONE, true).await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "recent-observed",
            cycle_id: CYCLE_ONE,
            source_ref_id: "recent-observed-turn",
            ts: NOW_UNIX_SECS,
            cache_read_input_tokens: 20_000,
            cost_micros: 6_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;

    let (status_24h, body_24h) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=24h&limit=10"),
    )
    .await;
    assert_eq!(status_24h, StatusCode::OK);
    assert_eq!(body_24h["summary"]["retained_successful_hits"], 1);
    assert_eq!(body_24h["summary"]["evidence_status"], "observed");
    assert_json_dollars(&body_24h["summary"]["total_spent"], 6_000);
    assert_json_dollars(&body_24h["summary"]["total_avoided"], 75_000);
    assert_json_dollars(&body_24h["summary"]["cost_saved"], 69_000);

    let (status_7d, body_7d) = get_json(
        &state,
        &format!("/admin/v1/principals/{principal_id}/cache-keepalive?horizon=7d&limit=10"),
    )
    .await;
    assert_eq!(status_7d, StatusCode::OK);
    assert_eq!(body_7d["summary"]["retained_successful_hits"], 2);
    assert_eq!(body_7d["summary"]["evidence_status"], "unknown");
    assert_json_dollars(&body_7d["summary"]["total_spent"], 21_000);
    // The legacy turn has no renewal evidence: it avoided $0 so far, so the
    // real-time net still counts its spend.
    assert_json_dollars(&body_7d["summary"]["cost_saved"], 54_000);
}

async fn delete_turn_evidence(storage: &cc_lb_storage_sqlite::SqliteStorage, source_ref_id: &str) {
    sqlx::query("DELETE FROM cache_keepalive_evidence WHERE event_ref = ?")
        .bind(source_ref_id)
        .execute(storage.pool())
        .await
        .expect("delete cache keepalive evidence");
}

async fn fetch_all_keepalive_pages(
    state: &cc_lb_admin::AdminState,
    principal_id: &str,
    horizon: &str,
) -> (Value, Vec<Value>) {
    let mut cursor = None;
    let mut summary = Value::Null;
    let mut rows = Vec::new();
    loop {
        let mut uri = format!(
            "/admin/v1/principals/{principal_id}/cache-keepalive?horizon={horizon}&limit=2"
        );
        if let Some(cursor_value) = cursor.as_deref() {
            uri.push_str("&cursor=");
            uri.push_str(cursor_value);
        }
        let (status, body) = get_json(state, &uri).await;
        assert_eq!(status, StatusCode::OK, "paged list: {body:?}");
        if summary.is_null() {
            summary = body["summary"].clone();
        }
        rows.extend(body["rows"].as_array().expect("paged rows").iter().cloned());
        cursor = body["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            return (summary, rows);
        }
    }
}

fn sum_json_micros(rows: &[Value], field: &str) -> i64 {
    rows.iter()
        .filter_map(|row| row[field].as_f64())
        .map(|value| (value * 1_000_000.0).round() as i64)
        .sum()
}

#[test]
fn header_avoided_rounds_per_renewal_like_rows() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::header_avoided_rounds_per_renewal_like_rows";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build rounding runtime")
            .block_on(header_avoided_rounds_per_renewal_like_rows_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn header_avoided_rounds_per_renewal_like_rows_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    // Two followed-up 1-token renewals: each prices to a fraction of a
    // microdollar, so summing tokens before rounding would disagree with rows.
    for (index, session) in ["tiny-one", "tiny-two"].into_iter().enumerate() {
        let cycle_id = Uuid::from_u128(0x0201 + index as u128);
        seed_session(&storage, &principal_id, session, cycle_id, true).await;
        append_turn(
            &storage,
            AppendTurnParams {
                principal_id: &principal_id,
                session_key_hash: session,
                cycle_id,
                source_ref_id: &format!("{session}-turn"),
                ts: NOW_UNIX_SECS.saturating_sub(60),
                cache_read_input_tokens: 1,
                cost_micros: 0,
                hit_miss: "hit",
                with_follow_up: true,
            },
        )
        .await;
    }
    let (summary, rows) = fetch_all_keepalive_pages(&state, &principal_id, "24h").await;
    let row_avoided = sum_json_micros(&rows, "total_avoided");
    assert!(row_avoided > 0, "rows must credit the follow-ups: {rows:?}");
    assert_json_dollars(&summary["total_avoided"], row_avoided);
    assert_json_dollars(&summary["cost_saved"], row_avoided);
}

#[test]
fn q1_q7_keepalive_contract_invariants_and_realtime_wire() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::q1_q7_keepalive_contract_invariants_and_realtime_wire";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build q1-q7 runtime")
            .block_on(q1_q7_keepalive_contract_invariants_and_realtime_wire_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn q1_q7_keepalive_contract_invariants_and_realtime_wire_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;

    seed_session(&storage, &principal_id, "old-and-new", CYCLE_ONE, false).await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "old-and-new",
            // Each real request opens a new cycle, so the old follow-up
            // belongs to an earlier cycle than the current one.
            cycle_id: Uuid::from_u128(0x0101),
            source_ref_id: "old-and-new-old",
            ts: NOW_UNIX_SECS.saturating_sub(3 * 86_400),
            cache_read_input_tokens: 20_000,
            cost_micros: 7_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "old-and-new",
            cycle_id: CYCLE_ONE,
            source_ref_id: "old-and-new-new",
            ts: NOW_UNIX_SECS.saturating_sub(1_800),
            cache_read_input_tokens: 20_000,
            cost_micros: 11_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;

    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "purged-with-evidence",
            cycle_id: Uuid::from_u128(0x0103),
            source_ref_id: "purged-with-evidence-turn",
            ts: NOW_UNIX_SECS.saturating_sub(1_200),
            cache_read_input_tokens: 20_000,
            cost_micros: 13_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "purged-without-evidence",
            cycle_id: Uuid::from_u128(0x0104),
            source_ref_id: "purged-without-evidence-turn",
            ts: NOW_UNIX_SECS.saturating_sub(900),
            cache_read_input_tokens: 20_000,
            cost_micros: 17_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    delete_turn_evidence(&storage, "purged-without-evidence-turn").await;

    seed_session(
        &storage,
        &principal_id,
        "pending-session",
        Uuid::from_u128(0x0105),
        false,
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "pending-session",
            cycle_id: Uuid::from_u128(0x0105),
            source_ref_id: "pending-session-turn",
            ts: NOW_UNIX_SECS.saturating_sub(600),
            cache_read_input_tokens: 20_000,
            cost_micros: 19_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;

    seed_session(
        &storage,
        &principal_id,
        "unknown-session",
        Uuid::from_u128(0x0106),
        false,
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "unknown-session",
            cycle_id: Uuid::from_u128(0x0106),
            source_ref_id: "unknown-session-turn",
            ts: NOW_UNIX_SECS.saturating_sub(300),
            cache_read_input_tokens: 20_000,
            cost_micros: 23_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    delete_turn_evidence(&storage, "unknown-session-turn").await;

    for horizon in ["1h", "24h", "all"] {
        let (summary, rows) = fetch_all_keepalive_pages(&state, &principal_id, horizon).await;
        let spent = sum_json_micros(&rows, "total_spent");
        let avoided = sum_json_micros(&rows, "total_avoided");
        let net = sum_json_micros(&rows, "net_pnl");
        let unlisted_spent =
            (summary["unlisted_spent"].as_f64().unwrap() * 1_000_000.0).round() as i64;
        assert_json_dollars(&summary["total_spent"], spent + unlisted_spent);
        assert_json_dollars(&summary["cost_saved"], net - unlisted_spent);
        assert_json_dollars(&summary["total_avoided"], avoided);
        assert!(summary.get("renewals_fired").is_none());
        assert!(summary.get("resolved_cost_saved").is_none());
        assert!(summary.get("resolved_total_avoided").is_none());
        assert!(summary["unlisted_sessions"].is_number());
        assert!(summary["unlisted_renewals"].is_number());
        assert!(summary["unlisted_spent"].is_number());
    }
}

#[test]
fn q2_q3_horizon_money_and_membership_boundary() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::q2_q3_horizon_money_and_membership_boundary";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build q2-q3 runtime")
            .block_on(q2_q3_horizon_money_and_membership_boundary_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn q2_q3_horizon_money_and_membership_boundary_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    seed_session(&storage, &principal_id, "horizon-scoped", CYCLE_ONE, false).await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "horizon-scoped",
            cycle_id: CYCLE_ONE,
            source_ref_id: "horizon-old",
            ts: NOW_UNIX_SECS.saturating_sub(2 * 86_400),
            cache_read_input_tokens: 20_000,
            cost_micros: 31_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "horizon-scoped",
            cycle_id: CYCLE_ONE,
            source_ref_id: "horizon-new",
            ts: NOW_UNIX_SECS.saturating_sub(1_800),
            cache_read_input_tokens: 20_000,
            cost_micros: 37_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;
    let (summary_24h, rows_24h) = fetch_all_keepalive_pages(&state, &principal_id, "24h").await;
    assert_eq!(rows_24h.len(), 1);
    assert_json_dollars(&rows_24h[0]["total_spent"], 37_000);
    assert_json_dollars(&summary_24h["total_spent"], 37_000);
    let (_summary_all, rows_all) = fetch_all_keepalive_pages(&state, &principal_id, "all").await;
    assert_eq!(rows_all.len(), 1);
    assert_json_dollars(&rows_all[0]["total_spent"], 68_000);

    sqlx::query(
        "UPDATE cache_keepalive_sessions SET last_message_at_ms = ? WHERE session_key_hash = ?",
    )
    .bind((NOW_UNIX_SECS.saturating_sub(2 * 86_400) * 1_000) as i64)
    .bind("horizon-scoped")
    .execute(storage.pool())
    .await
    .expect("freeze old last message timestamp");
    let (_summary_membership, rows_membership) =
        fetch_all_keepalive_pages(&state, &principal_id, "24h").await;
    assert_eq!(
        rows_membership
            .iter()
            .filter(|row| row["id"] == "horizon-scoped")
            .count(),
        1
    );
    seed_session(&storage, &principal_id, "boundary-scoped", CYCLE_TWO, false).await;
    for (source_ref_id, ts, cost_micros) in [
        (
            "boundary-included",
            NOW_UNIX_SECS.saturating_sub(3_600),
            5_000,
        ),
        (
            "boundary-excluded",
            NOW_UNIX_SECS.saturating_sub(3_601),
            7_000,
        ),
    ] {
        append_turn(
            &storage,
            AppendTurnParams {
                principal_id: &principal_id,
                session_key_hash: "boundary-scoped",
                cycle_id: CYCLE_TWO,
                source_ref_id,
                ts,
                cache_read_input_tokens: 20_000,
                cost_micros,
                hit_miss: "hit",
                with_follow_up: true,
            },
        )
        .await;
    }
    let (boundary_status, boundary_detail) = get_json(
        &state,
        &format!(
            "{}&horizon=1h",
            keepalive_detail_uri(&principal_id, "boundary-scoped")
        ),
    )
    .await;
    assert_eq!(boundary_status, StatusCode::OK);
    assert_json_dollars(&boundary_detail["total_spent"], 5_000);
}

#[test]
fn q4_q5_unknown_and_pending_realtime_money_and_gap_labels() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::q4_q5_unknown_and_pending_realtime_money_and_gap_labels";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build q4-q5 runtime")
            .block_on(q4_q5_unknown_and_pending_realtime_money_and_gap_labels_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn q4_q5_unknown_and_pending_realtime_money_and_gap_labels_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;
    // Terminal, so the gap is closed and its ending depends only on evidence.
    seed_session(&storage, &principal_id, "unknown-label", CYCLE_ONE, true).await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "unknown-label",
            cycle_id: CYCLE_ONE,
            source_ref_id: "unknown-label-turn",
            ts: NOW_UNIX_SECS.saturating_sub(300),
            cache_read_input_tokens: 20_000,
            cost_micros: 41_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    delete_turn_evidence(&storage, "unknown-label-turn").await;
    let (status, detail) = get_json(
        &state,
        &format!(
            "{}&horizon=24h",
            keepalive_detail_uri(&principal_id, "unknown-label")
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unknown detail: {detail:?}");
    assert_json_dollars(&detail["net_pnl"], -41_000);
    assert_json_dollars(&detail["total_avoided"], 0);
    assert_ne!(detail["turns"][0]["label"], "no follow-up");
    assert_eq!(detail["gaps"][0]["ending_kind"], "followup_unknown");

    seed_session(&storage, &principal_id, "pending-label", CYCLE_ONE, false).await;
    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "pending-label",
            cycle_id: CYCLE_ONE,
            source_ref_id: "pending-label-turn",
            ts: NOW_UNIX_SECS.saturating_sub(200),
            cache_read_input_tokens: 20_000,
            cost_micros: 43_000,
            hit_miss: "hit",
            with_follow_up: false,
        },
    )
    .await;
    let (summary, _) = fetch_all_keepalive_pages(&state, &principal_id, "24h").await;
    assert!(!summary["cost_saved"].is_null());
    assert!(!summary["total_avoided"].is_null());
    let (status, _body) = get_json(
        &state,
        &keepalive_detail_uri(&principal_id, "missing-keepalive-id"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[test]
fn q6_terminal_states_and_status_filters_match_rows() {
    const TEST_NAME: &str = "cache_keepalive_contracts::read_model_regressions::q6_terminal_states_and_status_filters_match_rows";
    if std::env::var(CHILD_ENV).as_deref() == Ok(TEST_NAME) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build q6 runtime")
            .block_on(q6_terminal_states_and_status_filters_match_rows_case());
        return;
    }
    run_isolated(TEST_NAME);
}

async fn q6_terminal_states_and_status_filters_match_rows_case() {
    install_regression_catalog();
    let (_directory, storage, state, principal_id) = create_principal_id().await;

    for (id, reason, with_turn) in [
        (
            "cancelled-row",
            CacheKeepaliveTerminalReason::Cancelled,
            true,
        ),
        (
            "failed-row",
            CacheKeepaliveTerminalReason::DispatchError,
            true,
        ),
        (
            "capped-row",
            CacheKeepaliveTerminalReason::MaxRefreshes,
            true,
        ),
        (
            "not-tracked-row",
            CacheKeepaliveTerminalReason::Cancelled,
            false,
        ),
    ] {
        seed_session(&storage, &principal_id, id, CYCLE_ONE, false).await;
        if with_turn {
            append_turn(
                &storage,
                AppendTurnParams {
                    principal_id: &principal_id,
                    session_key_hash: id,
                    cycle_id: CYCLE_ONE,
                    source_ref_id: id,
                    ts: NOW_UNIX_SECS.saturating_sub(100),
                    cache_read_input_tokens: 20_000,
                    cost_micros: 1_000,
                    hit_miss: "hit",
                    with_follow_up: false,
                },
            )
            .await;
        }
        storage
            .mark_cache_keepalive_terminal(id, 1, reason, None, NOW_UNIX_SECS + 1)
            .await
            .expect("terminalize state fixture");
    }
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET refresh_count = 1 WHERE session_key_hash IN (?, ?, ?)",
    )
    .bind("cancelled-row")
    .bind("failed-row")
    .bind("capped-row")
    .execute(storage.pool())
    .await
    .expect("set terminal filter refresh counts");

    append_turn(
        &storage,
        AppendTurnParams {
            principal_id: &principal_id,
            session_key_hash: "purged-followed",
            cycle_id: CYCLE_ONE,
            source_ref_id: "purged-followed-turn",
            ts: NOW_UNIX_SECS.saturating_sub(80),
            cache_read_input_tokens: 20_000,
            cost_micros: 1_000,
            hit_miss: "hit",
            with_follow_up: true,
        },
    )
    .await;

    let (_summary, rows) = fetch_all_keepalive_pages(&state, &principal_id, "all").await;
    let state_for = |id: &str| {
        rows.iter()
            .find(|row| row["id"] == id)
            .unwrap_or_else(|| panic!("missing state row {id}"))["state"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert_eq!(state_for("cancelled-row"), "cancelled");
    assert_eq!(state_for("failed-row"), "failed");
    assert_eq!(state_for("capped-row"), "capped");
    assert_eq!(state_for("not-tracked-row"), "not_tracked");
    assert_eq!(state_for("purged-followed"), "cancelled");

    for (filter, expected) in [
        ("cancelled", "cancelled"),
        ("failed", "failed"),
        ("capped", "capped"),
    ] {
        let (status, body) = get_json(
            &state,
            &format!(
                "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=2&status={filter}"
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "filtered list: {body:?}");
        let mut filtered_rows = body["rows"].as_array().cloned().unwrap_or_default();
        let mut cursor = body["next_cursor"].as_str().map(str::to_owned);
        while let Some(cursor_value) = cursor {
            let (status, page) = get_json(
                &state,
                &format!(
                    "/admin/v1/principals/{principal_id}/cache-keepalive?horizon=all&limit=2&status={filter}&cursor={cursor_value}"
                ),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            filtered_rows.extend(page["rows"].as_array().cloned().unwrap_or_default());
            cursor = page["next_cursor"].as_str().map(str::to_owned);
        }
        assert!(!filtered_rows.is_empty());
        assert!(filtered_rows.iter().all(|row| row["state"] == expected));
    }
}
