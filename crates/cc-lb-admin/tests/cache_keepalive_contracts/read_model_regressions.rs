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
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
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
        cost_micros,
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
    assert_eq!(body["summary"]["renewals_fired"], 1);
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
    assert_eq!(body["summary"]["renewals_fired"], 1);
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
    assert_eq!(body["summary"]["renewals_fired"], 1);
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
    assert_eq!(body["summary"]["renewals_fired"], 0);
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
