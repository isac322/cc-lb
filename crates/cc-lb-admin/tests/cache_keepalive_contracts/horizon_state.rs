use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use axum::http::StatusCode;
use cc_lb_clock::TestClock;
use cc_lb_config::Config;
use cc_lb_storage_api::{
    CacheKeepaliveEvidenceRow, CacheKeepaliveHitRefreshRequest, CacheKeepaliveSessionFilter,
    CacheKeepaliveSessionListQuery, CacheKeepaliveTerminalReason, CacheKeepaliveTurnRow, CacheTtl,
    MetaStore, RequestEvent, RequestEventProjections, Storage,
};
use serde_json::Value;

use super::{
    NOW_UNIX_SECS,
    fixtures::{cache_keepalive_decision, cache_keepalive_replace_request, principal_create_body},
    keepalive_detail_uri,
};
use crate::config_admin_common::{app, authed_json, test_state_with_clock};

#[tokio::test]
async fn selected_horizon_state_matrix_sqlite() {
    let (_directory, storage, state, principal_id) = super::create_principal_id().await;
    selected_horizon_state_matrix(storage.as_ref(), &state, &principal_id).await;
}

#[tokio::test]
async fn selected_horizon_state_matrix_postgres() {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skip: CI_POSTGRES_URL is required for PostgreSQL horizon parity");
        return;
    };
    let schema = format!("ka_horizon_{}", uuid::Uuid::new_v4().simple());
    let options = sqlx::postgres::PgConnectOptions::from_str(&url).expect("PostgreSQL URL");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .expect("PostgreSQL admin pool");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create isolated schema");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .expect("isolated pool");
    let clock = Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS));
    let storage = Arc::new(cc_lb_storage_postgres::PostgresStorage::new(
        pool,
        clock.clone(),
    ));
    let scenario_storage = storage.clone();
    let outcome = tokio::spawn(async move {
        let storage = scenario_storage;
        storage
            .initialize()
            .await
            .expect("initialize PostgreSQL storage");
        let mut state = test_state_with_clock(Config::default(), None, clock);
        state.storage = Some(storage.clone());
        let (status, _, principal, _) = authed_json(
            app(state.clone()),
            "POST",
            "/admin/v1/principals",
            Some(principal_create_body()),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "principal: {principal}");
        selected_horizon_state_matrix(storage.as_ref(), &state, principal["id"].as_str().unwrap())
            .await;
    })
    .await;
    storage.pool().close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .expect("drop isolated schema");
    admin.close().await;
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("horizon scenario task was cancelled: {error}");
    }
}

async fn get(state: &cc_lb_admin::AdminState, uri: &str) -> Value {
    let (status, _, body, _) = authed_json(app(state.clone()), "GET", uri, None).await;
    assert_eq!(status, StatusCode::OK, "{uri}: {body}");
    body
}

fn ids(body: &Value) -> BTreeSet<String> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap().to_owned())
        .collect()
}

async fn seed_turn(storage: &dyn Storage, principal: &str, id: &str, ts: u64) {
    seed_cycle_turn(
        storage,
        principal,
        id,
        format!("turn-{id}"),
        ts,
        Some(uuid::Uuid::nil()),
        Vec::new(),
    )
    .await;
}

async fn seed_cycle_turn(
    storage: &dyn Storage,
    principal: &str,
    id: &str,
    source: String,
    ts: u64,
    cycle_id: Option<uuid::Uuid>,
    evidence: Vec<CacheKeepaliveEvidenceRow>,
) {
    storage
        .append_request_event_with_projections(
            &RequestEvent {
                request_id: source.clone(),
                event_id: Some(source.clone()),
                source_kind: Some("renewal".to_owned()),
                source_ref_id: Some(source.clone()),
                ts,
                ts_ms: Some(ts * 1_000),
                principal_id: Some(principal.to_owned()),
                upstream_id: Some(uuid::Uuid::from_u128(0xa11ce)),
                model: Some("unknown-horizon-model".to_owned()),
                status: 200,
                duration_ms: 12,
                ..RequestEvent::default()
            },
            &RequestEventProjections {
                turn: Some(CacheKeepaliveTurnRow {
                    source_ref_id: source.clone(),
                    session_key_hash: id.to_owned(),
                    principal_id: principal.to_owned(),
                    accounting_key_id: None,
                    upstream_id: uuid::Uuid::from_u128(0xa11ce),
                    cycle_id,
                    model: "unknown-horizon-model".to_owned(),
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cache_creation_input_tokens_5m: 0,
                    cache_creation_input_tokens_1h: 0,
                    cache_read_input_tokens: 1,
                    cost_micros: Some(1_000),
                    hit_miss: "hit".to_owned(),
                    ts,
                }),
                decision: cache_keepalive_decision(principal, &source, ts * 1_000),
                evidence,
            },
        )
        .await
        .expect("seed horizon turn");
}

async fn selected_horizon_state_matrix(
    storage: &dyn Storage,
    state: &cc_lb_admin::AdminState,
    principal: &str,
) {
    // QA: all eight base states, all filters, five horizons, old and recent turns,
    // fresh/expired provisional markers, detail parity, groups and header conservation.
    for (id, reason, turn_age) in [
        ("renewed", None, Some(100)),
        ("scheduled", None, None),
        ("in-request", None, None),
        ("incomplete", None, None),
        (
            "capped",
            Some(CacheKeepaliveTerminalReason::MaxRefreshes),
            Some(100),
        ),
        (
            "cancelled",
            Some(CacheKeepaliveTerminalReason::Cancelled),
            Some(100),
        ),
        (
            "cancelled-no-counter",
            Some(CacheKeepaliveTerminalReason::Cancelled),
            Some(100),
        ),
        (
            "failed",
            Some(CacheKeepaliveTerminalReason::DispatchError),
            Some(100),
        ),
        (
            "expired",
            Some(CacheKeepaliveTerminalReason::Expired),
            Some(100),
        ),
        (
            "not-tracked",
            Some(CacheKeepaliveTerminalReason::Cancelled),
            None,
        ),
        (
            "cancelled-old-2h",
            Some(CacheKeepaliveTerminalReason::Cancelled),
            Some(2 * 3_600),
        ),
        (
            "cancelled-old-2d",
            Some(CacheKeepaliveTerminalReason::Cancelled),
            Some(2 * 86_400),
        ),
        (
            "cancelled-old-8d",
            Some(CacheKeepaliveTerminalReason::Cancelled),
            Some(8 * 86_400),
        ),
    ] {
        let mut request =
            cache_keepalive_replace_request(principal, id, NOW_UNIX_SECS, NOW_UNIX_SECS + 300);
        request.display_reason = "agent-in-turn".to_owned();
        let mut record = storage
            .replace_from_real_request(&request)
            .await
            .expect("seed state");
        if let Some(age) = turn_age {
            seed_turn(storage, principal, id, NOW_UNIX_SECS - age).await;
            if id != "cancelled-no-counter" {
                record = storage
                    .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
                        session_key_hash: id.to_owned(),
                        generation: record.generation,
                        cache_anchor_at_unix_secs: NOW_UNIX_SECS,
                        run_at_unix_secs: NOW_UNIX_SECS + 270,
                        expires_at_unix_secs: NOW_UNIX_SECS + 300,
                        encrypted_payload: None,
                        now_unix_secs: NOW_UNIX_SECS,
                    })
                    .await
                    .expect("set nonzero all-time refresh count")
                    .unwrap();
            }
        }
        if id == "in-request" || id == "incomplete" {
            let wall_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let start = if id == "in-request" {
                wall_now
            } else {
                wall_now - 120
            };
            assert!(
                storage
                    .begin_cache_keepalive_request_fenced(id, None, start, start + 60)
                    .await
                    .expect("begin request")
            );
        } else if let Some(reason) = reason {
            assert!(
                storage
                    .mark_cache_keepalive_terminal(
                        id,
                        record.generation,
                        reason,
                        None,
                        NOW_UNIX_SECS + 1
                    )
                    .await
                    .expect("terminal state")
            );
        }
    }
    // Decisions preserve the existing Visible exception: error or 12h only.
    for (id, ttl, error) in [
        ("decision-hidden", CacheTtl::Ttl5m, None),
        ("decision-12h", CacheTtl::Ttl12h, None),
        ("decision-error", CacheTtl::Ttl5m, Some("decision failed")),
    ] {
        let mut decision = cache_keepalive_decision(principal, id, NOW_UNIX_SECS * 1_000);
        decision.ttl = ttl;
        decision.error = error.map(str::to_owned);
        storage
            .append_cache_keepalive_decision(&decision)
            .await
            .expect("seed decision");
    }
    // A deleted session still contributes retained spend to the header, not a row.
    seed_turn(storage, principal, "deleted-session", NOW_UNIX_SECS - 100).await;

    let base = format!("/admin/v1/principals/{principal}/cache-keepalive");
    let mut cancellation_mismatches = Vec::new();
    for (horizon, expected) in [
        ("1h", "not_tracked"),
        ("24h", "not_tracked"),
        ("all", "cancelled"),
    ] {
        let all = get(
            state,
            &format!("{base}?horizon={horizon}&status=all&limit=100"),
        )
        .await;
        let row = all["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == "cancelled-old-2d")
            .unwrap();
        let detail = get(
            state,
            &format!(
                "{}&horizon={horizon}",
                keepalive_detail_uri(principal, "cancelled-old-2d")
            ),
        )
        .await;
        if row["state"] != expected || detail["state"] != expected {
            cancellation_mismatches.push(format!(
                "{horizon}: list={} detail={} expected={expected}",
                row["state"], detail["state"]
            ));
        }
    }
    assert!(
        cancellation_mismatches.is_empty(),
        "old renewal plus recent cancellation: {cancellation_mismatches:?}"
    );
    let states = [
        "renewed",
        "scheduled",
        "in_request",
        "capped",
        "cancelled",
        "failed",
        "expired",
        "not_tracked",
    ];
    for (horizon, duration) in [
        ("1h", Some(3_600)),
        ("6h", Some(21_600)),
        ("24h", Some(86_400)),
        ("7d", Some(604_800)),
        ("all", None),
    ] {
        let all = get(
            state,
            &format!("{base}?horizon={horizon}&status=all&limit=100"),
        )
        .await;
        let storage_rows = storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                principal_id: principal.to_owned(),
                horizon_start_ms: duration.map(|secs| (NOW_UNIX_SECS - secs) * 1_000),
                filter: CacheKeepaliveSessionFilter::All,
                cursor: None,
                limit: 100,
            })
            .await
            .expect("read horizon-local renewal existence in the list query");
        for row in &storage_rows.rows {
            let expected = match row.id.as_str() {
                "renewed"
                | "capped"
                | "cancelled"
                | "cancelled-no-counter"
                | "failed"
                | "expired" => true,
                "cancelled-old-2h" => duration.is_none_or(|secs| secs >= 7_200),
                "cancelled-old-2d" => duration.is_none_or(|secs| secs >= 172_800),
                "cancelled-old-8d" => duration.is_none_or(|secs| secs >= 691_200),
                _ => false,
            };
            assert_eq!(
                row.has_renewal_in_horizon, expected,
                "list existence projection {horizon}: {}",
                row.id
            );
        }
        for (id, age) in [
            ("cancelled-old-2h", 7_200),
            ("cancelled-old-2d", 172_800),
            ("cancelled-old-8d", 691_200),
        ] {
            let expected = if duration.is_none_or(|duration| age <= duration) {
                "cancelled"
            } else {
                "not_tracked"
            };
            let row = all["rows"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["id"] == id)
                .unwrap();
            assert_eq!(
                row["state"], expected,
                "horizon={horizon} id={id}: all-time count cannot project cancellation"
            );
            let detail = get(
                state,
                &format!("{}&horizon={horizon}", keepalive_detail_uri(principal, id)),
            )
            .await;
            assert_eq!(
                detail["state"], row["state"],
                "detail horizon={horizon} id={id}"
            );
        }
        let tracked = get(
            state,
            &format!("{base}?horizon={horizon}&status=tracked&limit=100"),
        )
        .await;
        let visible = get(
            state,
            &format!("{base}?horizon={horizon}&status=visible&limit=100"),
        )
        .await;
        let mut tracked_union = BTreeSet::new();
        let mut by_state = BTreeMap::new();
        for filter in states {
            let filtered = get(
                state,
                &format!("{base}?horizon={horizon}&status={filter}&limit=100"),
            )
            .await;
            assert!(
                !filtered["rows"].as_array().unwrap().is_empty(),
                "fixture covers {filter} in {horizon}"
            );
            for row in filtered["rows"].as_array().unwrap() {
                assert_eq!(
                    row["state"], filter,
                    "filter={filter} horizon={horizon} row={row}"
                );
            }
            let filtered_ids = ids(&filtered);
            // Explicit Expired exposes the no-turn incomplete marker, while
            // Visible/Tracked retain their existing cancellation-marker hiding.
            if filter != "not_tracked" {
                tracked_union.extend(
                    filtered_ids
                        .iter()
                        .filter(|id| id.as_str() != "incomplete")
                        .cloned(),
                );
            }
            by_state.insert(filter, filtered_ids);
        }
        assert_eq!(
            tracked_union,
            ids(&tracked),
            "base-state union after the same tracked visibility eligibility {horizon}"
        );
        for surface in [&all, &tracked, &visible] {
            for row in surface["rows"].as_array().unwrap() {
                assert!(
                    by_state[row["state"].as_str().unwrap()].contains(row["id"].as_str().unwrap()),
                    "every all/visible/tracked row belongs to its projected state filter: {horizon} {row}"
                );
            }
        }
        let mut expected_visible = ids(&tracked);
        expected_visible.extend(["decision-12h".to_owned(), "decision-error".to_owned()]);
        assert_eq!(
            ids(&visible),
            expected_visible,
            "Visible = Tracked + existing decision subset {horizon}"
        );
        let error = get(
            state,
            &format!("{base}?horizon={horizon}&error=true&limit=100"),
        )
        .await;
        assert!(
            error["rows"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| !row["error"].is_null())
        );
        let groups = visible["groups"].as_array().unwrap();
        assert_eq!(
            groups
                .iter()
                .map(|group| group["sessions"].as_u64().unwrap())
                .sum::<u64>(),
            visible["rows"].as_array().unwrap().len() as u64
        );
        let group_spend: f64 = groups
            .iter()
            .map(|group| group["total_spent"].as_f64().unwrap())
            .sum();
        let all_spend: f64 = all["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["total_spent"].as_f64().unwrap())
            .sum();
        let hidden_spend = all_spend - group_spend;
        let header = &visible["summary"];
        assert_eq!(header["unlisted_sessions"], 1);
        assert_eq!(header["unlisted_renewals"], 1);
        assert!(
            hidden_spend.abs() < 0.0000001,
            "hidden rows without in-horizon turns contribute no spend {horizon}"
        );
        assert!(
            (header["total_spent"].as_f64().unwrap()
                - group_spend
                - header["unlisted_spent"].as_f64().unwrap())
            .abs()
                < 0.0000001,
            "header = groups + deleted {horizon}"
        );
        let listed_renewals: u64 = visible["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["retained_renewals"].as_u64().unwrap())
            .sum();
        assert_eq!(
            header["retained_successful_hits"].as_u64().unwrap(),
            listed_renewals + header["unlisted_renewals"].as_u64().unwrap(),
            "header renewals = visible rows + deleted {horizon}"
        );
        assert!(
            visible["rows"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["id"] == "in-request" && row["state"] == "in_request")
        );
        assert!(
            all["rows"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["id"] == "incomplete"
                    && row["state"] == "expired"
                    && row["error"].is_null())
        );
    }
    detail_table_retains_the_rail_cycle_range(storage, state, principal).await;
    detail_follow_up_counts_preserve_mixed_gap_outcomes(storage, state, principal).await;
}

async fn detail_table_retains_the_rail_cycle_range(
    storage: &dyn Storage,
    state: &cc_lb_admin::AdminState,
    principal: &str,
) {
    let id = "cycle-replaced";
    let first_cycle = uuid::Uuid::from_u128(0xc1);
    let second_cycle = uuid::Uuid::from_u128(0xc2);
    let current_cycle = uuid::Uuid::from_u128(0xc3);
    let mut request =
        cache_keepalive_replace_request(principal, id, NOW_UNIX_SECS - 7_201, NOW_UNIX_SECS + 300);
    request.cycle_id = first_cycle;
    storage
        .replace_from_real_request(&request)
        .await
        .expect("old cycle");
    for index in 0..9 {
        let (at, cycle) = if index == 0 {
            (NOW_UNIX_SECS - 7_200, None)
        } else {
            (
                NOW_UNIX_SECS - 600 + index,
                Some(if index <= 4 {
                    first_cycle
                } else {
                    second_cycle
                }),
            )
        };
        seed_cycle_turn(
            storage,
            principal,
            id,
            format!("cycle-turn-{index}"),
            at,
            cycle,
            Vec::new(),
        )
        .await;
    }
    request.cycle_id = current_cycle;
    request.cache_anchor_at_unix_secs = NOW_UNIX_SECS;
    request.now_unix_secs = NOW_UNIX_SECS;
    storage
        .replace_from_real_request(&request)
        .await
        .expect("empty current cycle");
    for (horizon, expected_turns, unknown_cycles) in [("24h", 9, 1), ("1h", 8, 0)] {
        let detail = get(
            state,
            &format!("{}&horizon={horizon}", keepalive_detail_uri(principal, id),),
        )
        .await;
        let turns = detail["turns"].as_array().unwrap();
        let pulse_count = detail["gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|gap| gap["pulses"].as_array().unwrap().len())
            .sum::<usize>();
        assert_eq!(
            turns.len(),
            expected_turns,
            "{horizon}: table retains historical cycles"
        );
        assert_eq!(
            pulse_count, expected_turns,
            "{horizon}: table and rail share the range"
        );
        assert_eq!(detail["total_renewals_current_cycle"], 0);
        assert_eq!(detail["is_last_pending"], false);
        assert!(turns.iter().all(|turn| turn["pending"] == false));
        assert_eq!(
            turns
                .iter()
                .filter(|turn| turn["cycle_id"].is_null())
                .count(),
            unknown_cycles
        );
        for cycle in [first_cycle, second_cycle] {
            let cycle_label = cycle.to_string();
            let mut ordinals = turns
                .iter()
                .filter(|turn| turn["cycle_id"].as_str() == Some(cycle_label.as_str()))
                .map(|turn| turn["turn_number"].as_u64().unwrap())
                .collect::<Vec<_>>();
            ordinals.sort_unstable();
            assert_eq!(ordinals, [1, 2, 3, 4]);
        }
    }
}

async fn detail_follow_up_counts_preserve_mixed_gap_outcomes(
    storage: &dyn Storage,
    state: &cc_lb_admin::AdminState,
    principal: &str,
) {
    let id = "mixed-follow-up-outcomes";
    let mut request =
        cache_keepalive_replace_request(principal, id, NOW_UNIX_SECS - 7_201, NOW_UNIX_SECS + 300);
    storage
        .replace_from_real_request(&request)
        .await
        .expect("outcome session");
    for index in 0..11 {
        let cycle = uuid::Uuid::from_u128(0xf000 + index as u128);
        let source = format!("outcome-turn-{index}");
        let at = if index == 0 {
            NOW_UNIX_SECS - 7_200
        } else {
            NOW_UNIX_SECS - 120 + index * 2
        };
        let mut evidence = Vec::new();
        if index != 10 {
            let renewal = CacheKeepaliveEvidenceRow {
                evidence_id: format!("outcome-renewal-{index}"),
                cycle_id: Some(cycle),
                evidence_kind: "renewal".to_owned(),
                request_start_ms: at * 1_000,
                prefix_fingerprint: Some(source.clone()),
                prefix_hash: Some(source.clone()),
                prefix_policy_version: Some("policy".to_owned()),
                prefix_schema_version: Some("schema".to_owned()),
                principal_id: principal.to_owned(),
                upstream_id: uuid::Uuid::from_u128(0xa11ce),
                model: "unknown-horizon-model".to_owned(),
                ttl: CacheTtl::Ttl5m,
                hit_miss: Some("hit".to_owned()),
                cache_read_input_tokens: Some(1),
                cost_micros: Some(1_000),
                request_ref: Some(source.clone()),
                event_ref: Some(source.clone()),
            };
            if index < 2 {
                evidence.push(CacheKeepaliveEvidenceRow {
                    evidence_id: format!("outcome-follow-up-{index}"),
                    cycle_id: None,
                    evidence_kind: "follow_up".to_owned(),
                    request_start_ms: at * 1_000 + 1_000,
                    request_ref: Some(format!("outcome-follow-up-request-{index}")),
                    event_ref: None,
                    ..renewal.clone()
                });
            }
            evidence.push(renewal);
        }
        seed_cycle_turn(storage, principal, id, source, at, Some(cycle), evidence).await;
    }
    storage
        .cancel_cache_keepalive_fenced(
            id,
            None,
            NOW_UNIX_SECS,
            CacheKeepaliveTerminalReason::Cancelled,
            Some("user_turn_detected"),
            NOW_UNIX_SECS,
        )
        .await
        .expect("close historical outcome session");
    for (horizon, observed, total) in [("24h", 2, 11), ("1h", 1, 10)] {
        let detail = get(
            state,
            &format!("{}&horizon={horizon}", keepalive_detail_uri(principal, id),),
        )
        .await;
        assert_eq!(
            detail["evidence_status"], "unknown",
            "legacy evidence summary remains unchanged"
        );
        assert_eq!(detail["gaps"].as_array().unwrap().len(), total);
        assert_eq!(
            detail["follow_up_outcomes"],
            serde_json::json!({
                "observed": observed, "pending": 0, "no_follow_up": 8, "unknown": 1
            }),
            "mixed known outcomes must not collapse to the one unknown gap"
        );
    }
    request.cycle_id = uuid::Uuid::from_u128(0xffff);
    request.cache_anchor_at_unix_secs = NOW_UNIX_SECS;
    request.now_unix_secs = NOW_UNIX_SECS;
    storage
        .replace_from_real_request(&request)
        .await
        .expect("new pending cycle");
    let detail = get(
        state,
        &format!("{}&horizon=24h", keepalive_detail_uri(principal, id),),
    )
    .await;
    assert_eq!(detail["gaps"].as_array().unwrap().len(), 12);
    assert_eq!(
        detail["follow_up_outcomes"],
        serde_json::json!({
            "observed": 2, "pending": 1, "no_follow_up": 8, "unknown": 1
        })
    );
}
