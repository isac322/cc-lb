use std::collections::BTreeSet;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::{AdminState, router};
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::warmup_attempts::{
    WarmupAttemptCursor, WarmupAttemptOutcome, WarmupAttemptRecord, WarmupAttemptTrigger,
    WarmupDispatchKind, WarmupPermanentFailureReason, WarmupSkipReason, WarmupSuccessReason,
    WarmupTransientFailureReason,
};
use cc_lb_storage_api::{UpstreamStore, UpstreamWarmupAttemptStore};
use cc_lb_storage_sqlite::SqliteStorage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const TEST_TOKEN: &str = "test-token";
const TEST_NOW_UNIX_SECS: u64 = 1_700_000_000;
pub use super::scheduler_support::NEXT_SCHEDULED_AT;
pub const RECENT_7D_COUNTS: (u64, u64, u64, u64) = (6, 2, 3, 3);

pub struct Fixture {
    pub _dir: tempfile::TempDir,
    pub app: axum::Router,
    pub upstream_id: Uuid,
    pub attempts: Vec<WarmupAttemptRecord>,
    pub dialect_plugin: UpstreamWarmupDialectPlugin,
}

pub async fn new_fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("temp admin warmup dir");
    let test_clock = Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS));
    let clock: ClockHandle = test_clock.clone();
    let storage = crate::admin_test_common::sqlite_storage_with_clock(
        dir.path(),
        "admin_warmup.sqlite",
        clock.clone(),
    )
    .await;
    let dialect_plugin = UpstreamWarmupDialectPlugin {
        wasm_registry_id: Uuid::from_u128(0x1111_1111_2222_3333_4444_5555_6666_7777),
        config: json!({"mode": "compact", "max_tokens": 1}),
        wire_version: Some(1),
    };
    let upstream = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "warmup-primary".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: true,
            warmup_dialect_plugin: Some(dialect_plugin.clone()),
        },
    )
    .await
    .expect("upstream seed succeeds");
    let attempts = seed_attempts(
        storage.as_ref(),
        upstream.id,
        upstream.revision,
        &dialect_plugin,
        clock.as_ref(),
    )
    .await;
    let scheduler =
        super::scheduler_support::scheduler_with_next_warmup(upstream.id, clock.clone()).await;
    Fixture {
        _dir: dir,
        app: router(test_state(storage, scheduler, clock)),
        upstream_id: upstream.id,
        attempts,
        dialect_plugin,
    }
}

fn test_state(
    storage: Arc<SqliteStorage>,
    scheduler: cc_lb_scheduler::admin::SchedulerAdminHandle,
    clock: ClockHandle,
) -> AdminState {
    let config = Config::default();
    AdminState {
        storage: Some(storage.clone()),
        key_store: Some(crate::admin_test_common::key_store(storage)),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: crate::admin_test_common::limit_engine_with_clock(clock.clone()),
        lifecycle: None,
        subscription_metadata_hook: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        dynamic_view: crate::admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        scheduler: Some(scheduler),
        admin_auth: crate::admin_test_common::static_token_auth(TEST_TOKEN),
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock,
    }
}

async fn seed_attempts(
    storage: &SqliteStorage,
    upstream_id: Uuid,
    upstream_spec_revision: u64,
    dialect_plugin: &UpstreamWarmupDialectPlugin,
    clock: &dyn cc_lb_clock::Clock,
) -> Vec<WarmupAttemptRecord> {
    let now = i64::try_from(cc_lb_clock::unix_secs(clock.now())).unwrap_or(i64::MAX);
    let plugin_snapshot = serde_json::to_value(dialect_plugin).expect("plugin serializes");
    let attempts = (0..30)
        .map(|index| {
            seeded_attempt(
                upstream_id,
                upstream_spec_revision,
                &plugin_snapshot,
                now,
                index,
            )
        })
        .collect::<Vec<_>>();
    for attempt in &attempts {
        storage
            .insert_warmup_attempt(attempt)
            .await
            .expect("attempt seed succeeds");
    }
    attempts
}

fn seeded_attempt(
    upstream_id: Uuid,
    upstream_spec_revision: u64,
    plugin_snapshot: &Value,
    now: i64,
    index: usize,
) -> WarmupAttemptRecord {
    let outcome = outcome_for_index(index);
    let attempted_at_unix_secs = attempted_at_for_index(now, index);
    WarmupAttemptRecord {
        id: Uuid::from_u128(0xAAAA_0000_0000_0000_0000_0000_0000_0000 + index as u128),
        upstream_id,
        attempted_at_unix_secs,
        completed_at_unix_secs: Some(attempted_at_unix_secs + 2),
        scheduled_for_unix_secs: attempted_at_unix_secs - 60,
        trigger: trigger_for_index(index),
        outcome,
        dispatch_kind: Some(dispatch_kind_for_outcome(outcome)),
        http_status: http_status_for_outcome(outcome),
        cycle_key: cycle_key_for_outcome(outcome, attempted_at_unix_secs),
        expected_cycle_key: expected_cycle_key_for_outcome(outcome, attempted_at_unix_secs),
        idle_secs_since_prev_window: idle_secs_since_prev_window_for_outcome(outcome),
        replica_id: (index.is_multiple_of(2)).then_some(Uuid::from_u128(
            0xBBBB_0000_0000_0000_0000_0000_0000_0000 + index as u128,
        )),
        lease_holder: matches!(outcome, WarmupAttemptOutcome::Skipped(_))
            .then_some("replica-2".to_owned()),
        upstream_spec_revision: i64::try_from(upstream_spec_revision)
            .expect("test revision fits i64"),
        dialect_plugin_snapshot: Some(plugin_snapshot.clone()),
        error_detail: matches!(
            outcome,
            WarmupAttemptOutcome::TransientFailure(_) | WarmupAttemptOutcome::PermanentFailure(_)
        )
        .then_some("seeded warmup outcome".to_owned()),
    }
}

fn attempted_at_for_index(now: i64, index: usize) -> i64 {
    let index_secs = i64::try_from(index).expect("test index fits i64") * 40_000;
    if index < 14 {
        now - index_secs
    } else {
        now - (8 * 86_400) - index_secs
    }
}

const fn trigger_for_index(index: usize) -> WarmupAttemptTrigger {
    if index.is_multiple_of(3) {
        WarmupAttemptTrigger::Manual
    } else {
        WarmupAttemptTrigger::Scheduled
    }
}

const fn outcome_for_index(index: usize) -> WarmupAttemptOutcome {
    match index % 5 {
        0 => WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
        1 => WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
        2 => WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::Upstream5xx),
        3 => WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::AuthFailed),
        _ => WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDisabled),
    }
}

const fn dispatch_kind_for_outcome(outcome: WarmupAttemptOutcome) -> WarmupDispatchKind {
    match outcome {
        WarmupAttemptOutcome::Success(_)
        | WarmupAttemptOutcome::TransientFailure(_)
        | WarmupAttemptOutcome::PermanentFailure(_) => WarmupDispatchKind::Http,
        WarmupAttemptOutcome::Skipped(_) => WarmupDispatchKind::NotDispatched,
    }
}

const fn http_status_for_outcome(outcome: WarmupAttemptOutcome) -> Option<i32> {
    match outcome {
        WarmupAttemptOutcome::Success(_) => Some(200),
        WarmupAttemptOutcome::TransientFailure(_) => Some(503),
        WarmupAttemptOutcome::PermanentFailure(_) => Some(401),
        WarmupAttemptOutcome::Skipped(_) => None,
    }
}

const fn cycle_key_for_outcome(
    outcome: WarmupAttemptOutcome,
    attempted_at_unix_secs: i64,
) -> Option<i64> {
    match outcome {
        WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced) => {
            Some(attempted_at_unix_secs / (5 * 3600))
        }
        WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive)
        | WarmupAttemptOutcome::Skipped(_)
        | WarmupAttemptOutcome::TransientFailure(_)
        | WarmupAttemptOutcome::PermanentFailure(_) => None,
    }
}

const fn expected_cycle_key_for_outcome(
    outcome: WarmupAttemptOutcome,
    attempted_at_unix_secs: i64,
) -> Option<i64> {
    match outcome {
        WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive) => {
            Some((attempted_at_unix_secs / (5 * 3600)) + 1)
        }
        WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced)
        | WarmupAttemptOutcome::Skipped(_)
        | WarmupAttemptOutcome::TransientFailure(_)
        | WarmupAttemptOutcome::PermanentFailure(_) => None,
    }
}

const fn idle_secs_since_prev_window_for_outcome(outcome: WarmupAttemptOutcome) -> Option<i64> {
    match outcome {
        WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced) => Some(600),
        WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive)
        | WarmupAttemptOutcome::Skipped(_)
        | WarmupAttemptOutcome::TransientFailure(_)
        | WarmupAttemptOutcome::PermanentFailure(_) => None,
    }
}

pub async fn get_json(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let value = serde_json::from_slice(&body).expect("response is json");
    (status, value)
}

pub fn keys(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

pub fn object_keys(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .expect("json object")
        .keys()
        .cloned()
        .collect()
}

pub fn attempt_ids(value: &Value, key: &str) -> Vec<String> {
    value[key]
        .as_array()
        .expect("attempt array")
        .iter()
        .map(|attempt| attempt["id"].as_str().expect("attempt id").to_owned())
        .collect()
}

pub fn expected_ids(attempts: &[WarmupAttemptRecord]) -> Vec<String> {
    attempts
        .iter()
        .map(|attempt| attempt.id.to_string())
        .collect()
}

pub fn cursor_for(attempt: &WarmupAttemptRecord) -> WarmupAttemptCursor {
    WarmupAttemptCursor {
        attempted_at_unix_secs: attempt.attempted_at_unix_secs,
        id: attempt.id,
    }
}
