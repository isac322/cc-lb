use std::{fs, path::Path, sync::Arc};

use cc_lb_domain::{
    InternalError, InternalErrorKind, InternalErrorStage, RoutingTrace, StageDecision,
    SubscriptionPreferenceTrace, SubscriptionTier, TerminalDecision, TerminalStrategy,
};
use cc_lb_storage_api::{KeyStatus, MetaStore, PluginSlotKind, RequestEvent, RequestEventStore};
use sqlx::Row;
use uuid::Uuid;

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/reassembly"
);
const FIXTURE_NAMES: [&str; 4] = [
    "request_event.json",
    "request_events_v1_payload.json",
    "plugin_slots.json",
    "key_statuses.json",
];

#[tokio::test]
async fn golden_serde_bytes_match_current_types_and_storage_path() {
    let generated_dir = tempfile::tempdir().expect("create generated fixture directory");
    generate_fixtures(generated_dir.path()).await;

    if std::env::var_os("UPDATE_REASSEMBLY_FIXTURES").is_some() {
        fs::create_dir_all(FIXTURE_ROOT).expect("create committed fixture directory");
        for name in FIXTURE_NAMES {
            fs::copy(
                generated_dir.path().join(name),
                Path::new(FIXTURE_ROOT).join(name),
            )
            .expect("copy generated fixture");
        }
    }

    for name in FIXTURE_NAMES {
        let generated = fs::read(generated_dir.path().join(name)).expect("read generated fixture");
        let committed =
            fs::read(Path::new(FIXTURE_ROOT).join(name)).expect("read committed golden fixture");
        assert_eq!(generated, committed, "fixture bytes changed: {name}");
    }

    let request_bytes = fs::read(Path::new(FIXTURE_ROOT).join("request_event.json"))
        .expect("read request event fixture");
    let request: RequestEvent =
        serde_json::from_slice(&request_bytes).expect("deserialize request event fixture");
    assert_eq!(
        serde_json::to_vec(&request).expect("re-serialize request event fixture"),
        request_bytes
    );
    let trace = request
        .routing_trace
        .as_ref()
        .expect("routing trace is populated");
    assert!(!trace.stages.is_empty());
    assert!(
        trace
            .stages
            .iter()
            .any(|stage| stage.subscription_preference.is_some())
    );
    assert!(trace.terminal_decision.is_some());
    assert!(!request.internal_errors.is_empty());

    let stored_bytes = fs::read(Path::new(FIXTURE_ROOT).join("request_events_v1_payload.json"))
        .expect("read stored payload fixture");
    let stored: RequestEvent =
        serde_json::from_slice(&stored_bytes).expect("deserialize stored payload fixture");
    assert_eq!(
        serde_json::to_vec(&stored).expect("re-serialize stored payload fixture"),
        stored_bytes
    );

    let plugin_slot_bytes = fs::read(Path::new(FIXTURE_ROOT).join("plugin_slots.json"))
        .expect("read plugin slot fixture");
    let plugin_slots: Vec<PluginSlotKind> =
        serde_json::from_slice(&plugin_slot_bytes).expect("deserialize plugin slot fixture");
    assert_eq!(plugin_slots.len(), 2);
    assert_eq!(
        serde_json::to_vec(&plugin_slots).expect("re-serialize plugin slot fixture"),
        plugin_slot_bytes
    );
    for slot in plugin_slots {
        match slot {
            PluginSlotKind::Router => assert_eq!(slot.as_str(), "router"),
            PluginSlotKind::Shape => assert_eq!(slot.as_str(), "shape"),
        }
    }

    let key_status_bytes = fs::read(Path::new(FIXTURE_ROOT).join("key_statuses.json"))
        .expect("read key status fixture");
    let key_statuses: Vec<KeyStatus> =
        serde_json::from_slice(&key_status_bytes).expect("deserialize key status fixture");
    assert_eq!(key_statuses.len(), 3);
    assert_eq!(
        serde_json::to_vec(&key_statuses).expect("re-serialize key status fixture"),
        key_status_bytes
    );
    for status in key_statuses {
        let serialized = serde_json::to_string(&status).expect("serialize key status");
        match status {
            KeyStatus::Active => assert_eq!(serialized, "\"active\""),
            KeyStatus::Disabled => assert_eq!(serialized, "\"disabled\""),
            KeyStatus::Revoked => assert_eq!(serialized, "\"revoked\""),
        }
    }
}

async fn generate_fixtures(output_dir: &Path) {
    let upstream_id = Uuid::from_u128(0x018f_2a3b_4c5d_7e6f_8123_4567_89ab_cdef);
    let request = RequestEvent {
        ts: 1_720_000_000,
        ts_ms: Some(1_720_000_000_000),
        request_id: "req-reassembly-golden".to_owned(),
        event_id: Some("018f2a3b-4c5d-7e6f-8123-456789abcdef".to_owned()),
        principal_id: Some("principal-reassembly".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some("subscription-primary".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(1_024),
        output_tokens: Some(256),
        duration_ms: 73,
        routing_trace: Some(RoutingTrace {
            stages: vec![StageDecision {
                stage_name: "subscription_preference".to_owned(),
                upstream_id: Some(upstream_id),
                reason: Some("known base tier with warm cache".to_owned()),
                duration_us: 41,
                subscription_preference: Some(SubscriptionPreferenceTrace {
                    chosen_tier: SubscriptionTier::KnownBase,
                    candidates: Vec::new(),
                    formula_version: None,
                    cache_cost_basis_version: Some("v1".to_owned()),
                    formula_winner_upstream_id: Some(upstream_id),
                    kept_upstream_id: Some(upstream_id),
                    switch_gate_reason: Some("incumbent_is_formula_winner".to_owned()),
                    bucket_v3_cache_key: Some("v3:golden".to_owned()),
                }),
            }],
            terminal_decision: Some(TerminalDecision {
                upstream_id: Some(upstream_id),
                strategy: TerminalStrategy::FirstPick,
            }),
        }),
        internal_errors: vec![InternalError {
            stage: InternalErrorStage::RouterFilter,
            kind: InternalErrorKind::Unavailable,
            message: Some("golden non-fatal observability error".to_owned()),
        }],
        ..Default::default()
    };

    fs::write(
        output_dir.join("request_event.json"),
        serde_json::to_vec(&request).expect("serialize request event fixture"),
    )
    .expect("write request event fixture");
    fs::write(
        output_dir.join("plugin_slots.json"),
        serde_json::to_vec(&[PluginSlotKind::Router, PluginSlotKind::Shape])
            .expect("serialize plugin slot fixture"),
    )
    .expect("write plugin slot fixture");
    fs::write(
        output_dir.join("key_statuses.json"),
        serde_json::to_vec(&[KeyStatus::Active, KeyStatus::Disabled, KeyStatus::Revoked])
            .expect("serialize key status fixture"),
    )
    .expect("write key status fixture");

    let database_dir = tempfile::tempdir().expect("create sqlite fixture directory");
    let database_url = format!(
        "sqlite://{}",
        database_dir.path().join("reassembly.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open fixture sqlite database");
    storage
        .initialize()
        .await
        .expect("initialize fixture sqlite database");
    storage
        .append_request_event(&request)
        .await
        .expect("append request event through storage path");
    let row = sqlx::query("SELECT payload FROM request_events_v1 WHERE event_id = ?")
        .bind(request.event_id.as_deref())
        .fetch_one(storage.pool())
        .await
        .expect("read stored request event payload");
    let stored_payload: String = row.get("payload");
    fs::write(
        output_dir.join("request_events_v1_payload.json"),
        stored_payload.as_bytes(),
    )
    .expect("write stored payload fixture");
}
