use cc_lb_storage_redb::{
    RequestEvent, RequestEventUpstream, Storage, USAGE_ROLLUPS_V1, UsageRollup,
    UsageRollupResolution,
};
use redb::{ReadableDatabase, ReadableTable};
use serde_json::Value;

#[test]
fn fixture_events_roll_up_once_and_rerun_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollups.redb");
    let storage = Storage::open(&path, [41; 32])?;

    for event in fixture_events() {
        storage.append_request_event(&event)?;
    }

    let first = storage.rollup_usage_once()?;
    assert_eq!(first.processed_events, 4);
    assert_eq!(first.updated_rollups, 5);
    assert!(first.checkpoint.is_some());

    let rollups = storage.query_usage_rollups()?;
    assert_eq!(rollups.len(), 5);
    assert_rollup(
        find_rollup(
            &rollups,
            UsageRollupResolution::Minute,
            1_800_000_000,
            "principal-a",
            "anthropic_direct",
            "claude-sonnet-4-5",
        ),
        ExpectedRollup {
            request_count: 2,
            input_tokens: 15,
            output_tokens: 20,
            error_count: 1,
            latency_count: 2,
            latency_ms_sum: 400,
            latency_ms_min: Some(100),
            latency_ms_max: Some(300),
            virtual_cost_micros: 345,
        },
    );
    assert_rollup(
        find_rollup(
            &rollups,
            UsageRollupResolution::Minute,
            1_800_000_060,
            "principal-a",
            "anthropic_direct",
            "claude-sonnet-4-5",
        ),
        ExpectedRollup {
            request_count: 1,
            input_tokens: 7,
            output_tokens: 8,
            error_count: 0,
            latency_count: 1,
            latency_ms_sum: 50,
            latency_ms_min: Some(50),
            latency_ms_max: Some(50),
            virtual_cost_micros: 141,
        },
    );
    assert_rollup(
        find_rollup(
            &rollups,
            UsageRollupResolution::Hour,
            1_800_000_000,
            "principal-a",
            "anthropic_direct",
            "claude-sonnet-4-5",
        ),
        ExpectedRollup {
            request_count: 3,
            input_tokens: 22,
            output_tokens: 28,
            error_count: 1,
            latency_count: 3,
            latency_ms_sum: 450,
            latency_ms_min: Some(50),
            latency_ms_max: Some(300),
            virtual_cost_micros: 486,
        },
    );

    let second = storage.rollup_usage_once()?;
    assert_eq!(second.processed_events, 0);
    assert_eq!(second.updated_rollups, 0);
    assert_eq!(second.checkpoint, first.checkpoint);
    assert_eq!(storage.query_usage_rollups()?, rollups);
    drop(storage);

    let reopened = Storage::open(&path, [41; 32])?;
    assert_eq!(reopened.usage_rollup_checkpoint()?, first.checkpoint);
    assert_eq!(reopened.query_usage_rollups()?, rollups);
    Ok(())
}

#[test]
fn known_and_unknown_model_costs_roll_up() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollups.redb");
    let storage = Storage::open(&path, [41; 32])?;

    storage.append_request_event(&event(
        1_800_000_005,
        "req-known",
        "principal-a",
        RequestEventUpstream::AnthropicDirect,
        "claude-sonnet-4-5",
        200,
        Some(10),
        Some(20),
        100,
    ))?;
    storage.append_request_event(&event(
        1_800_000_010,
        "req-unknown",
        "principal-a",
        RequestEventUpstream::AnthropicDirect,
        "unknown-model",
        200,
        Some(10),
        Some(20),
        100,
    ))?;

    storage.rollup_usage_once()?;
    let rollups = storage.query_usage_rollups()?;
    let known = find_rollup(
        &rollups,
        UsageRollupResolution::Minute,
        1_800_000_000,
        "principal-a",
        "anthropic_direct",
        "claude-sonnet-4-5",
    );
    let unknown = find_rollup(
        &rollups,
        UsageRollupResolution::Minute,
        1_800_000_000,
        "principal-a",
        "anthropic_direct",
        "unknown-model",
    );

    assert!(known.virtual_cost_micros > 0);
    assert_eq!(known.virtual_cost_micros, 330);
    assert_eq!(unknown.virtual_cost_micros, 0);
    Ok(())
}

#[test]
fn query_usage_rollups_in_range_filters_resolution_and_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollups.redb");
    let storage = Storage::open(&path, [41; 32])?;

    for event in fixture_events() {
        storage.append_request_event(&event)?;
    }
    storage.rollup_usage_once()?;

    let first_minute = storage.query_usage_rollups_in_range(
        UsageRollupResolution::Minute,
        1_800_000_000,
        1_800_000_060,
    )?;
    assert_eq!(first_minute.len(), 2);
    assert!(first_minute.iter().all(|rollup| {
        rollup.resolution == UsageRollupResolution::Minute && rollup.bucket_start == 1_800_000_000
    }));

    let second_minute = storage.query_usage_rollups_in_range(
        UsageRollupResolution::Minute,
        1_800_000_060,
        1_800_000_120,
    )?;
    assert_eq!(second_minute.len(), 1);
    assert_eq!(second_minute[0].bucket_start, 1_800_000_060);

    let empty_hour = storage.query_usage_rollups_in_range(
        UsageRollupResolution::Hour,
        1_800_003_600,
        1_800_007_200,
    )?;
    assert!(empty_hour.is_empty());
    Ok(())
}

#[test]
fn usage_rollup_json_rows_exclude_payload_keys() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollups.redb");
    let storage = Storage::open(&path, [41; 32])?;

    for event in fixture_events() {
        let serialized = serde_json::to_value(&event)?;
        assert_forbidden_keys_absent(&serialized);
        storage.append_request_event(&event)?;
    }
    storage.rollup_usage_once()?;
    drop(storage);

    let db = redb::Database::create(&path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(USAGE_ROLLUPS_V1)?;
    let rows = table
        .iter()?
        .map(|row| {
            let (key, value) = row?;
            assert_forbidden_key_bytes_absent(key.value());
            Ok::<_, redb::StorageError>(value.value().to_vec())
        })
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(rows.len(), 5);
    for row in rows {
        let persisted: Value = serde_json::from_slice(&row)?;
        assert_forbidden_keys_absent(&persisted);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ExpectedRollup {
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    error_count: u64,
    latency_count: u64,
    latency_ms_sum: u64,
    latency_ms_min: Option<u64>,
    latency_ms_max: Option<u64>,
    virtual_cost_micros: u64,
}

fn assert_rollup(rollup: &UsageRollup, expected: ExpectedRollup) {
    assert_eq!(rollup.request_count, expected.request_count);
    assert_eq!(rollup.input_tokens, expected.input_tokens);
    assert_eq!(rollup.output_tokens, expected.output_tokens);
    assert_eq!(rollup.error_count, expected.error_count);
    assert_eq!(rollup.latency_count, expected.latency_count);
    assert_eq!(rollup.latency_ms_sum, expected.latency_ms_sum);
    assert_eq!(rollup.latency_ms_min, expected.latency_ms_min);
    assert_eq!(rollup.latency_ms_max, expected.latency_ms_max);
    assert_eq!(rollup.virtual_cost_micros, expected.virtual_cost_micros);
}

fn find_rollup<'a>(
    rollups: &'a [UsageRollup],
    resolution: UsageRollupResolution,
    bucket_start: u64,
    principal: &str,
    upstream: &str,
    model: &str,
) -> &'a UsageRollup {
    rollups
        .iter()
        .find(|rollup| {
            rollup.resolution == resolution
                && rollup.bucket_start == bucket_start
                && rollup.principal == principal
                && rollup.upstream == upstream
                && rollup.model == model
        })
        .expect("expected rollup row")
}

fn fixture_events() -> Vec<RequestEvent> {
    vec![
        event(
            1_800_000_005,
            "req-a-1",
            "principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            200,
            Some(10),
            Some(20),
            100,
        ),
        event(
            1_800_000_030,
            "req-a-2",
            "principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            429,
            Some(5),
            None,
            300,
        ),
        event(
            1_800_000_065,
            "req-a-3",
            "principal-a",
            RequestEventUpstream::AnthropicDirect,
            "claude-sonnet-4-5",
            200,
            Some(7),
            Some(8),
            50,
        ),
        event(
            1_800_000_045,
            "req-b-1",
            "principal-b",
            RequestEventUpstream::Vertex,
            "claude-opus-4-1",
            500,
            None,
            Some(1),
            700,
        ),
    ]
}

#[allow(clippy::too_many_arguments)]
fn event(
    ts: u64,
    request_id: &str,
    principal: &str,
    upstream: RequestEventUpstream,
    model: &str,
    status: u16,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    duration_ms: u64,
) -> RequestEvent {
    RequestEvent {
        ts,
        request_id: request_id.to_owned(),
        principal_id: Some(principal.to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(upstream),
        model: Some(model.to_owned()),
        status,
        input_tokens,
        output_tokens,
        duration_ms,
        error_code: None,
    }
}

fn assert_forbidden_keys_absent(value: &Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                assert!(!is_forbidden_key(key), "forbidden key present: {key}");
                assert_forbidden_keys_absent(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_forbidden_keys_absent(value);
            }
        }
        _ => {}
    }
}

fn assert_forbidden_key_bytes_absent(bytes: &[u8]) {
    for forbidden in ["messages", "system", "tools", "tool_use", "content"] {
        assert!(
            !bytes
                .windows(forbidden.len())
                .any(|window| window == forbidden.as_bytes()),
            "forbidden key bytes present: {forbidden}"
        );
    }
}

fn is_forbidden_key(key: &str) -> bool {
    matches!(
        key,
        "messages" | "system" | "tools" | "tool_use" | "content"
    )
}
