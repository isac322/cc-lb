use cc_lb_storage_redb::{REQUEST_EVENTS_V1, RedbStorage, RequestEvent, RequestEventUpstream};
use redb::{ReadableDatabase, ReadableTable};
use serde_json::Value;

#[test]
fn request_events_persist_across_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.redb");

    let storage = RedbStorage::open(&path)?;
    storage.append_request_event(&event(0))?;
    drop(storage);

    let storage = RedbStorage::open(&path)?;
    let events = storage.query_request_events(1_800_000_000, u64::MAX, 10)?;

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].request_id, "req-0000");
    assert_eq!(events[0].principal_id.as_deref(), Some("principal-a"));
    assert_eq!(
        events[0].upstream,
        Some(RequestEventUpstream::AnthropicDirect)
    );
    assert_eq!(events[0].status, 200);
    Ok(())
}

#[test]
fn request_events_query_returns_append_order_for_monotonic_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.redb");
    let storage = RedbStorage::open(&path)?;

    for index in 0..100 {
        storage.append_request_event(&event(index))?;
    }

    let events = storage.query_request_events(1_800_000_002, u64::MAX, 5)?;
    assert_eq!(events.len(), 5);
    assert_eq!(events[0].request_id, "req-0020");
    assert_eq!(events[4].request_id, "req-0024");
    assert!(events.iter().all(|event| event.ts >= 1_800_000_002));
    Ok(())
}

#[test]
fn request_event_json_rows_exclude_payload_keys() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.redb");
    let storage = RedbStorage::open(&path)?;
    let request_event = event(7);

    let serialized = serde_json::to_value(&request_event)?;
    assert_forbidden_keys_absent(&serialized);

    storage.append_request_event(&request_event)?;
    drop(storage);

    let db = redb::Database::create(&path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(REQUEST_EVENTS_V1)?;
    let rows = table
        .iter()?
        .map(|row| {
            let (_, value) = row?;
            Ok::<_, redb::StorageError>(value.value().to_vec())
        })
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(rows.len(), 1);
    let persisted: Value = serde_json::from_slice(&rows[0])?;
    assert_forbidden_keys_absent(&persisted);
    Ok(())
}

fn event(index: usize) -> RequestEvent {
    RequestEvent {
        ts: 1_800_000_000 + (index / 10) as u64,
        request_id: format!("req-{index:04}"),
        principal_id: Some("principal-a".to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(index as u64),
        output_tokens: Some((index * 2) as u64),
        duration_ms: 25,
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

fn is_forbidden_key(key: &str) -> bool {
    matches!(
        key,
        "messages" | "system" | "tools" | "tool_use" | "content"
    )
}
