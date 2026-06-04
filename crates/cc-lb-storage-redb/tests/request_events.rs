use cc_lb_storage_api::{RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestCacheState};
use cc_lb_storage_redb::{REQUEST_EVENTS_V1, RedbStorage, RequestEvent};
use redb::{ReadableDatabase, ReadableTable};
use serde_json::Value;

#[test]
fn request_events_persist_across_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.redb");

    let storage = RedbStorage::open(&path, [0; 32])?;
    storage.append_request_event(&event(0))?;
    drop(storage);

    let storage = RedbStorage::open(&path, [0; 32])?;
    let events = storage.query_request_events(1_800_000_000, u64::MAX, 10)?;

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key_id.as_deref(), Some("req-0000"));
    assert_eq!(events[0].principal_id.as_deref(), Some("principal-a"));
    assert_eq!(events[0].cache_state, Some(RequestCacheState::Hit));
    assert_eq!(events[0].thread_id.as_deref(), Some("thread-a"));
    assert_eq!(events[0].message_index, Some(0));
    assert_eq!(events[0].cache_control_message_indices, vec![0]);
    assert_eq!(events[0].cache_breakpoints.len(), 1);
    assert_eq!(
        events[0].cache_breakpoints[0].path,
        "messages[0].content[0]"
    );
    assert_eq!(events[0].status, 200);
    Ok(())
}

#[test]
fn request_events_query_returns_append_order_for_monotonic_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.redb");
    let storage = RedbStorage::open(&path, [0; 32])?;

    for index in 0..100 {
        storage.append_request_event(&event(index))?;
    }

    let events = storage.query_request_events(1_800_000_002, u64::MAX, 5)?;
    assert_eq!(events.len(), 5);
    assert_eq!(events[0].key_id.as_deref(), Some("req-0020"));
    assert_eq!(events[4].key_id.as_deref(), Some("req-0024"));
    assert!(
        events
            .iter()
            .all(|event| event.ts_ms.unwrap_or_default() >= 1_800_000_002)
    );
    Ok(())
}

#[test]
fn request_event_json_rows_exclude_payload_keys() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.redb");
    let storage = RedbStorage::open(&path, [0; 32])?;
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
        ts_ms: Some(1_800_000_000 + (index / 10) as u64),
        principal_id: Some("principal-a".to_owned()),
        key_id: Some(format!("req-{index:04}")),
        model: Some("claude-sonnet-4-5".to_owned()),
        input_tokens: Some(index as u64),
        output_tokens: Some((index * 2) as u64),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(0),
        cache_state: Some(RequestCacheState::Hit),
        thread_id: Some("thread-a".to_owned()),
        message_id: Some(format!("msg-{index:04}")),
        message_index: Some(index as u64),
        message_count: Some((index + 1) as u64),
        cache_control_block_count: Some(1),
        cache_control_message_indices: vec![index as u64],
        cache_breakpoints: vec![RequestCacheBreakpoint {
            block_index: 0,
            source: RequestCacheBreakpointSource::Message,
            path: format!("messages[{index}].content[0]"),
            message_index: Some(index as u64),
            ttl: Some("5m".to_owned()),
            prefix_hash: format!("{:064x}", index + 1),
        }],
        cache_prefix_hash: Some(format!("{index:064x}")),
        cost_usd_micros: Some(0),
        duration_ms: 25,
        status: 200,
        ..Default::default()
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
