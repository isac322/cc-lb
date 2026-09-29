use cc_lb_storage_api::{RequestCacheBreakpoint, RequestCacheBreakpointSource};
use serde_json::Value;

#[test]
fn request_cache_breakpoint_zero_token_count_is_omitted_from_serialization() {
    let breakpoint = RequestCacheBreakpoint {
        block_index: 0,
        source: RequestCacheBreakpointSource::System,
        path: "system[0]".to_owned(),
        message_index: None,
        ttl: Some("5m".to_owned()),
        prefix_hash: "deadbeef".to_owned(),
        prefix_token_count: 0,
        lookback_prefixes: Vec::new(),
        token_estimate_source: None,
    };
    let serialized: Value = serde_json::to_value(&breakpoint).expect("serialize");
    assert!(
        serialized.get("prefix_token_count").is_none(),
        "zero prefix_token_count should be omitted, got {serialized}"
    );
}

#[test]
fn request_cache_breakpoint_nonzero_token_count_roundtrips() {
    let breakpoint = RequestCacheBreakpoint {
        block_index: 1,
        source: RequestCacheBreakpointSource::Message,
        path: "messages[0].content[0]".to_owned(),
        message_index: Some(0),
        ttl: Some("1h".to_owned()),
        prefix_hash: "abc123".to_owned(),
        prefix_token_count: 3050,
        lookback_prefixes: Vec::new(),
        token_estimate_source: Some("test".to_owned()),
    };
    let serialized: Value = serde_json::to_value(&breakpoint).expect("serialize");
    assert_eq!(
        serialized.get("prefix_token_count").and_then(Value::as_u64),
        Some(3050)
    );
    let decoded: RequestCacheBreakpoint = serde_json::from_value(serialized).expect("decode");
    assert_eq!(decoded, breakpoint);
}
