use cc_lb_storage_api::{
    RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestEvent, RequestEventUpstream,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn request_cache_breakpoint_deserializes_without_prefix_token_count_field() {
    let legacy_json = json!({
        "block_index": 0,
        "source": "system",
        "path": "system[0]",
        "message_index": null,
        "ttl": "5m",
        "prefix_hash": "deadbeef"
    });
    let decoded: RequestCacheBreakpoint = serde_json::from_value(legacy_json).expect("decode");
    assert_eq!(decoded.prefix_token_count, 0);
    assert!(decoded.lookback_prefixes.is_empty());
    assert_eq!(decoded.token_estimate_source, None);
    assert_eq!(decoded.prefix_hash, "deadbeef");
    assert_eq!(decoded.ttl.as_deref(), Some("5m"));
}

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

#[test]
fn request_event_decodes_pre_existing_audit_row_without_prefix_token_count() {
    let upstream_id = Uuid::from_u128(0x11111111_1111_1111_1111_111111111111);
    let legacy_audit_row = json!({
        "ts": 1_700_000_000_u64,
        "request_id": "req-legacy",
        "principal_id": "principal-legacy",
        "principal_kind": "api_key",
        "upstream": "anthropic_direct",
        "upstream_id": upstream_id,
        "model": "claude-sonnet-4-5",
        "status": 200,
        "input_tokens": 100,
        "output_tokens": 50,
        "duration_ms": 25,
        "cache_breakpoints": [{
            "block_index": 0,
            "source": "system",
            "path": "system[0]",
            "ttl": "5m",
            "prefix_hash": "legacyhash"
        }]
    });
    let decoded: RequestEvent = serde_json::from_value(legacy_audit_row).expect("decode");
    assert_eq!(
        decoded.upstream,
        Some(RequestEventUpstream::AnthropicDirect)
    );
    assert_eq!(decoded.cache_breakpoints.len(), 1);
    assert_eq!(decoded.cache_breakpoints[0].prefix_token_count, 0);
    assert!(decoded.cache_breakpoints[0].lookback_prefixes.is_empty());
    assert_eq!(decoded.cache_breakpoints[0].token_estimate_source, None);
    assert_eq!(decoded.cache_breakpoints[0].prefix_hash, "legacyhash");
}
