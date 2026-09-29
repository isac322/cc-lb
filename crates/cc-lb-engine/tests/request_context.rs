use bytes::Bytes;
use cc_lb_domain::{
    CacheBreakpoint, CacheBreakpointSource, CacheLookbackPrefix, CachePricingSummary, TtlClass,
};
use cc_lb_engine::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;
use cc_lb_engine::request_context::RequestContext;
use http::{HeaderMap, Method};

#[test]
fn cache_fields_preserve_empty_and_populated_values() {
    let empty = RequestContext::builder()
        .request_id("req-1".to_owned())
        .thread_id(None)
        .downstream_headers(HeaderMap::new())
        .method(Method::POST)
        .path("/v1/messages".to_owned())
        .query(None)
        .body_bytes(Bytes::new())
        .cache_breakpoints(Vec::new())
        .canonical_model_id(String::new())
        .cache_pricing(CachePricingSummary::default())
        .build();
    assert!(empty.cache_breakpoints.is_empty());
    assert!(empty.canonical_model_id.is_empty());

    let populated = RequestContext::builder()
        .request_id("req-2".to_owned())
        .thread_id(None)
        .downstream_headers(HeaderMap::new())
        .method(Method::POST)
        .path("/v1/messages".to_owned())
        .query(Some("param=value".to_owned()))
        .body_bytes(Bytes::from_static(b"test"))
        .cache_breakpoints(vec![breakpoint()])
        .canonical_model_id("claude-sonnet-4-5-20250929".to_owned())
        .cache_pricing(CachePricingSummary::default())
        .build();
    assert_eq!(populated.cache_breakpoints.len(), 1);
    assert_eq!(populated.canonical_model_id, "claude-sonnet-4-5-20250929");
}

#[test]
fn cache_fields_remain_empty_when_builder_uses_empty_values() {
    let context = RequestContext::builder()
        .request_id("test".to_owned())
        .thread_id(None)
        .downstream_headers(HeaderMap::new())
        .method(Method::GET)
        .path("/test".to_owned())
        .query(None)
        .body_bytes(Bytes::new())
        .cache_breakpoints(Vec::new())
        .canonical_model_id(String::new())
        .cache_pricing(CachePricingSummary::default())
        .build();

    assert!(context.cache_breakpoints.is_empty());
    assert!(context.canonical_model_id.is_empty());
}

fn breakpoint() -> CacheBreakpoint {
    CacheBreakpoint {
        block_index: 1,
        source: CacheBreakpointSource::Message,
        path: "messages.0.content.0".to_owned(),
        message_index: Some(0),
        prefix_hash: "hash123".to_owned(),
        prefix_token_count: 150,
        requested_ttl: TtlClass::Ephemeral1h,
        lookback_prefixes: vec![CacheLookbackPrefix {
            prefix_hash: "hash123".to_owned(),
            content_block_index: 1,
            lookback_distance: 0,
        }],
        token_estimate_source: Some(V3_TOKEN_ESTIMATE_SOURCE.to_owned()),
    }
}
