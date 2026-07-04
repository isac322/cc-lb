use cc_lb_engine::StreamingUsage;

#[test]
fn lifecycle_rate_limit_headers_current_streaming_usage_smoke() {
    let usage = StreamingUsage::default();
    assert_eq!(usage.input_tokens, 0);
    assert_eq!(usage.output_tokens, 0);
}
