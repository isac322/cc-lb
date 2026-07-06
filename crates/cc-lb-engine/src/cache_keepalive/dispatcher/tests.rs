use super::*;

#[test]
fn classifies_successful_cache_read_as_hit() {
    let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"cache_read_input_tokens":1234,"input_tokens":0,"output_tokens":0}}"#;

    let outcome = classify_success_body(body);

    assert!(matches!(outcome, DispatchOutcome::CacheHit));
}

#[test]
fn classifies_zero_cache_read_as_miss() {
    let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"cache_read_input_tokens":0,"input_tokens":0,"output_tokens":0}}"#;

    let outcome = classify_success_body(body);

    assert!(matches!(outcome, DispatchOutcome::CacheMiss));
}

#[test]
fn classifies_missing_usage_as_miss() {
    let body = br#"{"content":[],"stop_reason":"max_tokens"}"#;

    let outcome = classify_success_body(body);

    assert!(matches!(outcome, DispatchOutcome::CacheMiss));
}

#[test]
fn api_key_upstream_without_stored_key_is_not_keepalive_safe() {
    let upstream = UpstreamRecord {
        kind: UpstreamKind::AnthropicApiKey,
        api_key_ciphertext: None,
        ..UpstreamRecord::default()
    };

    let error = keepalive_upstream_supported(&upstream).unwrap_err();

    assert!(error.contains("AnthropicApiKey"));
    assert!(error.contains("storage-backed signing"));
}

#[test]
fn api_key_upstream_with_stored_key_is_not_keepalive_safe_until_storage_signing() {
    let upstream = UpstreamRecord {
        kind: UpstreamKind::AnthropicApiKey,
        api_key_ciphertext: Some(vec![1, 2, 3]),
        ..UpstreamRecord::default()
    };

    let error = keepalive_upstream_supported(&upstream).unwrap_err();

    assert!(error.contains("AnthropicApiKey"));
    assert!(error.contains("storage-backed signing"));
}

#[test]
fn oauth_upstream_is_keepalive_safe_without_api_key_ciphertext() {
    let upstream = UpstreamRecord {
        kind: UpstreamKind::AnthropicOauth,
        api_key_ciphertext: None,
        ..UpstreamRecord::default()
    };

    assert!(keepalive_upstream_supported(&upstream).is_ok());
}
