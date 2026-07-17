use super::*;
use crate::attempt_rail::AttemptIntent;

fn accounting_guard() -> ResponseAccountingGuard {
    AttemptIntent::observe_only()
        .into_reserved()
        .into_response_accounting_guard()
}

#[test]
fn classifies_successful_cache_read_as_hit() {
    let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"cache_read_input_tokens":1234,"input_tokens":0,"output_tokens":0}}"#;

    let outcome = classify_success_body(
        body,
        Duration::from_secs(2),
        Duration::from_millis(50),
        accounting_guard(),
    );

    match outcome {
        DispatchOutcome::CacheHit {
            cache_anchor_age,
            finalization,
        } => {
            assert_eq!(cache_anchor_age, Duration::from_secs(2));
            assert_eq!(finalization.usage.cache_read_input_tokens, 1234);
            assert_eq!(finalization.status, StatusCode::OK.as_u16());
            assert_eq!(finalization.duration, Duration::from_millis(50));
        }
        DispatchOutcome::CacheMiss { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("expected cache hit"),
    }
}

#[test]
fn classifies_zero_cache_read_as_miss() {
    let body = br#"{"content":[],"stop_reason":"max_tokens","usage":{"cache_read_input_tokens":0,"input_tokens":0,"output_tokens":0}}"#;

    let outcome = classify_success_body(
        body,
        Duration::ZERO,
        Duration::from_millis(25),
        accounting_guard(),
    );

    match outcome {
        DispatchOutcome::CacheMiss { finalization } => {
            assert_eq!(finalization.usage.cache_read_input_tokens, 0);
            assert_eq!(finalization.status, StatusCode::OK.as_u16());
            assert_eq!(finalization.duration, Duration::from_millis(25));
        }
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("expected cache miss"),
    }
}

#[test]
fn classifies_missing_usage_as_miss() {
    let body = br#"{"content":[],"stop_reason":"max_tokens"}"#;

    let outcome = classify_success_body(
        body,
        Duration::ZERO,
        Duration::from_millis(10),
        accounting_guard(),
    );

    match outcome {
        DispatchOutcome::CacheMiss { finalization } => {
            assert_eq!(finalization.usage.cache_read_input_tokens, 0);
            assert_eq!(finalization.status, StatusCode::OK.as_u16());
            assert_eq!(finalization.duration, Duration::from_millis(10));
        }
        DispatchOutcome::CacheHit { .. }
        | DispatchOutcome::UnsupportedProvider(_)
        | DispatchOutcome::Error(_) => panic!("expected cache miss"),
    }
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
fn unsupported_upstream_maps_to_terminal_business_outcome() {
    let error = "unsupported".to_owned();

    let outcome = DispatchOutcome::UnsupportedProvider(error.clone());

    assert!(matches!(
        outcome,
        DispatchOutcome::UnsupportedProvider(reason) if reason == error
    ));
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
