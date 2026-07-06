use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::UpstreamRecord;
use cc_lb_storage_api::upstream::UpstreamKind;

use super::should_poll_oauth_usage;

#[test]
fn usage_poll_includes_disabled_registered_oauth_upstream() {
    let mut upstream = registered_oauth_upstream();
    upstream.enabled = false;
    upstream.warmup_enabled = false;

    assert!(should_poll_oauth_usage(&upstream));
}

#[test]
fn usage_poll_excludes_deleted_or_missing_credentials() {
    let mut deleted = registered_oauth_upstream();
    deleted.deleted_at_unix_secs = Some(1_800_000_000);
    let mut missing_credentials = registered_oauth_upstream();
    missing_credentials.oauth_credentials = None;

    assert!(!should_poll_oauth_usage(&deleted));
    assert!(!should_poll_oauth_usage(&missing_credentials));
}

#[test]
fn usage_poll_excludes_non_oauth_upstream() {
    let mut upstream = registered_oauth_upstream();
    upstream.kind = UpstreamKind::AnthropicApiKey;

    assert!(!should_poll_oauth_usage(&upstream));
}

fn registered_oauth_upstream() -> UpstreamRecord {
    UpstreamRecord {
        id: uuid::Uuid::new_v4(),
        name: "oauth-upstream".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        base_url: None,
        enabled: true,
        oauth_credentials: Some(EncryptedOAuthTokens::from_ciphertext(vec![1])),
        api_key_ciphertext: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 1,
        created_at_unix_secs: 1,
        updated_at_unix_secs: 1,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}
