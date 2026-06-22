use cc_lb_config::Config;
use tokio::test;

#[test]
async fn prompt_cache_shadow_disabled_is_no_op() {
    let mut config = Config::default();
    assert!(
        !config.prompt_cache_shadow.enabled,
        "default should be disabled"
    );

    assert_eq!(config.prompt_cache_shadow.grace_margin_secs, 30);
    assert_eq!(config.prompt_cache_shadow.refresh_debounce_secs, 60);
    assert_eq!(config.prompt_cache_shadow.warm_set_cap, 32);

    config.prompt_cache_shadow.enabled = true;
    assert!(config.prompt_cache_shadow.enabled);
}
