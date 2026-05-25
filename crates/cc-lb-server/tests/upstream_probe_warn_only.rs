mod preflight_common;

use cc_lb_config::UpstreamKind;
use cc_lb_server::preflight::{self, PreflightOptions};

#[tokio::test]
async fn upstream_probe_warn_only() {
    let _guard = preflight_common::EnvGuard::set(
        "CC_LB_TEST_MASTER_KEY_PREFLIGHT_UPSTREAM",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    let mut config = preflight_common::base_config();
    preflight_common::use_temp_redb(
        &mut config,
        "preflight-upstream",
        "CC_LB_TEST_MASTER_KEY_PREFLIGHT_UPSTREAM",
    );
    let (direct_name, direct) =
        preflight_common::upstream("direct", UpstreamKind::AnthropicDirect, None);
    config.upstreams.insert(direct_name, direct);
    let (custom_name, custom) = preflight_common::upstream(
        "custom",
        UpstreamKind::Custom,
        Some("http://127.0.0.1:9080"),
    );
    config.upstreams.insert(custom_name, custom);

    let report = preflight::run(&config, PreflightOptions { skip_bind: true })
        .await
        .unwrap();

    for name in ["direct", "custom"] {
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning
                    == &format!("upstream {name}: probe skipped (offline preflight)")),
            "missing warning for {name}: {:?}",
            report.warnings
        );
    }
}
