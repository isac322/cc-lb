#![cfg(target_os = "linux")]

mod preflight_common;

use cc_lb_server::preflight::{self, PreflightOptions};

struct LimitGuard {
    saved: (u64, u64),
}

impl Drop for LimitGuard {
    fn drop(&mut self) {
        let _ = nix::sys::resource::setrlimit(
            nix::sys::resource::Resource::RLIMIT_NOFILE,
            self.saved.0,
            self.saved.1,
        );
    }
}

#[tokio::test]
async fn low_ulimit_preflight_still_succeeds() {
    let saved = nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE).unwrap();
    let guard = LimitGuard { saved };

    let lowered_soft = if saved.1 > 1024 { 1024 } else { saved.0 };
    nix::sys::resource::setrlimit(
        nix::sys::resource::Resource::RLIMIT_NOFILE,
        lowered_soft,
        saved.1,
    )
    .unwrap();

    let _env_guard = preflight_common::EnvGuard::set(
        "CC_LB_TEST_MASTER_KEY_PREFLIGHT_ULIMIT",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    let mut config = preflight_common::base_config();
    preflight_common::use_temp_sqlite(
        &mut config,
        "preflight-ulimit",
        "CC_LB_TEST_MASTER_KEY_PREFLIGHT_ULIMIT",
    );
    let clock: cc_lb_core::ClockHandle = std::sync::Arc::new(cc_lb_core::SystemClock);
    let report = preflight::run(&config, PreflightOptions { skip_bind: true }, clock.clone())
        .await
        .unwrap();

    assert!(
        report.warnings.is_empty(),
        "unexpected warnings: {:?}",
        report.warnings
    );

    drop(guard);
}
