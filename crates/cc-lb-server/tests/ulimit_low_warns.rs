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
async fn ulimit_low_warns() {
    let saved = nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE).unwrap();
    let guard = LimitGuard { saved };

    let lowered_soft = if saved.1 > 1024 { 1024 } else { saved.0 };
    nix::sys::resource::setrlimit(
        nix::sys::resource::Resource::RLIMIT_NOFILE,
        lowered_soft,
        saved.1,
    )
    .unwrap();

    let config = preflight_common::base_config();
    let report = preflight::run(&config, PreflightOptions { skip_bind: true })
        .await
        .unwrap();

    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("ulimit")),
        "missing ulimit warning: {:?}",
        report.warnings
    );

    drop(guard);
}
