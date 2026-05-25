#![cfg(unix)]

mod common;

#[test]
fn crash_during_revoke() -> common::TestResult {
    if common::run_child_if_requested(common::CrashCase::KeyRevoke)? {
        return Ok(());
    }

    common::run_parent(common::CrashCase::KeyRevoke, "crash_during_revoke")
}
