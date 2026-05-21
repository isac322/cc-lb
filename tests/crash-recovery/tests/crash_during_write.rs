#![cfg(unix)]

mod common;

#[test]
fn crash_during_quota_write() -> common::TestResult {
    if common::run_child_if_requested(common::CrashCase::Quota)? {
        return Ok(());
    }

    common::run_parent(common::CrashCase::Quota, "crash_during_quota_write")
}
