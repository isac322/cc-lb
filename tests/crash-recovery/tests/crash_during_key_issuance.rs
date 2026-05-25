#![cfg(unix)]

mod common;

#[test]
fn crash_during_key_issuance() -> common::TestResult {
    if common::run_child_if_requested(common::CrashCase::KeyIssuance)? {
        return Ok(());
    }

    common::run_parent(common::CrashCase::KeyIssuance, "crash_during_key_issuance")
}
