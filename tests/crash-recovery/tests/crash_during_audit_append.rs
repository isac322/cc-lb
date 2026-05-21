#![cfg(unix)]

mod common;

#[test]
fn crash_during_audit_append() -> common::TestResult {
    if common::run_child_if_requested(common::CrashCase::Audit)? {
        return Ok(());
    }

    common::run_parent(common::CrashCase::Audit, "crash_during_audit_append")
}
