#![cfg(unix)]

mod common;

#[test]
fn crash_during_oauth_aead_write() -> common::TestResult {
    if common::run_child_if_requested(common::CrashCase::OAuthAead)? {
        return Ok(());
    }

    common::run_parent(
        common::CrashCase::OAuthAead,
        "crash_during_oauth_aead_write",
    )
}
