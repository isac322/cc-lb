#![cfg(unix)]

mod common;

#[test]
fn crash_during_price_catalog_swap() -> common::TestResult {
    if common::run_child_if_requested(common::CrashCase::PriceCatalog)? {
        return Ok(());
    }

    common::run_parent(
        common::CrashCase::PriceCatalog,
        "crash_during_price_catalog_swap",
    )
}
