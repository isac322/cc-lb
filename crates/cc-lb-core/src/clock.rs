//! Re-export of the workspace-wide `Clock` abstraction from the leaf
//! [`cc_lb_clock`] crate.
//!
//! The trait and its helpers live in their own crate so that low-level crates
//! such as `cc-lb-pricing` (which `cc-lb-core` already depends on) can pull in
//! the same injected wall-clock contract without forming a dependency cycle.
//! Everything is re-exported here so existing `cc_lb_core::clock::...` import
//! paths keep working unchanged.

pub use cc_lb_clock::{Clock, ClockHandle, SystemClock, TestClock, unix_millis, unix_secs};
