//! Re-export of the workspace-wide `Clock` abstraction from the leaf
//! [`cc_lb_clock`] crate.
//!
//! The trait and its helpers live in their own crate so that low-level crates can
//! pull in the same injected wall-clock contract without forming dependency cycles.
//! Everything is re-exported here so existing `cc_lb_engine::clock::...` import
//! paths keep working unchanged.

pub use cc_lb_clock::{Clock, ClockHandle, SystemClock, TestClock, unix_millis, unix_secs};
