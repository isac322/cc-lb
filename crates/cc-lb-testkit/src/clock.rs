use std::sync::Arc;

pub use cc_lb_clock::{Clock, ClockHandle, TestClock};

#[must_use]
pub fn fixed_clock(unix_secs: u64) -> ClockHandle {
    Arc::new(TestClock::new_at_secs(unix_secs))
}
