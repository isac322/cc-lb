use cc_lb_core::clock::{Clock, unix_millis};

pub(super) fn now_unix_millis(clock: &dyn Clock) -> u64 {
    unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64
}
