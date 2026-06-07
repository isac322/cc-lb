//! Clock abstraction for deterministic TTL math in cache layers.
//! SystemClock for prod, TestClock for tests. ClockHandle = Arc<dyn Clock>.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Trait for time sources used in TTL and quota calculations.
/// Implementors must be Send, Sync, and 'static for use in Arc<dyn Clock>.
pub trait Clock: Send + Sync + 'static {
    /// Current time as seconds since UNIX_EPOCH.
    fn now_unix_secs(&self) -> u64;

    /// Current time as milliseconds since UNIX_EPOCH.
    fn now_unix_millis(&self) -> u128 {
        u128::from(self.now_unix_secs()) * 1000
    }
}

/// Type alias for a boxed Clock trait object. Used for dependency injection.
pub type ClockHandle = Arc<dyn Clock>;

/// System clock backed by std::time::SystemTime. For production use.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

/// Test clock with manual time control. For unit tests and deterministic scenarios.
#[derive(Debug)]
pub struct TestClock {
    now_unix_secs: Arc<AtomicU64>,
}

impl TestClock {
    /// Create a TestClock starting at time 0.
    pub fn new() -> Self {
        Self {
            now_unix_secs: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a TestClock starting at the given number of seconds since UNIX_EPOCH.
    pub fn new_at_secs(secs: u64) -> Self {
        Self {
            now_unix_secs: Arc::new(AtomicU64::new(secs)),
        }
    }

    /// Advance the clock by the given number of seconds.
    pub fn advance_secs(&self, secs: u64) {
        self.now_unix_secs.fetch_add(secs, Ordering::SeqCst);
    }

    /// Set the clock to an absolute time in seconds since UNIX_EPOCH.
    pub fn set_unix_secs(&self, secs: u64) {
        self.now_unix_secs.store(secs, Ordering::SeqCst);
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TestClock {
    fn now_unix_secs(&self) -> u64 {
        self.now_unix_secs.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_now_is_close_to_systemtime() {
        let clock = SystemClock;
        let system_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let clock_secs = clock.now_unix_secs();

        // Clock should be within 1 second of SystemTime.
        assert!(
            (clock_secs as i64 - system_secs as i64).abs() <= 1,
            "SystemClock.now_unix_secs() = {}, SystemTime = {}",
            clock_secs,
            system_secs
        );
    }

    #[test]
    fn test_clock_set_and_advance() {
        let clock = TestClock::new();
        assert_eq!(clock.now_unix_secs(), 0, "TestClock should start at 0");

        clock.advance_secs(60);
        assert_eq!(clock.now_unix_secs(), 60, "After advance_secs(60), should be at 60");

        clock.set_unix_secs(100);
        assert_eq!(clock.now_unix_secs(), 100, "After set_unix_secs(100), should be at 100");

        clock.advance_secs(50);
        assert_eq!(clock.now_unix_secs(), 150, "After advance_secs(50), should be at 150");
    }

    #[test]
    fn test_clock_handle_dyn_dispatch() {
        let test_clock = TestClock::new_at_secs(100);
        let handle: ClockHandle = Arc::new(test_clock);

        assert_eq!(handle.now_unix_secs(), 100, "ClockHandle should dispatch to TestClock");
        assert_eq!(
            handle.now_unix_millis(),
            100_000,
            "now_unix_millis should convert secs to millis"
        );
    }
}
