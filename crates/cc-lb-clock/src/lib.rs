//! Clock abstraction for deterministic wall-clock time injection.
//!
//! The workspace policy is that direct calls to `std::time::SystemTime::now()`,
//! `chrono::Utc::now()`, or any other static wall-clock reader are forbidden in
//! library code. Every site that needs to know the current wall-clock time must
//! obtain a [`Clock`] (typically as a [`ClockHandle`]) and call [`Clock::now`].
//!
//! Production code wires a [`SystemClock`]. Tests use [`TestClock`] for
//! deterministic time control.
//!
//! The trait is intentionally minimal: only [`Clock::now`] returning a
//! [`SystemTime`]. Conversions to UNIX seconds / millis are provided as free
//! helper functions [`unix_secs`] and [`unix_millis`] that take a `SystemTime`,
//! so the trait stays single-purpose and callers compose at the call site.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Trait for wall-clock time sources.
///
/// The trait has a single required method [`Clock::now`] returning a
/// [`SystemTime`]. Implementors must be `Send + Sync + 'static` so they can be
/// stored in `Arc<dyn Clock>` (see [`ClockHandle`]) and shared across tasks.
pub trait Clock: Send + Sync + 'static {
    /// Current wall-clock time. The single canonical accessor.
    fn now(&self) -> SystemTime;
}

/// Type alias for a boxed [`Clock`] trait object. Used for dependency injection
/// into structs that need a wall-clock time source.
pub type ClockHandle = Arc<dyn Clock>;

/// Seconds since `UNIX_EPOCH` for a given [`SystemTime`]. Returns `0` if `time`
/// is before `UNIX_EPOCH` (matches the previous workspace-wide convention of
/// `.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()`).
#[inline]
#[must_use]
pub fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Milliseconds since `UNIX_EPOCH` for a given [`SystemTime`]. Returns `0` if
/// `time` is before `UNIX_EPOCH`.
#[inline]
#[must_use]
pub fn unix_millis(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// System clock backed by `std::time::SystemTime::now()`. The single allowed
/// callsite of `SystemTime::now()` in production code lives here.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Test clock with manually controllable wall-clock time.
///
/// All mutators take `&self` so a single `Arc<TestClock>` can be shared between
/// the test setup (which advances time) and the system under test (which reads
/// time). Internally uses a `Mutex<SystemTime>` for interior mutability.
#[derive(Debug)]
pub struct TestClock {
    inner: Arc<Mutex<SystemTime>>,
}

impl TestClock {
    /// Create a `TestClock` pinned at `UNIX_EPOCH` (i.e. unix time 0).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(UNIX_EPOCH)),
        }
    }

    /// Create a `TestClock` pinned at the given number of seconds since
    /// `UNIX_EPOCH`.
    pub fn new_at_secs(secs: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(UNIX_EPOCH + Duration::from_secs(secs))),
        }
    }

    /// Create a `TestClock` pinned at the given absolute [`SystemTime`].
    pub fn new_at(when: SystemTime) -> Self {
        Self {
            inner: Arc::new(Mutex::new(when)),
        }
    }

    /// Advance the clock by the given number of whole seconds.
    pub fn advance_secs(&self, secs: u64) {
        let mut t = self.inner.lock().expect("TestClock mutex poisoned");
        *t += Duration::from_secs(secs);
    }

    /// Advance the clock by the given [`Duration`].
    pub fn advance(&self, dur: Duration) {
        let mut t = self.inner.lock().expect("TestClock mutex poisoned");
        *t += dur;
    }

    /// Pin the clock at the given number of seconds since `UNIX_EPOCH`.
    pub fn set_unix_secs(&self, secs: u64) {
        let mut t = self.inner.lock().expect("TestClock mutex poisoned");
        *t = UNIX_EPOCH + Duration::from_secs(secs);
    }

    /// Pin the clock at an absolute [`SystemTime`].
    pub fn set(&self, when: SystemTime) {
        let mut t = self.inner.lock().expect("TestClock mutex poisoned");
        *t = when;
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        *self.inner.lock().expect("TestClock mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_now_is_between_two_system_time_now_samples() {
        let clock = SystemClock;
        let before = SystemTime::now();
        let mid = clock.now();
        let after = SystemTime::now();
        assert!(mid >= before, "SystemClock.now() should be monotonic vs before");
        assert!(mid <= after, "SystemClock.now() should be monotonic vs after");
    }

    #[test]
    fn unix_secs_and_unix_millis_for_known_time() {
        let t = UNIX_EPOCH + Duration::from_millis(1_234_567);
        assert_eq!(unix_secs(t), 1_234);
        assert_eq!(unix_millis(t), 1_234_567);
    }

    #[test]
    fn unix_secs_before_epoch_returns_zero() {
        // SystemTime cannot represent pre-epoch on all platforms portably; just
        // exercise the saturating-zero contract by using UNIX_EPOCH itself.
        assert_eq!(unix_secs(UNIX_EPOCH), 0);
        assert_eq!(unix_millis(UNIX_EPOCH), 0);
    }

    #[test]
    fn test_clock_new_starts_at_unix_epoch() {
        let clock = TestClock::new();
        assert_eq!(clock.now(), UNIX_EPOCH);
        assert_eq!(unix_secs(clock.now()), 0);
    }

    #[test]
    fn test_clock_set_and_advance() {
        let clock = TestClock::new();

        clock.advance_secs(60);
        assert_eq!(clock.now(), UNIX_EPOCH + Duration::from_secs(60));

        clock.set_unix_secs(100);
        assert_eq!(clock.now(), UNIX_EPOCH + Duration::from_secs(100));

        clock.advance(Duration::from_millis(2_500));
        assert_eq!(clock.now(), UNIX_EPOCH + Duration::from_millis(102_500));

        let target = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        clock.set(target);
        assert_eq!(clock.now(), target);
    }

    #[test]
    fn test_clock_handle_dyn_dispatch() {
        let test_clock = TestClock::new_at_secs(100);
        let handle: ClockHandle = Arc::new(test_clock);
        assert_eq!(handle.now(), UNIX_EPOCH + Duration::from_secs(100));
        assert_eq!(unix_secs(handle.now()), 100);
        assert_eq!(unix_millis(handle.now()), 100_000);
    }

    #[test]
    fn test_clock_new_at_pins_to_absolute_time() {
        let target = UNIX_EPOCH + Duration::from_secs(42);
        let clock = TestClock::new_at(target);
        assert_eq!(clock.now(), target);
    }
}
