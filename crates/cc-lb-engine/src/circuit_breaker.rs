use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use cc_lb_upstream::SignedRequest;
use dashmap::DashMap;
use http::Response;
use metrics::Unit;
use thiserror::Error;

use crate::attempt_rail::{DispatchBeforeError, DispatchDeadline};
use crate::lifecycle::{Body, DispatchError, UpstreamDispatch};
use cc_lb_clock::{Clock, ClockHandle, unix_secs};

const HALF_OPEN_INITIALIZING: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct FailureWindowState {
    last_failure_ts: u64,
    count: u32,
}

static REGISTER_CIRCUIT_BREAKER_METRICS: Once = Once::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl BreakerState {
    pub fn gauge_value(self) -> f64 {
        match self {
            Self::Closed => 0.0,
            Self::HalfOpen => 1.0,
            Self::Open => 2.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BreakerRuntimeConfig {
    pub failures_to_open: u32,
    pub failure_window: Duration,
    pub half_open_after: Duration,
    pub half_open_max_in_flight: u32,
}

impl Default for BreakerRuntimeConfig {
    fn default() -> Self {
        Self {
            failures_to_open: 5,
            failure_window: Duration::from_secs(10),
            half_open_after: Duration::from_secs(30),
            half_open_max_in_flight: 1,
        }
    }
}

impl From<cc_lb_config::CircuitBreakerConfig> for BreakerRuntimeConfig {
    fn from(config: cc_lb_config::CircuitBreakerConfig) -> Self {
        Self {
            failures_to_open: config.failures_to_open,
            failure_window: Duration::from_secs(config.window_secs.max(1)),
            half_open_after: Duration::from_secs(config.half_open_after_secs.max(1)),
            half_open_max_in_flight: 1,
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BreakerError {
    #[error("circuit breaker is open; retry after {retry_after:?}")]
    Open { retry_after: Duration },
    #[error("circuit breaker half-open probe limit reached")]
    HalfOpenFull,
}

pub struct CircuitBreaker {
    pub upstream_name: String,
    pub config: BreakerRuntimeConfig,
    pub state: ArcSwap<BreakerState>,
    failure_state: Mutex<FailureWindowState>,
    pub last_open_ts: AtomicU64,
    pub half_open_in_flight: AtomicU32,
    clock: Arc<dyn Clock>,
}

impl CircuitBreaker {
    pub fn new(
        upstream_name: impl Into<String>,
        config: BreakerRuntimeConfig,
        clock: ClockHandle,
    ) -> Arc<Self> {
        register_circuit_breaker_metrics();
        let breaker = Arc::new(Self {
            upstream_name: upstream_name.into(),
            config,
            state: ArcSwap::from_pointee(BreakerState::Closed),
            failure_state: Mutex::new(FailureWindowState::default()),
            last_open_ts: AtomicU64::new(0),
            half_open_in_flight: AtomicU32::new(0),
            clock,
        });
        breaker.emit_state(BreakerState::Closed);
        breaker
    }

    pub fn permit(self: &Arc<Self>) -> Result<Permit, BreakerError> {
        match self.current_state() {
            BreakerState::Closed => Ok(Permit::new(Arc::clone(self), false)),
            BreakerState::Open => self.permit_open(),
            BreakerState::HalfOpen => self.permit_half_open(),
        }
    }

    pub fn current_state(&self) -> BreakerState {
        **self.state.load()
    }

    pub fn failure_count(&self) -> u32 {
        self.lock_failure_state().count
    }

    fn lock_failure_state(&self) -> MutexGuard<'_, FailureWindowState> {
        self.failure_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn half_open_in_flight(&self) -> u32 {
        match self.half_open_in_flight.load(Ordering::SeqCst) {
            HALF_OPEN_INITIALIZING => 0,
            count => count,
        }
    }

    fn permit_open(self: &Arc<Self>) -> Result<Permit, BreakerError> {
        let now = unix_secs(self.clock.now());
        let opened_at = self.last_open_ts.load(Ordering::SeqCst);
        let elapsed = Duration::from_secs(now.saturating_sub(opened_at));
        if elapsed <= self.config.half_open_after {
            return Err(BreakerError::Open {
                retry_after: self.config.half_open_after.saturating_sub(elapsed),
            });
        }

        if self
            .half_open_in_flight
            .compare_exchange(
                0,
                HALF_OPEN_INITIALIZING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err(BreakerError::HalfOpenFull);
        }

        if self.transition(BreakerState::Open, BreakerState::HalfOpen) {
            self.half_open_in_flight.store(1, Ordering::SeqCst);
            self.emit_state(BreakerState::HalfOpen);
            Ok(Permit::new(Arc::clone(self), true))
        } else {
            self.half_open_in_flight.store(0, Ordering::SeqCst);
            match self.current_state() {
                BreakerState::Closed => Ok(Permit::new(Arc::clone(self), false)),
                BreakerState::Open => Err(BreakerError::Open {
                    retry_after: Duration::ZERO,
                }),
                BreakerState::HalfOpen => self.permit_half_open(),
            }
        }
    }

    fn permit_half_open(self: &Arc<Self>) -> Result<Permit, BreakerError> {
        let max_in_flight = self.config.half_open_max_in_flight;
        let mut current = self.half_open_in_flight.load(Ordering::SeqCst);
        loop {
            if current == HALF_OPEN_INITIALIZING || current >= max_in_flight {
                return Err(BreakerError::HalfOpenFull);
            }
            match self.half_open_in_flight.compare_exchange(
                current,
                current.saturating_add(1),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(Permit::new(Arc::clone(self), true)),
                Err(next) => current = next,
            }
        }
    }

    fn transition(&self, expected: BreakerState, next: BreakerState) -> bool {
        let current = self.state.load_full();
        if *current != expected {
            return false;
        }
        let previous = self.state.compare_and_swap(&current, Arc::new(next));
        Arc::ptr_eq(&*previous, &current)
    }

    fn open_from(&self, expected: BreakerState, now: u64) {
        self.last_open_ts.store(now, Ordering::SeqCst);
        if self.transition(expected, BreakerState::Open) {
            self.half_open_in_flight.store(0, Ordering::SeqCst);
            self.emit_state(BreakerState::Open);
        }
    }

    fn increment_failures(&self, now: u64) -> u32 {
        let window_secs = self.config.failure_window.as_secs();
        let mut state = self.lock_failure_state();
        let elapsed = now.saturating_sub(state.last_failure_ts);
        let outside_window = state.count == 0 || elapsed > window_secs;
        if outside_window {
            state.last_failure_ts = now;
            state.count = 1;
        } else {
            state.last_failure_ts = state.last_failure_ts.max(now);
            state.count = state.count.saturating_add(1);
        }
        state.count
    }

    fn reset_closed_counters(&self) {
        *self.lock_failure_state() = FailureWindowState::default();
        self.last_open_ts.store(0, Ordering::SeqCst);
        self.half_open_in_flight.store(0, Ordering::SeqCst);
    }

    fn release_half_open_slot(&self) {
        let _ =
            self.half_open_in_flight
                .try_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |current| match current {
                        0 | HALF_OPEN_INITIALIZING => None,
                        count => Some(count - 1),
                    },
                );
    }

    fn emit_state(&self, state: BreakerState) {
        metrics::gauge!(
            "cc_lb_circuit_breaker_state",
            "upstream" => self.upstream_name.clone()
        )
        .set(state.gauge_value());
    }
}

#[must_use = "a circuit breaker permit must record success or failure, or be dropped to release its half-open slot"]
pub struct Permit {
    breaker: Arc<CircuitBreaker>,
    half_open: bool,
    completed: bool,
}

impl Permit {
    fn new(breaker: Arc<CircuitBreaker>, half_open: bool) -> Self {
        Self {
            breaker,
            half_open,
            completed: false,
        }
    }

    pub fn is_half_open(&self) -> bool {
        self.half_open
    }

    pub fn record_success(mut self) {
        if self.half_open {
            if self
                .breaker
                .transition(BreakerState::HalfOpen, BreakerState::Closed)
            {
                self.breaker.reset_closed_counters();
                self.breaker.emit_state(BreakerState::Closed);
            } else {
                self.breaker.release_half_open_slot();
            }
        }
        self.completed = true;
    }

    pub fn record_failure(mut self) {
        let now = unix_secs(self.breaker.clock.now());
        let failure_count = self.breaker.increment_failures(now);
        if self.half_open {
            self.breaker.open_from(BreakerState::HalfOpen, now);
            self.breaker.half_open_in_flight.store(0, Ordering::SeqCst);
        } else if self.breaker.current_state() == BreakerState::Closed
            && failure_count >= self.breaker.config.failures_to_open
        {
            self.breaker.open_from(BreakerState::Closed, now);
        }
        self.completed = true;
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.half_open && !self.completed {
            self.breaker.release_half_open_slot();
        }
    }
}

#[derive(Default)]
pub struct BreakerRegistry {
    pub map: DashMap<String, Arc<CircuitBreaker>>,
}

impl BreakerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn breaker(
        &self,
        upstream_name: impl Into<String>,
        config: BreakerRuntimeConfig,
        clock: ClockHandle,
    ) -> Arc<CircuitBreaker> {
        let upstream_name = upstream_name.into();
        self.map
            .entry(upstream_name.clone())
            .or_insert_with(|| CircuitBreaker::new(upstream_name, config, clock))
            .clone()
    }

    pub fn get(&self, upstream_name: &str) -> Option<Arc<CircuitBreaker>> {
        self.map
            .get(upstream_name)
            .map(|breaker| Arc::clone(breaker.value()))
    }

    pub fn evict(&self, upstream_name: &str) -> bool {
        self.map.remove(upstream_name).is_some()
    }

    pub async fn drain(&self, upstream_name: &str, grace: Duration) -> bool {
        let removed = self.evict(upstream_name);
        tokio::time::sleep(grace).await;
        removed
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.map.iter().map(|entry| entry.key().clone()).collect();
        names.sort();
        names
    }
}

pub struct CircuitBreakerDispatch {
    inner: Arc<dyn UpstreamDispatch>,
    registry: Arc<BreakerRegistry>,
    config: BreakerRuntimeConfig,
    upstream_name: Arc<dyn Fn(&SignedRequest) -> String + Send + Sync>,
    clock: ClockHandle,
}

impl CircuitBreakerDispatch {
    pub fn new(
        inner: Arc<dyn UpstreamDispatch>,
        registry: Arc<BreakerRegistry>,
        config: BreakerRuntimeConfig,
        upstream_name: Arc<dyn Fn(&SignedRequest) -> String + Send + Sync>,
        clock: ClockHandle,
    ) -> Self {
        Self {
            inner,
            registry,
            config,
            upstream_name,
            clock,
        }
    }
}

#[async_trait]
impl UpstreamDispatch for CircuitBreakerDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let upstream_name = (self.upstream_name)(&request);
        let breaker = self
            .registry
            .breaker(upstream_name, self.config, self.clock.clone());
        let permit = breaker.permit().map_err(dispatch_error_from_breaker)?;
        match self.inner.dispatch(request).await {
            Ok(response) => {
                if response.status().is_server_error() {
                    permit.record_failure();
                } else {
                    permit.record_success();
                }
                Ok(response)
            }
            Err(source) => {
                permit.record_failure();
                Err(source)
            }
        }
    }

    async fn dispatch_before(
        &self,
        request: SignedRequest,
        deadline: &DispatchDeadline,
    ) -> Result<Response<Body>, DispatchBeforeError> {
        deadline.check()?;
        let upstream_name = (self.upstream_name)(&request);
        let breaker = self
            .registry
            .breaker(upstream_name, self.config, self.clock.clone());
        let permit = breaker
            .permit()
            .map_err(dispatch_error_from_breaker)
            .map_err(DispatchBeforeError::Dispatch)?;
        match self.inner.dispatch_before(request, deadline).await {
            Ok(response) => {
                if response.status().is_server_error() {
                    permit.record_failure();
                } else {
                    permit.record_success();
                }
                Ok(response)
            }
            // No provider request occurred: dropping a half-open permit releases
            // the probe slot without recording a success or failure.
            Err(DispatchBeforeError::DeadlineElapsed) => Err(DispatchBeforeError::DeadlineElapsed),
            Err(DispatchBeforeError::Dispatch(source)) => {
                permit.record_failure();
                Err(DispatchBeforeError::Dispatch(source))
            }
        }
    }
}

fn dispatch_error_from_breaker(error: BreakerError) -> DispatchError {
    DispatchError::Transport {
        source: Box::new(error),
    }
}

fn register_circuit_breaker_metrics() {
    REGISTER_CIRCUIT_BREAKER_METRICS.call_once(|| {
        metrics::describe_gauge!(
            "cc_lb_circuit_breaker_state",
            Unit::Count,
            "Circuit breaker state by upstream: 0 closed, 1 half-open, 2 open."
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use cc_lb_clock::SystemClock;

    #[test]
    fn evict_removes_entry() {
        let registry = BreakerRegistry::new();
        let clock = Arc::new(SystemClock);
        let _breaker = registry.breaker("test-upstream", BreakerRuntimeConfig::default(), clock);
        assert!(registry.get("test-upstream").is_some());

        let removed = registry.evict("test-upstream");
        assert!(removed);
        assert!(registry.get("test-upstream").is_none());
    }

    #[test]
    fn evict_missing_returns_false() {
        let registry = BreakerRegistry::new();
        let removed = registry.evict("nonexistent-upstream");
        assert!(!removed);
    }

    #[tokio::test]
    async fn drain_after_grace_period() {
        let registry = BreakerRegistry::new();
        let clock = Arc::new(SystemClock);
        let _breaker = registry.breaker("test-upstream", BreakerRuntimeConfig::default(), clock);
        assert!(registry.get("test-upstream").is_some());

        let start = std::time::Instant::now();
        let grace = Duration::from_millis(50);
        let removed = registry.drain("test-upstream", grace).await;

        let elapsed = start.elapsed();
        assert!(removed);
        assert!(registry.get("test-upstream").is_none());
        assert!(elapsed >= grace);
    }

    #[tokio::test]
    async fn concurrent_evict_and_breaker_creation_safe() {
        let registry = Arc::new(BreakerRegistry::new());
        let clock = Arc::new(SystemClock);
        let mut handles = vec![];

        for i in 0..5 {
            let reg_clone = Arc::clone(&registry);
            let clock = Arc::clone(&clock);
            let handle = tokio::spawn(async move {
                let upstream_name = format!("upstream-{}", i);
                let _breaker =
                    reg_clone.breaker(&upstream_name, BreakerRuntimeConfig::default(), clock);
                tokio::time::sleep(Duration::from_millis(1)).await;
                reg_clone.evict(&upstream_name)
            });
            handles.push(handle);
        }

        for i in 0..5 {
            let reg_clone = Arc::clone(&registry);
            let clock = Arc::clone(&clock);
            let handle = tokio::spawn(async move {
                let upstream_name = format!("upstream-{}", i);
                tokio::time::sleep(Duration::from_millis(2)).await;
                let _breaker =
                    reg_clone.breaker(&upstream_name, BreakerRuntimeConfig::default(), clock);
                true
            });
            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await;
            assert!(result.is_ok());
        }

        let names = registry.names();
        assert_eq!(names.len(), 5);
        assert!(names.iter().all(|n| n.starts_with("upstream-")));
    }

    #[test]
    fn concurrent_increment_failures_from_zero_never_loses_increments() {
        use std::sync::Barrier;

        const THREADS: usize = 8;
        const ROUNDS: usize = 50;

        for _ in 0..ROUNDS {
            let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
            let breaker = CircuitBreaker::new(
                "concurrent-zero",
                BreakerRuntimeConfig {
                    failures_to_open: 100,
                    failure_window: Duration::from_secs(10),
                    half_open_after: Duration::from_secs(30),
                    half_open_max_in_flight: 1,
                },
                clock,
            );
            let barrier = Arc::new(Barrier::new(THREADS));
            let mut results: Vec<u32> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..THREADS)
                    .map(|_| {
                        let breaker = Arc::clone(&breaker);
                        let barrier = Arc::clone(&barrier);
                        scope.spawn(move || {
                            barrier.wait();
                            breaker.increment_failures(100)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().expect("worker thread should succeed"))
                    .collect()
            });
            results.sort_unstable();
            assert_eq!(results, (1..=THREADS as u32).collect::<Vec<_>>());
            assert_eq!(breaker.failure_count(), THREADS as u32);
        }
    }

    #[test]
    fn concurrent_increment_failures_after_window_expiry_never_loses_increments() {
        use std::sync::Barrier;

        const THREADS: usize = 8;
        const ROUNDS: usize = 50;

        for _ in 0..ROUNDS {
            let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
            let breaker = CircuitBreaker::new(
                "concurrent-expired",
                BreakerRuntimeConfig {
                    failures_to_open: 100,
                    failure_window: Duration::from_secs(10),
                    half_open_after: Duration::from_secs(30),
                    half_open_max_in_flight: 1,
                },
                clock,
            );
            assert_eq!(breaker.increment_failures(100), 1);

            let barrier = Arc::new(Barrier::new(THREADS));
            let mut results: Vec<u32> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..THREADS)
                    .map(|_| {
                        let breaker = Arc::clone(&breaker);
                        let barrier = Arc::clone(&barrier);
                        scope.spawn(move || {
                            barrier.wait();
                            breaker.increment_failures(111)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().expect("worker thread should succeed"))
                    .collect()
            });
            results.sort_unstable();
            assert_eq!(results, (1..=THREADS as u32).collect::<Vec<_>>());
            assert_eq!(breaker.failure_count(), THREADS as u32);
        }
    }

    #[test]
    fn concurrent_failures_reliably_open_threshold_two_breaker() {
        use std::sync::Barrier;

        const ROUNDS: usize = 50;

        for after_window_expiry in [false, true] {
            for _ in 0..ROUNDS {
                let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
                let breaker = CircuitBreaker::new(
                    "threshold-two",
                    BreakerRuntimeConfig {
                        failures_to_open: 2,
                        failure_window: Duration::from_secs(10),
                        half_open_after: Duration::from_secs(30),
                        half_open_max_in_flight: 1,
                    },
                    Arc::clone(&clock) as ClockHandle,
                );
                if after_window_expiry {
                    breaker.permit().expect("closed permit").record_failure();
                    assert_eq!(breaker.current_state(), BreakerState::Closed);
                    assert_eq!(breaker.failure_count(), 1);
                    clock.advance_secs(11);
                }

                let p1 = breaker.permit().expect("first closed permit");
                let p2 = breaker.permit().expect("second closed permit");
                let barrier = Arc::new(Barrier::new(2));

                std::thread::scope(|scope| {
                    let b1 = Arc::clone(&barrier);
                    let h1 = scope.spawn(move || {
                        b1.wait();
                        p1.record_failure();
                    });
                    let b2 = Arc::clone(&barrier);
                    let h2 = scope.spawn(move || {
                        b2.wait();
                        p2.record_failure();
                    });
                    h1.join().expect("thread 1 should succeed");
                    h2.join().expect("thread 2 should succeed");
                });

                assert_eq!(
                    breaker.current_state(),
                    BreakerState::Open,
                    "after_window_expiry={after_window_expiry}"
                );
                assert_eq!(
                    breaker.failure_count(),
                    2,
                    "after_window_expiry={after_window_expiry}"
                );
            }
        }
    }

    #[test]
    fn high_u32_failure_thresholds_and_u64_timestamps_are_preserved() {
        let large_threshold = 16_777_216_u32;
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let breaker = CircuitBreaker::new(
            "large-threshold",
            BreakerRuntimeConfig {
                failures_to_open: large_threshold,
                failure_window: Duration::from_secs(10),
                half_open_after: Duration::from_secs(30),
                half_open_max_in_flight: 1,
            },
            Arc::clone(&clock) as ClockHandle,
        );
        *breaker.lock_failure_state() = FailureWindowState {
            last_failure_ts: 100,
            count: large_threshold - 1,
        };
        breaker.permit().expect("closed permit").record_failure();
        assert_eq!(breaker.failure_count(), large_threshold);
        assert_eq!(breaker.current_state(), BreakerState::Open);

        let far_future_ts = 1_u64 << 42;
        let max_clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(far_future_ts));
        let max_breaker = CircuitBreaker::new(
            "max-u32-threshold",
            BreakerRuntimeConfig {
                failures_to_open: u32::MAX,
                failure_window: Duration::from_secs(10),
                half_open_after: Duration::from_secs(30),
                half_open_max_in_flight: 1,
            },
            max_clock,
        );
        *max_breaker.lock_failure_state() = FailureWindowState {
            last_failure_ts: far_future_ts,
            count: u32::MAX - 1,
        };
        max_breaker
            .permit()
            .expect("closed permit")
            .record_failure();
        assert_eq!(max_breaker.failure_count(), u32::MAX);
        assert_eq!(max_breaker.current_state(), BreakerState::Open);
        assert_eq!(max_breaker.increment_failures(far_future_ts), u32::MAX);
        assert_eq!(max_breaker.increment_failures(u64::MAX - 5), 1);
        assert_eq!(max_breaker.increment_failures(u64::MAX), 2);
    }

    #[test]
    fn failure_window_boundary_and_reset_closed_counters() {
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let breaker = CircuitBreaker::new(
            "window-boundary",
            BreakerRuntimeConfig {
                failures_to_open: 5,
                failure_window: Duration::from_secs(10),
                half_open_after: Duration::from_secs(30),
                half_open_max_in_flight: 1,
            },
            clock,
        );

        assert_eq!(breaker.increment_failures(100), 1);
        assert_eq!(breaker.increment_failures(110), 2);
        assert_eq!(breaker.increment_failures(121), 1);

        breaker.reset_closed_counters();
        assert_eq!(breaker.failure_count(), 0);
        assert_eq!(breaker.increment_failures(121), 1);
    }
}
