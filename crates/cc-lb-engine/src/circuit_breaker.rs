use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_upstream::SignedRequest;
use dashmap::DashMap;
use http::{Response, StatusCode};
use metrics::Unit;
use thiserror::Error;

use crate::attempt_rail::{DispatchBeforeError, DispatchDeadline};
use crate::lifecycle::{Body, DispatchError, UpstreamDispatch};
use cc_lb_clock::{Clock, ClockHandle};

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

struct BreakerInner {
    state: BreakerState,
    generation: u64,
    failures: u32,
    last_failure_at: Option<Duration>,
    opened_at: Option<Duration>,
    half_open_in_flight: u32,
}

impl BreakerInner {
    fn record_closed_failure(&mut self, window: Duration, now: Duration) -> u32 {
        let outside_window = self.failures == 0
            || self
                .last_failure_at
                .is_none_or(|last| now.saturating_sub(last) > window);
        self.last_failure_at = Some(now);
        self.failures = if outside_window {
            1
        } else {
            self.failures.saturating_add(1)
        };
        self.failures
    }
}

pub struct CircuitBreaker {
    pub upstream_name: String,
    pub config: BreakerRuntimeConfig,
    inner: Mutex<BreakerInner>,
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
            inner: Mutex::new(BreakerInner {
                state: BreakerState::Closed,
                generation: 0,
                failures: 0,
                last_failure_at: None,
                opened_at: None,
                half_open_in_flight: 0,
            }),
            clock,
        });
        breaker.emit_state(BreakerState::Closed);
        breaker
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, BreakerInner> {
        self.inner
            .lock()
            .expect("circuit breaker mutex should not be poisoned")
    }

    fn now_duration(&self) -> Duration {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
    }

    pub fn permit(self: &Arc<Self>) -> Result<Permit, BreakerError> {
        let (result, emit) = {
            let mut inner = self.lock_inner();
            match inner.state {
                BreakerState::Closed => (
                    Ok(Permit::new(Arc::clone(self), false, inner.generation)),
                    None,
                ),
                BreakerState::Open => {
                    let now = self.now_duration();
                    let opened_at = inner.opened_at.unwrap_or(Duration::ZERO);
                    let elapsed = now.saturating_sub(opened_at);
                    if elapsed < self.config.half_open_after {
                        (
                            Err(BreakerError::Open {
                                retry_after: self.config.half_open_after.saturating_sub(elapsed),
                            }),
                            None,
                        )
                    } else if self.config.half_open_max_in_flight == 0 {
                        (Err(BreakerError::HalfOpenFull), None)
                    } else {
                        inner.state = BreakerState::HalfOpen;
                        inner.generation = inner.generation.saturating_add(1);
                        inner.half_open_in_flight = 1;
                        let generation = inner.generation;
                        (
                            Ok(Permit::new(Arc::clone(self), true, generation)),
                            Some(BreakerState::HalfOpen),
                        )
                    }
                }
                BreakerState::HalfOpen => {
                    if inner.half_open_in_flight >= self.config.half_open_max_in_flight {
                        (Err(BreakerError::HalfOpenFull), None)
                    } else {
                        inner.half_open_in_flight = inner.half_open_in_flight.saturating_add(1);
                        let generation = inner.generation;
                        (Ok(Permit::new(Arc::clone(self), true, generation)), None)
                    }
                }
            }
        };
        if let Some(state) = emit {
            self.emit_state(state);
        }
        result
    }

    pub fn current_state(&self) -> BreakerState {
        self.lock_inner().state
    }

    pub fn failure_count(&self) -> u32 {
        self.lock_inner().failures
    }

    pub fn half_open_in_flight(&self) -> u32 {
        self.lock_inner().half_open_in_flight
    }

    #[cfg(test)]
    pub(crate) fn increment_failures(&self, now_secs: u64) -> u32 {
        self.lock_inner()
            .record_closed_failure(self.config.failure_window, Duration::from_secs(now_secs))
    }

    #[cfg(test)]
    pub(crate) fn reset_closed_counters(&self) {
        let mut inner = self.lock_inner();
        inner.failures = 0;
        inner.last_failure_at = None;
        inner.opened_at = None;
        inner.half_open_in_flight = 0;
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
    generation: u64,
    completed: bool,
}

impl Permit {
    fn new(breaker: Arc<CircuitBreaker>, half_open: bool, generation: u64) -> Self {
        Self {
            breaker,
            half_open,
            generation,
            completed: false,
        }
    }

    pub fn is_half_open(&self) -> bool {
        self.half_open
    }

    pub fn record_success(mut self) {
        let emit = if self.half_open {
            let mut inner = self.breaker.lock_inner();
            if inner.state == BreakerState::HalfOpen && inner.generation == self.generation {
                inner.state = BreakerState::Closed;
                inner.generation = inner.generation.saturating_add(1);
                inner.failures = 0;
                inner.last_failure_at = None;
                inner.opened_at = None;
                inner.half_open_in_flight = 0;
                Some(BreakerState::Closed)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(state) = emit {
            self.breaker.emit_state(state);
        }
        self.completed = true;
    }

    pub fn record_failure(mut self) {
        let now = self.breaker.now_duration();
        let emit = {
            let mut inner = self.breaker.lock_inner();
            if self.half_open {
                if inner.state == BreakerState::HalfOpen && inner.generation == self.generation {
                    let outside_window = inner.failures == 0
                        || inner.last_failure_at.is_none_or(|last| {
                            now.saturating_sub(last) > self.breaker.config.failure_window
                        });
                    inner.last_failure_at = Some(now);
                    inner.failures = if outside_window {
                        1
                    } else {
                        inner.failures.saturating_add(1)
                    };
                    inner.state = BreakerState::Open;
                    inner.generation = inner.generation.saturating_add(1);
                    inner.opened_at = Some(now);
                    inner.half_open_in_flight = 0;
                    Some(BreakerState::Open)
                } else {
                    None
                }
            } else if inner.state == BreakerState::Closed && inner.generation == self.generation {
                let failures = inner.record_closed_failure(self.breaker.config.failure_window, now);
                if failures >= self.breaker.config.failures_to_open {
                    inner.state = BreakerState::Open;
                    inner.generation = inner.generation.saturating_add(1);
                    inner.opened_at = Some(now);
                    inner.half_open_in_flight = 0;
                    Some(BreakerState::Open)
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(state) = emit {
            self.breaker.emit_state(state);
        }
        self.completed = true;
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.half_open && !self.completed {
            let mut inner = self.breaker.lock_inner();
            if inner.state == BreakerState::HalfOpen && inner.generation == self.generation {
                inner.half_open_in_flight = inner.half_open_in_flight.saturating_sub(1);
            }
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

    pub fn retain_keys(&self, keep: impl Fn(&str) -> bool) {
        self.map.retain(|key, _| keep(key));
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
                if response.status() == StatusCode::TOO_MANY_REQUESTS {
                    drop(permit);
                } else if response.status().is_server_error() {
                    permit.record_failure();
                } else {
                    permit.record_success();
                }
                Ok(response)
            }
            Err(DispatchError::BulkheadFull { retry_after }) => {
                drop(permit);
                Err(DispatchError::BulkheadFull { retry_after })
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
                if response.status() == StatusCode::TOO_MANY_REQUESTS {
                    drop(permit);
                } else if response.status().is_server_error() {
                    permit.record_failure();
                } else {
                    permit.record_success();
                }
                Ok(response)
            }
            // No provider request occurred: dropping a half-open permit releases
            // the probe slot without recording a success or failure.
            Err(DispatchBeforeError::DeadlineElapsed) => Err(DispatchBeforeError::DeadlineElapsed),
            Err(DispatchBeforeError::Dispatch(DispatchError::BulkheadFull { retry_after })) => {
                drop(permit);
                Err(DispatchBeforeError::Dispatch(DispatchError::BulkheadFull {
                    retry_after,
                }))
            }
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
        {
            let mut inner = breaker.lock_inner();
            inner.last_failure_at = Some(Duration::from_secs(100));
            inner.failures = large_threshold - 1;
        }
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
        {
            let mut inner = max_breaker.lock_inner();
            inner.last_failure_at = Some(Duration::from_secs(far_future_ts));
            inner.failures = u32::MAX - 1;
        }
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

    #[test]
    fn wall_clock_rollback_updates_last_failure_ts_and_resets_after_window() {
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let breaker = CircuitBreaker::new(
            "clock-rollback-reset",
            BreakerRuntimeConfig {
                failures_to_open: 3,
                failure_window: Duration::from_secs(10),
                half_open_after: Duration::from_secs(30),
                half_open_max_in_flight: 1,
            },
            Arc::clone(&clock) as ClockHandle,
        );
        breaker.permit().expect("permit 1").record_failure();
        assert_eq!(breaker.failure_count(), 1);
        assert_eq!(breaker.current_state(), BreakerState::Closed);

        clock.set_unix_secs(80);
        breaker.permit().expect("permit 2").record_failure();
        assert_eq!(breaker.failure_count(), 2);
        assert_eq!(breaker.current_state(), BreakerState::Closed);

        // 11s after the post-correction failure at t=80 exceeds the 10s window and resets count.
        clock.set_unix_secs(91);
        breaker.permit().expect("permit 3").record_failure();
        assert_eq!(breaker.failure_count(), 1);
        assert_eq!(breaker.current_state(), BreakerState::Closed);

        let boundary_clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let boundary_breaker = CircuitBreaker::new(
            "clock-rollback-boundary",
            BreakerRuntimeConfig {
                failures_to_open: 3,
                failure_window: Duration::from_secs(10),
                half_open_after: Duration::from_secs(30),
                half_open_max_in_flight: 1,
            },
            Arc::clone(&boundary_clock) as ClockHandle,
        );
        boundary_breaker
            .permit()
            .expect("boundary permit 1")
            .record_failure();
        assert_eq!(boundary_breaker.failure_count(), 1);

        boundary_clock.set_unix_secs(80);
        boundary_breaker
            .permit()
            .expect("boundary permit 2")
            .record_failure();
        assert_eq!(boundary_breaker.failure_count(), 2);

        // Exactly 10s after t=80 stays inside the inclusive window and opens at threshold 3.
        boundary_clock.set_unix_secs(90);
        boundary_breaker
            .permit()
            .expect("boundary permit 3")
            .record_failure();
        assert_eq!(boundary_breaker.failure_count(), 3);
        assert_eq!(boundary_breaker.current_state(), BreakerState::Open);
    }

    #[test]
    fn exact_half_open_cooldown_boundary_transitions_without_extra_second() {
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let breaker = CircuitBreaker::new(
            "exact-boundary",
            BreakerRuntimeConfig {
                failures_to_open: 1,
                failure_window: Duration::from_secs(10),
                half_open_after: Duration::from_secs(5),
                half_open_max_in_flight: 1,
            },
            clock.clone(),
        );
        breaker.permit().unwrap().record_failure();
        assert_eq!(breaker.current_state(), BreakerState::Open);

        clock.advance_secs(4);
        assert!(matches!(
            breaker.permit(),
            Err(BreakerError::Open { retry_after }) if retry_after == Duration::from_secs(1)
        ));

        clock.advance_secs(1);
        let probe = breaker
            .permit()
            .expect("exact cooldown elapsed opens probe");
        assert!(probe.is_half_open());
        assert_eq!(breaker.current_state(), BreakerState::HalfOpen);
        probe.record_success();
    }

    #[test]
    fn stale_half_open_permit_cannot_corrupt_newer_generation() {
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let breaker = CircuitBreaker::new(
            "aba-guard",
            BreakerRuntimeConfig {
                failures_to_open: 1,
                failure_window: Duration::from_secs(10),
                half_open_after: Duration::from_secs(5),
                half_open_max_in_flight: 1,
            },
            clock.clone(),
        );
        breaker.permit().unwrap().record_failure();
        clock.advance_secs(5);
        let gen1_a = breaker.permit().expect("gen 1 probe a");
        drop(gen1_a);
        assert_eq!(breaker.half_open_in_flight(), 0);

        let stale_probe = breaker.permit().expect("gen 1 probe b");
        assert_eq!(breaker.half_open_in_flight(), 1);
        let closed_probe = breaker.permit().err().unwrap();
        assert_eq!(closed_probe, BreakerError::HalfOpenFull);

        // Fail stale_probe to re-open, advance cooldown, then acquire a fresh gen 2 probe.
        stale_probe.record_failure();
        assert_eq!(breaker.current_state(), BreakerState::Open);

        // Now repeat with a dropped uncompleted stale permit across generations.
        clock.advance_secs(5);
        let stale_drop = breaker.permit().expect("gen 2 probe");
        let fresh_same_gen_check = breaker.permit();
        assert_eq!(fresh_same_gen_check.err(), Some(BreakerError::HalfOpenFull));
        // Close via another generation jump by recording failure then recovering cleanly.
        let gen3_holder = {
            // Re-open with a synthetic failure in gen 2 is already prevented because stale_drop holds the slot;
            // instead test stale_drop NOT touching gen 3 if gen 2 recovered via another probe when max_in_flight=2.
            stale_drop
        };
        drop(gen3_holder);

        let breaker2 = CircuitBreaker::new(
            "aba-guard-2",
            BreakerRuntimeConfig {
                failures_to_open: 1,
                failure_window: Duration::from_secs(10),
                half_open_after: Duration::from_secs(5),
                half_open_max_in_flight: 2,
            },
            clock.clone(),
        );
        breaker2.permit().unwrap().record_failure();
        clock.advance_secs(5);
        let p_stale_drop = breaker2.permit().expect("probe 1");
        let p_stale_fail = breaker2.permit().expect("probe 2");
        let p_winner = {
            drop(p_stale_fail);
            breaker2.permit().expect("probe 3")
        };
        p_winner.record_success();
        assert_eq!(breaker2.current_state(), BreakerState::Closed);

        breaker2.permit().unwrap().record_failure();
        clock.advance_secs(5);
        let new_gen_probe = breaker2.permit().expect("new generation probe");
        assert_eq!(breaker2.half_open_in_flight(), 1);

        // Dropping an older generation's permit must not decrement new_gen_probe's slot or change state.
        drop(p_stale_drop);
        assert_eq!(breaker2.current_state(), BreakerState::HalfOpen);
        assert_eq!(breaker2.half_open_in_flight(), 1);
        new_gen_probe.record_success();
        assert_eq!(breaker2.current_state(), BreakerState::Closed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_breaker_transitions_preserve_half_open_invariants() {
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let config = BreakerRuntimeConfig {
            failures_to_open: 2,
            failure_window: Duration::from_secs(60),
            half_open_after: Duration::from_secs(5),
            half_open_max_in_flight: 1,
        };
        let breaker = CircuitBreaker::new("stress", config, clock.clone());
        let mut tasks = tokio::task::JoinSet::new();
        for worker in 0..8 {
            let breaker = Arc::clone(&breaker);
            let clock = Arc::clone(&clock);
            tasks.spawn(async move {
                for step in 0..50 {
                    if step % 7 == 0 {
                        clock.advance_secs(6);
                    }
                    match breaker.permit() {
                        Ok(permit) => {
                            assert!(breaker.half_open_in_flight() <= 1);
                            match (worker + step) % 3 {
                                0 => permit.record_failure(),
                                1 => permit.record_success(),
                                _ => drop(permit),
                            }
                        }
                        Err(BreakerError::Open { .. } | BreakerError::HalfOpenFull) => {}
                    }
                    assert!(breaker.half_open_in_flight() <= 1);
                }
            });
        }
        while let Some(res) = tasks.join_next().await {
            res.expect("stress worker completes without panic");
        }
        assert_eq!(breaker.half_open_in_flight(), 0);
    }

    #[tokio::test]
    async fn dispatch_exempts_429_and_bulkhead_full_from_tripping_and_releases_half_open_on_429() {
        struct FixedReplyDispatch {
            next:
                std::sync::Mutex<std::collections::VecDeque<Result<Response<Body>, DispatchError>>>,
        }

        #[async_trait]
        impl UpstreamDispatch for FixedReplyDispatch {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<Response<Body>, DispatchError> {
                self.next.lock().unwrap().pop_front().unwrap()
            }
        }

        async fn sample_signed() -> SignedRequest {
            struct PassDialect;
            impl cc_lb_upstream::UpstreamDialect for PassDialect {
                fn shape(
                    &self,
                    context: &cc_lb_upstream::DialectShapeContext,
                    _upstream: &cc_lb_domain::Upstream,
                    _principal: &cc_lb_domain::Principal,
                    builder: &mut cc_lb_upstream::ShapedRequestBuilder,
                ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError>
                {
                    Ok(builder.shaped_request(
                        "https://api.example.test/v1/messages".parse().unwrap(),
                        context.method.clone(),
                        context.downstream_headers.clone(),
                        context.body_bytes.clone(),
                    ))
                }
            }
            let upstream = cc_lb_domain::Upstream::AnthropicDirect { base_url: None };
            let context = cc_lb_upstream::DialectShapeContext {
                request_id: "req-1".to_owned(),
                downstream_headers: http::HeaderMap::new(),
                method: http::Method::POST,
                path: "/v1/messages".to_owned(),
                query: None,
                body_bytes: bytes::Bytes::from_static(b"{}"),
            };
            let principal = cc_lb_domain::Principal {
                id: "p1".to_owned(),
                kind: cc_lb_domain::PrincipalKind::ApiKey,
            };
            let shaped =
                cc_lb_upstream::shape_request(&PassDialect, &context, &upstream, &principal)
                    .unwrap();
            struct PassSigner;
            #[async_trait]
            impl cc_lb_upstream::Signer for PassSigner {
                async fn sign(
                    &self,
                    shaped: cc_lb_upstream::ShapedRequest,
                    capability: &mut cc_lb_upstream::SigningCapability,
                ) -> Result<SignedRequest, cc_lb_upstream::SignerError> {
                    Ok(SignedRequest::from_shaped(shaped, capability))
                }
                async fn on_unauthorized(
                    &self,
                    _err: &cc_lb_upstream::UpstreamError,
                ) -> cc_lb_upstream::RetryDecision {
                    cc_lb_upstream::RetryDecision::Fail
                }
            }
            cc_lb_upstream::sign_request(&PassSigner, shaped)
                .await
                .unwrap()
        }

        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let registry = Arc::new(BreakerRegistry::new());
        let inner = Arc::new(FixedReplyDispatch {
            next: std::sync::Mutex::new(std::collections::VecDeque::from([
                Ok(Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .body(Body::from(bytes::Bytes::new()))
                    .unwrap()),
                Err(DispatchError::BulkheadFull {
                    retry_after: Duration::from_secs(1),
                }),
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(bytes::Bytes::new()))
                    .unwrap()),
                Ok(Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .body(Body::from(bytes::Bytes::new()))
                    .unwrap()),
            ])),
        });
        let config = BreakerRuntimeConfig {
            failures_to_open: 1,
            failure_window: Duration::from_secs(10),
            half_open_after: Duration::from_secs(5),
            half_open_max_in_flight: 1,
        };
        let dispatch = CircuitBreakerDispatch::new(
            inner,
            Arc::clone(&registry),
            config,
            Arc::new(|_| "u1".to_owned()),
            clock.clone(),
        );

        let r1 = dispatch.dispatch(sample_signed().await).await.unwrap();
        assert_eq!(r1.status(), StatusCode::TOO_MANY_REQUESTS);
        let r2 = dispatch.dispatch(sample_signed().await).await;
        assert!(matches!(r2, Err(DispatchError::BulkheadFull { .. })));
        let breaker = registry.get("u1").unwrap();
        assert_eq!(breaker.current_state(), BreakerState::Closed);
        assert_eq!(breaker.failure_count(), 0);

        let r3 = dispatch.dispatch(sample_signed().await).await.unwrap();
        assert_eq!(r3.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(breaker.current_state(), BreakerState::Open);

        clock.advance_secs(5);
        let r4 = dispatch.dispatch(sample_signed().await).await.unwrap();
        assert_eq!(r4.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(breaker.current_state(), BreakerState::HalfOpen);
        assert_eq!(breaker.half_open_in_flight(), 0);

        registry.retain_keys(|k| k != "u1");
        assert!(registry.get("u1").is_none());
    }
}
