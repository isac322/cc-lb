use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use cc_lb_plugin_api::SignedRequest;
use dashmap::DashMap;
use http::Response;
use metrics::Unit;
use thiserror::Error;

use crate::clock::{Clock, SystemClock};
use crate::lifecycle::{Body, DispatchError, UpstreamDispatch};

const HALF_OPEN_INITIALIZING: u32 = u32::MAX;

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
pub struct BreakerConfig {
    pub failures_to_open: u32,
    pub failure_window: Duration,
    pub half_open_after: Duration,
    pub half_open_max_in_flight: u32,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failures_to_open: 5,
            failure_window: Duration::from_secs(10),
            half_open_after: Duration::from_secs(30),
            half_open_max_in_flight: 1,
        }
    }
}

pub type CircuitBreakerConfig = BreakerConfig;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BreakerError {
    #[error("circuit breaker is open; retry after {retry_after:?}")]
    Open { retry_after: Duration },
    #[error("circuit breaker half-open probe limit reached")]
    HalfOpenFull,
}

pub struct CircuitBreaker {
    pub upstream_name: String,
    pub config: BreakerConfig,
    pub state: ArcSwap<BreakerState>,
    pub failures: AtomicU32,
    pub last_failure_ts: AtomicU64,
    pub last_open_ts: AtomicU64,
    pub half_open_in_flight: AtomicU32,
    clock: Arc<dyn Clock>,
}

impl CircuitBreaker {
    pub fn new(upstream_name: impl Into<String>, config: BreakerConfig) -> Arc<Self> {
        Self::with_clock(upstream_name, config, Arc::new(SystemClock))
    }

    pub fn with_clock(
        upstream_name: impl Into<String>,
        config: BreakerConfig,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        register_circuit_breaker_metrics();
        let breaker = Arc::new(Self {
            upstream_name: upstream_name.into(),
            config,
            state: ArcSwap::from_pointee(BreakerState::Closed),
            failures: AtomicU32::new(0),
            last_failure_ts: AtomicU64::new(0),
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
        self.failures.load(Ordering::SeqCst)
    }

    pub fn half_open_in_flight(&self) -> u32 {
        match self.half_open_in_flight.load(Ordering::SeqCst) {
            HALF_OPEN_INITIALIZING => 0,
            count => count,
        }
    }

    fn permit_open(self: &Arc<Self>) -> Result<Permit, BreakerError> {
        let now = self.clock.now_unix_secs();
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
        let last_failure = self.last_failure_ts.load(Ordering::SeqCst);
        let elapsed = now.saturating_sub(last_failure);
        let outside_window =
            self.failure_count() == 0 || elapsed > self.config.failure_window.as_secs();
        self.last_failure_ts.store(now, Ordering::SeqCst);
        if outside_window {
            self.failures.store(1, Ordering::SeqCst);
            1
        } else {
            self.failures
                .fetch_add(1, Ordering::SeqCst)
                .saturating_add(1)
        }
    }

    fn reset_closed_counters(&self) {
        self.failures.store(0, Ordering::SeqCst);
        self.last_failure_ts.store(0, Ordering::SeqCst);
        self.last_open_ts.store(0, Ordering::SeqCst);
        self.half_open_in_flight.store(0, Ordering::SeqCst);
    }

    fn release_half_open_slot(&self) {
        let _ =
            self.half_open_in_flight
                .fetch_update(
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
        let now = self.breaker.clock.now_unix_secs();
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
        config: BreakerConfig,
    ) -> Arc<CircuitBreaker> {
        self.breaker_with_clock(upstream_name, config, Arc::new(SystemClock))
    }

    pub fn breaker_with_clock(
        &self,
        upstream_name: impl Into<String>,
        config: BreakerConfig,
        clock: Arc<dyn Clock>,
    ) -> Arc<CircuitBreaker> {
        let upstream_name = upstream_name.into();
        self.map
            .entry(upstream_name.clone())
            .or_insert_with(|| CircuitBreaker::with_clock(upstream_name, config, clock))
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
    config: BreakerConfig,
    upstream_name: Arc<dyn Fn(&SignedRequest) -> String + Send + Sync>,
}

impl CircuitBreakerDispatch {
    pub fn new(
        inner: Arc<dyn UpstreamDispatch>,
        registry: Arc<BreakerRegistry>,
        config: BreakerConfig,
        upstream_name: Arc<dyn Fn(&SignedRequest) -> String + Send + Sync>,
    ) -> Self {
        Self {
            inner,
            registry,
            config,
            upstream_name,
        }
    }
}

#[async_trait]
impl UpstreamDispatch for CircuitBreakerDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let upstream_name = (self.upstream_name)(&request);
        let breaker = self.registry.breaker(upstream_name, self.config);
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
}

fn dispatch_error_from_breaker(error: BreakerError) -> DispatchError {
    DispatchError::Transport {
        reason: error.to_string(),
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

    #[test]
    fn evict_removes_entry() {
        let registry = BreakerRegistry::new();
        let _breaker = registry.breaker("test-upstream", BreakerConfig::default());
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
        let _breaker = registry.breaker("test-upstream", BreakerConfig::default());
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
        let mut handles = vec![];

        for i in 0..5 {
            let reg_clone = Arc::clone(&registry);
            let handle = tokio::spawn(async move {
                let upstream_name = format!("upstream-{}", i);
                let _breaker = reg_clone.breaker(&upstream_name, BreakerConfig::default());
                tokio::time::sleep(Duration::from_millis(1)).await;
                reg_clone.evict(&upstream_name)
            });
            handles.push(handle);
        }

        for i in 0..5 {
            let reg_clone = Arc::clone(&registry);
            let handle = tokio::spawn(async move {
                let upstream_name = format!("upstream-{}", i);
                tokio::time::sleep(Duration::from_millis(2)).await;
                let _breaker = reg_clone.breaker(&upstream_name, BreakerConfig::default());
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
}
