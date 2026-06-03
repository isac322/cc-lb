use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct EstimatorConfig {
    pub bootstrap_attempts: u32,
    pub bootstrap_default_interval_secs: u64,
    pub safety_factor: f64,
    pub min_interval_secs: u64,
    pub max_interval_secs: u64,
    pub fallback_interval_secs: u64,
    pub history_capacity: usize,
    pub throttle_ladder_secs: Vec<u64>,
}

impl Default for EstimatorConfig {
    fn default() -> Self {
        Self {
            bootstrap_attempts: 3,
            bootstrap_default_interval_secs: 60,
            safety_factor: 2.0,
            min_interval_secs: 5,
            max_interval_secs: 3_600,
            fallback_interval_secs: 300,
            history_capacity: 32,
            throttle_ladder_secs: vec![60, 300, 900, 1_800, 3_600],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThrottleObservation {
    pub observed_at: SystemTime,
    pub window_secs: u64,
    pub capacity: u64,
}

#[derive(Debug)]
pub struct PollScheduleEstimator {
    config: EstimatorConfig,
    state: Mutex<HashMap<Uuid, EstimatorState>>,
}

#[derive(Clone, Debug)]
struct EstimatorState {
    attempt_count: u32,
    last_throttle_at: Option<SystemTime>,
    last_success_at: Option<SystemTime>,
    first_success_after_throttle_at: Option<SystemTime>,
    successes_since_throttle: u32,
    consecutive_throttles: u32,
    observations: VecDeque<ThrottleObservation>,
    next_poll_at: SystemTime,
}

impl EstimatorState {
    fn new(now: SystemTime) -> Self {
        Self {
            attempt_count: 0,
            last_throttle_at: None,
            last_success_at: None,
            first_success_after_throttle_at: None,
            successes_since_throttle: 0,
            consecutive_throttles: 0,
            observations: VecDeque::new(),
            next_poll_at: now,
        }
    }
}

impl PollScheduleEstimator {
    pub fn new(config: EstimatorConfig) -> Self {
        Self {
            config,
            state: Mutex::new(HashMap::new()),
        }
    }

    pub fn next_poll_at(&self, upstream_id: Uuid) -> Option<SystemTime> {
        self.state
            .lock()
            .expect("poll estimator state lock poisoned")
            .get(&upstream_id)
            .map(|state| state.next_poll_at)
    }

    pub fn is_due(&self, upstream_id: Uuid, now: SystemTime) -> bool {
        self.next_poll_at(upstream_id)
            .is_none_or(|next| next <= now)
    }

    pub fn observation_count(&self, upstream_id: Uuid) -> usize {
        self.state
            .lock()
            .expect("poll estimator state lock poisoned")
            .get(&upstream_id)
            .map(|state| state.observations.len())
            .unwrap_or(0)
    }

    pub fn attempt_count(&self, upstream_id: Uuid) -> u32 {
        self.state
            .lock()
            .expect("poll estimator state lock poisoned")
            .get(&upstream_id)
            .map(|state| state.attempt_count)
            .unwrap_or(0)
    }

    pub fn record_success(&self, upstream_id: Uuid, now: SystemTime) {
        let mut state = self
            .state
            .lock()
            .expect("poll estimator state lock poisoned");
        let state = state
            .entry(upstream_id)
            .or_insert_with(|| EstimatorState::new(now));
        state.attempt_count = state.attempt_count.saturating_add(1);
        if state.last_throttle_at.is_some() && state.successes_since_throttle == 0 {
            state.first_success_after_throttle_at = Some(now);
        }
        state.last_success_at = Some(now);
        state.successes_since_throttle = state.successes_since_throttle.saturating_add(1);
        state.consecutive_throttles = 0;
        let interval = self.compute_normal_interval(state);
        state.next_poll_at = now + Duration::from_secs(interval);
    }

    pub fn record_throttle(&self, upstream_id: Uuid, observation: ThrottleObservation) {
        let mut state = self
            .state
            .lock()
            .expect("poll estimator state lock poisoned");
        let state = state
            .entry(upstream_id)
            .or_insert_with(|| EstimatorState::new(observation.observed_at));
        state.attempt_count = state.attempt_count.saturating_add(1);
        state.last_throttle_at = Some(observation.observed_at);
        state.first_success_after_throttle_at = None;
        state.successes_since_throttle = 0;
        state.consecutive_throttles = state.consecutive_throttles.saturating_add(1);
        if self.config.history_capacity > 0 {
            while state.observations.len() >= self.config.history_capacity {
                state.observations.pop_front();
            }
            state.observations.push_back(observation.clone());
        }
        let interval = self.throttle_interval(state.consecutive_throttles);
        state.next_poll_at = observation.observed_at + Duration::from_secs(interval);
    }

    pub fn record_network_failure(&self, upstream_id: Uuid, now: SystemTime) {
        let mut state = self
            .state
            .lock()
            .expect("poll estimator state lock poisoned");
        let state = state
            .entry(upstream_id)
            .or_insert_with(|| EstimatorState::new(now));
        state.attempt_count = state.attempt_count.saturating_add(1);
        let interval = if state.attempt_count < self.config.bootstrap_attempts {
            self.bootstrap_backoff(state.attempt_count)
        } else {
            self.clamp_interval(self.config.fallback_interval_secs)
        };
        state.next_poll_at = now + Duration::from_secs(interval);
    }

    fn compute_normal_interval(&self, state: &EstimatorState) -> u64 {
        if state.attempt_count < self.config.bootstrap_attempts {
            return self.bootstrap_backoff(state.attempt_count);
        }

        let mut intervals = state
            .observations
            .iter()
            .filter(|observation| observation.capacity > 0)
            .map(|observation| observation.window_secs as f64 / observation.capacity as f64)
            .collect::<Vec<_>>();
        if intervals.is_empty() || self.config.safety_factor <= 0.0 {
            return self.clamp_interval(self.config.fallback_interval_secs);
        }

        intervals.sort_by(|left, right| left.total_cmp(right));
        let median = intervals[intervals.len() / 2];
        self.clamp_interval((median / self.config.safety_factor).ceil() as u64)
    }

    fn bootstrap_backoff(&self, attempt_count: u32) -> u64 {
        let shift = attempt_count.saturating_sub(1).min(6);
        let multiplier = 1_u64 << shift;
        self.clamp_interval(
            self.config
                .bootstrap_default_interval_secs
                .saturating_mul(multiplier),
        )
    }

    fn throttle_interval(&self, consecutive_throttles: u32) -> u64 {
        if self.config.throttle_ladder_secs.is_empty() {
            return self.clamp_interval(self.config.fallback_interval_secs);
        }
        let index = consecutive_throttles
            .saturating_sub(1)
            .min((self.config.throttle_ladder_secs.len() - 1) as u32) as usize;
        self.clamp_interval(self.config.throttle_ladder_secs[index])
    }

    fn clamp_interval(&self, interval_secs: u64) -> u64 {
        interval_secs.clamp(self.config.min_interval_secs, self.config.max_interval_secs)
    }
}

impl Default for PollScheduleEstimator {
    fn default() -> Self {
        Self::new(EstimatorConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> EstimatorConfig {
        EstimatorConfig {
            bootstrap_attempts: 4,
            bootstrap_default_interval_secs: 10,
            safety_factor: 2.0,
            min_interval_secs: 1,
            max_interval_secs: 1_000,
            fallback_interval_secs: 100,
            history_capacity: 8,
            throttle_ladder_secs: vec![5, 20, 60],
        }
    }

    fn at(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn next_secs(estimator: &PollScheduleEstimator, upstream_id: Uuid) -> u64 {
        estimator
            .next_poll_at(upstream_id)
            .expect("next poll exists")
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("after epoch")
            .as_secs()
    }

    #[test]
    fn bootstrap_exponential_backoff_progression() {
        let estimator = PollScheduleEstimator::new(config());
        let upstream_id = Uuid::new_v4();

        estimator.record_success(upstream_id, at(100));
        assert_eq!(next_secs(&estimator, upstream_id), 110);
        estimator.record_success(upstream_id, at(200));
        assert_eq!(next_secs(&estimator, upstream_id), 220);
        estimator.record_success(upstream_id, at(300));
        assert_eq!(next_secs(&estimator, upstream_id), 340);
    }

    #[test]
    fn bootstrap_clamps_to_max_interval_secs() {
        let estimator = PollScheduleEstimator::new(EstimatorConfig {
            max_interval_secs: 15,
            ..config()
        });
        let upstream_id = Uuid::new_v4();

        estimator.record_success(upstream_id, at(100));
        estimator.record_success(upstream_id, at(200));

        assert_eq!(next_secs(&estimator, upstream_id), 215);
    }

    #[test]
    fn network_failure_during_bootstrap_uses_exponential() {
        let estimator = PollScheduleEstimator::new(config());
        let upstream_id = Uuid::new_v4();

        estimator.record_network_failure(upstream_id, at(100));
        assert_eq!(next_secs(&estimator, upstream_id), 110);
        estimator.record_network_failure(upstream_id, at(200));
        assert_eq!(next_secs(&estimator, upstream_id), 220);
    }

    #[test]
    fn qps_inferred_steady_state_uses_median_and_safety_factor() {
        let estimator = PollScheduleEstimator::new(config());
        let upstream_id = Uuid::new_v4();

        estimator.record_throttle(upstream_id, throttle(at(100), 100, 10));
        estimator.record_throttle(upstream_id, throttle(at(200), 200, 10));
        estimator.record_throttle(upstream_id, throttle(at(300), 300, 10));
        estimator.record_success(upstream_id, at(400));

        assert_eq!(next_secs(&estimator, upstream_id), 410);
    }

    #[test]
    fn throttle_ladder_progresses_then_sticks_at_tail() {
        let estimator = PollScheduleEstimator::new(config());
        let upstream_id = Uuid::new_v4();

        estimator.record_throttle(upstream_id, throttle(at(100), 60, 10));
        assert_eq!(next_secs(&estimator, upstream_id), 105);
        estimator.record_throttle(upstream_id, throttle(at(200), 60, 10));
        assert_eq!(next_secs(&estimator, upstream_id), 220);
        estimator.record_throttle(upstream_id, throttle(at(300), 60, 10));
        assert_eq!(next_secs(&estimator, upstream_id), 360);
        estimator.record_throttle(upstream_id, throttle(at(400), 60, 10));
        assert_eq!(next_secs(&estimator, upstream_id), 460);
    }

    #[test]
    fn is_due_gates_by_next_poll_at() {
        let estimator = PollScheduleEstimator::new(config());
        let upstream_id = Uuid::new_v4();

        assert!(estimator.is_due(upstream_id, at(100)));
        estimator.record_success(upstream_id, at(100));

        assert!(!estimator.is_due(upstream_id, at(109)));
        assert!(estimator.is_due(upstream_id, at(110)));
    }

    #[test]
    fn record_success_resets_throttle_counter() {
        let estimator = PollScheduleEstimator::new(config());
        let upstream_id = Uuid::new_v4();

        estimator.record_throttle(upstream_id, throttle(at(100), 60, 10));
        estimator.record_throttle(upstream_id, throttle(at(200), 60, 10));
        estimator.record_success(upstream_id, at(300));
        estimator.record_throttle(upstream_id, throttle(at(400), 60, 10));

        assert_eq!(next_secs(&estimator, upstream_id), 405);
    }

    #[test]
    fn observation_count_reports_capped_history() {
        let estimator = PollScheduleEstimator::new(EstimatorConfig {
            history_capacity: 2,
            ..config()
        });
        let upstream_id = Uuid::new_v4();

        estimator.record_throttle(upstream_id, throttle(at(100), 60, 10));
        estimator.record_throttle(upstream_id, throttle(at(200), 60, 10));
        estimator.record_throttle(upstream_id, throttle(at(300), 60, 10));

        assert_eq!(estimator.observation_count(upstream_id), 2);
    }

    #[test]
    fn attempt_count_reports_recorded_attempts() {
        let estimator = PollScheduleEstimator::new(config());
        let upstream_id = Uuid::new_v4();

        estimator.record_success(upstream_id, at(100));
        estimator.record_network_failure(upstream_id, at(200));
        estimator.record_throttle(upstream_id, throttle(at(300), 60, 10));

        assert_eq!(estimator.attempt_count(upstream_id), 3);
    }

    #[test]
    fn steady_state_falls_back_without_valid_observations() {
        let estimator = PollScheduleEstimator::new(config());
        let upstream_id = Uuid::new_v4();

        estimator.record_success(upstream_id, at(100));
        estimator.record_success(upstream_id, at(200));
        estimator.record_success(upstream_id, at(300));
        estimator.record_success(upstream_id, at(400));

        assert_eq!(next_secs(&estimator, upstream_id), 500);
    }

    fn throttle(observed_at: SystemTime, window_secs: u64, capacity: u64) -> ThrottleObservation {
        ThrottleObservation {
            observed_at,
            window_secs,
            capacity,
        }
    }
}
