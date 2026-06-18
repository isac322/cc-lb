use serde::Serialize;
use sqlx::{Database, Pool};
use uuid::Uuid;

macro_rules! define_store {
    ($name:ident) => {
        #[derive(Debug)]
        pub struct $name<Db: Database> {
            pub(super) pool: Pool<Db>,
        }

        impl<Db: Database> Clone for $name<Db> {
            fn clone(&self) -> Self {
                Self {
                    pool: self.pool.clone(),
                }
            }
        }

        impl<Db: Database> $name<Db> {
            pub fn new(pool: Pool<Db>) -> Self {
                Self { pool }
            }
        }
    };
}

define_store!(WarmupEffectsStore);
define_store!(OAuthRefreshClaimsStore);
define_store!(OAuthUsagePollCursorsStore);
define_store!(AnthropicCompatEtagsStore);
define_store!(PriceCatalogVersionsStore);
define_store!(SchedulerFailuresStore);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthUsagePollCursor {
    pub upstream_id: Uuid,
    pub last_window_start_unix_millis: Option<u64>,
    pub last_window_end_unix_millis: Option<u64>,
    pub last_throttle_at_unix_secs: Option<u64>,
    pub last_throttle_count: u32,
    pub last_status: Option<i32>,
    pub attempt_count: u32,
    pub last_observed_at_unix_secs: Option<u64>,
    pub recent_successes_unix_secs: Vec<u64>,
    pub recent_throttles_unix_secs: Vec<u64>,
}

impl OAuthUsagePollCursor {
    pub fn new(upstream_id: Uuid) -> Self {
        Self {
            upstream_id,
            last_window_start_unix_millis: None,
            last_window_end_unix_millis: None,
            last_throttle_at_unix_secs: None,
            last_throttle_count: 0,
            last_status: None,
            attempt_count: 0,
            last_observed_at_unix_secs: None,
            recent_successes_unix_secs: Vec::new(),
            recent_throttles_unix_secs: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthUsagePollScheduleConfig {
    pub bootstrap_attempts: u32,
    pub bootstrap_default_interval_secs: u64,
    pub min_interval_secs: u64,
    pub max_interval_secs: u64,
    pub fallback_interval_secs: u64,
    pub history_capacity: usize,
    pub throttle_ladder_secs: Vec<u64>,
    pub success_window_secs: u64,
    pub success_capacity: usize,
    pub success_safety_secs: u64,
}

impl Default for OAuthUsagePollScheduleConfig {
    fn default() -> Self {
        Self {
            bootstrap_attempts: 0,
            bootstrap_default_interval_secs: 60,
            min_interval_secs: 60,
            max_interval_secs: 3_600,
            fallback_interval_secs: 60,
            history_capacity: 8,
            throttle_ladder_secs: vec![300, 300, 300, 300, 300],
            success_window_secs: 300,
            success_capacity: 5,
            success_safety_secs: 5,
        }
    }
}

impl OAuthUsagePollScheduleConfig {
    pub fn compute_next_run_at(
        &self,
        now_unix_secs: u64,
        cursor: Option<&OAuthUsagePollCursor>,
    ) -> u64 {
        let Some(cursor) = cursor else {
            return now_unix_secs;
        };
        match cursor.last_status {
            Some(200) => self.success_next_run_at(now_unix_secs, cursor),
            Some(429) => cursor
                .last_throttle_at_unix_secs
                .unwrap_or(now_unix_secs)
                .saturating_add(self.throttle_interval(cursor.last_throttle_count)),
            Some(_) | None => cursor
                .last_observed_at_unix_secs
                .unwrap_or(now_unix_secs)
                .saturating_add(self.failure_interval(cursor.attempt_count)),
        }
    }

    pub fn throttle_interval(&self, consecutive_throttles: u32) -> u64 {
        if self.throttle_ladder_secs.is_empty() {
            return self.clamp_interval(self.fallback_interval_secs);
        }
        let index = consecutive_throttles
            .saturating_sub(1)
            .min((self.throttle_ladder_secs.len() - 1) as u32) as usize;
        self.clamp_interval(self.throttle_ladder_secs[index])
    }

    pub fn success_history_cap(&self) -> usize {
        self.success_capacity
    }

    fn success_next_run_at(&self, now_unix_secs: u64, cursor: &OAuthUsagePollCursor) -> u64 {
        let observed_at = cursor.last_observed_at_unix_secs.unwrap_or(now_unix_secs);
        let base = if self.success_capacity > 0 {
            self.min_interval_secs.max(
                self.sliding_window_unlock_at(now_unix_secs, cursor)
                    .saturating_sub(observed_at),
            )
        } else if cursor.attempt_count < self.bootstrap_attempts {
            self.bootstrap_backoff(cursor.attempt_count)
        } else {
            self.fallback_interval_secs
        };
        observed_at.saturating_add(self.clamp_interval(base))
    }

    fn sliding_window_unlock_at(&self, now_unix_secs: u64, cursor: &OAuthUsagePollCursor) -> u64 {
        let cutoff = now_unix_secs.saturating_sub(
            self.success_window_secs
                .saturating_add(self.success_safety_secs),
        );
        let mut recent_successes = cursor
            .recent_successes_unix_secs
            .iter()
            .copied()
            .filter(|observed_at| *observed_at >= cutoff)
            .collect::<Vec<_>>();
        recent_successes.sort_unstable();
        if recent_successes.len() < self.success_capacity {
            return now_unix_secs;
        }
        recent_successes[recent_successes.len() - self.success_capacity]
            .saturating_add(self.success_window_secs)
            .saturating_add(self.success_safety_secs)
    }

    fn failure_interval(&self, attempt_count: u32) -> u64 {
        if attempt_count < self.bootstrap_attempts {
            self.bootstrap_backoff(attempt_count)
        } else {
            self.clamp_interval(self.fallback_interval_secs)
        }
    }

    fn bootstrap_backoff(&self, attempt_count: u32) -> u64 {
        let multiplier = 1_u64 << attempt_count.saturating_sub(1).min(6);
        self.clamp_interval(
            self.bootstrap_default_interval_secs
                .saturating_mul(multiplier),
        )
    }

    fn clamp_interval(&self, interval_secs: u64) -> u64 {
        interval_secs.clamp(self.min_interval_secs, self.max_interval_secs)
    }
}

impl<Db: Database> OAuthUsagePollCursorsStore<Db> {
    pub fn compute_next_run_at(
        &self,
        now_unix_secs: u64,
        config: &OAuthUsagePollScheduleConfig,
        cursor: Option<&OAuthUsagePollCursor>,
    ) -> u64 {
        config.compute_next_run_at(now_unix_secs, cursor)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnthropicCompatEtag {
    pub key: String,
    pub etag: Option<String>,
    pub last_applied_at_unix_secs: u64,
    pub last_value_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PriceCatalogVersion {
    pub source: String,
    pub fingerprint: String,
    pub fetched_at_unix_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SchedulerFailure {
    pub id: u64,
    pub job_type: String,
    pub payload_summary: String,
    pub last_error: String,
    pub attempts: u32,
    pub first_failed_at_unix_secs: u64,
    pub last_failed_at_unix_secs: u64,
}
