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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OAuthUsagePollScheduleConfig {
    pub success_window_secs: u64,
    pub success_capacity: usize,
    pub throttle_backoff_secs: u64,
}

impl Default for OAuthUsagePollScheduleConfig {
    fn default() -> Self {
        Self {
            success_window_secs: 300,
            success_capacity: 5,
            throttle_backoff_secs: 60,
        }
    }
}

impl<Db: Database> OAuthUsagePollCursorsStore<Db> {
    pub fn compute_next_run_at(
        &self,
        now_unix_secs: u64,
        config: &OAuthUsagePollScheduleConfig,
        cursor: Option<&OAuthUsagePollCursor>,
    ) -> u64 {
        let Some(cursor) = cursor else {
            return now_unix_secs;
        };
        let mut next_run_at = now_unix_secs;
        if let Some(last_throttle_at) = cursor.last_throttle_at_unix_secs {
            next_run_at =
                next_run_at.max(last_throttle_at.saturating_add(config.throttle_backoff_secs));
        }
        if config.success_capacity > 0 {
            let cutoff = now_unix_secs.saturating_sub(config.success_window_secs);
            let mut recent_successes = cursor
                .recent_successes_unix_secs
                .iter()
                .copied()
                .filter(|observed_at| *observed_at >= cutoff)
                .collect::<Vec<_>>();
            recent_successes.sort_unstable();
            if recent_successes.len() >= config.success_capacity {
                let first_counted =
                    recent_successes[recent_successes.len() - config.success_capacity];
                next_run_at =
                    next_run_at.max(first_counted.saturating_add(config.success_window_secs));
            }
        }
        next_run_at
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchedulerFailure {
    pub id: u64,
    pub job_type: String,
    pub payload_summary: String,
    pub last_error: String,
    pub attempts: u32,
    pub first_failed_at_unix_secs: u64,
    pub last_failed_at_unix_secs: u64,
}
