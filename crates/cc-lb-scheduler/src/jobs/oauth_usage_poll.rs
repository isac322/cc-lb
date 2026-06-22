use std::future::Future;

use apalis_core::task::{Task, builder::TaskBuilder};
use serde::{Deserialize, Serialize};
use sqlx::Database;
use uuid::Uuid;

use crate::error::Result;
use crate::middleware::TraceparentCarrier;
use crate::retry::JobOutcome;
use crate::state_stores::{
    OAuthUsagePollCursor, OAuthUsagePollCursorsStore, OAuthUsagePollScheduleConfig,
};

mod repository;
pub use repository::{OAuthUsagePollCursorRepository, OAuthUsagePollFuture};

const NETWORK_FAILURE_STATUS: i32 = -1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OAuthUsagePollJob {
    pub upstream_id: Uuid,
    pub traceparent: Option<String>,
}

impl OAuthUsagePollJob {
    pub const fn new(upstream_id: Uuid) -> Self {
        Self {
            upstream_id,
            traceparent: None,
        }
    }

    pub fn idempotency_key(&self, unlock_at_unix_secs: u64) -> String {
        format!(
            "adaptive:oauth_usage_poll:{}:{}",
            self.upstream_id, unlock_at_unix_secs
        )
    }

    pub fn into_apalis_task<Ctx, IdType>(self, run_at_unix_secs: u64) -> Task<Self, Ctx, IdType>
    where
        Ctx: Default,
    {
        let idempotency_key = self.idempotency_key(run_at_unix_secs);
        TaskBuilder::<Self, Ctx, IdType>::new(self)
            .run_at_timestamp(run_at_unix_secs)
            .with_idempotency_key(idempotency_key)
            .build()
    }
}

impl TraceparentCarrier for OAuthUsagePollJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OAuthUsagePollObservation {
    Skip,
    Success {
        observed_at_unix_secs: u64,
        window_start_unix_millis: u64,
        window_end_unix_millis: u64,
    },
    Throttled {
        observed_at_unix_secs: u64,
    },
    StatusFailure {
        observed_at_unix_secs: u64,
        status: u16,
    },
    NetworkFailure {
        observed_at_unix_secs: u64,
    },
}

#[derive(Clone, Debug)]
pub struct OAuthUsagePollHandler<Db: Database> {
    cursors: OAuthUsagePollCursorsStore<Db>,
    config: OAuthUsagePollScheduleConfig,
}

impl<Db: Database> OAuthUsagePollHandler<Db> {
    pub fn new(
        cursors: OAuthUsagePollCursorsStore<Db>,
        config: OAuthUsagePollScheduleConfig,
    ) -> Self {
        Self { cursors, config }
    }

    pub async fn handle<Poll, Polled>(
        &self,
        job: OAuthUsagePollJob,
        now_unix_secs: u64,
        poll: Poll,
    ) -> Result<JobOutcome>
    where
        Self: OAuthUsagePollCursorRepository,
        Poll: FnOnce(OAuthUsagePollJob) -> Polled,
        Polled: Future<Output = Result<OAuthUsagePollObservation>>,
    {
        let cursor = self.read_cursor(job.upstream_id).await?;
        let next_run_at = compute_next_run_at(now_unix_secs, &self.config, cursor.as_ref());
        if next_run_at > now_unix_secs {
            return Ok(JobOutcome::Done);
        }

        let observation = poll(job.clone()).await?;
        self.record_observation(job.upstream_id, cursor, observation)
            .await
    }

    async fn record_observation(
        &self,
        upstream_id: Uuid,
        cursor: Option<OAuthUsagePollCursor>,
        observation: OAuthUsagePollObservation,
    ) -> Result<JobOutcome>
    where
        Self: OAuthUsagePollCursorRepository,
    {
        match observation {
            OAuthUsagePollObservation::Skip => Ok(JobOutcome::Done),
            OAuthUsagePollObservation::Success {
                observed_at_unix_secs,
                window_start_unix_millis,
                window_end_unix_millis,
            } => {
                self.record_success_cursor(
                    upstream_id,
                    observed_at_unix_secs,
                    window_start_unix_millis,
                    window_end_unix_millis,
                    self.config.success_history_cap(),
                )
                .await?;
                Ok(JobOutcome::Done)
            }
            OAuthUsagePollObservation::Throttled {
                observed_at_unix_secs,
            } => {
                let throttle_count = next_throttle_count(cursor.as_ref());
                self.record_throttle_cursor(
                    upstream_id,
                    observed_at_unix_secs,
                    throttle_count,
                    self.config.history_capacity,
                )
                .await?;
                Ok(JobOutcome::Done)
            }
            OAuthUsagePollObservation::StatusFailure {
                observed_at_unix_secs,
                status,
            } => {
                self.record_failure(
                    upstream_id,
                    cursor,
                    observed_at_unix_secs,
                    i32::from(status),
                )
                .await
            }
            OAuthUsagePollObservation::NetworkFailure {
                observed_at_unix_secs,
            } => {
                self.record_failure(
                    upstream_id,
                    cursor,
                    observed_at_unix_secs,
                    NETWORK_FAILURE_STATUS,
                )
                .await
            }
        }
    }

    async fn record_failure(
        &self,
        upstream_id: Uuid,
        cursor: Option<OAuthUsagePollCursor>,
        observed_at_unix_secs: u64,
        status: i32,
    ) -> Result<JobOutcome>
    where
        Self: OAuthUsagePollCursorRepository,
    {
        let mut cursor = cursor.unwrap_or_else(|| OAuthUsagePollCursor::new(upstream_id));
        cursor.last_status = Some(status);
        cursor.attempt_count = cursor.attempt_count.saturating_add(1);
        cursor.last_observed_at_unix_secs = Some(observed_at_unix_secs);
        self.upsert_cursor(&cursor).await?;
        Ok(JobOutcome::Done)
    }
}

pub fn compute_next_run_at(
    now_unix_secs: u64,
    config: &OAuthUsagePollScheduleConfig,
    cursor: Option<&OAuthUsagePollCursor>,
) -> u64 {
    config.compute_next_run_at(now_unix_secs, cursor)
}

fn next_throttle_count(cursor: Option<&OAuthUsagePollCursor>) -> u32 {
    match cursor.and_then(|cursor| cursor.last_status) {
        Some(429) => cursor
            .map(|cursor| cursor.last_throttle_count.saturating_add(1))
            .unwrap_or(1),
        Some(_) | None => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_includes_unlock_at_unix_secs() {
        let upstream_id = Uuid::from_u128(0x12345678123456781234567812345678);
        let job = OAuthUsagePollJob::new(upstream_id);

        assert_eq!(
            job.idempotency_key(1_800_000_060),
            "adaptive:oauth_usage_poll:12345678-1234-5678-1234-567812345678:1800000060"
        );
    }

    #[test]
    fn compute_next_run_at_returns_one_minute_after_success_cursor() {
        let upstream_id = Uuid::nil();
        let observed_at_unix_secs = 1_800_000_000;
        let cursor = OAuthUsagePollCursor {
            upstream_id,
            last_status: Some(200),
            last_observed_at_unix_secs: Some(observed_at_unix_secs),
            recent_successes_unix_secs: vec![observed_at_unix_secs],
            ..OAuthUsagePollCursor::new(upstream_id)
        };

        assert_eq!(
            compute_next_run_at(
                observed_at_unix_secs,
                &OAuthUsagePollScheduleConfig::default(),
                Some(&cursor),
            ),
            observed_at_unix_secs + 60
        );
    }
}
