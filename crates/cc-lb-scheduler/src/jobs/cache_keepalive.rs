use std::future::Future;
use std::pin::Pin;

use cc_lb_storage_api::{
    CacheKeepaliveGenerationCheck, CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore,
    CacheTtl, StorageError, cache_keepalive_job_key,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Result, SchedulerError};
use crate::middleware::TraceparentCarrier;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CacheKeepaliveJob {
    pub session_key_hash: String,
    pub generation: u64,
    pub principal_id: String,
    pub upstream_id: Uuid,
    pub ttl: CacheTtl,
    pub cache_anchor_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
    pub refresh_delay_secs: u64,
    pub max_refreshes: u32,
    pub max_total_duration_secs: u64,
    pub traceparent: Option<String>,
}

impl CacheKeepaliveJob {
    pub fn idempotency_key(&self) -> String {
        cache_keepalive_job_key(&self.session_key_hash, self.generation)
    }
}

impl TraceparentCarrier for CacheKeepaliveJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheKeepaliveJobOutcome {
    Ready,
    Missing,
    Stale,
    AlreadyTerminal,
}

pub trait CacheKeepaliveSessionFence {
    fn load_generation<'a>(
        &'a self,
        session_key_hash: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<CacheKeepaliveGenerationCheck>>> + Send + 'a>>;
}

impl<T> CacheKeepaliveSessionFence for T
where
    T: CacheKeepaliveSessionStore + ?Sized,
{
    fn load_generation<'a>(
        &'a self,
        session_key_hash: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<CacheKeepaliveGenerationCheck>>> + Send + 'a>>
    {
        Box::pin(async move {
            CacheKeepaliveSessionStore::check_cache_keepalive_generation(self, session_key_hash)
                .await
                .map_err(storage_error)
        })
    }
}

#[derive(Clone, Debug)]
pub struct CacheKeepaliveJobHandler<'a, Store: ?Sized> {
    store: &'a Store,
}

impl<'a, Store: ?Sized> CacheKeepaliveJobHandler<'a, Store> {
    pub const fn new(store: &'a Store) -> Self {
        Self { store }
    }
}

impl<Store> CacheKeepaliveJobHandler<'_, Store>
where
    Store: CacheKeepaliveSessionFence + ?Sized,
{
    pub async fn handle(&self, job: CacheKeepaliveJob) -> Result<CacheKeepaliveJobOutcome> {
        let Some(check) = self.store.load_generation(&job.session_key_hash).await? else {
            return Ok(CacheKeepaliveJobOutcome::Missing);
        };
        if check.generation != job.generation {
            return Ok(CacheKeepaliveJobOutcome::Stale);
        }
        if check.status == CacheKeepaliveSessionStatus::Terminal {
            return Ok(CacheKeepaliveJobOutcome::AlreadyTerminal);
        }
        Ok(CacheKeepaliveJobOutcome::Ready)
    }
}

fn storage_error(error: StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> CacheKeepaliveJob {
        CacheKeepaliveJob {
            session_key_hash: "session-hash".to_owned(),
            generation: 7,
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::from_u128(42),
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 1_800_000_000,
            expires_at_unix_secs: 1_800_000_300,
            refresh_delay_secs: 270,
            max_refreshes: 12,
            max_total_duration_secs: 14_400,
            traceparent: None,
        }
    }

    fn active_generation(generation: u64) -> CacheKeepaliveGenerationCheck {
        CacheKeepaliveGenerationCheck {
            generation,
            status: CacheKeepaliveSessionStatus::Active,
            enqueue_state: cc_lb_storage_api::CacheKeepaliveEnqueueState::Enqueued,
        }
    }

    fn terminal_generation(generation: u64) -> CacheKeepaliveGenerationCheck {
        CacheKeepaliveGenerationCheck {
            generation,
            status: CacheKeepaliveSessionStatus::Terminal,
            enqueue_state: cc_lb_storage_api::CacheKeepaliveEnqueueState::Enqueued,
        }
    }

    #[test]
    fn idempotency_key_is_generation_scoped() {
        assert_eq!(job().idempotency_key(), "cache_keepalive:session-hash:7");
    }

    #[test]
    fn serialized_payload_contains_only_lightweight_references() {
        let json = serde_json::to_string(&job()).expect("serialize cache keepalive job");

        assert!(json.contains("session-hash"));
        assert!(json.contains("principal"));
        assert!(!json.contains("prompt"));
        assert!(!json.contains("authorization"));
        assert!(!json.contains("x-api-key"));
    }

    #[tokio::test]
    async fn handler_reports_missing_session_as_noop_business_outcome() -> Result<()> {
        let store = FakeFence { check: None };

        let outcome = CacheKeepaliveJobHandler::new(&store).handle(job()).await?;

        assert_eq!(outcome, CacheKeepaliveJobOutcome::Missing);
        Ok(())
    }

    #[tokio::test]
    async fn handler_reports_generation_mismatch_as_stale_noop() -> Result<()> {
        let store = FakeFence {
            check: Some(active_generation(8)),
        };

        let outcome = CacheKeepaliveJobHandler::new(&store).handle(job()).await?;

        assert_eq!(outcome, CacheKeepaliveJobOutcome::Stale);
        Ok(())
    }

    #[tokio::test]
    async fn handler_reports_terminal_session_as_already_terminal_noop() -> Result<()> {
        let store = FakeFence {
            check: Some(terminal_generation(7)),
        };

        let outcome = CacheKeepaliveJobHandler::new(&store).handle(job()).await?;

        assert_eq!(outcome, CacheKeepaliveJobOutcome::AlreadyTerminal);
        Ok(())
    }

    #[tokio::test]
    async fn handler_accepts_matching_active_generation_for_dispatch() -> Result<()> {
        let store = FakeFence {
            check: Some(active_generation(7)),
        };

        let outcome = CacheKeepaliveJobHandler::new(&store).handle(job()).await?;

        assert_eq!(outcome, CacheKeepaliveJobOutcome::Ready);
        Ok(())
    }

    struct FakeFence {
        check: Option<CacheKeepaliveGenerationCheck>,
    }

    impl CacheKeepaliveSessionFence for FakeFence {
        fn load_generation<'a>(
            &'a self,
            _session_key_hash: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<CacheKeepaliveGenerationCheck>>> + Send + 'a>>
        {
            Box::pin(async move { Ok(self.check) })
        }
    }
}
