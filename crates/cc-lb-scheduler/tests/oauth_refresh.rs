use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_scheduler::error::{Result, SchedulerError};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{
    OAuthRefreshConfig, OAuthRefreshJob, OAuthRefreshJobHandler,
};
use cc_lb_scheduler::retry::JobOutcome;
use uuid::Uuid;

#[path = "oauth_refresh/support.rs"]
mod support;
use support::{
    FakeClaims, FakeUpstreams, capture_metadata, count_refresh_calls, encrypted_tokens,
    refreshable_record,
};

mod jobs {
    pub mod oauth_refresh {
        use super::super::*;

        #[tokio::test]
        async fn success_bumps_generation_and_enqueues_metadata() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let claims = FakeClaims::new(true);
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 5);
            let handler =
                OAuthRefreshJobHandler::new(claims.clone(), upstreams.clone(), Uuid::new_v4());
            let enqueued = Arc::new(Mutex::new(None));

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    |_| async { Ok(encrypted_tokens(2)) },
                    capture_metadata(Arc::clone(&enqueued)),
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Done);
            assert_eq!(upstreams.complete_calls(), 1);
            assert_eq!(claims.completed_generation(), Some(5));
            assert_eq!(
                enqueued.lock().expect("enqueued lock").clone(),
                Some(MetadataRefreshJob::new(upstream_id, 5))
            );
            Ok(())
        }

        #[tokio::test]
        async fn contended_claim_joins_when_generation_advances() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 4)
                .with_read_generations([Some(5)]);
            let config = OAuthRefreshConfig {
                contention_wait: Duration::from_millis(1),
                contention_poll_interval: Duration::from_millis(1),
                ..OAuthRefreshConfig::default()
            };
            let handler = OAuthRefreshJobHandler::with_config(
                FakeClaims::new(false),
                upstreams.clone(),
                Uuid::new_v4(),
                config,
            );
            let refresh_calls = Arc::new(Mutex::new(0usize));

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    count_refresh_calls(Arc::clone(&refresh_calls)),
                    |_| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Done);
            assert_eq!(*refresh_calls.lock().expect("refresh lock"), 0);
            assert_eq!(upstreams.complete_calls(), 0);
            Ok(())
        }

        #[tokio::test]
        async fn refresh_failure_releases_claim_and_retries() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let claims = FakeClaims::new(true);
            let config = OAuthRefreshConfig {
                retry_delay: Duration::from_secs(7),
                ..OAuthRefreshConfig::default()
            };
            let handler = OAuthRefreshJobHandler::with_config(
                claims.clone(),
                FakeUpstreams::new(refreshable_record(upstream_id, 4), 5),
                Uuid::new_v4(),
                config,
            );

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    |_| async { Err(SchedulerError::Job("token endpoint 401".to_owned())) },
                    |_| async { Ok(()) },
                )
                .await?;

            assert_eq!(
                outcome,
                JobOutcome::Retry {
                    delay: Duration::from_secs(7)
                }
            );
            assert_eq!(claims.release_count(), 1);
            Ok(())
        }
    }
}
