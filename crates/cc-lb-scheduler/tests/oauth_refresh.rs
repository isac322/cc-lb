use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_scheduler::error::{Result, SchedulerError};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{
    OAuthRefreshConfig, OAuthRefreshJob, OAuthRefreshJobHandler, RefreshedOAuthTokens,
};
use cc_lb_scheduler::retry::JobOutcome;
use uuid::Uuid;

#[path = "oauth_refresh/support.rs"]
mod support;
use support::{FakeUpstreams, capture_metadata, encrypted_tokens, refreshable_record};

mod jobs {
    pub mod oauth_refresh {
        use super::super::*;

        #[tokio::test]
        async fn success_bumps_generation_and_enqueues_metadata() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 5);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone(), Uuid::new_v4());
            let enqueued = Arc::new(Mutex::new(None));
            let scheduled = Arc::new(Mutex::new(None));
            let scheduled_clone = Arc::clone(&scheduled);

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    |_| async {
                        Ok(RefreshedOAuthTokens {
                            encrypted_tokens: encrypted_tokens(2),
                            expires_at_unix_secs: 2_000,
                        })
                    },
                    capture_metadata(Arc::clone(&enqueued)),
                    move |id, expires_at| {
                        *scheduled_clone.lock().expect("scheduled lock") = Some((id, expires_at));
                        async { Ok(()) }
                    },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Done);
            assert_eq!(upstreams.complete_calls(), 1);
            assert_eq!(
                enqueued.lock().expect("enqueued lock").clone(),
                Some(MetadataRefreshJob::new(upstream_id, 5))
            );
            assert_eq!(
                *scheduled.lock().expect("scheduled lock"),
                Some((upstream_id, 2_000))
            );
            Ok(())
        }

        #[tokio::test]
        async fn refresh_failure_retries() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let config = OAuthRefreshConfig {
                retry_delay: Duration::from_secs(7),
            };
            let handler = OAuthRefreshJobHandler::with_config(
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
                    |_, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(
                outcome,
                JobOutcome::Retry {
                    delay: Duration::from_secs(7)
                }
            );
            Ok(())
        }
    }
}
