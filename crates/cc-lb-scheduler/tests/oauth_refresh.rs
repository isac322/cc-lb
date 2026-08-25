use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_scheduler::error::{Result, SchedulerError};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{
    OAuthRefreshConfig, OAuthRefreshJob, OAuthRefreshJobHandler, RefreshedOAuthTokens,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_storage_api::upstream::UpstreamKind;
use tokio::sync::Notify;
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
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());
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
                    move |id, generation, expires_at| {
                        *scheduled_clone.lock().expect("scheduled lock") =
                            Some((id, generation, expires_at));
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
                Some((upstream_id, 5, 2_000))
            );
            Ok(())
        }

        #[tokio::test]
        async fn refresh_failure_retries() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let config = OAuthRefreshConfig {
                retry_delay: Duration::from_secs(7),
            };
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 5);
            let handler = OAuthRefreshJobHandler::with_config(upstreams.clone(), config);

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    |_| async { Err(SchedulerError::Job("token endpoint 401".to_owned())) },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(
                outcome,
                JobOutcome::Retry {
                    delay: Duration::from_secs(7)
                }
            );
            assert_eq!(upstreams.fail_calls(), 1);
            Ok(())
        }

        #[tokio::test]
        async fn terminal_refresh_failure_dead_letters_after_recording_failure() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 5);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    |_| async { Err(SchedulerError::TerminalJob("invalid_grant".to_owned())) },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::DeadLetter);
            assert_eq!(upstreams.fail_calls(), 1);
            assert_eq!(upstreams.failure_reason().as_deref(), Some("invalid_grant"));
            assert_eq!(upstreams.complete_calls(), 0);
            Ok(())
        }

        #[tokio::test]
        async fn retry_of_terminal_generation_dead_letters_without_provider_call() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 5);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());
            let job = OAuthRefreshJob::for_generation(upstream_id, 4);

            let first_outcome = handler
                .handle(
                    job.clone(),
                    1_000,
                    |_| async { Err(SchedulerError::TerminalJob("invalid_grant".to_owned())) },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;
            assert_eq!(first_outcome, JobOutcome::DeadLetter);

            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let refresh_calls_for_handler = Arc::clone(&refresh_calls);
            let second_outcome = handler
                .handle(
                    job,
                    1_001,
                    move |_| {
                        refresh_calls_for_handler.fetch_add(1, Ordering::SeqCst);
                        async {
                            Err(SchedulerError::Job(
                                "terminal retry called the provider".to_owned(),
                            ))
                        }
                    },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(second_outcome, JobOutcome::DeadLetter);
            assert_eq!(refresh_calls.load(Ordering::SeqCst), 0);
            assert_eq!(upstreams.claim_calls(), 2);
            assert_eq!(upstreams.fail_calls(), 1);
            Ok(())
        }

        #[tokio::test]
        async fn credential_replacement_after_claim_skips_stale_provider_call() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 6)
                .with_replacement_after_claim(5);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());
            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let refresh_calls_for_handler = refresh_calls.clone();

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    move |_| {
                        refresh_calls_for_handler.fetch_add(1, Ordering::SeqCst);
                        async {
                            Ok(RefreshedOAuthTokens {
                                encrypted_tokens: encrypted_tokens(2),
                                expires_at_unix_secs: 2_000,
                            })
                        }
                    },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Done);
            assert_eq!(refresh_calls.load(Ordering::SeqCst), 0);
            assert_eq!(upstreams.complete_calls(), 0);
            Ok(())
        }

        #[tokio::test]
        async fn provider_failure_adopts_concurrently_completed_generation() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 6);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());
            let upstreams_for_refresh = upstreams.clone();

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    move |_| {
                        upstreams_for_refresh.replace_credentials(5);
                        async { Err(SchedulerError::TerminalJob("invalid_grant".to_owned())) }
                    },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Done);
            assert_eq!(upstreams.fail_calls(), 0);
            assert_eq!(upstreams.complete_calls(), 0);
            Ok(())
        }

        #[tokio::test]
        async fn busy_refresh_retry_stops_after_generation_advances() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams =
                FakeUpstreams::new(refreshable_record(upstream_id, 4), 5).with_busy_lease();
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());
            let job = OAuthRefreshJob::for_generation(upstream_id, 4);
            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let refresh_calls_for_handler = refresh_calls.clone();
            let metadata_calls = Arc::new(AtomicUsize::new(0));

            let first_outcome = handler
                .handle(
                    job.clone(),
                    1_000,
                    move |_| {
                        refresh_calls_for_handler.fetch_add(1, Ordering::SeqCst);
                        async {
                            Ok(RefreshedOAuthTokens {
                                encrypted_tokens: encrypted_tokens(2),
                                expires_at_unix_secs: 2_000,
                            })
                        }
                    },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(
                first_outcome,
                JobOutcome::Retry {
                    delay: Duration::from_secs(30),
                }
            );
            upstreams.replace_credentials(5);
            let metadata_calls_for_handler = metadata_calls.clone();
            let second_outcome = handler
                .handle(
                    job,
                    1_030,
                    |_| async {
                        Err(SchedulerError::Job(
                            "superseded retry called the provider".to_owned(),
                        ))
                    },
                    move |_| {
                        metadata_calls_for_handler.fetch_add(1, Ordering::SeqCst);
                        async { Ok(()) }
                    },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(second_outcome, JobOutcome::Done);
            assert_eq!(refresh_calls.load(Ordering::SeqCst), 0);
            assert_eq!(metadata_calls.load(Ordering::SeqCst), 1);
            assert_eq!(upstreams.claim_calls(), 1);
            Ok(())
        }

        #[tokio::test]
        async fn legacy_job_losing_refresh_lease_defers_to_watchdog() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams =
                FakeUpstreams::new(refreshable_record(upstream_id, 4), 5).with_busy_lease();
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());

            let outcome = handler
                .handle(
                    OAuthRefreshJob::new(upstream_id),
                    1_000,
                    |_| async {
                        Err(SchedulerError::Job(
                            "legacy loser called the provider".to_owned(),
                        ))
                    },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Done);
            assert_eq!(upstreams.claim_calls(), 1);
            Ok(())
        }

        #[tokio::test]
        async fn concurrent_scheduled_refreshes_allow_one_provider_owner() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeUpstreams::new(refreshable_record(upstream_id, 4), 5);
            let first_handler = OAuthRefreshJobHandler::new(upstreams.clone());
            let second_handler = OAuthRefreshJobHandler::new(upstreams.clone());
            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let refresh_started = Arc::new(Notify::new());
            let release_refresh = Arc::new(Notify::new());

            let first_calls = refresh_calls.clone();
            let first_started = refresh_started.clone();
            let first_release = release_refresh.clone();
            let first = tokio::spawn(async move {
                first_handler
                    .handle(
                        OAuthRefreshJob::for_generation(upstream_id, 4),
                        1_000,
                        move |_| async move {
                            first_calls.fetch_add(1, Ordering::SeqCst);
                            first_started.notify_one();
                            first_release.notified().await;
                            Ok(RefreshedOAuthTokens {
                                encrypted_tokens: encrypted_tokens(2),
                                expires_at_unix_secs: 2_000,
                            })
                        },
                        |_| async { Ok(()) },
                        |_, _, _| async { Ok(()) },
                    )
                    .await
            });
            refresh_started.notified().await;

            let second_calls = refresh_calls.clone();
            let second_outcome = second_handler
                .handle(
                    OAuthRefreshJob::for_generation(upstream_id, 4),
                    1_000,
                    move |_| {
                        second_calls.fetch_add(1, Ordering::SeqCst);
                        async {
                            Ok(RefreshedOAuthTokens {
                                encrypted_tokens: encrypted_tokens(3),
                                expires_at_unix_secs: 2_000,
                            })
                        }
                    },
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(
                second_outcome,
                JobOutcome::Retry {
                    delay: Duration::from_secs(30),
                }
            );
            assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
            release_refresh.notify_one();
            assert_eq!(first.await.expect("first refresh joins")?, JobOutcome::Done);
            assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
            Ok(())
        }

        #[tokio::test]
        async fn disabled_registered_oauth_upstream_refreshes() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let mut upstream = refreshable_record(upstream_id, 4);
            upstream.enabled = false;
            upstream.warmup_enabled = false;
            let upstreams = FakeUpstreams::new(upstream, 5);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());

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
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Done);
            assert_eq!(upstreams.complete_calls(), 1);
            Ok(())
        }

        #[tokio::test]
        async fn deleted_oauth_upstream_skips_refresh() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let mut upstream = refreshable_record(upstream_id, 4);
            upstream.deleted_at_unix_secs = Some(1_800_000_000);
            let upstreams = FakeUpstreams::new(upstream, 5);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());

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
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Skip);
            assert_eq!(upstreams.complete_calls(), 0);
            Ok(())
        }

        #[tokio::test]
        async fn missing_oauth_credentials_skip_refresh() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let mut upstream = refreshable_record(upstream_id, 4);
            upstream.oauth_credentials = None;
            let upstreams = FakeUpstreams::new(upstream, 5);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());

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
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Skip);
            assert_eq!(upstreams.complete_calls(), 0);
            Ok(())
        }

        #[tokio::test]
        async fn non_oauth_upstream_skips_refresh() -> Result<()> {
            let upstream_id = Uuid::new_v4();
            let mut upstream = refreshable_record(upstream_id, 4);
            upstream.kind = UpstreamKind::AnthropicApiKey;
            let upstreams = FakeUpstreams::new(upstream, 5);
            let handler = OAuthRefreshJobHandler::new(upstreams.clone());

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
                    |_| async { Ok(()) },
                    |_, _, _| async { Ok(()) },
                )
                .await?;

            assert_eq!(outcome, JobOutcome::Skip);
            assert_eq!(upstreams.complete_calls(), 0);
            Ok(())
        }
    }
}
