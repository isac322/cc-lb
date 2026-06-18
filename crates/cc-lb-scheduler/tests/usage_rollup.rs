mod jobs {
    pub mod usage_rollup {
        use std::collections::VecDeque;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        use async_trait::async_trait;
        use cc_lb_scheduler::jobs::usage_rollup::{
            USAGE_ROLLUP_RETRY_DELAY, UsageRollupJob, UsageRollupJobResult, handle_usage_rollup_job,
        };
        use cc_lb_storage_api::{
            StorageError, StorageResult, UsageRollup, UsageRollupResolution, UsageRollupRun,
            UsageRollupStore,
        };
        use tokio::sync::{Mutex, Notify, oneshot};

        #[tokio::test]
        async fn rollup_execution_calls_storage_once_and_returns_done() {
            let store = RecordingRollupStore::new(vec![Ok(UsageRollupRun {
                processed_events: 7,
                updated_rollups: 3,
                checkpoint: Some(42),
            })]);

            let result = handle_usage_rollup_job(UsageRollupJob::default(), &store).await;

            assert_eq!(store.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                result,
                UsageRollupJobResult::Done {
                    run: UsageRollupRun {
                        processed_events: 7,
                        updated_rollups: 3,
                        checkpoint: Some(42),
                    },
                }
            );
        }

        #[tokio::test]
        async fn rollup_execution_returns_retry_when_storage_fails() {
            let store = RecordingRollupStore::new(vec![Err(StorageError::Unavailable {
                message: "database unavailable".to_owned(),
            })]);

            let result = handle_usage_rollup_job(UsageRollupJob::default(), &store).await;

            assert_eq!(store.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                result,
                UsageRollupJobResult::Retry {
                    delay: USAGE_ROLLUP_RETRY_DELAY,
                    error: "unavailable: database unavailable".to_owned(),
                }
            );
        }

        #[tokio::test]
        async fn advisory_lock_coordination_treats_contended_rollup_as_done() {
            let (entered_tx, entered_rx) = oneshot::channel();
            let store = Arc::new(AdvisoryRollupStore::new(entered_tx));
            let first_store = Arc::clone(&store);
            let first = tokio::spawn(async move {
                handle_usage_rollup_job(UsageRollupJob::default(), first_store.as_ref()).await
            });

            entered_rx.await.expect("first rollup enters advisory lock");

            let second = handle_usage_rollup_job(UsageRollupJob::default(), store.as_ref()).await;
            store.release.notify_waiters();
            let first = first.await.expect("first rollup task joins");

            assert_eq!(store.calls.load(Ordering::SeqCst), 2);
            assert_eq!(store.lock_contention.load(Ordering::SeqCst), 1);
            assert_eq!(
                first,
                UsageRollupJobResult::Done {
                    run: UsageRollupRun {
                        processed_events: 11,
                        updated_rollups: 4,
                        checkpoint: Some(90),
                    },
                }
            );
            assert_eq!(
                second,
                UsageRollupJobResult::Done {
                    run: UsageRollupRun {
                        processed_events: 0,
                        updated_rollups: 0,
                        checkpoint: Some(90),
                    },
                }
            );
        }

        struct RecordingRollupStore {
            calls: AtomicUsize,
            responses: Mutex<VecDeque<StorageResult<UsageRollupRun>>>,
        }

        impl RecordingRollupStore {
            fn new(responses: Vec<StorageResult<UsageRollupRun>>) -> Self {
                Self {
                    calls: AtomicUsize::new(0),
                    responses: Mutex::new(VecDeque::from(responses)),
                }
            }
        }

        struct AdvisoryRollupStore {
            calls: AtomicUsize,
            lock_contention: AtomicUsize,
            lock_held: AtomicBool,
            first_entered: Mutex<Option<oneshot::Sender<()>>>,
            release: Notify,
        }

        impl AdvisoryRollupStore {
            fn new(first_entered: oneshot::Sender<()>) -> Self {
                Self {
                    calls: AtomicUsize::new(0),
                    lock_contention: AtomicUsize::new(0),
                    lock_held: AtomicBool::new(false),
                    first_entered: Mutex::new(Some(first_entered)),
                    release: Notify::new(),
                }
            }
        }

        #[async_trait]
        impl UsageRollupStore for RecordingRollupStore {
            async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.responses.lock().await.pop_front().unwrap_or_else(|| {
                    Err(StorageError::Fatal {
                        message: "unexpected rollup call".to_owned(),
                    })
                })
            }

            async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
                Ok(Vec::new())
            }

            async fn query_usage_rollups_in_range(
                &self,
                _resolution: UsageRollupResolution,
                _window_start_unix_secs: u64,
                _window_end_unix_secs: u64,
            ) -> StorageResult<Vec<UsageRollup>> {
                Ok(Vec::new())
            }

            async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
                Ok(None)
            }

            async fn advance_rollup_checkpoint_and_persist(
                &self,
                _run: &UsageRollupRun,
            ) -> StorageResult<()> {
                Ok(())
            }
        }

        #[async_trait]
        impl UsageRollupStore for AdvisoryRollupStore {
            async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.lock_held.swap(true, Ordering::SeqCst) {
                    self.lock_contention.fetch_add(1, Ordering::SeqCst);
                    return Ok(UsageRollupRun {
                        processed_events: 0,
                        updated_rollups: 0,
                        checkpoint: Some(90),
                    });
                }

                if let Some(sender) = self.first_entered.lock().await.take() {
                    let _ = sender.send(());
                }
                self.release.notified().await;
                self.lock_held.store(false, Ordering::SeqCst);

                Ok(UsageRollupRun {
                    processed_events: 11,
                    updated_rollups: 4,
                    checkpoint: Some(90),
                })
            }

            async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
                Ok(Vec::new())
            }

            async fn query_usage_rollups_in_range(
                &self,
                _resolution: UsageRollupResolution,
                _window_start_unix_secs: u64,
                _window_end_unix_secs: u64,
            ) -> StorageResult<Vec<UsageRollup>> {
                Ok(Vec::new())
            }

            async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
                Ok(None)
            }

            async fn advance_rollup_checkpoint_and_persist(
                &self,
                _run: &UsageRollupRun,
            ) -> StorageResult<()> {
                Ok(())
            }
        }
    }
}
