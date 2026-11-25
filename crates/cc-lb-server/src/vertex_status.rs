use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cc_lb_storage_api::{UpstreamStatusUpdate, UpstreamStore};
use cc_lb_vertex_auth::{BearerSource, BearerToken, MintError};
use parking_lot::Mutex;
use uuid::Uuid;

const STATUS_RETRY_INTERVAL: Duration = Duration::from_secs(30);

/// Asynchronously persists terminal Vertex credential failures without making
/// request signing wait on storage or allowing transient failures to clear a
/// terminal latch.
#[derive(Clone)]
pub(crate) struct VertexStatusSink {
    store: Arc<dyn UpstreamStore>,
    state: Arc<Mutex<VertexStatusWriteState>>,
}

#[derive(Default)]
struct VertexStatusWriteState {
    in_flight: HashSet<Uuid>,
    last_written: HashMap<Uuid, Instant>,
}

struct InFlightGuard {
    upstream_id: Uuid,
    state: Arc<Mutex<VertexStatusWriteState>>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.state.lock().in_flight.remove(&self.upstream_id);
    }
}

impl VertexStatusSink {
    pub(crate) fn new(store: Arc<dyn UpstreamStore>) -> Self {
        Self {
            store,
            state: Arc::new(Mutex::new(VertexStatusWriteState::default())),
        }
    }

    pub(crate) fn record_terminal(
        &self,
        upstream_id: Uuid,
        expected_revision: u64,
        reason: &'static str,
    ) {
        let now = Instant::now();
        {
            let mut state = self.state.lock();
            if state.in_flight.contains(&upstream_id)
                || state
                    .last_written
                    .get(&upstream_id)
                    .is_some_and(|at| now.duration_since(*at) < STATUS_RETRY_INTERVAL)
            {
                return;
            }
            state.in_flight.insert(upstream_id);
        }
        let store = self.store.clone();
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let _guard = InFlightGuard {
                upstream_id,
                state: Arc::clone(&state),
            };
            let result = store
                .set_status(
                    upstream_id,
                    UpstreamStatusUpdate {
                        last_apply_error: Some(Some(reason.to_owned())),
                        expected_spec_revision: Some(expected_revision),
                        ..Default::default()
                    },
                )
                .await;
            match result {
                Ok(()) => {
                    state
                        .lock()
                        .last_written
                        .insert(upstream_id, Instant::now());
                }
                Err(error) => {
                    tracing::warn!(upstream_id = %upstream_id, %error, "vertex terminal credential status write failed");
                }
            }
        });
    }
}

pub(crate) struct ReportingBearerSource {
    source: Arc<dyn BearerSource>,
    sink: VertexStatusSink,
    upstream_id: Uuid,
    expected_revision: u64,
}

impl ReportingBearerSource {
    pub(crate) fn new(
        source: Arc<dyn BearerSource>,
        sink: VertexStatusSink,
        upstream_id: Uuid,
        expected_revision: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            source,
            sink,
            upstream_id,
            expected_revision,
        })
    }
}

#[async_trait]
impl BearerSource for ReportingBearerSource {
    async fn bearer(&self) -> Result<BearerToken, MintError> {
        match self.source.bearer().await {
            Ok(token) => Ok(token),
            Err(error) => {
                if let Some(reason) = error.terminal_reason() {
                    self.sink
                        .record_terminal(self.upstream_id, self.expected_revision, reason);
                }
                Err(error)
            }
        }
    }

    fn invalidate_if_current(&self, token_id: u64) -> bool {
        self.source.invalidate_if_current(token_id)
    }

    fn quota_project(&self) -> Option<&str> {
        self.source.quota_project()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use cc_lb_storage_api::{
        StorageError, StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStatusUpdate,
        UpstreamStore, UpstreamUpdate,
    };
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::VertexStatusSink;

    struct FlakyStatusStore {
        fail_writes: AtomicBool,
        calls: AtomicUsize,
        done_notify: Notify,
    }

    #[async_trait]
    impl UpstreamStore for FlakyStatusStore {
        async fn create(&self, _create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }
        async fn get_by_id(&self, _id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
            unimplemented!()
        }
        async fn get_by_name(&self, _name: &str) -> StorageResult<Option<UpstreamRecord>> {
            unimplemented!()
        }
        async fn list(
            &self,
            _after: Option<cc_lb_storage_api::UpstreamRecordId>,
            _limit: usize,
        ) -> StorageResult<Vec<UpstreamRecord>> {
            unimplemented!()
        }
        async fn update(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _update: UpstreamUpdate,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }
        async fn set_status(&self, _id: Uuid, _status: UpstreamStatusUpdate) -> StorageResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let fail = self.fail_writes.load(Ordering::SeqCst);
            self.done_notify.notify_one();
            if fail {
                Err(StorageError::Unavailable {
                    message: "transient".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn store_oauth_tokens(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _tokens: cc_lb_aead::EncryptedOAuthTokens,
            _never_refresh: bool,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }
        async fn complete_refresh(
            &self,
            _id: Uuid,
            _holder: Uuid,
            _tokens: cc_lb_aead::EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }
        async fn set_last_apply_error(
            &self,
            _id: Uuid,
            _error: Option<String>,
        ) -> StorageResult<()> {
            unimplemented!()
        }
        async fn soft_delete(&self, _id: Uuid, _expected_revision: u64) -> StorageResult<()> {
            unimplemented!()
        }
        async fn hard_delete(&self, _id: Uuid) -> StorageResult<()> {
            unimplemented!()
        }
        async fn clear_warmup_dialect_plugin(
            &self,
            _id: Uuid,
            _expected_revision: u64,
        ) -> StorageResult<Option<UpstreamRecord>> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn vertex_status_sink_only_sets_cooldown_after_successful_write() {
        let store = Arc::new(FlakyStatusStore {
            fail_writes: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            done_notify: Notify::new(),
        });
        let sink = VertexStatusSink::new(store.clone());
        let id = Uuid::new_v4();

        let n1 = store.done_notify.notified();
        sink.record_terminal(id, 1, "invalid_grant");
        n1.await;
        tokio::task::yield_now().await;
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);

        // Next call must NOT be debounced because the previous DB write failed.
        store.fail_writes.store(false, Ordering::SeqCst);
        let n2 = store.done_notify.notified();
        sink.record_terminal(id, 1, "invalid_grant");
        n2.await;
        tokio::task::yield_now().await;
        assert_eq!(store.calls.load(Ordering::SeqCst), 2);

        // After Ok(()), calls within 30s cooldown are suppressed.
        sink.record_terminal(id, 1, "invalid_grant");
        tokio::task::yield_now().await;
        assert_eq!(store.calls.load(Ordering::SeqCst), 2);
    }
}
