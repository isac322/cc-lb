use std::collections::HashMap;
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
    last_written: Arc<Mutex<HashMap<Uuid, Instant>>>,
}

impl VertexStatusSink {
    pub(crate) fn new(store: Arc<dyn UpstreamStore>) -> Self {
        Self {
            store,
            last_written: Arc::new(Mutex::new(HashMap::new())),
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
            let mut last_written = self.last_written.lock();
            if last_written
                .get(&upstream_id)
                .is_some_and(|at| now.duration_since(*at) < STATUS_RETRY_INTERVAL)
            {
                return;
            }
            last_written.insert(upstream_id, now);
        }
        let store = self.store.clone();
        tokio::spawn(async move {
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
            if let Err(error) = result {
                tracing::warn!(upstream_id = %upstream_id, %error, "vertex terminal credential status write failed");
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
