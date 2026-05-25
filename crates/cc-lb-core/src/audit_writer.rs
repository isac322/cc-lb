use std::sync::Arc;
use std::time::Duration;

use cc_lb_storage_redb::{AuditEntry as StoredAuditEntry, Storage};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const AUDIT_BATCH_CAPACITY: usize = 16;
const AUDIT_FLUSH_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Clone)]
pub struct AuditWriterSink {
    tx: mpsc::Sender<AuditEntry>,
}

#[derive(Debug)]
pub struct AuditDropped;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuditEntry {
    pub ts: u64,
    pub request_id: String,
    pub principal_id: String,
    pub route: String,
    pub upstream: String,
    pub model: Option<String>,
    pub status: u16,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub duration_ms: u64,
    pub agent_label: Option<String>,
    pub api_key_id: Option<String>,
    pub cost_usd_micros: Option<u64>,
    pub limit_violation: Option<String>,
    pub admin_action: Option<String>,
    pub actor: Option<String>,
}

impl AuditWriterSink {
    pub fn try_enqueue(&self, entry: AuditEntry) -> Result<(), AuditDropped> {
        self.tx.try_send(entry).map_err(|_| {
            metrics::counter!("cclb_audit_writer_dropped_total").increment(1);
            AuditDropped
        })
    }
}

pub fn spawn_audit_writer(
    storage: Arc<Storage>,
    capacity: usize,
) -> (AuditWriterSink, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel(capacity);
    let join = tokio::spawn(async move {
        let mut batch = Vec::with_capacity(AUDIT_BATCH_CAPACITY);
        let mut ticker = tokio::time::interval(AUDIT_FLUSH_INTERVAL);

        loop {
            tokio::select! {
                maybe_entry = rx.recv() => {
                    let Some(entry) = maybe_entry else {
                        break;
                    };
                    batch.push(StoredAuditEntry::from(entry));
                    if batch.len() >= AUDIT_BATCH_CAPACITY {
                        flush_batch(storage.clone(), take_batch(&mut batch)).await;
                    }
                }
                _ = ticker.tick() => {
                    if !batch.is_empty() {
                        flush_batch(storage.clone(), take_batch(&mut batch)).await;
                    }
                }
            }
        }

        if !batch.is_empty() {
            flush_batch(storage, batch).await;
        }
    });

    (AuditWriterSink { tx }, join)
}

impl From<AuditEntry> for StoredAuditEntry {
    fn from(value: AuditEntry) -> Self {
        Self {
            ts: value.ts,
            request_id: value.request_id,
            principal_id: value.principal_id,
            route: value.route,
            upstream: value.upstream,
            model: value.model,
            status: value.status,
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            duration_ms: value.duration_ms,
            agent_label: value.agent_label,
            api_key_id: value.api_key_id,
            cost_usd_micros: value.cost_usd_micros,
            limit_violation: value.limit_violation,
            admin_action: value.admin_action,
            actor: value.actor,
        }
    }
}

fn take_batch(batch: &mut Vec<StoredAuditEntry>) -> Vec<StoredAuditEntry> {
    std::mem::replace(batch, Vec::with_capacity(AUDIT_BATCH_CAPACITY))
}

async fn flush_batch(storage: Arc<Storage>, batch: Vec<StoredAuditEntry>) {
    let result = tokio::task::spawn_blocking(move || storage.append_audit_entries(&batch)).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(error = %error, "audit writer batch append failed"),
        Err(error) => tracing::warn!(error = %error, "audit writer blocking task failed"),
    }
}
