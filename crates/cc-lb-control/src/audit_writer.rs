use std::sync::Arc;
use std::time::Duration;

pub use cc_lb_storage_api::AuditEntry;
use cc_lb_storage_api::{AuditStore, Storage as StorageTrait};
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

pub trait AuditStorageHandle {
    fn into_audit_storage(self) -> Arc<dyn AuditStore>;
}

impl AuditStorageHandle for Arc<dyn AuditStore> {
    fn into_audit_storage(self) -> Arc<dyn AuditStore> {
        self
    }
}

impl AuditStorageHandle for Arc<dyn StorageTrait> {
    fn into_audit_storage(self) -> Arc<dyn AuditStore> {
        self
    }
}

impl<T> AuditStorageHandle for Arc<T>
where
    T: AuditStore + 'static,
{
    fn into_audit_storage(self) -> Arc<dyn AuditStore> {
        self
    }
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
    storage: impl AuditStorageHandle,
    capacity: usize,
) -> (AuditWriterSink, JoinHandle<()>) {
    let storage = storage.into_audit_storage();
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
                    batch.push(entry);
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

fn take_batch(batch: &mut Vec<AuditEntry>) -> Vec<AuditEntry> {
    std::mem::replace(batch, Vec::with_capacity(AUDIT_BATCH_CAPACITY))
}

async fn flush_batch(storage: Arc<dyn AuditStore>, batch: Vec<AuditEntry>) {
    match storage.append_audit_entries(&batch).await {
        Ok(()) => {}
        Err(error) => tracing::warn!(error = %error, "audit writer batch append failed"),
    }
}
