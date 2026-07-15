use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::Ordering},
};

use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, MissedTickBehavior},
};

use super::{CaptureMessage, SharedStats, SinkConfig};
use crate::{
    schema::{CaptureRecord, CapturedRequestInput, CapturedResponse, Disposition},
    store::{CaptureStore, CaptureStoreError},
};

struct PendingRecord {
    seed: Option<SeedMetadata>,
    input: Option<CapturedRequestInput>,
    response: Option<(CapturedResponse, Disposition)>,
    updated_at: Instant,
}

impl PendingRecord {
    fn new(now: Instant) -> Self {
        Self {
            seed: None,
            input: None,
            response: None,
            updated_at: now,
        }
    }
}

struct SeedMetadata {
    request_id: String,
    ts_unix_ms: u64,
}

pub(super) struct CaptureWriter {
    store: CaptureStore,
    receiver: mpsc::Receiver<CaptureMessage>,
    stats: Arc<SharedStats>,
    config: SinkConfig,
    partials: HashMap<String, PendingRecord>,
    pending_writes: VecDeque<CaptureRecord>,
    #[cfg(test)]
    sweep_observer: Option<oneshot::Sender<()>>,
}

enum WriterEvent {
    Shutdown,
    Message(Option<CaptureMessage>),
    Sweep,
    Flush,
}

impl CaptureWriter {
    pub(super) fn new(
        store: CaptureStore,
        receiver: mpsc::Receiver<CaptureMessage>,
        stats: Arc<SharedStats>,
        config: SinkConfig,
    ) -> Self {
        Self {
            store,
            receiver,
            stats,
            config,
            partials: HashMap::with_capacity(config.max_partials),
            pending_writes: VecDeque::with_capacity(config.batch_size),
            #[cfg(test)]
            sweep_observer: None,
        }
    }

    #[cfg(test)]
    fn with_sweep_observer(mut self, observer: oneshot::Sender<()>) -> Self {
        self.sweep_observer = Some(observer);
        self
    }

    pub(super) async fn run(mut self, mut shutdown: oneshot::Receiver<()>) {
        let mut sweeper = tokio::time::interval(self.config.sweep_interval);
        let mut flusher = tokio::time::interval(self.config.flush_interval);
        sweeper.set_missed_tick_behavior(MissedTickBehavior::Delay);
        flusher.set_missed_tick_behavior(MissedTickBehavior::Delay);
        sweeper.tick().await;
        flusher.tick().await;

        loop {
            let event = tokio::select! {
                biased;
                _ = &mut shutdown => WriterEvent::Shutdown,
                message = self.receiver.recv() => WriterEvent::Message(message),
                _ = sweeper.tick() => WriterEvent::Sweep,
                _ = flusher.tick() => WriterEvent::Flush,
            };
            match event {
                WriterEvent::Shutdown => {
                    self.receiver.close();
                    break;
                }
                WriterEvent::Message(Some(message)) => {
                    self.handle_message(message);
                    if self.pending_writes.len() >= self.config.batch_size {
                        self.flush_one_batch().await;
                    }
                }
                WriterEvent::Message(None) => break,
                WriterEvent::Sweep => {
                    self.sweep_partials();
                    self.prune_retention().await;
                    #[cfg(test)]
                    if let Some(observer) = self.sweep_observer.take() {
                        let _ = observer.send(());
                    }
                }
                WriterEvent::Flush => self.flush_one_batch().await,
            }
        }

        while let Some(message) = self.receiver.recv().await {
            self.handle_message(message);
            if self.pending_writes.len() >= self.config.batch_size {
                self.flush_one_batch().await;
            }
        }
        while !self.pending_writes.is_empty() {
            self.flush_one_batch().await;
        }
        self.checkpoint_wal().await;
    }

    fn handle_message(&mut self, message: CaptureMessage) {
        let now = Instant::now();
        match message {
            CaptureMessage::Seed {
                event_id,
                request_id,
                ts_unix_ms,
            } => {
                let partial = self.partial_mut(event_id, now);
                partial.seed = Some(SeedMetadata {
                    request_id,
                    ts_unix_ms,
                });
                partial.updated_at = now;
            }
            CaptureMessage::Input(input) => {
                let event_id = input.event_id.clone();
                let partial = self.partial_mut(event_id, now);
                partial.input = Some(*input);
                partial.updated_at = now;
            }
            CaptureMessage::Response {
                event_id,
                response,
                disposition,
                terminal,
            } => {
                let partial = self.partial_mut(event_id.clone(), now);
                partial.response = Some((response, disposition));
                partial.updated_at = now;
                if terminal {
                    self.finalize(event_id);
                }
            }
        }
    }

    fn partial_mut(&mut self, event_id: String, now: Instant) -> &mut PendingRecord {
        if !self.partials.contains_key(&event_id) && self.partials.len() >= self.config.max_partials
        {
            self.drop_oldest_partial();
        }
        self.partials
            .entry(event_id)
            .or_insert_with(|| PendingRecord::new(now))
    }

    fn finalize(&mut self, event_id: String) {
        let Some(partial) = self.partials.remove(&event_id) else {
            return;
        };
        let Some(mut input) = partial.input else {
            self.stats
                .response_without_input
                .fetch_add(1, Ordering::Relaxed);
            metrics::counter!("cc_lb_capture_response_without_input_total").increment(1);
            return;
        };
        if let Some(seed) = partial.seed {
            if input.request_id.is_empty() {
                input.request_id = seed.request_id;
            }
            if input.captured_at_unix_ms == 0 {
                input.captured_at_unix_ms = seed.ts_unix_ms;
            }
        }
        let Some((response, disposition)) = partial.response else {
            return;
        };
        self.pending_writes.push_back(CaptureRecord {
            input,
            response,
            disposition,
        });
    }

    fn sweep_partials(&mut self) {
        let now = Instant::now();
        let before = self.partials.len();
        self.partials
            .retain(|_, partial| now.duration_since(partial.updated_at) < self.config.partial_ttl);
        let removed = before.saturating_sub(self.partials.len());
        self.stats
            .partial_ttl_evicted
            .fetch_add(removed as u64, Ordering::Relaxed);
        metrics::counter!("cc_lb_capture_partial_ttl_evicted_total").increment(removed as u64);
    }

    fn drop_oldest_partial(&mut self) {
        let oldest = self
            .partials
            .iter()
            .min_by_key(|(_, partial)| partial.updated_at)
            .map(|(event_id, _)| event_id.clone());
        if let Some(event_id) = oldest {
            self.partials.remove(&event_id);
        }
    }

    async fn prune_retention(&self) {
        if let Err(error) =
            crate::retention::prune_by_row_cap(self.store.pool(), self.config.retention_max_rows)
                .await
        {
            tracing::warn!(error = ?error, "capture retention prune failed");
        }
    }

    async fn flush_one_batch(&mut self) {
        let count = self.pending_writes.len().min(self.config.batch_size);
        if count == 0 {
            return;
        }
        let batch = self.pending_writes.drain(..count).collect::<Vec<_>>();
        let failed = batch.len() as u64;
        let first_disposition = batch
            .first()
            .map(|record| disposition_name(&record.disposition));
        if let Err(error) = persist_batch(&self.store, &batch).await {
            self.stats.write_failed.fetch_add(failed, Ordering::Relaxed);
            metrics::counter!("cc_lb_capture_write_failed_total").increment(failed);
            tracing::warn!(error = ?error, records = failed, first_disposition, "capture batch write failed");
        }
    }

    async fn checkpoint_wal(&self) {
        if let Err(error) = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(self.store.pool())
            .await
        {
            self.stats.write_failed.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("cc_lb_capture_write_failed_total").increment(1);
            tracing::warn!(error = ?error, "capture WAL checkpoint failed");
        }
    }
}

fn disposition_name(disposition: &Disposition) -> &'static str {
    match disposition {
        Disposition::RoutedPreDispatchError => "routed_pre_dispatch_error",
        Disposition::RoutedLimitRejected => "routed_limit_rejected",
        Disposition::RoutedDispatchedSuccess => "routed_dispatched_success",
        Disposition::RoutedDispatchedError => "routed_dispatched_error",
        Disposition::RoutedClientDisconnected => "routed_client_disconnected",
    }
}

async fn persist_batch(
    store: &CaptureStore,
    records: &[CaptureRecord],
) -> Result<(), CaptureStoreError> {
    let mut transaction = store.begin_immediate().await?;
    for record in records {
        store.insert_record_in_tx(&mut transaction, record).await?;
    }
    transaction.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
