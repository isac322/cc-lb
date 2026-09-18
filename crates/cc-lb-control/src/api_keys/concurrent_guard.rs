#[cfg(loom)]
use loom::sync::Arc;
#[cfg(loom)]
use loom::sync::atomic::AtomicU32;
#[cfg(not(loom))]
use std::sync::Arc;
#[cfg(not(loom))]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use dashmap::DashMap;

use cc_lb_storage_api::Storage;

pub struct KeyConcurrencyManager {
    counters: Arc<DashMap<String, Arc<AtomicU32>>>,
}

impl KeyConcurrencyManager {
    pub fn new() -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
        }
    }

    pub fn try_acquire(
        &self,
        key_id: &str,
        cap: u32,
    ) -> Result<KeyConcurrencyGuard, ConcurrencyRejected> {
        let counter = if let Some(counter) = self.counters.get(key_id) {
            Arc::clone(counter.value())
        } else {
            let entry = self
                .counters
                .entry(key_id.to_owned())
                .or_insert_with(|| Arc::new(AtomicU32::new(0)));
            Arc::clone(entry.value())
        };

        loop {
            let current = counter.load(Ordering::Acquire);
            if current >= cap {
                return Err(ConcurrencyRejected::Limit(cap));
            }

            if counter
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                metrics::gauge!("cclb_api_key_concurrent", "key_id" => key_id.to_owned())
                    .set(f64::from(current + 1));
                return Ok(KeyConcurrencyGuard {
                    key_id: key_id.to_owned(),
                    counter,
                });
            }
        }
    }

    pub fn current(&self, key_id: &str) -> u32 {
        self.counters
            .get(key_id)
            .map(|counter| counter.value().load(Ordering::Acquire))
            .unwrap_or(0)
    }
}

impl Default for KeyConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}

pub struct KeyConcurrencyGuard {
    key_id: String,
    counter: Arc<AtomicU32>,
}

impl KeyConcurrencyGuard {
    pub fn key_id(&self) -> &str {
        &self.key_id
    }
}

impl Drop for KeyConcurrencyGuard {
    fn drop(&mut self) {
        let previous = self.counter.fetch_sub(1, Ordering::AcqRel);
        metrics::gauge!(
            "cclb_api_key_concurrent",
            "key_id" => self.key_id.clone()
        )
        .set(f64::from(previous.saturating_sub(1)));
    }
}

/// RAII guard for a cluster-wide concurrency hold row
/// (`api_key_concurrency_holds_v1`, issue 807).
///
/// Dropping the guard deletes the hold row on a spawned task so the slot is
/// released for every replica. If no Tokio runtime is available at drop time
/// the row is left to expire via the writer-lease age cutoff.
pub struct DurableConcurrencyHold {
    storage: Arc<dyn Storage>,
    hold_id: uuid::Uuid,
    key_id: String,
}

impl DurableConcurrencyHold {
    pub fn new(storage: Arc<dyn Storage>, hold_id: uuid::Uuid, key_id: String) -> Self {
        Self {
            storage,
            hold_id,
            key_id,
        }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }
}

impl Drop for DurableConcurrencyHold {
    fn drop(&mut self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                hold_id = %self.hold_id,
                key_id = %self.key_id,
                "no Tokio runtime to release API-key concurrency hold; leaving it to expire"
            );
            return;
        };
        let storage = Arc::clone(&self.storage);
        let hold_id = self.hold_id;
        let key_id = self.key_id.clone();
        handle.spawn(async move {
            if let Err(error) = storage.delete_api_key_concurrency_hold(hold_id).await {
                tracing::warn!(%error, %hold_id, %key_id, "failed to release API-key concurrency hold");
            }
        });
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConcurrencyRejected {
    Limit(u32),
}
