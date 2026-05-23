use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

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
        let counter = {
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
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConcurrencyRejected {
    Limit(u32),
}
