use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

#[derive(Clone)]
pub struct PartialRetentionCache {
    entries: Arc<DashMap<String, RetainedPartial>>,
    ttl: Duration,
    max_entries: usize,
}

#[derive(Clone)]
struct RetainedPartial {
    inserted_at: Instant,
    payload: Vec<u8>,
}

impl PartialRetentionCache {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            ttl,
            max_entries: max_entries.max(1),
        }
    }

    pub fn insert(&self, event_id: String, payload: Vec<u8>) {
        self.prune_expired();
        if self.entries.len() >= self.max_entries {
            self.evict_one();
        }
        self.entries.insert(
            event_id,
            RetainedPartial {
                inserted_at: Instant::now(),
                payload,
            },
        );
    }

    pub fn get(&self, event_id: &str) -> Option<Vec<u8>> {
        let entry = self.entries.get(event_id)?;
        if entry.inserted_at.elapsed() > self.ttl {
            drop(entry);
            self.entries.remove(event_id);
            return None;
        }
        Some(entry.payload.clone())
    }

    pub fn prune_expired(&self) {
        let ttl = self.ttl;
        self.entries
            .retain(|_, retained| retained.inserted_at.elapsed() <= ttl);
    }

    fn evict_one(&self) {
        let oldest = self
            .entries
            .iter()
            .min_by_key(|entry| entry.value().inserted_at)
            .map(|entry| entry.key().clone());
        if let Some(key) = oldest {
            self.entries.remove(&key);
        }
    }
}
