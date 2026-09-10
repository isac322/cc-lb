use std::collections::{HashMap, HashSet};

use cc_lb_domain::{CacheScore, TtlClass, WarmCacheEntry};
use cc_lb_engine::clock::{ClockHandle, unix_secs};
use cc_lb_engine::lifecycle::{
    PromptCacheObservationCacheLike, PromptCacheObservationInput, PromptCacheThreadUsage,
};
use cc_lb_engine::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;
use cc_lb_storage_api::{PromptCacheObservationStore, StorageResult};
use parking_lot::RwLock;
use uuid::Uuid;

pub const HASH_SCHEMA_VERSION: u8 = cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION;

const DEFAULT_REFRESH_DEBOUNCE_SECS: u64 = 60;
const EXPIRY_SWEEP_INTERVAL_SECS: u64 = 60;
const DEFAULT_MAX_ENTRIES_PER_PARTITION: usize = 50_000;
const THREAD_USAGE_CAP_PER_UPSTREAM: usize = 2048;
const THREAD_USAGE_TTL_SECS: u64 = 5 * 60;
const CREATION_READ_EQUIVALENT_DIVISOR: u64 = 4;
type ThreadUsageKey = (String, String);
type ThreadUsageByUpstream = HashMap<Uuid, HashMap<ThreadUsageKey, ThreadUsageEntry>>;

pub struct PromptCacheObservationCache {
    #[allow(clippy::type_complexity)]
    entries: RwLock<
        HashMap<
            (Uuid, String /*canonical_model*/),
            HashMap<(String /*prefix_hash*/, TtlClass), CacheEntry>,
        >,
    >,
    clock: ClockHandle,
    thread_usage: RwLock<ThreadUsageByUpstream>,
    grace_margin_secs: u64,
    refresh_debounce_secs: u64,
    max_entries_per_partition: usize,
    overloaded: RwLock<HashSet<(Uuid, String /*canonical_model*/)>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheEntry {
    pub expires_at_unix_secs: u64,
    pub last_observed_at_unix_secs: u64,
    pub ttl_class: TtlClass,
    pub last_persisted_at_unix_secs: u64,
    pub prefix_content_block_index: u32,
    pub estimated_prefix_tokens: u64,
    pub token_estimate_source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptCacheObservationUpsert {
    pub upstream_id: Uuid,
    pub canonical_model: String,
    pub prefix_hash: String,
    pub ttl_class: TtlClass,
    pub expires_at_unix_secs: u64,
    pub last_observed_at_unix_secs: u64,
    pub prefix_content_block_index: u32,
    pub estimated_prefix_tokens: u64,
    pub token_estimate_source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ThreadUsageEntry {
    predicted_cache_read_tokens: u32,
    expires_at_unix_secs: u64,
    last_observed_at_unix_secs: u64,
}

impl PromptCacheObservationCache {
    pub fn new(clock: ClockHandle, grace_margin_secs: u64) -> Self {
        Self::new_with_debounce(clock, grace_margin_secs, DEFAULT_REFRESH_DEBOUNCE_SECS)
    }

    pub fn new_with_debounce(
        clock: ClockHandle,
        grace_margin_secs: u64,
        refresh_debounce_secs: u64,
    ) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            clock,
            thread_usage: RwLock::new(HashMap::new()),
            grace_margin_secs,
            refresh_debounce_secs,
            max_entries_per_partition: DEFAULT_MAX_ENTRIES_PER_PARTITION,
            overloaded: RwLock::new(HashSet::new()),
        }
    }

    #[must_use]
    pub fn with_max_entries_per_partition(mut self, max_entries_per_partition: usize) -> Self {
        self.max_entries_per_partition = max_entries_per_partition;
        self
    }

    pub fn grace_margin_secs(&self) -> u64 {
        self.grace_margin_secs
    }

    pub fn sweep_expired(&self, now_unix_secs: u64) -> usize {
        let mut removed = 0usize;
        let live_count: HashMap<(Uuid, String), usize> = {
            let mut guard = self.entries.write();
            guard.retain(|_partition_key, submap| {
                let before = submap.len();
                submap.retain(|_key, entry| entry.expires_at_unix_secs > now_unix_secs);
                removed += before - submap.len();
                !submap.is_empty()
            });
            guard
                .iter()
                .map(|(key, submap)| (key.clone(), submap.len()))
                .collect()
        };
        let exit_threshold = self.max_entries_per_partition * 9 / 10;
        let mut overloaded = self.overloaded.write();
        overloaded.retain(|key| {
            live_count
                .get(key)
                .is_some_and(|&live| live >= exit_threshold)
        });
        removed
    }

    pub fn spawn_expiry_sweeper(self: &std::sync::Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let weak = std::sync::Arc::downgrade(self);
        handle.spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(EXPIRY_SWEEP_INTERVAL_SECS));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(cache) = weak.upgrade() else {
                    break;
                };
                let now = unix_secs(cache.clock.now());
                cache.sweep_expired(now);
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn map_len(&self) -> usize {
        self.entries.read().values().map(HashMap::len).sum()
    }

    pub async fn hydrate_from_store(
        &self,
        store: &dyn PromptCacheObservationStore,
        upstream_ids: &[Uuid],
    ) -> StorageResult<usize> {
        let mut loaded = 0;
        for upstream_id in upstream_ids {
            let records = store
                .list_active_for_upstream(*upstream_id, unix_secs(self.clock.now()))
                .await?;
            for record in records {
                if record.hash_schema_version != HASH_SCHEMA_VERSION {
                    continue;
                }
                self.upsert_observation(PromptCacheObservationUpsert {
                    upstream_id: record.upstream_id,
                    canonical_model: record.canonical_model_id,
                    prefix_hash: record.v3_prefix_key,
                    ttl_class: record.ttl_class,
                    expires_at_unix_secs: record.expires_at_unix_secs,
                    last_observed_at_unix_secs: record.last_observed_at_unix_secs,
                    prefix_content_block_index: record.prefix_content_block_index,
                    estimated_prefix_tokens: record.estimated_prefix_tokens,
                    token_estimate_source: record.token_estimate_source,
                });
                loaded += 1;
            }
        }
        Ok(loaded)
    }

    pub fn upsert_observation(&self, observation: PromptCacheObservationUpsert) {
        let PromptCacheObservationUpsert {
            upstream_id,
            canonical_model,
            prefix_hash,
            ttl_class,
            expires_at_unix_secs,
            last_observed_at_unix_secs,
            prefix_content_block_index,
            estimated_prefix_tokens,
            token_estimate_source,
        } = observation;
        let mut guard = self.entries.write();
        let partition = guard
            .entry((upstream_id, canonical_model.clone()))
            .or_default();
        let key = (prefix_hash, ttl_class);
        if !partition.contains_key(&key) && partition.len() >= self.max_entries_per_partition {
            drop(guard);
            if self
                .overloaded
                .write()
                .insert((upstream_id, canonical_model.clone()))
            {
                tracing::warn!(
                    upstream_id = %upstream_id,
                    canonical_model,
                    ceiling = self.max_entries_per_partition,
                    "prompt-cache observation partition overloaded; rejecting new entries and routing cache-negative until it drains"
                );
            }
            return;
        }
        let last_persisted_at_unix_secs = partition
            .get(&key)
            .map_or(last_observed_at_unix_secs, |entry| {
                entry.last_persisted_at_unix_secs
            });
        partition.insert(
            key,
            CacheEntry {
                expires_at_unix_secs,
                last_observed_at_unix_secs,
                ttl_class,
                last_persisted_at_unix_secs,
                prefix_content_block_index,
                estimated_prefix_tokens,
                token_estimate_source,
            },
        );
    }

    pub fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        request_breakpoint_hashes: &[(String, TtlClass)],
        now_unix_secs: u64,
    ) -> Vec<WarmCacheEntry> {
        let guard = self.entries.read();
        let Some(entries) = guard.get(&(upstream_id, canonical_model.to_owned())) else {
            return Vec::new();
        };

        let mut snapshot: Vec<WarmCacheEntry> = entries
            .iter()
            .filter_map(|((prefix_hash, entry_ttl_class), entry)| {
                if entry.expires_at_unix_secs <= now_unix_secs {
                    return None;
                }
                if entry.token_estimate_source != V3_TOKEN_ESTIMATE_SOURCE {
                    return None;
                }
                let requested =
                    request_breakpoint_hashes
                        .iter()
                        .any(|(request_hash, request_ttl)| {
                            request_hash == prefix_hash
                                && ttl_matches_request(*request_ttl, *entry_ttl_class)
                        });
                requested.then(|| WarmCacheEntry {
                    prefix_hash: prefix_hash.clone(),
                    expires_at_unix_secs: entry.expires_at_unix_secs,
                    ttl_class: entry.ttl_class,
                    last_observed_at_unix_secs: entry.last_observed_at_unix_secs,
                    content_block_index: entry.prefix_content_block_index,
                    estimated_prefix_tokens: entry.estimated_prefix_tokens,
                    token_estimate_source: entry.token_estimate_source.clone(),
                    hash_schema_version: HASH_SCHEMA_VERSION,
                })
            })
            .collect();

        snapshot.sort_by(|left, right| {
            right
                .last_observed_at_unix_secs
                .cmp(&left.last_observed_at_unix_secs)
        });
        snapshot
    }

    pub fn lookup_warm_entry(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        prefix_hash: &str,
        eligible_ttls: &[TtlClass],
        now_unix_secs: u64,
    ) -> Option<WarmCacheEntry> {
        let partition_key = (upstream_id, canonical_model.to_owned());
        if self.overloaded.read().contains(&partition_key) {
            return None;
        }
        let guard = self.entries.read();
        let entries = guard.get(&partition_key)?;
        let mut best: Option<WarmCacheEntry> = None;
        for ttl in eligible_ttls {
            record_map_entry_inspected();
            let key = (prefix_hash.to_owned(), *ttl);
            let Some(entry) = entries.get(&key) else {
                continue;
            };
            if entry.expires_at_unix_secs <= now_unix_secs {
                continue;
            }
            if entry.token_estimate_source != V3_TOKEN_ESTIMATE_SOURCE {
                continue;
            }
            let candidate = WarmCacheEntry {
                prefix_hash: prefix_hash.to_owned(),
                expires_at_unix_secs: entry.expires_at_unix_secs,
                ttl_class: entry.ttl_class,
                last_observed_at_unix_secs: entry.last_observed_at_unix_secs,
                content_block_index: entry.prefix_content_block_index,
                estimated_prefix_tokens: entry.estimated_prefix_tokens,
                token_estimate_source: entry.token_estimate_source.clone(),
                hash_schema_version: HASH_SCHEMA_VERSION,
            };
            best = match best {
                Some(current) if current.expires_at_unix_secs >= candidate.expires_at_unix_secs => {
                    Some(current)
                }
                _ => Some(candidate),
            };
        }
        best
    }

    pub fn refresh_on_hit(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        prefix_hash: &str,
        ttl_class: TtlClass,
        now_unix_secs: u64,
    ) -> bool {
        let mut guard = self.entries.write();
        let Some(entries) = guard.get_mut(&(upstream_id, canonical_model.to_owned())) else {
            return false;
        };
        let key = (prefix_hash.to_owned(), ttl_class);
        let Some(entry) = entries.get_mut(&key) else {
            return false;
        };

        // Deliberately does NOT extend `expires_at_unix_secs`. A verified hit already refreshes it
        // via the `Kind::Hit` observation, which carries a fresh `now + ttl - grace` and is written
        // through `upsert_observation` in
        // `lifecycle_prompt_cache_observation_subscriber::handle_event`. This method owns only the
        // last-observed timestamp and the storage write-through debounce.
        let should_persist = now_unix_secs.saturating_sub(entry.last_persisted_at_unix_secs)
            > self.refresh_debounce_secs;
        entry.last_observed_at_unix_secs = now_unix_secs;
        if should_persist {
            entry.last_persisted_at_unix_secs = now_unix_secs;
        }
        should_persist
    }

    pub fn thread_usage_score(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        thread_id: &str,
        now_unix_secs: u64,
    ) -> Option<CacheScore> {
        let guard = self.thread_usage.read();
        let entry = guard
            .get(&upstream_id)?
            .get(&(canonical_model.to_owned(), thread_id.to_owned()))?;
        if entry.expires_at_unix_secs <= now_unix_secs {
            return None;
        }
        Some(CacheScore {
            predicted_cache_read_tokens: entry.predicted_cache_read_tokens,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            predicted_expires_at_unix_secs: Some(entry.expires_at_unix_secs),
            matched_breakpoint_index: None,
            confidence: 0.5,
            ambiguity_reason: Some("thread_usage_lineage".to_owned()),
            matched_v3_cache_key: None,
            breakpoint_content_block_index: None,
            matched_content_block_index: None,
            lookback_distance: None,
            token_estimate_source: None,
        })
    }

    pub fn record_thread_usage(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        thread_id: &str,
        usage: PromptCacheThreadUsage,
        now_unix_secs: u64,
    ) {
        if canonical_model.is_empty() || thread_id.is_empty() {
            return;
        }
        // Analysis-only lineage measurement for v3 post-hoc validation. This must not feed routing, WRH keying, or candidate scoring. Delete after v3 validation proves it is no longer needed.
        let creation_equivalent = usage
            .cache_creation_input_tokens_5m
            .saturating_add(usage.cache_creation_input_tokens_1h)
            / CREATION_READ_EQUIVALENT_DIVISOR;
        let predicted_cache_read_tokens = usage.cache_read_input_tokens.max(creation_equivalent);
        if predicted_cache_read_tokens == 0 {
            return;
        }
        let expires_at_unix_secs = now_unix_secs
            .saturating_add(THREAD_USAGE_TTL_SECS)
            .saturating_sub(self.grace_margin_secs);
        let mut guard = self.thread_usage.write();
        let entries = guard.entry(upstream_id).or_default();
        entries.insert(
            (canonical_model.to_owned(), thread_id.to_owned()),
            ThreadUsageEntry {
                predicted_cache_read_tokens: saturating_u64_to_u32(predicted_cache_read_tokens),
                expires_at_unix_secs,
                last_observed_at_unix_secs: now_unix_secs,
            },
        );
        if entries.len() > THREAD_USAGE_CAP_PER_UPSTREAM
            && let Some(oldest_key) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_observed_at_unix_secs)
                .map(|(key, _)| key.clone())
        {
            entries.remove(&oldest_key);
        }
    }
}

fn saturating_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
thread_local! {
    static MAP_ENTRIES_INSPECTED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn map_entries_inspected_count() -> u64 {
    MAP_ENTRIES_INSPECTED.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_map_entries_inspected() {
    MAP_ENTRIES_INSPECTED.with(|inspected| inspected.set(0));
}

#[inline]
fn record_map_entry_inspected() {
    #[cfg(test)]
    MAP_ENTRIES_INSPECTED.with(|inspected| inspected.set(inspected.get() + 1));
}

impl PromptCacheObservationCacheLike for PromptCacheObservationCache {
    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        request_breakpoint_hashes: &[(String, TtlClass)],
        now_unix_secs: u64,
    ) -> Vec<WarmCacheEntry> {
        Self::snapshot_for_upstream(
            self,
            upstream_id,
            canonical_model,
            request_breakpoint_hashes,
            now_unix_secs,
        )
    }

    fn lookup_warm_entry(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        prefix_hash: &str,
        eligible_ttls: &[TtlClass],
        now_unix_secs: u64,
    ) -> Option<WarmCacheEntry> {
        Self::lookup_warm_entry(
            self,
            upstream_id,
            canonical_model,
            prefix_hash,
            eligible_ttls,
            now_unix_secs,
        )
    }

    fn upsert_observation(&self, observation: PromptCacheObservationInput) {
        let PromptCacheObservationInput {
            upstream_id,
            canonical_model,
            prefix_hash,
            ttl_class,
            expires_at_unix_secs,
            observed_at_unix_secs,
            prefix_content_block_index,
            estimated_prefix_tokens,
            token_estimate_source,
        } = observation;
        Self::upsert_observation(
            self,
            PromptCacheObservationUpsert {
                upstream_id,
                canonical_model,
                prefix_hash,
                ttl_class,
                expires_at_unix_secs,
                last_observed_at_unix_secs: observed_at_unix_secs,
                prefix_content_block_index,
                estimated_prefix_tokens,
                token_estimate_source,
            },
        );
    }

    fn refresh_on_hit(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        prefix_hash: &str,
        ttl_class: TtlClass,
        now_unix_secs: u64,
    ) -> bool {
        Self::refresh_on_hit(
            self,
            upstream_id,
            canonical_model,
            prefix_hash,
            ttl_class,
            now_unix_secs,
        )
    }

    fn thread_usage_score(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        thread_id: &str,
        now_unix_secs: u64,
    ) -> Option<CacheScore> {
        Self::thread_usage_score(self, upstream_id, canonical_model, thread_id, now_unix_secs)
    }

    fn record_thread_usage(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        thread_id: &str,
        usage: PromptCacheThreadUsage,
        now_unix_secs: u64,
    ) {
        Self::record_thread_usage(
            self,
            upstream_id,
            canonical_model,
            thread_id,
            usage,
            now_unix_secs,
        );
    }

    fn grace_margin_secs(&self) -> u64 {
        Self::grace_margin_secs(self)
    }

    fn clock_now_unix_secs(&self) -> u64 {
        unix_secs(self.clock.now())
    }
}

fn ttl_matches_request(request_ttl: TtlClass, entry_ttl: TtlClass) -> bool {
    match request_ttl {
        TtlClass::Ephemeral5m => true,
        TtlClass::Ephemeral1h => entry_ttl == TtlClass::Ephemeral1h,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use async_trait::async_trait;
    use cc_lb_domain::TtlClass as StorageTtlClass;
    use cc_lb_engine::clock::{Clock, ClockHandle, TestClock};
    use cc_lb_engine::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;
    use cc_lb_storage_api::{
        PromptCacheObservationRecord, PromptCacheObservationStore, StorageResult,
    };
    use std::sync::{Arc, Mutex};

    use super::*;

    const BASE_TS: u64 = 1_700_000_000;
    const MODEL: &str = "claude-sonnet-4-5-20250929";

    fn test_cache() -> PromptCacheObservationCache {
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
        PromptCacheObservationCache::new(clock, 30)
    }

    fn base_now() -> u64 {
        unix_secs(TestClock::new_at_secs(BASE_TS).now())
    }

    fn upsert(
        cache: &PromptCacheObservationCache,
        upstream_id: Uuid,
        prefix_hash: &str,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) {
        cache.upsert_observation(PromptCacheObservationUpsert {
            upstream_id,
            canonical_model: MODEL.to_owned(),
            prefix_hash: prefix_hash.to_owned(),
            ttl_class,
            expires_at_unix_secs,
            last_observed_at_unix_secs: now_unix_secs,
            prefix_content_block_index: 0,
            estimated_prefix_tokens: 0,
            token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
        });
    }

    #[test]
    fn trait_upsert_preserves_live_metadata_before_hydration() {
        let cache = test_cache();
        let upstream_id = Uuid::new_v4();
        let now_unix_secs = base_now();

        PromptCacheObservationCacheLike::upsert_observation(
            &cache,
            PromptCacheObservationInput {
                upstream_id,
                canonical_model: MODEL.to_owned(),
                prefix_hash: "live-prefix".to_owned(),
                ttl_class: TtlClass::Ephemeral5m,
                expires_at_unix_secs: now_unix_secs + 300,
                observed_at_unix_secs: now_unix_secs,
                prefix_content_block_index: 2,
                estimated_prefix_tokens: 1_234,
                token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
            },
        );

        let entry = (&cache as &dyn PromptCacheObservationCacheLike)
            .lookup_warm_entry(
                upstream_id,
                MODEL,
                "live-prefix",
                &[TtlClass::Ephemeral5m],
                now_unix_secs,
            )
            .expect("live trait upsert is immediately available before hydration");
        assert_eq!(entry.content_block_index, 2);
        assert_eq!(entry.estimated_prefix_tokens, 1_234);
        assert_eq!(entry.token_estimate_source, V3_TOKEN_ESTIMATE_SOURCE);
        assert_eq!(entry.hash_schema_version, HASH_SCHEMA_VERSION);
    }

    #[test]
    fn overloaded_partition_rejects_new_entries_and_serves_cache_negative() {
        let cache = test_cache().with_max_entries_per_partition(4);
        let upstream = Uuid::new_v4();
        let now = base_now();
        for index in 0..4 {
            upsert(
                &cache,
                upstream,
                &format!("k{index}"),
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            );
        }
        assert_eq!(cache.map_len(), 4);
        upsert(
            &cache,
            upstream,
            "overflow",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        assert_eq!(cache.map_len(), 4);
        assert!(
            cache
                .lookup_warm_entry(
                    upstream,
                    MODEL,
                    "k0",
                    &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
                    now,
                )
                .is_none()
        );
        assert!(
            cache
                .lookup_warm_entry(upstream, MODEL, "overflow", &[TtlClass::Ephemeral5m], now)
                .is_none()
        );
    }

    #[test]
    fn overload_isolated_per_model() {
        const SECOND_MODEL: &str = "claude-opus-4-1-20250805";

        let cache = test_cache().with_max_entries_per_partition(2);
        let upstream = Uuid::new_v4();
        let now = base_now();
        for prefix_hash in ["model-a-0", "model-a-1", "model-a-overflow"] {
            upsert(
                &cache,
                upstream,
                prefix_hash,
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            );
        }
        cache.upsert_observation(PromptCacheObservationUpsert {
            upstream_id: upstream,
            canonical_model: SECOND_MODEL.to_owned(),
            prefix_hash: "model-b".to_owned(),
            ttl_class: TtlClass::Ephemeral5m,
            expires_at_unix_secs: now + 300,
            last_observed_at_unix_secs: now,
            prefix_content_block_index: 0,
            estimated_prefix_tokens: 0,
            token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
        });

        assert_eq!(cache.map_len(), 3);
        assert!(
            cache
                .entries
                .read()
                .get(&(upstream, SECOND_MODEL.to_owned()))
                .is_some_and(
                    |entries| entries.contains_key(&("model-b".to_owned(), TtlClass::Ephemeral5m,))
                )
        );
        assert!(
            cache
                .lookup_warm_entry(
                    upstream,
                    SECOND_MODEL,
                    "model-b",
                    &[TtlClass::Ephemeral5m],
                    now,
                )
                .is_some()
        );
        assert!(
            cache
                .lookup_warm_entry(upstream, MODEL, "model-a-0", &[TtlClass::Ephemeral5m], now,)
                .is_none()
        );
    }

    #[test]
    fn overloaded_partition_recovers_below_ninety_percent_after_sweep() {
        let cache = test_cache().with_max_entries_per_partition(10);
        let upstream = Uuid::new_v4();
        let now = base_now();
        for index in 0..3 {
            upsert(
                &cache,
                upstream,
                &format!("live{index}"),
                TtlClass::Ephemeral5m,
                now + 3600,
                now,
            );
        }
        for index in 0..7 {
            upsert(
                &cache,
                upstream,
                &format!("short{index}"),
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            );
        }
        assert_eq!(cache.map_len(), 10);
        upsert(
            &cache,
            upstream,
            "overflow",
            TtlClass::Ephemeral5m,
            now + 3600,
            now,
        );
        assert!(
            cache
                .lookup_warm_entry(upstream, MODEL, "live0", &[TtlClass::Ephemeral5m], now)
                .is_none()
        );
        assert_eq!(cache.sweep_expired(now + 600), 7);
        assert_eq!(cache.map_len(), 3);
        assert_eq!(
            cache
                .lookup_warm_entry(
                    upstream,
                    MODEL,
                    "live0",
                    &[TtlClass::Ephemeral5m],
                    now + 600
                )
                .map(|entry| entry.prefix_hash),
            Some("live0".to_owned())
        );
        upsert(
            &cache,
            upstream,
            "fresh",
            TtlClass::Ephemeral5m,
            now + 3600,
            now + 600,
        );
        assert_eq!(cache.map_len(), 4);
    }

    #[test]
    fn lookup_warm_entry_hit_at_window_head_and_tail() {
        let cache = test_cache();
        let upstream = Uuid::new_v4();
        let now = base_now();
        upsert(
            &cache,
            upstream,
            "head",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        upsert(
            &cache,
            upstream,
            "tail",
            TtlClass::Ephemeral1h,
            now + 3600,
            now,
        );
        let head = cache.lookup_warm_entry(
            upstream,
            MODEL,
            "head",
            &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
            now,
        );
        assert_eq!(head.map(|entry| entry.prefix_hash), Some("head".to_owned()));
        let tail = cache.lookup_warm_entry(upstream, MODEL, "tail", &[TtlClass::Ephemeral1h], now);
        assert_eq!(tail.map(|entry| entry.prefix_hash), Some("tail".to_owned()));
        assert!(
            cache
                .lookup_warm_entry(
                    upstream,
                    MODEL,
                    "absent",
                    &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
                    now,
                )
                .is_none()
        );
    }

    #[test]
    fn lookup_warm_entry_ineligible_ttl_returns_none() {
        let cache = test_cache();
        let upstream = Uuid::new_v4();
        let now = base_now();
        upsert(
            &cache,
            upstream,
            "k5m",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        assert!(
            cache
                .lookup_warm_entry(upstream, MODEL, "k5m", &[TtlClass::Ephemeral1h], now)
                .is_none()
        );
        upsert(
            &cache,
            upstream,
            "k1h",
            TtlClass::Ephemeral1h,
            now + 3600,
            now,
        );
        let hit = cache.lookup_warm_entry(
            upstream,
            MODEL,
            "k1h",
            &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
            now,
        );
        assert_eq!(
            hit.map(|entry| entry.ttl_class),
            Some(TtlClass::Ephemeral1h)
        );
    }

    #[test]
    fn same_key_greatest_expiry_eligible() {
        let cache = test_cache();
        let upstream = Uuid::new_v4();
        let now = base_now();
        upsert(
            &cache,
            upstream,
            "shared",
            TtlClass::Ephemeral5m,
            now + 9000,
            now,
        );
        upsert(
            &cache,
            upstream,
            "shared",
            TtlClass::Ephemeral1h,
            now + 3600,
            now,
        );
        let for_1h = cache
            .lookup_warm_entry(upstream, MODEL, "shared", &[TtlClass::Ephemeral1h], now)
            .expect("1h entry");
        assert_eq!(for_1h.ttl_class, TtlClass::Ephemeral1h);
        assert_eq!(for_1h.expires_at_unix_secs, now + 3600);
        let for_5m = cache
            .lookup_warm_entry(
                upstream,
                MODEL,
                "shared",
                &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
                now,
            )
            .expect("either entry");
        assert_eq!(for_5m.expires_at_unix_secs, now + 9000);
    }

    #[test]
    fn lookup_warm_entry_expired_returns_none() {
        let cache = test_cache();
        let upstream = Uuid::new_v4();
        let now = base_now();
        upsert(&cache, upstream, "old", TtlClass::Ephemeral5m, now, now);
        assert!(
            cache
                .lookup_warm_entry(
                    upstream,
                    MODEL,
                    "old",
                    &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
                    now,
                )
                .is_none()
        );
    }

    #[test]
    fn lookup_warm_entry_inspects_bounded_entries() {
        let cache = test_cache();
        let upstream = Uuid::new_v4();
        let now = base_now();
        for index in 0..1000 {
            upsert(
                &cache,
                upstream,
                &format!("noise-{index}"),
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            );
        }
        upsert(
            &cache,
            upstream,
            "target",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        reset_map_entries_inspected();
        let hit = cache.lookup_warm_entry(
            upstream,
            MODEL,
            "target",
            &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
            now,
        );
        assert!(hit.is_some());
        assert!(map_entries_inspected_count() <= 2);
    }

    #[test]
    fn deep_entry_survives_beyond_recency_without_warm_set_cap() {
        let cache = test_cache();
        let upstream = Uuid::new_v4();
        let now = base_now();
        upsert(
            &cache,
            upstream,
            "deep",
            TtlClass::Ephemeral5m,
            now + 3600,
            now,
        );
        for index in 0u64..200 {
            upsert(
                &cache,
                upstream,
                &format!("recent-{index}"),
                TtlClass::Ephemeral5m,
                now + 3600,
                now + 1 + index,
            );
        }
        let hit = cache.lookup_warm_entry(
            upstream,
            MODEL,
            "deep",
            &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
            now + 10,
        );
        assert!(hit.is_some());
        assert_eq!(cache.map_len(), 201);
    }

    #[test]
    fn expiry_eviction_shrinks_map() {
        let cache = test_cache();
        let upstream = Uuid::new_v4();
        let now = base_now();
        upsert(
            &cache,
            upstream,
            "live-a",
            TtlClass::Ephemeral5m,
            now + 900,
            now,
        );
        upsert(
            &cache,
            upstream,
            "live-b",
            TtlClass::Ephemeral1h,
            now + 900,
            now,
        );
        upsert(
            &cache,
            upstream,
            "expired-a",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        upsert(
            &cache,
            upstream,
            "expired-b",
            TtlClass::Ephemeral1h,
            now + 300,
            now,
        );
        assert_eq!(cache.map_len(), 4);

        let removed = cache.sweep_expired(now + 600);
        assert_eq!(removed, 2);
        assert_eq!(cache.map_len(), 2);

        assert!(
            cache
                .lookup_warm_entry(
                    upstream,
                    MODEL,
                    "expired-a",
                    &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
                    now + 600,
                )
                .is_none()
        );
        assert!(
            cache
                .lookup_warm_entry(
                    upstream,
                    MODEL,
                    "live-a",
                    &[TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
                    now + 600,
                )
                .is_some()
        );
    }

    #[derive(Clone, Default)]
    struct MockStore {
        records: Arc<Mutex<Vec<PromptCacheObservationRecord>>>,
        purge_calls: Arc<Mutex<Vec<u64>>>,
    }

    impl MockStore {
        fn new(records: Vec<PromptCacheObservationRecord>) -> Self {
            Self {
                records: Arc::new(Mutex::new(records)),
                purge_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl PromptCacheObservationStore for MockStore {
        async fn upsert_observation(
            &self,
            record: &PromptCacheObservationRecord,
        ) -> StorageResult<()> {
            self.records.lock().unwrap().push(record.clone());
            Ok(())
        }

        async fn list_active_for_upstream(
            &self,
            upstream_id: Uuid,
            not_expired_at_unix_secs: u64,
        ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .iter()
                .filter(|record| {
                    record.upstream_id == upstream_id
                        && record.expires_at_unix_secs > not_expired_at_unix_secs
                })
                .cloned()
                .collect())
        }

        async fn purge_expired_before(&self, ts_unix_secs: u64) -> StorageResult<u64> {
            self.purge_calls.lock().unwrap().push(ts_unix_secs);
            let mut records = self.records.lock().unwrap();
            let before = records.len();
            records.retain(|record| record.expires_at_unix_secs >= ts_unix_secs);
            Ok((before - records.len()) as u64)
        }

        async fn count(&self) -> StorageResult<u64> {
            Ok(self.records.lock().unwrap().len() as u64)
        }
    }

    fn storage_record(
        upstream_id: Uuid,
        prefix_hash: &str,
        ttl_class: StorageTtlClass,
        expires_at_unix_secs: u64,
        last_observed_at_unix_secs: u64,
        hash_schema_version: u8,
    ) -> PromptCacheObservationRecord {
        PromptCacheObservationRecord {
            upstream_id,
            canonical_model_id: MODEL.to_owned(),
            v3_prefix_key: prefix_hash.to_owned(),
            ttl_class,
            expires_at_unix_secs,
            last_observed_at_unix_secs,
            hash_schema_version,
            prefix_content_block_index: 0,
            estimated_prefix_tokens: 0,
            token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
        }
    }

    #[tokio::test]
    async fn hydrate_filters() {
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
        let cache = PromptCacheObservationCache::new(clock, 30);
        let upstream_id = Uuid::new_v4();
        let store = MockStore::new(vec![
            storage_record(
                upstream_id,
                "active",
                StorageTtlClass::Ephemeral5m,
                BASE_TS + 300,
                BASE_TS - 10,
                HASH_SCHEMA_VERSION,
            ),
            storage_record(
                upstream_id,
                "expired",
                StorageTtlClass::Ephemeral5m,
                BASE_TS,
                BASE_TS - 20,
                HASH_SCHEMA_VERSION,
            ),
            storage_record(
                upstream_id,
                "schema-v1",
                StorageTtlClass::Ephemeral5m,
                BASE_TS + 300,
                BASE_TS - 30,
                1,
            ),
        ]);

        let loaded = cache
            .hydrate_from_store(&store, &[upstream_id])
            .await
            .unwrap();

        assert_eq!(loaded, 1);
        let snapshot = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &[
                ("active".to_owned(), TtlClass::Ephemeral5m),
                ("expired".to_owned(), TtlClass::Ephemeral5m),
                ("schema-v1".to_owned(), TtlClass::Ephemeral5m),
            ],
            BASE_TS,
        );
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].prefix_hash, "active");
    }

    #[tokio::test]
    async fn hydrate_cold_starts_previous_schema_and_keeps_current() {
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
        let cache = PromptCacheObservationCache::new(clock, 30);
        let upstream_id = Uuid::new_v4();
        let store = MockStore::new(vec![
            storage_record(
                upstream_id,
                "current-schema",
                StorageTtlClass::Ephemeral5m,
                BASE_TS + 300,
                BASE_TS - 10,
                HASH_SCHEMA_VERSION,
            ),
            storage_record(
                upstream_id,
                "previous-schema",
                StorageTtlClass::Ephemeral5m,
                BASE_TS + 300,
                BASE_TS - 20,
                HASH_SCHEMA_VERSION - 1,
            ),
        ]);

        let loaded = cache
            .hydrate_from_store(&store, &[upstream_id])
            .await
            .unwrap();

        assert_eq!(
            HASH_SCHEMA_VERSION, 5,
            "ADR 0008 schema-5 amendment pins schema v5"
        );

        assert_eq!(
            loaded, 1,
            "only records carrying the current hash schema version hydrate"
        );
        let snapshot = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &[
                ("current-schema".to_owned(), TtlClass::Ephemeral5m),
                ("previous-schema".to_owned(), TtlClass::Ephemeral5m),
            ],
            BASE_TS,
        );
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].prefix_hash, "current-schema");
    }

    #[test]
    fn refresh_debounce() {
        let clock = Arc::new(TestClock::new_at_secs(BASE_TS));
        let cache = PromptCacheObservationCache::new_with_debounce(clock.clone(), 30, 60);
        let upstream_id = Uuid::new_v4();

        upsert(
            &cache,
            upstream_id,
            "debounced",
            TtlClass::Ephemeral5m,
            BASE_TS + 300,
            unix_secs(clock.now()),
        );

        clock.advance_secs(30);
        assert!(!cache.refresh_on_hit(
            upstream_id,
            MODEL,
            "debounced",
            TtlClass::Ephemeral5m,
            unix_secs(clock.now()),
        ));
        let partition_key = (upstream_id, MODEL.to_owned());
        let key = ("debounced".to_owned(), TtlClass::Ephemeral5m);
        {
            let guard = cache.entries.read();
            let entry = guard.get(&partition_key).unwrap().get(&key).unwrap();
            assert_eq!(entry.last_observed_at_unix_secs, BASE_TS + 30);
            assert_eq!(entry.last_persisted_at_unix_secs, BASE_TS);
        }

        clock.advance_secs(40);
        assert!(cache.refresh_on_hit(
            upstream_id,
            MODEL,
            "debounced",
            TtlClass::Ephemeral5m,
            unix_secs(clock.now()),
        ));
        let guard = cache.entries.read();
        let entry = guard.get(&partition_key).unwrap().get(&key).unwrap();
        assert_eq!(entry.last_observed_at_unix_secs, BASE_TS + 70);
        assert_eq!(entry.last_persisted_at_unix_secs, BASE_TS + 70);
    }

    #[tokio::test]
    async fn hydrate_returns_zero_when_store_empty() {
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
        let cache = PromptCacheObservationCache::new(clock, 30);
        let upstream_id = Uuid::new_v4();
        let store = MockStore::default();

        let loaded = cache
            .hydrate_from_store(&store, &[upstream_id])
            .await
            .unwrap();

        assert_eq!(loaded, 0);
        let snapshot = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &[("missing".to_owned(), TtlClass::Ephemeral5m)],
            BASE_TS,
        );
        assert!(snapshot.is_empty());
    }

    #[test]
    fn snapshot_asymmetric_ttl() {
        let cache = test_cache();
        let upstream_id = Uuid::new_v4();
        let now = base_now();

        upsert(
            &cache,
            upstream_id,
            "shared",
            TtlClass::Ephemeral5m,
            now + 60,
            now + 1,
        );
        upsert(
            &cache,
            upstream_id,
            "shared",
            TtlClass::Ephemeral1h,
            now + 3_600,
            now + 2,
        );

        let five_minute_request = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &[("shared".to_owned(), TtlClass::Ephemeral5m)],
            now,
        );
        assert_eq!(five_minute_request.len(), 2);
        assert_eq!(five_minute_request[0].ttl_class, TtlClass::Ephemeral1h);
        assert_eq!(five_minute_request[1].ttl_class, TtlClass::Ephemeral5m);

        let one_hour_request = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &[("shared".to_owned(), TtlClass::Ephemeral1h)],
            now,
        );
        assert_eq!(one_hour_request.len(), 1);
        assert_eq!(one_hour_request[0].ttl_class, TtlClass::Ephemeral1h);
    }

    #[test]
    fn upsert_replaces_existing_and_preserves_last_persisted_at() {
        let cache = test_cache();
        let upstream_id = Uuid::new_v4();
        let now = base_now();
        let partition_key = (upstream_id, MODEL.to_owned());
        let key = ("same-prefix".to_owned(), TtlClass::Ephemeral5m);

        upsert(
            &cache,
            upstream_id,
            "same-prefix",
            TtlClass::Ephemeral5m,
            now + 60,
            now,
        );
        upsert(
            &cache,
            upstream_id,
            "same-prefix",
            TtlClass::Ephemeral5m,
            now + 120,
            now + 10,
        );

        let guard = cache.entries.read();
        let entry = guard.get(&partition_key).unwrap().get(&key).unwrap();
        assert_eq!(entry.expires_at_unix_secs, now + 120);
        assert_eq!(entry.last_observed_at_unix_secs, now + 10);
        assert_eq!(entry.last_persisted_at_unix_secs, now);
        assert_eq!(entry.ttl_class, TtlClass::Ephemeral5m);
    }

    #[test]
    fn snapshot_excludes_expired_at_now() {
        let cache = test_cache();
        let upstream_id = Uuid::new_v4();
        let now = base_now();

        upsert(
            &cache,
            upstream_id,
            "expires-now",
            TtlClass::Ephemeral5m,
            now,
            now + 1,
        );
        upsert(
            &cache,
            upstream_id,
            "active",
            TtlClass::Ephemeral5m,
            now + 1,
            now + 2,
        );

        let snapshot = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &[
                ("expires-now".to_owned(), TtlClass::Ephemeral5m),
                ("active".to_owned(), TtlClass::Ephemeral5m),
            ],
            now,
        );

        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].prefix_hash, "active");
    }

    #[test]
    fn hit_upsert_extends_expiry_and_keeps_snapshot_warm() {
        let cache = test_cache();
        let upstream_id = Uuid::new_v4();
        let now = base_now();
        let original_expiry = now + 60;
        let refreshed_expiry = now + 270;
        let request_breakpoints = [("system".to_owned(), TtlClass::Ephemeral5m)];

        upsert(
            &cache,
            upstream_id,
            "system",
            TtlClass::Ephemeral5m,
            original_expiry,
            now,
        );

        let baseline =
            cache.snapshot_for_upstream(upstream_id, MODEL, &request_breakpoints, original_expiry);
        assert!(
            baseline.is_empty(),
            "sanity: without sliding refresh the entry would be filtered at its original expiry"
        );

        upsert(
            &cache,
            upstream_id,
            "system",
            TtlClass::Ephemeral5m,
            refreshed_expiry,
            now + 30,
        );

        let after_refresh = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &request_breakpoints,
            original_expiry + 60,
        );
        assert_eq!(
            after_refresh.len(),
            1,
            "sliding refresh must keep the entry warm past the original expiry"
        );
        assert_eq!(after_refresh[0].prefix_hash, "system");
        assert_eq!(after_refresh[0].expires_at_unix_secs, refreshed_expiry);
    }

    #[test]
    fn snapshot_filters_to_request_breakpoints_only() {
        let cache = test_cache();
        let upstream_id = Uuid::new_v4();
        let now = base_now();

        upsert(
            &cache,
            upstream_id,
            "requested",
            TtlClass::Ephemeral5m,
            now + 60,
            now + 1,
        );
        upsert(
            &cache,
            upstream_id,
            "unrequested",
            TtlClass::Ephemeral5m,
            now + 60,
            now + 2,
        );

        let snapshot = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &[("requested".to_owned(), TtlClass::Ephemeral5m)],
            now,
        );

        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].prefix_hash, "requested");
    }

    #[test]
    fn snapshot_ignores_other_upstream() {
        let cache = test_cache();
        let upstream_id = Uuid::new_v4();
        let other_upstream_id = Uuid::new_v4();
        let now = base_now();

        upsert(
            &cache,
            other_upstream_id,
            "same-prefix",
            TtlClass::Ephemeral5m,
            now + 60,
            now + 1,
        );

        let snapshot = cache.snapshot_for_upstream(
            upstream_id,
            MODEL,
            &[("same-prefix".to_owned(), TtlClass::Ephemeral5m)],
            now,
        );

        assert!(snapshot.is_empty());
    }
}
