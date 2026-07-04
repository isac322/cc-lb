use std::collections::HashMap;

use cc_lb_core::clock::{ClockHandle, unix_secs};
use cc_lb_core::lifecycle::PromptCacheObservationCacheLike;
use cc_lb_plugin_api::types::{TtlClass, WarmCacheEntry};
use cc_lb_storage_api::{PromptCacheObservationStore, StorageResult};
use parking_lot::RwLock;
use uuid::Uuid;

pub const HASH_SCHEMA_VERSION: u8 = cc_lb_core::lifecycle::HASH_SCHEMA_VERSION;

const DEFAULT_WARM_SET_CAP: usize = 32;
const DEFAULT_REFRESH_DEBOUNCE_SECS: u64 = 60;

pub struct PromptCacheObservationCache {
    #[allow(clippy::type_complexity)]
    entries: RwLock<
        HashMap<
            Uuid,
            HashMap<
                (
                    String, /*canonical_model*/
                    String, /*prefix_hash*/
                    TtlClass,
                ),
                CacheEntry,
            >,
        >,
    >,
    #[allow(dead_code)]
    clock: ClockHandle,
    grace_margin_secs: u64,
    warm_set_cap: usize,
    refresh_debounce_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheEntry {
    pub expires_at_unix_secs: u64,
    pub last_observed_at_unix_secs: u64,
    pub ttl_class: TtlClass,
    pub last_persisted_at_unix_secs: u64,
}

impl PromptCacheObservationCache {
    pub fn new(clock: ClockHandle, grace_margin_secs: u64, warm_set_cap: usize) -> Self {
        Self::new_with_debounce(
            clock,
            grace_margin_secs,
            warm_set_cap,
            DEFAULT_REFRESH_DEBOUNCE_SECS,
        )
    }

    pub fn new_with_debounce(
        clock: ClockHandle,
        grace_margin_secs: u64,
        warm_set_cap: usize,
        refresh_debounce_secs: u64,
    ) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            clock,
            grace_margin_secs,
            warm_set_cap: if warm_set_cap == 0 {
                DEFAULT_WARM_SET_CAP
            } else {
                warm_set_cap
            },
            refresh_debounce_secs,
        }
    }

    pub fn grace_margin_secs(&self) -> u64 {
        self.grace_margin_secs
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
                self.upsert_observation(
                    record.upstream_id,
                    record.canonical_model_id,
                    record.prefix_hash,
                    ttl_class_from_storage(record.ttl_class),
                    record.expires_at_unix_secs,
                    record.last_observed_at_unix_secs,
                );
                loaded += 1;
            }
        }
        Ok(loaded)
    }

    pub fn upsert_observation(
        &self,
        upstream_id: Uuid,
        canonical_model: String,
        prefix_hash: String,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) {
        let mut guard = self.entries.write();
        let entries = guard.entry(upstream_id).or_default();
        let key = (canonical_model, prefix_hash, ttl_class);
        let last_persisted_at_unix_secs = entries
            .get(&key)
            .map_or(now_unix_secs, |entry| entry.last_persisted_at_unix_secs);
        entries.insert(
            key,
            CacheEntry {
                expires_at_unix_secs,
                last_observed_at_unix_secs: now_unix_secs,
                ttl_class,
                last_persisted_at_unix_secs,
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
        let Some(entries) = guard.get(&upstream_id) else {
            return Vec::new();
        };

        let mut snapshot: Vec<WarmCacheEntry> = entries
            .iter()
            .filter_map(|((entry_model, prefix_hash, entry_ttl_class), entry)| {
                if entry_model != canonical_model || entry.expires_at_unix_secs <= now_unix_secs {
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
                })
            })
            .collect();

        snapshot.sort_by(|left, right| {
            right
                .last_observed_at_unix_secs
                .cmp(&left.last_observed_at_unix_secs)
        });
        snapshot.truncate(self.warm_set_cap);
        snapshot
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
        let Some(entries) = guard.get_mut(&upstream_id) else {
            return false;
        };
        let key = (
            canonical_model.to_owned(),
            prefix_hash.to_owned(),
            ttl_class,
        );
        let Some(entry) = entries.get_mut(&key) else {
            return false;
        };

        let should_persist = now_unix_secs.saturating_sub(entry.last_persisted_at_unix_secs)
            > self.refresh_debounce_secs;
        entry.last_observed_at_unix_secs = now_unix_secs;
        if should_persist {
            entry.last_persisted_at_unix_secs = now_unix_secs;
        }
        should_persist
    }
}

fn ttl_class_from_storage(ttl_class: cc_lb_storage_api::TtlClass) -> TtlClass {
    match ttl_class {
        cc_lb_storage_api::TtlClass::Ephemeral5m => TtlClass::Ephemeral5m,
        cc_lb_storage_api::TtlClass::Ephemeral1h => TtlClass::Ephemeral1h,
    }
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

    fn upsert_observation(
        &self,
        upstream_id: Uuid,
        canonical_model: String,
        prefix_hash: String,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) {
        Self::upsert_observation(
            self,
            upstream_id,
            canonical_model,
            prefix_hash,
            ttl_class,
            expires_at_unix_secs,
            now_unix_secs,
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
    use cc_lb_core::clock::{Clock, ClockHandle, TestClock};
    use cc_lb_storage_api::{
        PromptCacheObservationRecord, PromptCacheObservationStore, StorageResult,
        TtlClass as StorageTtlClass,
    };
    use std::sync::{Arc, Mutex};

    use super::*;

    const BASE_TS: u64 = 1_700_000_000;
    const MODEL: &str = "claude-sonnet-4-5-20250929";

    fn cache_with_cap(warm_set_cap: usize) -> PromptCacheObservationCache {
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
        PromptCacheObservationCache::new(clock, 30, warm_set_cap)
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
        cache.upsert_observation(
            upstream_id,
            MODEL.to_owned(),
            prefix_hash.to_owned(),
            ttl_class,
            expires_at_unix_secs,
            now_unix_secs,
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
            prefix_hash: prefix_hash.to_owned(),
            ttl_class,
            expires_at_unix_secs,
            last_observed_at_unix_secs,
            hash_schema_version,
        }
    }

    #[tokio::test]
    async fn hydrate_filters() {
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
        let cache = PromptCacheObservationCache::new(clock, 30, 32);
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

    #[test]
    fn refresh_debounce() {
        let clock = Arc::new(TestClock::new_at_secs(BASE_TS));
        let cache = PromptCacheObservationCache::new_with_debounce(clock.clone(), 30, 32, 60);
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
        let key = (
            MODEL.to_owned(),
            "debounced".to_owned(),
            TtlClass::Ephemeral5m,
        );
        {
            let guard = cache.entries.read();
            let entry = guard.get(&upstream_id).unwrap().get(&key).unwrap();
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
        let entry = guard.get(&upstream_id).unwrap().get(&key).unwrap();
        assert_eq!(entry.last_observed_at_unix_secs, BASE_TS + 70);
        assert_eq!(entry.last_persisted_at_unix_secs, BASE_TS + 70);
    }

    #[tokio::test]
    async fn hydrate_returns_zero_when_store_empty() {
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
        let cache = PromptCacheObservationCache::new(clock, 30, 32);
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
        let cache = cache_with_cap(32);
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
    fn snapshot_cap() {
        let cache = cache_with_cap(2);
        let upstream_id = Uuid::new_v4();
        let now = base_now();

        let mut request_breakpoints = Vec::new();
        for index in 0..5 {
            let prefix_hash = format!("hash-{index}");
            upsert(
                &cache,
                upstream_id,
                &prefix_hash,
                TtlClass::Ephemeral5m,
                now + 300,
                now + index,
            );
            request_breakpoints.push((prefix_hash, TtlClass::Ephemeral5m));
        }

        let snapshot = cache.snapshot_for_upstream(upstream_id, MODEL, &request_breakpoints, now);

        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].prefix_hash, "hash-4");
        assert_eq!(snapshot[1].prefix_hash, "hash-3");
    }

    #[test]
    fn upsert_replaces_existing_and_preserves_last_persisted_at() {
        let cache = cache_with_cap(32);
        let upstream_id = Uuid::new_v4();
        let now = base_now();
        let key = (
            MODEL.to_owned(),
            "same-prefix".to_owned(),
            TtlClass::Ephemeral5m,
        );

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
        let entry = guard.get(&upstream_id).unwrap().get(&key).unwrap();
        assert_eq!(entry.expires_at_unix_secs, now + 120);
        assert_eq!(entry.last_observed_at_unix_secs, now + 10);
        assert_eq!(entry.last_persisted_at_unix_secs, now);
        assert_eq!(entry.ttl_class, TtlClass::Ephemeral5m);
    }

    #[test]
    fn snapshot_excludes_expired_at_now() {
        let cache = cache_with_cap(32);
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
        let cache = cache_with_cap(32);
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
        let cache = cache_with_cap(32);
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
        let cache = cache_with_cap(32);
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
