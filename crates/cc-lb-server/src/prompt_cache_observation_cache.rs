use std::collections::HashMap;

use cc_lb_core::clock::ClockHandle;
use cc_lb_plugin_api::types::{TtlClass, WarmCacheEntry};
use parking_lot::RwLock;
use uuid::Uuid;

const DEFAULT_WARM_SET_CAP: usize = 32;

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
    #[allow(dead_code)]
    grace_margin_secs: u64,
    warm_set_cap: usize,
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
        Self {
            entries: RwLock::new(HashMap::new()),
            clock,
            grace_margin_secs,
            warm_set_cap: if warm_set_cap == 0 {
                DEFAULT_WARM_SET_CAP
            } else {
                warm_set_cap
            },
        }
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
}

fn ttl_matches_request(request_ttl: TtlClass, entry_ttl: TtlClass) -> bool {
    match request_ttl {
        TtlClass::Ephemeral5m => true,
        TtlClass::Ephemeral1h => entry_ttl == TtlClass::Ephemeral1h,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use cc_lb_core::clock::{Clock, ClockHandle, TestClock};

    use super::*;

    const BASE_TS: u64 = 1_700_000_000;
    const MODEL: &str = "claude-sonnet-4-5-20250929";

    fn cache_with_cap(warm_set_cap: usize) -> PromptCacheObservationCache {
        let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
        PromptCacheObservationCache::new(clock, 30, warm_set_cap)
    }

    fn base_now() -> u64 {
        TestClock::new_at_secs(BASE_TS).now_unix_secs()
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
