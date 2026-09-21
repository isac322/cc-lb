use std::collections::HashMap;

use cc_lb_domain::CacheScore;
use cc_lb_engine::lifecycle::{PromptCacheThreadUsage, PromptCacheThreadUsageTrackerLike};
use parking_lot::RwLock;
use uuid::Uuid;

const THREAD_USAGE_CAP_PER_UPSTREAM: usize = 2048;
const THREAD_USAGE_TTL_SECS: u64 = 5 * 60;
const CREATION_READ_EQUIVALENT_DIVISOR: u64 = 4;
type ThreadUsageKey = (String, String);
type ThreadUsageByUpstream = HashMap<Uuid, HashMap<ThreadUsageKey, ThreadUsageEntry>>;

/// Process-local diagnostic tracker for thread-lineage cache usage.
///
/// This is NOT a warmth authority: routing reads prompt-cache observations
/// from the shared store directly. The tracker only feeds the
/// `thread_usage_lineage` counterfactual telemetry recorded on request events.
/// Entries are pod-local, TTL-bound, and capped per upstream.
pub struct PromptCacheThreadUsageTracker {
    thread_usage: RwLock<ThreadUsageByUpstream>,
    grace_margin_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ThreadUsageEntry {
    predicted_cache_read_tokens: u32,
    expires_at_unix_secs: u64,
    last_observed_at_unix_secs: u64,
}

impl PromptCacheThreadUsageTracker {
    pub fn new(grace_margin_secs: u64) -> Self {
        Self {
            thread_usage: RwLock::new(HashMap::new()),
            grace_margin_secs,
        }
    }

    pub fn grace_margin_secs(&self) -> u64 {
        self.grace_margin_secs
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
        // Creation-only lineage measurement for v3 post-hit validation.
        // This must not feed routing. Delete after v3 validation proves it
        // is no longer needed.
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
        if entries.len() > THREAD_USAGE_CAP_PER_UPSTREAM {
            let oldest_key = entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_observed_at_unix_secs)
                .map(|(key, _)| key.clone());
            if let Some(oldest_key) = oldest_key {
                entries.remove(&oldest_key);
            }
        }
    }
}

fn saturating_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

impl PromptCacheThreadUsageTrackerLike for PromptCacheThreadUsageTracker {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const MODEL: &str = "claude-sonnet-4-5-20250929";
    const BASE_TS: u64 = 1_700_000_000;

    fn usage(read: u64, create_5m: u64, create_1h: u64) -> PromptCacheThreadUsage {
        PromptCacheThreadUsage {
            cache_read_input_tokens: read,
            cache_creation_input_tokens_5m: create_5m,
            cache_creation_input_tokens_1h: create_1h,
        }
    }

    #[test]
    fn recorded_thread_usage_scores_until_ttl_minus_grace() {
        let tracker = PromptCacheThreadUsageTracker::new(30);
        let upstream_id = Uuid::new_v4();
        tracker.record_thread_usage(upstream_id, MODEL, "thread-1", usage(1_024, 0, 0), BASE_TS);

        let score = tracker
            .thread_usage_score(upstream_id, MODEL, "thread-1", BASE_TS + 60)
            .expect("fresh thread usage scores");
        assert_eq!(score.predicted_cache_read_tokens, 1_024);
        assert_eq!(
            score.predicted_expires_at_unix_secs,
            Some(BASE_TS + THREAD_USAGE_TTL_SECS - 30)
        );
        assert_eq!(
            score.ambiguity_reason.as_deref(),
            Some("thread_usage_lineage")
        );

        let after_expiry = BASE_TS + THREAD_USAGE_TTL_SECS - 30 + 1;
        assert!(
            tracker
                .thread_usage_score(upstream_id, MODEL, "thread-1", after_expiry)
                .is_none(),
            "entry past expires_at must not score"
        );
    }

    #[test]
    fn creation_only_usage_uses_read_equivalent() {
        let tracker = PromptCacheThreadUsageTracker::new(30);
        let upstream_id = Uuid::new_v4();
        tracker.record_thread_usage(upstream_id, MODEL, "thread-1", usage(0, 400, 400), BASE_TS);

        let score = tracker
            .thread_usage_score(upstream_id, MODEL, "thread-1", BASE_TS)
            .expect("creation-only lineage still scores");
        assert_eq!(score.predicted_cache_read_tokens, 200);
    }

    #[test]
    fn zero_usage_records_nothing() {
        let tracker = PromptCacheThreadUsageTracker::new(30);
        let upstream_id = Uuid::new_v4();
        tracker.record_thread_usage(upstream_id, MODEL, "thread-1", usage(0, 0, 0), BASE_TS);
        assert!(
            tracker
                .thread_usage_score(upstream_id, MODEL, "thread-1", BASE_TS)
                .is_none()
        );
    }

    #[test]
    fn empty_model_or_thread_records_nothing() {
        let tracker = PromptCacheThreadUsageTracker::new(30);
        let upstream_id = Uuid::new_v4();
        tracker.record_thread_usage(upstream_id, "", "thread-1", usage(10, 0, 0), BASE_TS);
        tracker.record_thread_usage(upstream_id, MODEL, "", usage(10, 0, 0), BASE_TS);
        assert!(
            tracker
                .thread_usage_score(upstream_id, MODEL, "thread-1", BASE_TS)
                .is_none()
        );
    }

    #[test]
    fn cap_evicts_oldest_entry_per_upstream() {
        let tracker = PromptCacheThreadUsageTracker::new(30);
        let upstream_id = Uuid::new_v4();
        for index in 0..THREAD_USAGE_CAP_PER_UPSTREAM {
            tracker.record_thread_usage(
                upstream_id,
                MODEL,
                &format!("thread-{index}"),
                usage(1, 0, 0),
                BASE_TS + index as u64,
            );
        }
        tracker.record_thread_usage(
            upstream_id,
            MODEL,
            "thread-newest",
            usage(1, 0, 0),
            BASE_TS + THREAD_USAGE_CAP_PER_UPSTREAM as u64,
        );

        assert!(
            tracker
                .thread_usage_score(upstream_id, MODEL, "thread-0", BASE_TS)
                .is_none(),
            "oldest entry evicted once the per-upstream cap is exceeded"
        );
        assert!(
            tracker
                .thread_usage_score(upstream_id, MODEL, "thread-newest", BASE_TS)
                .is_some()
        );
    }
}
