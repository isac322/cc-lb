//! Cache-affinity router filter.
//!
//! v7 (2026-07-06) recommends omitting this filter from principal router
//! chains: subscription-preference now computes cache-weighted WRH inline
//! (see `docs/adr/0004-cache-weighted-subscription-preference.md`), and
//! running cache_affinity beforehand re-introduces the v6 tier-eviction
//! spill bug — the max-ranker drops non-max candidates before subscription
//! can consider them as fallback when the max-cache upstream saturates.
//!
//! The filter is retained for chains that explicitly reference
//! `BUILTIN_CACHE_AFFINITY_ID` and for backwards-compatibility with
//! `request_events_v1` payloads that carry `cache_affinity` trace rows.

use cc_lb_domain::{
    BUILTIN_CACHE_AFFINITY_ID, BUILTIN_CACHE_AFFINITY_NAME, CacheAffinityCandidate,
    CacheAffinityTrace, Principal, UpstreamCandidate,
};
use cc_lb_plugin_api::{FilterError, FilterOutput, FilterPlugin, RequestContext};
use cc_lb_storage_api::PluginMetadata;
use uuid::Uuid;

pub const PURPOSE: &str = "Prefer upstreams whose prompt cache is deepest for this request.";
pub const KEEPS: &str = "Candidates whose `predicted_cache_read_tokens` ties the maximum observed on this pool. Shallow-cache candidates (same shared BP0 hash but no session-specific deeper prefix) are dropped so downstream filters do not scatter a warm session to an upstream that only has the shared prefix.";
pub const DROPS: &str = "Candidates below the maximum `predicted_cache_read_tokens` — only when at least one candidate has a positive score; otherwise nothing is dropped.";
pub const EMPTY_BEHAVIOR: &str = "Never drops everything. Falls back to passing all candidates through when every candidate is cache-cold.";

const HIT_REASON: &str = "cache-max-hit-keep";
const MISS_REASON: &str = "cache-miss-passthrough";

pub fn metadata() -> PluginMetadata {
    PluginMetadata {
        purpose: PURPOSE.to_owned(),
        keeps: KEEPS.to_owned(),
        drops: DROPS.to_owned(),
        empty_behavior: EMPTY_BEHAVIOR.to_owned(),
        examples: vec![
            "4 candidates, one at 250K (deep session cache) and three at 15K (shared system-prompt only) → keep only the 250K candidate.".to_owned(),
            "4 candidates all tied at 15K (only the shared system prompt is warm) → keep all 4 and forward to the next filter.".to_owned(),
            "5 candidates, all with zero cache score → pass all 5 through.".to_owned(),
            "Exactly 1 candidate → no change.".to_owned(),
        ],
    }
}

#[derive(Clone, Debug, Default)]
pub struct CacheAffinityFilter;

impl CacheAffinityFilter {
    pub fn new() -> Self {
        Self
    }
}

impl FilterPlugin for CacheAffinityFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let max_read_tokens = candidates
            .iter()
            .map(predicted_read_tokens)
            .max()
            .unwrap_or(0);
        let has_hit = max_read_tokens > 0;
        let reason = if has_hit { HIT_REASON } else { MISS_REASON };
        let mut kept_upstream_ids = Vec::with_capacity(candidates.len());
        let mut trace_rows = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let kept = !has_hit || predicted_read_tokens(candidate) == max_read_tokens;
            if kept {
                kept_upstream_ids.push(candidate.upstream_id);
            }
            trace_rows.push(CacheAffinityCandidate {
                upstream_id: candidate.upstream_id,
                kept,
                predicted_cache_read_tokens: candidate
                    .cache_score
                    .as_ref()
                    .map(|score| score.predicted_cache_read_tokens),
                predicted_expires_at_unix_secs: candidate
                    .cache_score
                    .as_ref()
                    .and_then(|score| score.predicted_expires_at_unix_secs),
            });
        }
        let cache_affinity = if trace_rows.is_empty() {
            None
        } else {
            Some(CacheAffinityTrace {
                candidates: trace_rows,
            })
        };
        Ok(FilterOutput {
            kept_upstream_ids,
            reason: reason.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity,
        })
    }

    fn plugin_id(&self) -> Uuid {
        BUILTIN_CACHE_AFFINITY_ID
    }

    fn plugin_name(&self) -> &str {
        BUILTIN_CACHE_AFFINITY_NAME
    }
}

/// Read-token count for max-ranker comparison. `None` cache_score and
/// `predicted_cache_read_tokens == 0` both collapse to zero: they represent
/// "this candidate offers no cache-read benefit for this request", so they
/// tie with each other and are dropped whenever any candidate reports a
/// positive score.
fn predicted_read_tokens(candidate: &UpstreamCandidate) -> u32 {
    candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_cache_read_tokens)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cc_lb_domain::CacheScore;
    use cc_lb_plugin_api::{PrincipalKind, UpstreamKind};
    use http::Method;

    use super::*;

    #[test]
    fn hit_filters_to_cache_hits() {
        let warm = candidate("warm", 10);
        let cold = candidate("cold", 0);
        let output = filter(&[warm.clone(), cold.clone()]);

        assert_eq!(output.kept_upstream_ids, vec![warm.upstream_id]);
        assert_eq!(output.reason, HIT_REASON);
        assert!(output.per_candidate_reasons.is_empty());
        let trace = output.cache_affinity.expect("hit path must emit trace");
        assert_eq!(trace.candidates.len(), 2);
        assert!(trace.candidates[0].kept);
        assert_eq!(trace.candidates[0].predicted_cache_read_tokens, Some(10));
        assert!(!trace.candidates[1].kept);
        assert_eq!(trace.candidates[1].predicted_cache_read_tokens, Some(0));
    }

    #[test]
    fn no_hit_passthrough_keeps_all() {
        let first = candidate("first", 0);
        let second = candidate_without_score("second");
        let output = filter(&[first.clone(), second.clone()]);

        assert_eq!(
            output.kept_upstream_ids,
            vec![first.upstream_id, second.upstream_id]
        );
        assert_eq!(output.reason, MISS_REASON);
        assert!(output.per_candidate_reasons.is_empty());
        let trace = output
            .cache_affinity
            .expect("miss path must emit trace with per-candidate rows");
        assert_eq!(trace.candidates.len(), 2);
        assert!(trace.candidates.iter().all(|row| row.kept));
        assert_eq!(trace.candidates[0].predicted_cache_read_tokens, Some(0));
        assert_eq!(trace.candidates[1].predicted_cache_read_tokens, None);
    }

    #[test]
    fn empty_candidate_list_passthroughs_empty() {
        let output = filter(&[]);

        assert!(output.kept_upstream_ids.is_empty());
        assert_eq!(output.reason, MISS_REASON);
        assert!(output.per_candidate_reasons.is_empty());
        assert!(output.cache_affinity.is_none());
    }

    #[test]
    fn max_ranker_keeps_only_deepest_positive_score() {
        // Given: two hits with different read-token counts (mimicking one
        //   upstream having only the shared BP0 warm and another having
        //   BP0+BP1+BP2 all warm for this session).
        // When: the filter runs.
        // Then: only the deepest candidate survives; the shallow hit is
        //   dropped and the trace records the drop reason via `kept: false`.
        let shallow = candidate("shallow", 15_000);
        let deep = candidate("deep", 250_000);
        let output = filter(&[shallow.clone(), deep.clone()]);

        assert_eq!(output.kept_upstream_ids, vec![deep.upstream_id]);
        assert_eq!(output.reason, HIT_REASON);
        let trace = output.cache_affinity.expect("hit path must emit trace");
        assert_eq!(trace.candidates.len(), 2);
        assert!(!trace.candidates[0].kept, "shallow hit must be dropped");
        assert!(trace.candidates[1].kept, "deep hit must be kept");
    }

    #[test]
    fn all_hits_tied_at_max_keeps_all() {
        // Given: two hits at identical read-token counts (mimicking the
        //   shared-BP0-only case where every upstream has warmed the exact
        //   same shallow prefix; no candidate carries session-specific
        //   deeper cache).
        // When: the filter runs.
        // Then: both survive so the next filter (subscription_preference)
        //   can pick among cache-equivalent candidates by quota/urgency.
        let a = candidate("a", 15_000);
        let b = candidate("b", 15_000);
        let output = filter(&[a.clone(), b.clone()]);

        assert_eq!(output.kept_upstream_ids, vec![a.upstream_id, b.upstream_id]);
        assert_eq!(output.reason, HIT_REASON);
        let trace = output
            .cache_affinity
            .expect("all-tied warm path must emit trace");
        assert!(trace.candidates.iter().all(|row| row.kept));
    }

    #[test]
    fn trace_surfaces_predicted_expiry_and_missing_score() {
        let mut warm = candidate("warm", 10);
        if let Some(score) = warm.cache_score.as_mut() {
            score.predicted_expires_at_unix_secs = Some(1_700_000_500);
        }
        let cold_no_score = candidate_without_score("cold");
        let output = filter(&[warm.clone(), cold_no_score.clone()]);

        let trace = output.cache_affinity.expect("mixed pool must emit trace");
        assert_eq!(
            trace.candidates[0].predicted_expires_at_unix_secs,
            Some(1_700_000_500)
        );
        assert_eq!(
            trace.candidates[1].predicted_cache_read_tokens, None,
            "candidate without cache_score must serialize as None, not Some(0)"
        );
        assert_eq!(trace.candidates[1].predicted_expires_at_unix_secs, None);
    }

    fn filter(candidates: &[UpstreamCandidate]) -> FilterOutput {
        CacheAffinityFilter::new()
            .filter(&ctx(), &principal(), candidates)
            .expect("builtin filter cannot fail")
    }

    fn ctx() -> RequestContext {
        RequestContext {
            request_id: "req".to_owned(),
            thread_id: None,
            downstream_headers: http::HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::new(),
            cache_breakpoints: Vec::new(),
            canonical_model_id: "claude".to_owned(),
            cache_pricing: cc_lb_domain::CachePricingSummary::default(),
        }
    }

    fn principal() -> Principal {
        Principal {
            id: "principal".to_owned(),
            kind: PrincipalKind::InternalKey,
            claims: serde_json::Map::new(),
        }
    }

    fn candidate(name: &str, predicted_cache_read_tokens: u32) -> UpstreamCandidate {
        UpstreamCandidate {
            cache_score: Some(CacheScore {
                predicted_cache_read_tokens,
                predicted_cache_creation_tokens_5m: 0,
                predicted_cache_creation_tokens_1h: 0,
                predicted_uncached_input_tokens: 0,
                predicted_expires_at_unix_secs: None,
                matched_breakpoint_index: None,
                confidence: 1.0,
                ambiguity_reason: None,
                matched_v3_cache_key: None,
                breakpoint_content_block_index: None,
                matched_content_block_index: None,
                lookback_distance: None,
                token_estimate_source: None,
            }),
            ..candidate_without_score(name)
        }
    }

    fn candidate_without_score(name: &str) -> UpstreamCandidate {
        UpstreamCandidate {
            upstream_id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
            cache_score: None,
            base_url: None,
            plan_capacity_ratio: None,
            organization_type: None,
            rate_limit_tier: None,
            seat_tier: None,
        }
    }
}
