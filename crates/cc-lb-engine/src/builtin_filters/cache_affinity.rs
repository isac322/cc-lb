use cc_lb_plugin_api::{
    BUILTIN_CACHE_AFFINITY_ID, BUILTIN_CACHE_AFFINITY_NAME, FilterError, FilterOutput,
    FilterPlugin, Principal, RequestContext, UpstreamCandidate,
};
use cc_lb_storage_api::PluginMetadata;
use uuid::Uuid;

pub const PURPOSE: &str = "Prefer upstreams whose prompt cache is already warm for this request.";
pub const KEEPS: &str =
    "Candidates with a positive prefill_cache_score (the upstream has already cached the prefix).";
pub const DROPS: &str = "Candidates with zero cache score — only when at least one candidate is a cache hit; otherwise nothing is dropped.";
pub const EMPTY_BEHAVIOR: &str = "Never drops everything. Falls back to passing all candidates through when no cache hit exists.";

const HIT_REASON: &str = "cache-hit-keep";
const MISS_REASON: &str = "cache-miss-passthrough";

pub fn metadata() -> PluginMetadata {
    PluginMetadata {
        purpose: PURPOSE.to_owned(),
        keeps: KEEPS.to_owned(),
        drops: DROPS.to_owned(),
        empty_behavior: EMPTY_BEHAVIOR.to_owned(),
        examples: vec![
            "5 candidates, 2 with positive cache score → keep the 2 hits.".to_owned(),
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
        let has_hit = candidates.iter().any(is_cache_hit);
        let reason = if has_hit { HIT_REASON } else { MISS_REASON };
        let kept_upstream_ids = candidates
            .iter()
            .filter(|candidate| !has_hit || is_cache_hit(candidate))
            .map(|candidate| candidate.upstream_id)
            .collect::<Vec<_>>();
        Ok(FilterOutput {
            kept_upstream_ids,
            reason: reason.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        BUILTIN_CACHE_AFFINITY_ID
    }

    fn plugin_name(&self) -> &str {
        BUILTIN_CACHE_AFFINITY_NAME
    }
}

fn is_cache_hit(candidate: &UpstreamCandidate) -> bool {
    candidate
        .cache_score
        .as_ref()
        .is_some_and(|score| score.predicted_cache_read_tokens > 0)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cc_lb_plugin_api::types::CacheScore;
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
    }

    #[test]
    fn empty_candidate_list_passthroughs_empty() {
        let output = filter(&[]);

        assert!(output.kept_upstream_ids.is_empty());
        assert_eq!(output.reason, MISS_REASON);
        assert!(output.per_candidate_reasons.is_empty());
    }

    #[test]
    fn all_hit_keeps_all() {
        let first = candidate("first", 1);
        let second = candidate("second", 2);
        let output = filter(&[first.clone(), second.clone()]);

        assert_eq!(
            output.kept_upstream_ids,
            vec![first.upstream_id, second.upstream_id]
        );
        assert_eq!(output.reason, HIT_REASON);
        assert!(output.per_candidate_reasons.is_empty());
    }

    fn filter(candidates: &[UpstreamCandidate]) -> FilterOutput {
        CacheAffinityFilter::new()
            .filter(&ctx(), &principal(), candidates)
            .expect("builtin filter cannot fail")
    }

    fn ctx() -> RequestContext {
        RequestContext {
            request_id: "req".to_owned(),
            downstream_headers: http::HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::new(),
            cache_breakpoints: Vec::new(),
            canonical_model_id: "claude".to_owned(),
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
