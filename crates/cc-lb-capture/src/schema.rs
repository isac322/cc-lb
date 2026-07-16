//! Schema definitions for capture data.

use cc_lb_domain::{CacheBreakpoint, CachePricingSummary, RoutingTrace, UpstreamCandidate};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Derived routing inputs captured at the formula decision point.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapturedRequestInput {
    pub event_id: String,
    pub request_id: String,
    pub thread_id: Option<String>,
    pub canonical_model_id: String,
    pub cache_pricing: CachePricingSummary,
    pub breakpoints: Vec<CacheBreakpoint>,
    pub candidates: Vec<UpstreamCandidate>,
    pub subscription_preference_input_upstream_ids: Vec<Uuid>,
    pub routing_trace: RoutingTrace,
    pub captured_at_unix_ms: u64,
    pub salt_version: String,
    pub cache_cost_basis_version: String,
    pub capture_schema_version: u32,
    pub build_version: String,
}

/// Observed response metadata joined to a captured routing input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapturedResponse {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_input_tokens_5m: Option<u64>,
    pub cache_creation_input_tokens_1h: Option<u64>,
    pub chosen_upstream_id: Option<Uuid>,
    pub upstream_status: Option<u16>,
    pub client_status: Option<u16>,
    pub duration_ms: Option<u64>,
    pub attempt_num: Option<u32>,
}

/// Terminal outcome for a request that reached routing input capture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    RoutedPreDispatchError,
    RoutedLimitRejected,
    RoutedDispatchedSuccess,
    RoutedDispatchedError,
    RoutedClientDisconnected,
}

/// Complete persisted capture with an always-present routing input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CaptureRecord {
    pub input: CapturedRequestInput,
    pub response: CapturedResponse,
    pub disposition: Disposition,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{CaptureRecord, CapturedRequestInput, CapturedResponse, Disposition, Uuid};
    use cc_lb_domain::*;

    fn candidate(upstream_id: Uuid, ordinal: u64) -> UpstreamCandidate {
        UpstreamCandidate {
            upstream_id,
            name: format!("upstream-{ordinal}"),
            kind: UpstreamKind::AnthropicOauth,
            observed_rate_limits: vec![RateLimitObservation {
                kind: RateLimitKind::InputTokens,
                window: "1m".to_owned(),
                limit: Some(10_000 + ordinal),
                remaining: Some(8_000 + ordinal),
                reset: Some("30s".to_owned()),
            }],
            subscription_quotas: vec![SubscriptionQuotaCandidateSnapshot {
                window: "5h".to_owned(),
                state: SubscriptionQuotaDataState::Fresh,
                source: Some("api".to_owned()),
                utilization: Some(0.25),
                status: Some("allowed".to_owned()),
                resets_at_unix_secs: Some(1_800_000_000 + ordinal),
                surpassed_threshold: Some(0.8),
                representative_claim: Some("claim".to_owned()),
                disabled_reason: Some("not_disabled".to_owned()),
                extra_usage_enabled: Some(true),
                extra_usage_monthly_limit: Some(100.0),
                extra_usage_used_credits: Some(12.5),
                observed_at_unix_millis: Some(1_700_000_000_000 + ordinal),
                max_staleness_secs: 300,
                fallback_available: Some(true),
                overage_in_use: Some(false),
                overage_period_monthly_utilization: Some(0.1),
                upgrade_paths: Some(vec!["max".to_owned()]),
            }],
            observed_at_unix_secs: 1_700_000_000 + ordinal,
            cache_score: Some(CacheScore {
                predicted_cache_read_tokens: 1_000,
                predicted_cache_creation_tokens_5m: 200,
                predicted_cache_creation_tokens_1h: 300,
                predicted_uncached_input_tokens: 400,
                predicted_expires_at_unix_secs: Some(1_700_000_300 + ordinal),
                matched_breakpoint_index: Some(0),
                confidence: 0.75,
                ambiguity_reason: Some("multiple_prefixes".to_owned()),
                matched_v3_cache_key: Some(format!("cache-key-{ordinal}")),
                breakpoint_content_block_index: Some(20),
                matched_content_block_index: Some(19),
                lookback_distance: Some(1),
                token_estimate_source: Some("tokenizer".to_owned()),
            }),
            base_url: Some(format!("https://upstream-{ordinal}.example")),
            plan_capacity_ratio: Some(2.0),
            organization_type: Some("team".to_owned()),
            rate_limit_tier: Some("tier_2".to_owned()),
            seat_tier: Some("premium".to_owned()),
        }
    }

    fn urgency(upstream_id: Uuid, ordinal: u32) -> CandidateUrgency {
        CandidateUrgency {
            upstream_id,
            tier: SubscriptionTier::KnownBase,
            urgency: f64::from(ordinal) + 0.5,
            quota_urgency: 0.6,
            quota_urgency_5h: Some(0.7),
            quota_urgency_7d: Some(0.8),
            quota_urgency_combined: Some(0.9),
            quota_weight_factor: 1.1,
            quota_uniform_fallback: false,
            predicted_cache_read_tokens: 1_000,
            predicted_cache_creation_tokens_5m: 200,
            predicted_cache_creation_tokens_1h: 300,
            predicted_uncached_input_tokens: 400,
            cache_ratio: 0.5,
            cache_weight_multiplier: 1.2,
            warning_multiplier: 1.0,
            cache_savings_ratio: 0.4,
            estimated_input_cost_micros: 42,
            effective_weight: 1.3,
            cache_value_micros: Some(99),
            matched_v3_cache_key: Some(format!("cache-key-{ordinal}")),
            matched_content_block_index: Some(19),
            breakpoint_content_block_index: Some(20),
            lookback_distance: Some(1),
            token_estimate_source: Some("tokenizer".to_owned()),
        }
    }

    fn capture_record() -> CaptureRecord {
        let first_upstream_id = Uuid::from_u128(1);
        let second_upstream_id = Uuid::from_u128(2);

        CaptureRecord {
            input: CapturedRequestInput {
                event_id: "event-01".to_owned(),
                request_id: "client-collidable-request-id".to_owned(),
                thread_id: Some("thread-01".to_owned()),
                canonical_model_id: "claude-sonnet-4-5".to_owned(),
                cache_pricing: CachePricingSummary {
                    status: "available".to_owned(),
                    input_micros_per_million: Some(3_000_000),
                    cache_creation_5m_micros_per_million: Some(3_750_000),
                    cache_creation_1h_micros_per_million: Some(6_000_000),
                    cache_read_micros_per_million: Some(300_000),
                },
                breakpoints: vec![CacheBreakpoint {
                    block_index: 20,
                    source: CacheBreakpointSource::Message,
                    path: "messages.3.content.2".to_owned(),
                    message_index: Some(3),
                    prefix_hash: "breakpoint-prefix".to_owned(),
                    prefix_token_count: 1_500,
                    requested_ttl: TtlClass::Ephemeral1h,
                    origin: BreakpointOrigin::Explicit,
                    lookback_prefixes: vec![CacheLookbackPrefix {
                        prefix_hash: "prefix-n".to_owned(),
                        content_block_index: 20,
                        lookback_distance: 0,
                    }],
                    token_estimate_source: Some("tokenizer".to_owned()),
                }],
                candidates: vec![
                    candidate(first_upstream_id, 1),
                    candidate(second_upstream_id, 2),
                ],
                subscription_preference_input_upstream_ids: vec![
                    first_upstream_id,
                    second_upstream_id,
                ],
                routing_trace: RoutingTrace {
                    stages: vec![StageDecision {
                        stage_name: "subscription-preference".to_owned(),
                        upstream_id: Some(first_upstream_id),
                        reason: Some("keep:best_subscription_candidate".to_owned()),
                        duration_us: 17,
                        subscription_preference: Some(SubscriptionPreferenceTrace {
                            chosen_tier: SubscriptionTier::KnownBase,
                            candidates: vec![
                                urgency(first_upstream_id, 1),
                                urgency(second_upstream_id, 2),
                            ],
                            wrh_key_source: WrhKeySource::CacheHash,
                            previous_tier: Some(SubscriptionTier::PartialBase),
                            rendezvous_salt_version: Some("v11".to_owned()),
                            formula_version: None,
                            cache_cost_basis_version: Some("v1".to_owned()),
                            formula_winner_upstream_id: Some(first_upstream_id),
                            kept_upstream_id: Some(first_upstream_id),
                            incumbent_upstream_id: Some(second_upstream_id),
                            estimated_switch_cache_loss_micros: Some(123),
                            cache_loss_status: Some("available".to_owned()),
                            switch_gate_reason: Some("formula_winner".to_owned()),
                            bucket_v3_cache_affinity_key: Some("bucket-key".to_owned()),
                            lineage_would_have_predicted_read_tokens: Some(900),
                            lineage_would_have_picked_upstream_id: Some(second_upstream_id),
                        }),
                        cache_affinity: None,
                    }],
                    terminal_decision: Some(TerminalDecision {
                        upstream_id: Some(first_upstream_id),
                        strategy: TerminalStrategy::FirstPick,
                    }),
                },
                captured_at_unix_ms: 1_700_000_000_123,
                salt_version: "v11".to_owned(),
                cache_cost_basis_version: "v1".to_owned(),
                capture_schema_version: 1,
                build_version: "0.1.0-test".to_owned(),
            },
            response: CapturedResponse {
                input_tokens: Some(2_000),
                output_tokens: Some(500),
                cache_read_input_tokens: Some(1_000),
                cache_creation_input_tokens_5m: Some(200),
                cache_creation_input_tokens_1h: Some(300),
                chosen_upstream_id: Some(first_upstream_id),
                upstream_status: Some(200),
                client_status: Some(200),
                duration_ms: Some(345),
                attempt_num: Some(2),
            },
            disposition: Disposition::RoutedDispatchedSuccess,
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

    #[test]
    fn capture_record_round_trips_when_fully_populated() {
        // Given
        let record = capture_record();

        // When
        let json = serde_json::to_string(&record).unwrap();
        let decoded = serde_json::from_str::<CaptureRecord>(&json).unwrap();

        // Then
        assert_eq!(decoded, record);
        let _disposition_name = disposition_name(&decoded.disposition);
    }

    #[test]
    fn capture_record_rejects_unknown_disposition() {
        // Given
        let mut value = serde_json::to_value(capture_record()).unwrap();
        value["disposition"] = serde_json::Value::String("bogus_variant".to_owned());
        let json = serde_json::to_string(&value).unwrap();

        // When
        let decoded = serde_json::from_str::<CaptureRecord>(&json);

        // Then
        assert!(decoded.is_err());
    }
}
