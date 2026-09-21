use std::collections::HashMap;

use async_trait::async_trait;
use cc_lb_domain::{
    Principal, PrincipalKind, SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState,
    WarmCacheEntry,
};
use cc_lb_engine::{SubscriptionQuotaCacheLike, lifecycle::HASH_SCHEMA_VERSION};
use cc_lb_storage_api::principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore, StorageResult,
    SubscriptionQuotaSample,
};
use uuid::Uuid;

pub const TEST_MODEL: &str = "claude-sonnet-4-5-20250929";
pub const TEST_QUOTA_NOW_SECS: u64 = 1_700_000_000;

pub fn plugin_principal() -> Principal {
    Principal {
        id: "principal".to_owned(),
        kind: PrincipalKind::InternalKey,
        claims: serde_json::Map::new(),
    }
}

pub fn seeded_uuid(seed: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = seed;
    Uuid::from_bytes(bytes)
}

pub fn known_base_quota_snapshots(util: f64) -> Vec<SubscriptionQuotaCandidateSnapshot> {
    vec![
        quota_snapshot("5h", util, TEST_QUOTA_NOW_SECS + 18_000),
        quota_snapshot("7d", util, TEST_QUOTA_NOW_SECS + 604_800),
    ]
}

pub fn test_cache_pricing() -> cc_lb_domain::CachePricingSummary {
    cc_lb_domain::CachePricingSummary {
        status: "known".to_owned(),
        input_micros_per_million: Some(5_000_000),
        cache_creation_5m_micros_per_million: Some(6_250_000),
        cache_creation_1h_micros_per_million: Some(10_000_000),
        cache_read_micros_per_million: Some(500_000),
    }
}

pub fn principal_record(name: &str) -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        kind: StoragePrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: None,
    }
}

pub fn upstream_record(id: Uuid) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: format!("upstream-{id}"),
        kind: StorageUpstreamKind::AnthropicOauth,
        enabled: true,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        ..UpstreamRecord::default()
    }
}

fn quota_snapshot(
    window: &str,
    utilization: f64,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.to_owned(),
        state: SubscriptionQuotaDataState::Fresh,
        source: Some("test".to_owned()),
        utilization: Some(utilization),
        status: Some("allowed".to_owned()),
        resets_at_unix_secs: Some(resets_at_unix_secs),
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis: Some(TEST_QUOTA_NOW_SECS * 1_000),
        max_staleness_secs: 60,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}

pub struct TestPromptCacheObservationCache {
    warm_entries: HashMap<Uuid, Vec<WarmCacheEntry>>,
}

impl TestPromptCacheObservationCache {
    pub fn new(warm_entries: HashMap<Uuid, Vec<WarmCacheEntry>>) -> Self {
        Self { warm_entries }
    }
}

#[async_trait]
impl PromptCacheObservationStore for TestPromptCacheObservationCache {
    async fn list_active_for_candidates(
        &self,
        upstream_ids: &[Uuid],
        canonical_model_id: &str,
        v3_prefix_keys: &[String],
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        let mut records = upstream_ids
            .iter()
            .flat_map(|upstream_id| {
                self.warm_entries
                    .get(upstream_id)
                    .into_iter()
                    .flatten()
                    .filter(|entry| {
                        entry.expires_at_unix_secs > not_expired_at_unix_secs
                            && v3_prefix_keys.iter().any(|key| key == &entry.prefix_hash)
                    })
                    .map(|entry| PromptCacheObservationRecord {
                        upstream_id: *upstream_id,
                        canonical_model_id: canonical_model_id.to_owned(),
                        v3_prefix_key: entry.prefix_hash.clone(),
                        ttl_class: entry.ttl_class,
                        expires_at_unix_secs: entry.expires_at_unix_secs,
                        last_observed_at_unix_secs: entry.last_observed_at_unix_secs,
                        hash_schema_version: HASH_SCHEMA_VERSION,
                        prefix_content_block_index: entry.content_block_index,
                        estimated_prefix_tokens: entry.estimated_prefix_tokens,
                        token_estimate_source: entry.token_estimate_source.clone(),
                    })
            })
            .collect::<Vec<_>>();
        records.sort_by(|a, b| {
            (a.upstream_id, &a.v3_prefix_key, ttl_rank(a.ttl_class)).cmp(&(
                b.upstream_id,
                &b.v3_prefix_key,
                ttl_rank(b.ttl_class),
            ))
        });
        Ok(records)
    }
}

pub struct TestSubscriptionQuotaCache {
    snapshots: HashMap<Uuid, Vec<SubscriptionQuotaCandidateSnapshot>>,
}

impl TestSubscriptionQuotaCache {
    pub fn new(snapshots: HashMap<Uuid, Vec<SubscriptionQuotaCandidateSnapshot>>) -> Self {
        Self { snapshots }
    }
}

impl SubscriptionQuotaCacheLike for TestSubscriptionQuotaCache {
    fn upsert_observation(&self, _record: &SubscriptionQuotaSample) {}

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        _now_unix_millis: u64,
        _max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        self.snapshots
            .get(&upstream_id)
            .cloned()
            .unwrap_or_default()
    }
}

fn ttl_rank(ttl: cc_lb_domain::TtlClass) -> u8 {
    match ttl {
        cc_lb_domain::TtlClass::Ephemeral5m => 0,
        cc_lb_domain::TtlClass::Ephemeral1h => 1,
    }
}
