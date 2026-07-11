use std::collections::HashMap;

use cc_lb_engine::{PromptCacheObservationCacheLike, SubscriptionQuotaCacheLike};
use cc_lb_plugin_api::types::{CacheScore, TtlClass, WarmCacheEntry};
use cc_lb_plugin_api::{
    Principal, PrincipalKind, SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState,
};
use cc_lb_storage_api::SubscriptionQuotaSample;
use cc_lb_storage_api::principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
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

pub fn test_cache_pricing() -> cc_lb_plugin_api::CachePricingSummary {
    cc_lb_plugin_api::CachePricingSummary {
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

impl PromptCacheObservationCacheLike for TestPromptCacheObservationCache {
    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        _canonical_model: &str,
        _request_breakpoint_hashes: &[(String, TtlClass)],
        _now_unix_secs: u64,
    ) -> Vec<WarmCacheEntry> {
        self.warm_entries
            .get(&upstream_id)
            .cloned()
            .unwrap_or_default()
    }

    fn upsert_observation(
        &self,
        _upstream_id: Uuid,
        _canonical_model: String,
        _prefix_hash: String,
        _ttl_class: TtlClass,
        _expires_at_unix_secs: u64,
        _now_unix_secs: u64,
    ) {
    }

    fn refresh_on_hit(
        &self,
        _upstream_id: Uuid,
        _canonical_model: &str,
        _prefix_hash: &str,
        _ttl_class: TtlClass,
        _now_unix_secs: u64,
    ) -> bool {
        false
    }

    fn thread_usage_score(
        &self,
        _upstream_id: Uuid,
        _canonical_model: &str,
        _thread_id: &str,
        _now_unix_secs: u64,
    ) -> Option<CacheScore> {
        None
    }

    fn grace_margin_secs(&self) -> u64 {
        30
    }

    fn clock_now_unix_secs(&self) -> u64 {
        TEST_QUOTA_NOW_SECS
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
