// tier-allow(silent-skip): storage trait optional-return signatures never skip assertions until=2027-03-31
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_engine::{ApplyStatus, DynamicView};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::{
    BackfillApplyOutcome, MetadataTierMappingOverrideRecord, OrganizationMetadataRecord,
    OrganizationMetadataStore, PlanTierRatioRecord, PlanTierStore, PrincipalCreate, PrincipalKind,
    PrincipalStore, StorageResult, TierResolutionSource, UpstreamCreate, UpstreamPlanTierRecord,
    UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_testkit::InMemoryStorage as Storage;

fn oauth_config() -> AnthropicOAuthConfig {
    AnthropicOAuthConfig::default()
}

fn stores(storage: Arc<Storage>, plan_metadata: Arc<PlanMetadataStore>) -> Stores {
    Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: Arc::new(EmptyRateLimitStore),
        upstream_subscription_quotas: storage.clone(),
        upstream_subscription_metadata: plan_metadata.clone(),
        organization_metadata: plan_metadata.clone(),
        plan_tiers: plan_metadata,
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage,
        audit: None,
    }
}

fn storage_fixture() -> (Arc<Storage>, Arc<PlanMetadataStore>) {
    (
        Arc::new(Storage::with_clock(cc_lb_testkit::fixed_clock(
            1_800_000_000,
        ))),
        Arc::new(PlanMetadataStore::default()),
    )
}

#[derive(Default)]
struct PlanMetadataStore {
    subscription_metadata: Mutex<Vec<UpstreamSubscriptionMetadataRecord>>,
    organization_metadata: Mutex<Vec<OrganizationMetadataRecord>>,
    ratio_catalog: Mutex<Vec<PlanTierRatioRecord>>,
    upstream_plan_tiers: Mutex<Vec<UpstreamPlanTierRecord>>,
}

#[async_trait]
impl UpstreamSubscriptionMetadataStore for PlanMetadataStore {
    async fn put_upstream_subscription_metadata(
        &self,
        record: &UpstreamSubscriptionMetadataRecord,
    ) -> StorageResult<()> {
        let mut records = self
            .subscription_metadata
            .lock()
            .expect("subscription metadata lock");
        records.retain(|current| current.upstream_id != record.upstream_id);
        records.push(record.clone());
        Ok(())
    }

    async fn get_upstream_subscription_metadata(
        &self,
        upstream_id: uuid::Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>> {
        Ok(self
            .subscription_metadata
            .lock()
            .expect("subscription metadata lock")
            .iter()
            .find(|record| record.upstream_id == upstream_id)
            .cloned())
    }

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>> {
        Ok(self
            .subscription_metadata
            .lock()
            .expect("subscription metadata lock")
            .clone())
    }
}

#[async_trait]
impl OrganizationMetadataStore for PlanMetadataStore {
    async fn put_organization_metadata(
        &self,
        record: &OrganizationMetadataRecord,
    ) -> StorageResult<()> {
        let mut records = self
            .organization_metadata
            .lock()
            .expect("organization metadata lock");
        records.retain(|current| current.organization_uuid != record.organization_uuid);
        records.push(record.clone());
        Ok(())
    }

    async fn get_organization_metadata(
        &self,
        organization_uuid: &str,
    ) -> StorageResult<Option<OrganizationMetadataRecord>> {
        Ok(self
            .organization_metadata
            .lock()
            .expect("organization metadata lock")
            .iter()
            .find(|record| record.organization_uuid == organization_uuid)
            .cloned())
    }

    async fn list_organization_metadata(&self) -> StorageResult<Vec<OrganizationMetadataRecord>> {
        Ok(self
            .organization_metadata
            .lock()
            .expect("organization metadata lock")
            .clone())
    }
}

#[async_trait]
impl PlanTierStore for PlanMetadataStore {
    async fn upsert_plan_tier_ratio(&self, record: &PlanTierRatioRecord) -> StorageResult<()> {
        let mut ratios = self.ratio_catalog.lock().expect("plan tier ratio lock");
        ratios.retain(|current| {
            current.tier_key != record.tier_key || current.effective_to_unix_millis.is_some()
        });
        ratios.push(record.clone());
        Ok(())
    }

    async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>> {
        Ok(self
            .ratio_catalog
            .lock()
            .expect("plan tier ratio lock")
            .iter()
            .filter(|record| record.effective_to_unix_millis.is_none())
            .cloned()
            .collect())
    }

    async fn list_plan_tier_ratios_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<PlanTierRatioRecord>> {
        Ok(self
            .ratio_catalog
            .lock()
            .expect("plan tier ratio lock")
            .iter()
            .filter(|record| {
                record.effective_from_unix_millis <= as_of_unix_millis
                    && record
                        .effective_to_unix_millis
                        .is_none_or(|end| end > as_of_unix_millis)
            })
            .cloned()
            .collect())
    }

    async fn upsert_metadata_tier_override(
        &self,
        _record: &MetadataTierMappingOverrideRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        Ok(Vec::new())
    }

    async fn list_metadata_tier_overrides_as_of(
        &self,
        _as_of_unix_millis: i64,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        Ok(Vec::new())
    }

    async fn append_upstream_plan_tier(
        &self,
        record: &UpstreamPlanTierRecord,
    ) -> StorageResult<()> {
        self.upstream_plan_tiers
            .lock()
            .expect("upstream plan tier lock")
            .push(record.clone());
        Ok(())
    }

    async fn backfill_upstream_plan_tier_intervals(
        &self,
        _upstream_id: uuid::Uuid,
        _intervals: &[UpstreamPlanTierRecord],
        _terminal_cap_unix_millis: i64,
        _provenance: &str,
    ) -> StorageResult<BackfillApplyOutcome> {
        Ok(BackfillApplyOutcome::Skipped)
    }

    async fn list_current_upstream_plan_tiers(&self) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        Ok(self
            .upstream_plan_tiers
            .lock()
            .expect("upstream plan tier lock")
            .iter()
            .filter(|record| record.effective_to_unix_millis.is_none())
            .cloned()
            .collect())
    }

    async fn list_upstream_plan_tiers_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        Ok(self
            .upstream_plan_tiers
            .lock()
            .expect("upstream plan tier lock")
            .iter()
            .filter(|record| {
                record.effective_from_unix_millis <= as_of_unix_millis
                    && record
                        .effective_to_unix_millis
                        .is_none_or(|end| end > as_of_unix_millis)
            })
            .cloned()
            .collect())
    }
}

struct EmptyRateLimitStore;

#[async_trait]
impl UpstreamRateLimitStateStore for EmptyRateLimitStore {
    async fn put_observation(
        &self,
        _record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn list_for_upstream_ids(
        &self,
        _upstream_ids: &[uuid::Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        Ok(Vec::new())
    }
}

async fn create_principal(storage: &Storage, name: &str) -> cc_lb_storage_api::PrincipalRecord {
    PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await
    .expect("principal created")
}

async fn create_api_key_upstream(
    storage: &Storage,
    name: &str,
) -> cc_lb_storage_api::UpstreamRecord {
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: Some(vec![1, 2, 3]),
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("upstream created")
}

async fn attach_plan_metadata(storage: &PlanMetadataStore, upstream_id: uuid::Uuid) {
    UpstreamSubscriptionMetadataStore::put_upstream_subscription_metadata(
        storage,
        &UpstreamSubscriptionMetadataRecord {
            upstream_id,
            organization_uuid: Some("org-max-5x".to_owned()),
            organization_role: None,
            workspace_role: None,
            observed_at_unix_millis: 1_800_000_000_000,
            last_error: None,
            raw_roles: None,
            raw_bootstrap: None,
        },
    )
    .await
    .expect("subscription metadata stored");
    OrganizationMetadataStore::put_organization_metadata(
        storage,
        &OrganizationMetadataRecord {
            organization_uuid: "org-max-5x".to_owned(),
            organization_name: Some("Max 5x Org".to_owned()),
            organization_type: Some("claude_max".to_owned()),
            rate_limit_tier: Some("default_claude_max_5x".to_owned()),
            seat_tier: None,
            has_extra_usage_enabled: None,
            billing_type: None,
            subscription_created_at_unix_secs: None,
            account_email: None,
            account_display_name: None,
            account_uuid: None,
            overage_credit_amount_minor_units: None,
            overage_credit_currency: None,
            overage_credit_granted: None,
            overage_credit_eligible: None,
            observed_at_unix_millis: 1_800_000_000_000,
            last_error: None,
            raw_profile: None,
            raw_overage_grant: None,
        },
    )
    .await
    .expect("organization metadata stored");
}

async fn seed_plan_ratio_catalog(storage: &PlanMetadataStore) {
    PlanTierStore::upsert_plan_tier_ratio(
        storage,
        &PlanTierRatioRecord {
            tier_key: "max_5x".to_owned(),
            pro_relative_ratio: 5.0,
            effective_from_unix_millis: 1_800_000_000_000,
            effective_to_unix_millis: None,
            provenance: "test_catalog".to_owned(),
            created_at_unix_millis: 1_800_000_000_000,
        },
    )
    .await
    .expect("plan ratio catalog seeded");
}

async fn build(
    stores: &Stores,
    current_generation: u64,
    runtime: &Arc<WasmtimeRuntime>,
    data_dir: &Path,
) -> Arc<DynamicView> {
    build_dynamic_view(
        stores,
        &oauth_config(),
        Arc::new(AeadService::from_master_key([1; 32])),
        None,
        current_generation,
        runtime,
        data_dir,
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        None,
        None,
        1800,
        cc_lb_testkit::fixed_clock(1_800_000_000),
    )
    .await
    .expect("dynamic view builds")
}

#[tokio::test]
async fn t2__principals_delete_rebuild_removes_deleted_and_increments_generation() {
    let (storage, plan_metadata) = storage_fixture();
    let stores = stores(storage.clone(), plan_metadata);
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let principal_a = create_principal(&storage, "principal-a").await;
    create_principal(&storage, "principal-b").await;

    let view = build(&stores, 0, &runtime, Path::new(".")).await;
    assert_eq!(view.generation, 1);
    assert_eq!(
        view.principal_view.principal_status("principal-a"),
        cc_lb_engine::api_keys::principal_view::PrincipalStatus::Active
    );
    assert_eq!(
        view.principal_view.principal_status("principal-b"),
        cc_lb_engine::api_keys::principal_view::PrincipalStatus::Active
    );

    PrincipalStore::soft_delete(&*storage, principal_a.id, principal_a.revision, 2)
        .await
        .expect("soft delete");
    let view = build(&stores, view.generation, &runtime, Path::new(".")).await;

    assert_eq!(view.generation, 2);
    assert_eq!(
        view.principal_view.principal_status("principal-a"),
        cc_lb_engine::api_keys::principal_view::PrincipalStatus::Missing
    );
    assert_eq!(
        view.principal_view.principal_status("principal-b"),
        cc_lb_engine::api_keys::principal_view::PrincipalStatus::Active
    );
}

#[tokio::test]
async fn t2__corrupt_oauth_upstream_is_error_while_other_upstreams_stay_active() {
    let (storage, plan_metadata) = storage_fixture();
    let stores = stores(storage.clone(), plan_metadata);
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    create_principal(&storage, "principal-a").await;
    create_api_key_upstream(&storage, "healthy").await;
    let corrupt = UpstreamStore::create(
        &*storage,
        UpstreamCreate {
            name: "corrupt".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("oauth upstream created");
    storage
        .store_oauth_tokens(
            corrupt.id,
            corrupt.revision,
            EncryptedOAuthTokens::from_ciphertext(vec![9]),
        )
        .await
        .expect("corrupt oauth stored");

    let view = build(&stores, 10, &runtime, Path::new(".")).await;
    assert_eq!(view.generation, 11);
    let mut upstream_names = view
        .upstreams_snapshot()
        .iter()
        .map(|record| record.name.as_str())
        .collect::<Vec<_>>();
    upstream_names.sort_unstable();
    assert_eq!(upstream_names, ["corrupt", "healthy"]);

    let healthy = view
        .upstream_status_snapshot
        .entries
        .get("healthy")
        .expect("healthy status");
    assert_eq!(healthy.status, ApplyStatus::Active);
    assert!(healthy.last_apply_error.is_none());

    let corrupt = view
        .upstream_status_snapshot
        .entries
        .get("corrupt")
        .expect("corrupt status");
    assert_eq!(corrupt.status, ApplyStatus::Error);
    assert!(
        corrupt
            .last_apply_error
            .as_ref()
            .is_some_and(|message| !message.is_empty())
    );

    let persisted = UpstreamStore::get_by_name(&*storage, "corrupt")
        .await
        .expect("load corrupt")
        .expect("corrupt exists");
    assert!(persisted.last_apply_error.is_some());
}

#[tokio::test]
async fn t2__plan_info_uses_catalog_ratio_and_reconciles_history() {
    let (storage, plan_metadata) = storage_fixture();
    let stores = stores(storage.clone(), plan_metadata.clone());
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let upstream = create_api_key_upstream(&storage, "max-5x").await;
    attach_plan_metadata(&plan_metadata, upstream.id).await;
    seed_plan_ratio_catalog(&plan_metadata).await;

    let view = build(&stores, 0, &runtime, Path::new(".")).await;

    let plan_info = view
        .plan_info_by_upstream
        .get(&upstream.id)
        .expect("upstream plan info");
    assert_eq!(plan_info.organization_type.as_deref(), Some("claude_max"));
    assert_eq!(
        plan_info.rate_limit_tier.as_deref(),
        Some("default_claude_max_5x")
    );
    assert_eq!(plan_info.capacity_ratio, 5.0);

    let history = PlanTierStore::list_current_upstream_plan_tiers(&*plan_metadata)
        .await
        .expect("plan tier history listed");
    let resolved = history
        .iter()
        .find(|record| record.upstream_id == upstream.id)
        .expect("upstream tier history");
    assert_eq!(resolved.organization_uuid.as_deref(), Some("org-max-5x"));
    assert_eq!(resolved.organization_type.as_deref(), Some("claude_max"));
    assert_eq!(
        resolved.rate_limit_tier.as_deref(),
        Some("default_claude_max_5x")
    );
    assert_eq!(resolved.seat_tier, None);
    assert_eq!(resolved.tier_key.as_deref(), Some("max_5x"));
    assert_eq!(resolved.resolution_source, TierResolutionSource::Builtin);
    assert_eq!(resolved.resolved_ratio_snapshot, Some(5.0));
    assert_eq!(resolved.effective_to_unix_millis, None);
    assert_eq!(resolved.provenance, "dynamic_view_reconcile");
}
