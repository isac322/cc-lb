use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::clock::fixed_clock;
use async_trait::async_trait;
use cc_lb_clock::{ClockHandle, unix_secs};
use cc_lb_storage_api as storage_api;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

macro_rules! unscripted {
    ($trait_name:ident, $method:ident) => {
        unimplemented!(concat!(
            "InMemoryStorage::",
            stringify!($trait_name),
            "::",
            stringify!($method),
            " is not scripted"
        ))
    };
}
#[derive(Default)]
struct PrincipalState {
    records: HashMap<Uuid, storage_api::PrincipalRecord>,
    registry_entries: HashMap<Uuid, storage_api::WasmRegistryEntry>,
    chain_entries: HashMap<Uuid, storage_api::PluginChainEntry>,
}

type SubscriptionQuotaKey = (
    Uuid,
    storage_api::SubscriptionQuotaWindow,
    storage_api::SubscriptionQuotaSource,
);

#[derive(Default)]
struct SubscriptionQuotaState {
    latest: BTreeMap<SubscriptionQuotaKey, storage_api::SubscriptionQuotaSample>,
    checkpoints: Vec<storage_api::SubscriptionQuotaCheckpointRecord>,
}

struct PlanTierState {
    ratios: Vec<storage_api::PlanTierRatioRecord>,
    overrides: Vec<storage_api::MetadataTierMappingOverrideRecord>,
    upstreams: Vec<storage_api::UpstreamPlanTierRecord>,
}

impl Default for PlanTierState {
    fn default() -> Self {
        let provenance = "migration:0039_plan_tier_history".to_owned();
        let ratios = [
            ("pro", 1.0),
            ("team_standard", 1.25),
            ("max_5x", 5.0),
            ("team_premium", 6.25),
            ("max_20x", 20.0),
        ]
        .into_iter()
        .map(
            |(tier_key, pro_relative_ratio)| storage_api::PlanTierRatioRecord {
                tier_key: tier_key.to_owned(),
                pro_relative_ratio,
                effective_from_unix_millis: 0,
                effective_to_unix_millis: None,
                provenance: provenance.clone(),
                created_at_unix_millis: 0,
            },
        )
        .collect();
        Self {
            ratios,
            overrides: Vec::new(),
            upstreams: Vec::new(),
        }
    }
}

pub struct InMemoryStorage {
    api_key_usage_buckets:
        Mutex<HashMap<(Uuid, storage_api::ApiKeyUsageBucketKey), storage_api::ApiKeyUsage>>,
    api_key_usage_writers: Mutex<HashMap<Uuid, (u64, Option<Uuid>)>>,
    audit_entries: Mutex<Vec<storage_api::AuditEntry>>,
    backend_kind: Mutex<storage_api::BackendKind>,
    cache_keepalive_sessions: Mutex<HashMap<String, storage_api::CacheKeepaliveSessionRecord>>,
    cache_keepalive_decisions:
        Mutex<HashMap<(String, String), storage_api::CacheKeepaliveDecisionRecord>>,
    cache_keepalive_turns: Mutex<Vec<storage_api::CacheKeepaliveTurnRecord>>,
    config_draft: Mutex<storage_api::ConfigDraftState>,
    config_history: Mutex<BTreeMap<u64, storage_api::HistoryEntry>>,
    killswitch_enabled: Mutex<bool>,
    meta_values: Mutex<HashMap<String, String>>,
    managed_keys: Mutex<BTreeMap<(String, String), storage_api::StoredApiKeyRecord>>,
    entity_id_cursor: AtomicU64,
    principal_state: Mutex<PrincipalState>,
    plugin_blobs: Mutex<BTreeMap<[u8; 32], Vec<u8>>>,
    price_catalog_snapshot: Mutex<Option<(String, storage_api::PriceCatalogSnapshotRecord)>>,
    pool_quota_snapshots: Mutex<
        BTreeMap<(storage_api::SubscriptionQuotaWindow, i64), storage_api::PoolQuotaSnapshotRecord>,
    >,
    plan_tiers: Mutex<PlanTierState>,
    subscription_quota: Mutex<SubscriptionQuotaState>,
    organization_metadata: Mutex<BTreeMap<String, storage_api::OrganizationMetadataRecord>>,
    subscription_quota_sample_batches: Mutex<Vec<Vec<storage_api::SubscriptionQuotaSample>>>,
    upstream_subscription_metadata:
        Mutex<BTreeMap<Uuid, storage_api::UpstreamSubscriptionMetadataRecord>>,
    request_event_cursor: AtomicU64,
    request_event_notify: Notify,
    request_events: Mutex<Vec<(u64, storage_api::RequestEvent)>>,
    upstreams: Mutex<BTreeMap<Uuid, storage_api::UpstreamRecord>>,
    upstream_rate_limit_observations: Mutex<Vec<storage_api::UpstreamRateLimitObservationRecord>>,
    usage_rollup_checkpoint: Mutex<Option<u64>>,
    usage_rollups: Mutex<Vec<storage_api::UsageRollup>>,
    clock: ClockHandle,
}

impl InMemoryStorage {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[must_use]
    pub fn with_clock(clock: ClockHandle) -> Self {
        Self {
            clock,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn as_request_event_store(self: &Arc<Self>) -> Arc<dyn storage_api::RequestEventStore> {
        self.clone()
    }

    pub async fn wait_for_request_events(&self, n: usize) -> Vec<storage_api::RequestEvent> {
        const DEFAULT_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        tokio::time::timeout(DEFAULT_TEST_TIMEOUT, async {
            loop {
                if let Some(events) = self.request_events_if_ready(n) {
                    return events;
                }

                let notified = self.request_event_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                if let Some(events) = self.request_events_if_ready(n) {
                    return events;
                }

                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {n} request events"))
    }

    fn request_events_if_ready(&self, n: usize) -> Option<Vec<storage_api::RequestEvent>> {
        let events = lock_or_storage_error(&self.request_events)
            .expect("InMemoryStorage request_events mutex poisoned");
        (events.len() >= n).then(|| events.iter().map(|(_, event)| event.clone()).collect())
    }

    fn next_entity_id(&self) -> Uuid {
        loop {
            let id = Uuid::from_u128(u128::from(
                self.entity_id_cursor.fetch_add(1, Ordering::Relaxed) + 1,
            ));
            if id != storage_api::BUILTIN_SUBSCRIPTION_PREFERENCE_ID {
                return id;
            }
        }
    }

    fn principal_revision_conflict() -> storage_api::StorageError {
        storage_api::StorageError::Conflict {
            message: "principal revision conflict".to_owned(),
        }
    }

    fn principal_name_conflict(name: &str) -> storage_api::StorageError {
        storage_api::StorageError::Conflict {
            message: format!("live principal name already exists: {name}"),
        }
    }

    fn upstream_conflict(message: &str) -> storage_api::StorageError {
        storage_api::StorageError::Conflict {
            message: message.to_owned(),
        }
    }

    fn now_unix_secs(&self) -> u64 {
        unix_secs(self.clock.now())
    }

    fn update_upstream_record(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: storage_api::UpstreamUpdate,
        always_bump_spec_revision: bool,
        include_secret_fields: bool,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        if let Some(name) = update.name.as_deref() {
            storage_api::validate_identifier("upstream.name", name)?;
        }
        let now = self.now_unix_secs();
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        if let Some(name) = update.name.as_deref()
            && upstreams.values().any(|record| {
                record.id != id && record.deleted_at_unix_secs.is_none() && record.name == name
            })
        {
            return Err(Self::upstream_conflict("live upstream name already exists"));
        }
        let record = upstreams
            .get_mut(&id)
            .ok_or_else(|| Self::upstream_conflict("upstream not found"))?;
        if record.deleted_at_unix_secs.is_some() {
            return Err(Self::upstream_conflict("upstream not found"));
        }
        if record.revision != expected_revision {
            return Err(Self::upstream_conflict("stale upstream revision"));
        }

        let has_spec_update = update.name.is_some()
            || update.base_url.is_some()
            || update.enabled.is_some()
            || update.warmup_enabled.is_some()
            || update.warmup_dialect_plugin.is_some();
        if let Some(name) = update.name {
            record.name = name;
        }
        if let Some(base_url) = update.base_url {
            record.base_url = Some(base_url);
        }
        if let Some(enabled) = update.enabled {
            record.enabled = enabled;
        }
        if let Some(warmup_enabled) = update.warmup_enabled {
            record.warmup_enabled = warmup_enabled;
        }
        if let Some(warmup_dialect_plugin) = update.warmup_dialect_plugin {
            record.warmup_dialect_plugin = Some(warmup_dialect_plugin);
        }
        if include_secret_fields {
            if let Some(api_key_ciphertext) = update.api_key_ciphertext {
                record.api_key_ciphertext = Some(api_key_ciphertext);
            }
            if let Some(oauth_token_generation) = update.oauth_token_generation {
                record.oauth_token_generation = oauth_token_generation;
            }
        }
        if always_bump_spec_revision || has_spec_update {
            record.revision = record.revision.saturating_add(1);
        }
        record.updated_at_unix_secs = now;
        Ok(record.clone())
    }

    fn validate_key_component(field: &str, value: &str) -> storage_api::StorageResult<()> {
        if value.contains('\0') {
            return Err(storage_api::StorageError::InvalidInput {
                field: field.to_owned(),
                reason: "must not contain NUL bytes".to_owned(),
            });
        }
        Ok(())
    }

    fn base64_url_no_pad(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
        for chunk in bytes.chunks(3) {
            let bits = (u32::from(chunk[0]) << 16)
                | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
                | u32::from(*chunk.get(2).unwrap_or(&0));
            output.push(char::from(ALPHABET[((bits >> 18) & 0x3f) as usize]));
            output.push(char::from(ALPHABET[((bits >> 12) & 0x3f) as usize]));
            if chunk.len() > 1 {
                output.push(char::from(ALPHABET[((bits >> 6) & 0x3f) as usize]));
            }
            if chunk.len() > 2 {
                output.push(char::from(ALPHABET[(bits & 0x3f) as usize]));
            }
        }
        output
    }

    fn remove_principal_chains(state: &mut PrincipalState, principal_id: Uuid) {
        state
            .chain_entries
            .retain(|_, entry| entry.principal_id != principal_id);
    }

    fn same_wasm_entry_metadata(
        existing: &storage_api::WasmRegistryEntry,
        input: &storage_api::WasmRegistryEntryInput,
    ) -> bool {
        let schema_hash_matches = match (existing.schema_hash, input.schema_hash) {
            (Some(existing), Some(input)) => existing == input,
            _ => true,
        };
        existing.name == input.name
            && existing.version == input.version
            && existing.original_filename == input.original_filename
            && existing.label == input.label
            && existing.description == input.description
            && existing.usage == input.usage
            && existing.hook_metadata == input.hook_metadata
            && existing.supported_slots == input.supported_slots
            && schema_hash_matches
    }

    fn registry_entry_with_live_refcount(
        entry: &storage_api::WasmRegistryEntry,
        principal_state: &PrincipalState,
        upstreams: &BTreeMap<Uuid, storage_api::UpstreamRecord>,
    ) -> storage_api::WasmRegistryEntry {
        let chain_references = principal_state
            .chain_entries
            .values()
            .filter(|chain| chain.wasm_registry_id == entry.id)
            .count();
        let warmup_references = upstreams
            .values()
            .filter(|upstream| {
                upstream.deleted_at_unix_secs.is_none()
                    && upstream
                        .warmup_dialect_plugin
                        .as_ref()
                        .is_some_and(|plugin| plugin.wasm_registry_id == entry.id)
            })
            .count();
        let mut entry = entry.clone();
        entry.refcount =
            i64::try_from(chain_references.saturating_add(warmup_references)).unwrap_or(i64::MAX);
        entry
    }

    fn subscription_quota_checkpoint_ranges(
        &self,
        query: &storage_api::SubscriptionQuotaCheckpointRangeQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::SubscriptionQuotaCheckpointRange>> {
        if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
            return Ok(Vec::new());
        }

        let state = lock_or_storage_error(&self.subscription_quota)?;
        let mut grouped = BTreeMap::<
            SubscriptionQuotaKey,
            Vec<storage_api::SubscriptionQuotaCheckpointRecord>,
        >::new();
        for record in state.checkpoints.iter().filter(|record| {
            query.upstream_ids.contains(&record.upstream_id)
                && query.windows.contains(&record.window)
                && query.sources.contains(&record.source)
        }) {
            grouped
                .entry((record.upstream_id, record.window, record.source))
                .or_default()
                .push(record.clone());
        }

        let mut ranges = Vec::with_capacity(grouped.len());
        for ((upstream_id, window, source), mut records) in grouped {
            records.sort_by_key(|record| (record.changed_at_unix_millis, record.sample_id));
            let left_anchor = records
                .iter()
                .filter(|record| record.changed_at_unix_millis < query.since_unix_millis)
                .max_by_key(|record| (record.changed_at_unix_millis, record.sample_id))
                .cloned();
            let checkpoints = records
                .into_iter()
                .filter(|record| {
                    record.changed_at_unix_millis >= query.since_unix_millis
                        && record.changed_at_unix_millis <= query.until_unix_millis
                })
                .collect::<Vec<_>>();
            if left_anchor.is_some() || !checkpoints.is_empty() {
                ranges.push(storage_api::SubscriptionQuotaCheckpointRange {
                    upstream_id,
                    window,
                    source,
                    left_anchor,
                    checkpoints,
                });
            }
        }
        Ok(ranges)
    }

    pub fn script_usage_rollups(
        &self,
        rollups: Vec<storage_api::UsageRollup>,
    ) -> storage_api::StorageResult<()> {
        *lock_or_storage_error(&self.usage_rollups)? = rollups;
        Ok(())
    }

    pub fn subscription_quota_sample_batches(
        &self,
    ) -> storage_api::StorageResult<Vec<Vec<storage_api::SubscriptionQuotaSample>>> {
        Ok(lock_or_storage_error(&self.subscription_quota_sample_batches)?.clone())
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self {
            api_key_usage_buckets: Mutex::new(HashMap::new()),
            api_key_usage_writers: Mutex::new(HashMap::new()),
            audit_entries: Mutex::new(Vec::new()),
            backend_kind: Mutex::new(storage_api::BackendKind::Sqlite),
            cache_keepalive_sessions: Mutex::new(HashMap::new()),
            cache_keepalive_decisions: Mutex::new(HashMap::new()),
            cache_keepalive_turns: Mutex::new(Vec::new()),
            config_draft: Mutex::new(storage_api::ConfigDraftState::default()),
            config_history: Mutex::new(BTreeMap::new()),
            killswitch_enabled: Mutex::new(false),
            entity_id_cursor: AtomicU64::new(0),
            principal_state: Mutex::new(PrincipalState {
                registry_entries: HashMap::from([(
                    storage_api::BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
                    storage_api::WasmRegistryEntry::builtin_subscription_preference(0),
                )]),
                ..PrincipalState::default()
            }),
            meta_values: Mutex::new(HashMap::new()),
            managed_keys: Mutex::new(BTreeMap::new()),
            plugin_blobs: Mutex::new(BTreeMap::from([(
                storage_api::BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256,
                Vec::new(),
            )])),
            pool_quota_snapshots: Mutex::new(BTreeMap::new()),
            plan_tiers: Mutex::new(PlanTierState::default()),
            subscription_quota: Mutex::new(SubscriptionQuotaState::default()),
            subscription_quota_sample_batches: Mutex::new(Vec::new()),
            organization_metadata: Mutex::new(BTreeMap::new()),
            upstream_subscription_metadata: Mutex::new(BTreeMap::new()),
            request_event_cursor: AtomicU64::new(0),
            price_catalog_snapshot: Mutex::new(None),
            request_event_notify: Notify::new(),
            request_events: Mutex::new(Vec::new()),
            upstreams: Mutex::new(BTreeMap::new()),
            upstream_rate_limit_observations: Mutex::new(Vec::new()),
            usage_rollup_checkpoint: Mutex::new(None),
            usage_rollups: Mutex::new(Vec::new()),
            clock: fixed_clock(1_700_000_000),
        }
    }
}

impl storage_api::AuditSink for InMemoryStorage {
    fn sink_audit(&self, entry: storage_api::AuditEntry) {
        self.audit_entries
            .lock()
            .expect("InMemoryStorage audit_entries mutex poisoned")
            .push(entry);
    }
}

#[async_trait]
impl storage_api::AuditStore for InMemoryStorage {
    async fn append_audit(
        &self,
        entry: &storage_api::AuditEntry,
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.audit_entries)?.push(entry.clone());
        Ok(())
    }

    async fn append_audit_entries(
        &self,
        entries: &[storage_api::AuditEntry],
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.audit_entries)?.extend_from_slice(entries);
        Ok(())
    }

    async fn query_audit(
        &self,
        principal_id: Option<&str>,
        since: u64,
        until: u64,
        limit: usize,
    ) -> storage_api::StorageResult<Vec<storage_api::AuditEntry>> {
        Ok(lock_or_storage_error(&self.audit_entries)?
            .iter()
            .filter(|entry| {
                entry.ts >= since
                    && entry.ts <= until
                    && principal_id.is_none_or(|id| entry.principal_id == id)
            })
            .take(limit)
            .cloned()
            .collect())
    }

    async fn prune_audit(&self, older_than: u64) -> storage_api::StorageResult<u64> {
        let mut entries = lock_or_storage_error(&self.audit_entries)?;
        let before = entries.len();
        entries.retain(|entry| entry.ts >= older_than);
        Ok(u64::try_from(before.saturating_sub(entries.len())).unwrap_or(u64::MAX))
    }

    async fn prune_audit_before(
        &self,
        cutoff_ts_x_1m: u64,
        batch_size: usize,
    ) -> storage_api::StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        const KEY_SEQUENCE_SCALE: u64 = 1_000_000;
        let cutoff = cutoff_ts_x_1m / KEY_SEQUENCE_SCALE;
        let mut entries = lock_or_storage_error(&self.audit_entries)?;
        let mut remaining_to_remove = batch_size;
        let before = entries.len();
        entries.retain(|entry| {
            if entry.ts < cutoff && remaining_to_remove > 0 {
                remaining_to_remove -= 1;
                false
            } else {
                true
            }
        });
        Ok(u64::try_from(before - entries.len()).unwrap_or(u64::MAX))
    }
}

#[async_trait]
impl storage_api::RequestEventStore for InMemoryStorage {
    async fn append_request_event(
        &self,
        event: &storage_api::RequestEvent,
    ) -> storage_api::StorageResult<u64> {
        let cursor = {
            let mut events = lock_or_storage_error(&self.request_events)?;
            if let Some(event_id) = event.event_id.as_deref()
                && let Some((cursor, _)) = events
                    .iter()
                    .find(|(_, stored)| stored.event_id.as_deref() == Some(event_id))
            {
                return Ok(*cursor);
            }

            let cursor = self.request_event_cursor.fetch_add(1, Ordering::Relaxed) + 1;
            events.push((cursor, event.clone()));
            cursor
        };
        self.request_event_notify.notify_waiters();
        Ok(cursor)
    }

    async fn append_request_event_with_projections(
        &self,
        event: &storage_api::RequestEvent,
        projections: &storage_api::RequestEventProjections,
    ) -> storage_api::StorageResult<u64> {
        let mut events = lock_or_storage_error(&self.request_events)?;
        let mut turns = lock_or_storage_error(&self.cache_keepalive_turns)?;
        let mut decisions = lock_or_storage_error(&self.cache_keepalive_decisions)?;

        let existing_cursor = event.event_id.as_deref().and_then(|event_id| {
            events
                .iter()
                .find(|(_, stored)| stored.event_id.as_deref() == Some(event_id))
                .map(|(cursor, _)| *cursor)
        });
        let inserted_event = existing_cursor.is_none();
        let cursor = existing_cursor.unwrap_or_else(|| {
            let cursor = self.request_event_cursor.fetch_add(1, Ordering::Relaxed) + 1;
            events.push((cursor, event.clone()));
            cursor
        });

        if let Some(turn) = projections.turn.as_ref()
            && !turns
                .iter()
                .any(|stored| stored.source_ref_id == turn.source_ref_id)
        {
            turns.push(storage_api::CacheKeepaliveTurnRecord {
                source_ref_id: turn.source_ref_id.clone(),
                session_key_hash: turn.session_key_hash.clone(),
                principal_id: turn.principal_id.clone(),
                accounting_key_id: turn.accounting_key_id.clone(),
                upstream_id: turn.upstream_id,
                model: turn.model.clone(),
                input_tokens: turn.input_tokens,
                output_tokens: turn.output_tokens,
                cache_creation_input_tokens: turn.cache_creation_input_tokens,
                cache_creation_input_tokens_5m: turn.cache_creation_input_tokens_5m,
                cache_creation_input_tokens_1h: turn.cache_creation_input_tokens_1h,
                cache_read_input_tokens: turn.cache_read_input_tokens,
                cost_micros: turn.cost_micros,
                hit_miss: turn.hit_miss.clone(),
                ts: turn.ts,
            });
        }
        let decision = &projections.decision;
        decisions
            .entry((
                decision.principal_id.clone(),
                decision.source_ref_id.clone(),
            ))
            .or_insert_with(|| storage_api::CacheKeepaliveDecisionRecord {
                source_ref_id: decision.source_ref_id.clone(),
                principal_id: decision.principal_id.clone(),
                session_key_hash: decision.session_key_hash.clone(),
                upstream_id: decision.upstream_id,
                decision: decision.decision.clone(),
                reason: decision.reason.clone(),
                error: decision.error.clone(),
                generation: decision.generation,
                ttl: decision.ttl,
                config_snapshot: decision.config_snapshot.clone(),
                last_message_at_ms: decision.last_message_at_ms,
                ts: decision.ts,
            });
        drop(decisions);
        drop(turns);
        drop(events);
        if inserted_event {
            self.request_event_notify.notify_waiters();
        }
        Ok(cursor)
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> storage_api::StorageResult<Vec<storage_api::RequestEvent>> {
        Ok(lock_or_storage_error(&self.request_events)?
            .iter()
            .filter(|(_, event)| event.ts >= since && event.ts <= until)
            .take(limit)
            .map(|(_, event)| event.clone())
            .collect())
    }

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> storage_api::StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        const KEY_SEQUENCE_SCALE: u64 = 1_000_000;
        let cutoff_ms = cutoff_ms_x_1m / KEY_SEQUENCE_SCALE;
        let mut events = lock_or_storage_error(&self.request_events)?;
        let cursors_to_remove = events
            .iter()
            .filter(|(_, event)| {
                event
                    .ts_ms
                    .unwrap_or_else(|| event.ts.saturating_mul(1_000))
                    < cutoff_ms
            })
            .take(batch_size)
            .map(|(cursor, _)| *cursor)
            .collect::<BTreeSet<_>>();
        let removed = cursors_to_remove.len();
        events.retain(|(cursor, _)| !cursors_to_remove.contains(cursor));
        Ok(u64::try_from(removed).unwrap_or(u64::MAX))
    }

    async fn current_request_event_cursor(&self) -> storage_api::StorageResult<u64> {
        Ok(self.request_event_cursor.load(Ordering::Relaxed))
    }

    async fn query_request_events_between_cursors(
        &self,
        after: u64,
        until: u64,
        limit: usize,
        filters: &storage_api::RequestEventStreamFilters,
    ) -> storage_api::StorageResult<Vec<(u64, storage_api::RequestEvent)>> {
        Ok(lock_or_storage_error(&self.request_events)?
            .iter()
            .filter(|(cursor, event)| {
                *cursor > after && *cursor <= until && request_event_matches_filters(event, filters)
            })
            .take(limit.min(500))
            .cloned()
            .collect())
    }

    async fn list_request_events(
        &self,
        query: &storage_api::RequestEventListQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::RequestEventListItem>> {
        if query.limit == 0 || query.until_unix_secs < query.since_unix_secs {
            return Ok(Vec::new());
        }
        let mut events = lock_or_storage_error(&self.request_events)?
            .iter()
            .filter(|(_, event)| {
                event.ts >= query.since_unix_secs
                    && event.ts <= query.until_unix_secs
                    && request_event_matches_filters(event, &query.filters)
                    && match query.source_kind.as_deref() {
                        Some("all") => true,
                        Some(kind) => event.source_kind.as_deref() == Some(kind),
                        None => event.source_kind.as_deref() != Some("renewal"),
                    }
                    && query.until_ts_ms.is_none_or(|until_ts_ms| {
                        let ts_ms = request_event_ts_ms(event);
                        ts_ms < until_ts_ms
                            || (ts_ms == until_ts_ms
                                && query.until_event_id.as_ref().is_some_and(|until_event_id| {
                                    request_event_list_key(event) < until_event_id.as_str()
                                }))
                    })
            })
            .map(|(cursor, event)| (*cursor, event.clone()))
            .collect::<Vec<_>>();
        events.sort_unstable_by(|(left_cursor, left), (right_cursor, right)| {
            request_event_ts_ms(right)
                .cmp(&request_event_ts_ms(left))
                .then_with(|| request_event_list_key(right).cmp(request_event_list_key(left)))
                .then_with(|| right_cursor.cmp(left_cursor))
        });
        Ok(events
            .into_iter()
            .take(query.limit)
            .map(|(_, event)| request_event_list_projection(&event))
            .collect())
    }

    async fn request_event_key_last_used(
        &self,
        query: &storage_api::RequestEventKeyLastUsedQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::RequestEventKeyLastUsed>> {
        if query.until_unix_secs < query.since_unix_secs {
            return Ok(Vec::new());
        }
        let mut last_used = BTreeMap::<String, u64>::new();
        for (_, event) in lock_or_storage_error(&self.request_events)?.iter() {
            let ts = event.ts_ms.map_or(event.ts, |ts_ms| ts_ms / 1_000);
            if event.principal_id.as_deref() != Some(query.principal_id.as_str())
                || ts < query.since_unix_secs
                || ts > query.until_unix_secs
            {
                continue;
            }
            let Some(key_id) = event.key_id.as_deref().filter(|key_id| !key_id.is_empty()) else {
                continue;
            };
            last_used
                .entry(key_id.to_owned())
                .and_modify(|stored| *stored = (*stored).max(ts))
                .or_insert(ts);
        }
        Ok(last_used
            .into_iter()
            .map(
                |(key_id, last_used_at_unix_secs)| storage_api::RequestEventKeyLastUsed {
                    key_id,
                    last_used_at_unix_secs,
                },
            )
            .collect())
    }
    async fn request_event_principal_costs(
        &self,
        _query: &storage_api::RequestEventPrincipalCostQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::RequestEventPrincipalCostBucket>> {
        unscripted!(RequestEventStore, request_event_principal_costs)
    }

    async fn get_request_event(
        &self,
        event_id: &str,
    ) -> storage_api::StorageResult<Option<storage_api::RequestEvent>> {
        Ok(lock_or_storage_error(&self.request_events)?
            .iter()
            .find(|(_, event)| event.event_id.as_deref() == Some(event_id))
            .map(|(_, event)| event.clone()))
    }
}

fn request_event_matches_filters(
    event: &storage_api::RequestEvent,
    filters: &storage_api::RequestEventStreamFilters,
) -> bool {
    if let Some(principal_id) = filters.principal_id.as_deref()
        && event.principal_id.as_deref() != Some(principal_id)
    {
        return false;
    }
    if let Some(thread_id) = filters.thread_id.as_deref()
        && event.thread_id.as_deref() != Some(thread_id)
    {
        return false;
    }
    if let Some(model) = filters.model.as_deref()
        && !storage_api::model_filter_matches(model, event.model.as_deref())
    {
        return false;
    }
    if let Some(upstream) = filters.upstream
        && event.upstream != Some(upstream)
    {
        return false;
    }
    if let Some(upstream_id) = filters.upstream_id
        && event.upstream_id != Some(upstream_id)
    {
        return false;
    }
    if let Some(status_class) = filters.status_class
        && !status_class.matches(event.status)
    {
        return false;
    }
    true
}

fn request_event_ts_ms(event: &storage_api::RequestEvent) -> u64 {
    event
        .ts_ms
        .unwrap_or_else(|| event.ts.saturating_mul(1_000))
}

fn request_event_list_key(event: &storage_api::RequestEvent) -> &str {
    event.event_id.as_deref().unwrap_or(&event.request_id)
}

fn request_event_list_projection(
    event: &storage_api::RequestEvent,
) -> storage_api::RequestEventListItem {
    storage_api::RequestEventListItem {
        ts: event.ts,
        ts_ms: Some(request_event_ts_ms(event)),
        request_id: event.request_id.clone(),
        event_id: event.event_id.clone(),
        source_kind: event.source_kind.clone(),
        principal_id: event.principal_id.clone(),
        upstream: event.upstream,
        upstream_id: event.upstream_id,
        upstream_name: event.upstream_name.clone(),
        thread_id: event.thread_id.clone(),
        observed_session_id: event.observed_session_id.clone(),
        request_kind: event.request_kind.clone(),
        claude_agent_id: event.claude_agent_id.clone(),
        claude_parent_agent_id: event.claude_parent_agent_id.clone(),
        parent_session_id: event.parent_session_id.clone(),
        client_app: event.client_app.clone(),
        session_id_source: event.session_id_source.clone(),
        model: event.model.clone(),
        reasoning_effort: event.reasoning_effort.clone(),
        thinking_budget_tokens: event.thinking_budget_tokens,
        thinking_tokens: event.thinking_tokens,
        service_tier: event.service_tier.clone(),
        status: event.status,
        error_code: event.error_code.clone(),
        upstream_error_type: event.upstream_error_type.clone(),
        upstream_error_message: event.upstream_error_message.clone(),
        duration_ms: event.duration_ms,
        request_body_read_ms: event.request_body_read_ms,
        request_body_first_chunk_ms: event.request_body_first_chunk_ms,
        request_body_receive_ms: event.request_body_receive_ms,
        request_body_wait_ms: event.request_body_wait_ms,
        request_body_process_ms: event.request_body_process_ms,
        request_body_chunk_count: event.request_body_chunk_count,
        request_body_bytes: event.request_body_bytes,
        auth_ms: event.auth_ms,
        route_ms: event.route_ms,
        limit_reserve_ms: event.limit_reserve_ms,
        json_parse_ms: event.json_parse_ms,
        cache_structure_ms: event.cache_structure_ms,
        cache_token_key_ms: event.cache_token_key_ms,
        cache_count_lookup_ms: event.cache_count_lookup_ms,
        cache_tokenizer_queue_ms: event.cache_tokenizer_queue_ms,
        cache_serialize_ms: event.cache_serialize_ms,
        cache_tokenize_ms: event.cache_tokenize_ms,
        prepare_signer_ms: event.prepare_signer_ms,
        bulkhead_wait_ms: event.bulkhead_wait_ms,
        dns_ms: event.dns_ms,
        connect_ms: event.connect_ms,
        connection_reused: event.connection_reused,
        limit_reconcile_ms: event.limit_reconcile_ms,
        observability_post_ms: event.observability_post_ms,
        proxy_setup_ms: event.proxy_setup_ms,
        shape_ms: event.shape_ms,
        sign_ms: event.sign_ms,
        upstream_ttfb_ms: event.upstream_ttfb_ms,
        upstream_body_ms: event.upstream_body_ms,
        response_body_wait_ms: event.response_body_wait_ms,
        response_body_process_ms: event.response_body_process_ms,
        response_body_downstream_poll_gap_ms: event.response_body_downstream_poll_gap_ms,
        retry_overhead_ms: event.retry_overhead_ms,
        finalize_ms: event.finalize_ms,
        stream_first_content_delta_ms: event.stream_first_content_delta_ms,
        stream_last_content_delta_ms: event.stream_last_content_delta_ms,
        inter_token_avg_ms: event.inter_token_avg_ms,
        input_tokens: event.input_tokens,
        output_tokens: event.output_tokens,
        cache_creation_input_tokens: event.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: event.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: event.cache_creation_input_tokens_1h,
        cache_read_input_tokens: event.cache_read_input_tokens,
        cost_usd_micros: event.cost_usd_micros,
        cost_input_micros: event.cost_input_micros,
        cost_output_micros: event.cost_output_micros,
        cost_cache_creation_5m_micros: event.cost_cache_creation_5m_micros,
        cost_cache_creation_1h_micros: event.cost_cache_creation_1h_micros,
        cost_cache_read_micros: event.cost_cache_read_micros,
    }
}

#[async_trait]
impl storage_api::ManagedKeyStore for InMemoryStorage {
    async fn issue(
        &self,
        principal_id: &str,
        key_id: &str,
        params: storage_api::IssueParams,
    ) -> storage_api::StorageResult<storage_api::StoredApiKeyRecord> {
        Self::validate_key_component("principal_id", principal_id)?;
        Self::validate_key_component("key_id", key_id)?;
        let mut records = lock_or_storage_error(&self.managed_keys)?;
        let key = (principal_id.to_owned(), key_id.to_owned());
        if records.contains_key(&key)
            || records
                .values()
                .any(|record| record.index_hash == params.index_hash)
        {
            return Err(storage_api::StorageError::Conflict {
                message: "managed API key already exists".to_owned(),
            });
        }
        let record = storage_api::StoredApiKeyRecord {
            label: params.label,
            issued_at_unix_secs: unix_secs(self.clock.now()),
            revoked_at_unix_secs: None,
            key_hash_b64: Self::base64_url_no_pad(&params.verify_hash),
            verify_hash: params.verify_hash,
            secret_salt: params.secret_salt,
            upstream_kind: params.upstream_kind,
            limit_overrides: params.limit_overrides,
            status: storage_api::KeyStatus::Active,
            expires_at_unix_secs: params.expires_at_unix_secs,
            last_4: params.last_4,
            description: params.description,
            principal_kind: params.principal_kind,
            index_hash: params.index_hash,
        };
        records.insert(key, record.clone());
        Ok(record)
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> storage_api::StorageResult<Option<storage_api::StoredApiKeyRecord>> {
        Self::validate_key_component("principal_id", principal_id)?;
        Self::validate_key_component("key_id", key_id)?;
        Ok(lock_or_storage_error(&self.managed_keys)?
            .get(&(principal_id.to_owned(), key_id.to_owned()))
            .cloned())
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> storage_api::StorageResult<Option<(String, String, storage_api::StoredApiKeyRecord)>> {
        Ok(lock_or_storage_error(&self.managed_keys)?
            .iter()
            .find(|(_, record)| record.index_hash == *index_hash)
            .map(|((principal_id, key_id), record)| {
                (principal_id.clone(), key_id.clone(), record.clone())
            }))
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> storage_api::StorageResult<Vec<storage_api::StoredApiKeyRecord>> {
        Self::validate_key_component("principal_id", principal_id)?;
        Ok(lock_or_storage_error(&self.managed_keys)?
            .iter()
            .filter(|((stored_principal_id, _), _)| stored_principal_id == principal_id)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn list_all(
        &self,
    ) -> storage_api::StorageResult<Vec<(String, String, storage_api::StoredApiKeyRecord)>> {
        Ok(lock_or_storage_error(&self.managed_keys)?
            .iter()
            .map(|((principal_id, key_id), record)| {
                (principal_id.clone(), key_id.clone(), record.clone())
            })
            .collect())
    }

    async fn update(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: storage_api::ApiKeyMutation,
    ) -> storage_api::StorageResult<()> {
        Self::validate_key_component("principal_id", principal_id)?;
        Self::validate_key_component("key_id", key_id)?;
        let mut records = lock_or_storage_error(&self.managed_keys)?;
        let record = records
            .get_mut(&(principal_id.to_owned(), key_id.to_owned()))
            .ok_or_else(|| storage_api::StorageError::Fatal {
                message: "managed API key not found".to_owned(),
            })?;
        if let Some(label) = mutation.label {
            record.label = label;
        }
        if let Some(description) = mutation.description {
            record.description = description;
        }
        if let Some(expires_at_unix_secs) = mutation.expires_at_unix_secs {
            record.expires_at_unix_secs = expires_at_unix_secs;
        }
        if let Some(limit_overrides) = mutation.limit_overrides {
            record.limit_overrides = limit_overrides;
        }
        if let Some(status) = mutation.status {
            record.status = status;
        }
        Ok(())
    }

    async fn revoke_zero_secrets(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> storage_api::StorageResult<()> {
        Self::validate_key_component("principal_id", principal_id)?;
        Self::validate_key_component("key_id", key_id)?;
        let mut records = lock_or_storage_error(&self.managed_keys)?;
        let record = records
            .get_mut(&(principal_id.to_owned(), key_id.to_owned()))
            .ok_or_else(|| storage_api::StorageError::Fatal {
                message: "managed API key not found".to_owned(),
            })?;
        record.status = storage_api::KeyStatus::Revoked;
        record.revoked_at_unix_secs = Some(unix_secs(self.clock.now()));
        record.index_hash = [0; 32];
        record.verify_hash = [0; 32];
        record.secret_salt = [0; 16];
        record.key_hash_b64.clear();
        Ok(())
    }
}

#[async_trait]
impl storage_api::ConfigStore for InMemoryStorage {
    async fn get_config_draft(&self) -> storage_api::StorageResult<storage_api::ConfigDraftState> {
        Ok(lock_or_storage_error(&self.config_draft)?.clone())
    }
    async fn put_config_draft(
        &self,
        mut new: storage_api::ConfigDraftState,
        expected_revision: u64,
    ) -> storage_api::StorageResult<u64> {
        let mut draft = lock_or_storage_error(&self.config_draft)?;
        if draft.revision != expected_revision {
            return Err(storage_api::StorageError::Conflict {
                message: "stale config draft revision".to_owned(),
            });
        }
        let revision =
            expected_revision
                .checked_add(1)
                .ok_or_else(|| storage_api::StorageError::Fatal {
                    message: "config draft revision overflow".to_owned(),
                })?;
        new.revision = revision;
        new.last_validated_revision = None;
        new.last_validation_error = None;
        *draft = new;
        Ok(revision)
    }

    async fn set_last_validated_revision(
        &self,
        revision: u64,
        error: Option<String>,
    ) -> storage_api::StorageResult<()> {
        let mut draft = lock_or_storage_error(&self.config_draft)?;
        if draft.revision != revision {
            return Err(storage_api::StorageError::Conflict {
                message: "stale config draft revision".to_owned(),
            });
        }
        match error {
            Some(error) => draft.last_validation_error = Some(error),
            None => {
                draft.last_validated_revision = Some(revision);
                draft.last_validation_error = None;
            }
        }
        Ok(())
    }

    async fn append_config_history(
        &self,
        revision: u64,
        config_toml: String,
        applied_at_unix_secs: u64,
        summary: storage_api::HistorySummary,
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.config_history)?.insert(
            revision,
            storage_api::HistoryEntry {
                revision,
                config_toml,
                applied_at_unix_secs,
                summary,
            },
        );
        Ok(())
    }

    async fn list_config_history(
        &self,
        limit: usize,
    ) -> storage_api::StorageResult<Vec<storage_api::HistoryEntry>> {
        Ok(lock_or_storage_error(&self.config_history)?
            .values()
            .rev()
            .take(limit)
            .cloned()
            .collect())
    }

    async fn get_config_history(
        &self,
        revision: u64,
    ) -> storage_api::StorageResult<Option<storage_api::HistoryEntry>> {
        Ok(lock_or_storage_error(&self.config_history)?
            .get(&revision)
            .cloned())
    }
}

#[async_trait]
impl storage_api::MetaStore for InMemoryStorage {
    async fn initialize(
        &self,
        requested: storage_api::BackendKind,
    ) -> storage_api::StorageResult<()> {
        let stored = *lock_or_storage_error(&self.backend_kind)?;
        if stored != requested {
            return Err(storage_api::StorageError::BackendKindMismatch {
                stored,
                configured: requested,
            });
        }
        Ok(())
    }

    async fn contract_version(&self) -> storage_api::StorageResult<u32> {
        Ok(storage_api::CURRENT_CONTRACT_VERSION)
    }

    async fn backend_kind(&self) -> storage_api::StorageResult<storage_api::BackendKind> {
        Ok(*lock_or_storage_error(&self.backend_kind)?)
    }

    async fn killswitch_enabled(&self) -> storage_api::StorageResult<bool> {
        Ok(*lock_or_storage_error(&self.killswitch_enabled)?)
    }

    async fn set_killswitch_enabled(&self, enabled: bool) -> storage_api::StorageResult<()> {
        *lock_or_storage_error(&self.killswitch_enabled)? = enabled;
        Ok(())
    }

    async fn get_meta_value(&self, key: &str) -> storage_api::StorageResult<Option<String>> {
        Ok(lock_or_storage_error(&self.meta_values)?.get(key).cloned())
    }

    async fn put_meta_value(&self, key: &str, value: &str) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.meta_values)?.insert(key.to_owned(), value.to_owned());
        Ok(())
    }
}

#[async_trait]
impl storage_api::PluginRegistryStore for InMemoryStorage {
    async fn persist_wasm_upload(
        &self,
        blob: storage_api::WasmBlob,
        entry: storage_api::WasmRegistryEntryInput,
    ) -> storage_api::StorageResult<(storage_api::WasmRegistryEntry, bool)> {
        storage_api::validate_identifier("plugin.name", &entry.name)?;
        if blob.size_bytes > storage_api::MAX_WASM_BLOB_BYTES
            || blob.bytes.len() as u64 != blob.size_bytes
        {
            return Err(storage_api::StorageError::InvalidInput {
                field: "wasm_blob.bytes".to_owned(),
                reason: "blob must be <= 32 MiB and size_bytes must match bytes length".to_owned(),
            });
        }

        let mut state = lock_or_storage_error(&self.principal_state)?;
        if let Some(existing) = state
            .registry_entries
            .values()
            .find(|existing| existing.sha256 == blob.sha256)
            .cloned()
        {
            if !Self::same_wasm_entry_metadata(&existing, &entry) {
                return Err(storage_api::StorageError::Conflict {
                    message: "sha256 already registered for a different wasm entry".to_owned(),
                });
            }
            let mut blobs = lock_or_storage_error(&self.plugin_blobs)?;
            let was_fully_existing = blobs.contains_key(&blob.sha256);
            blobs.entry(blob.sha256).or_insert(blob.bytes);
            drop(blobs);
            let upstreams = lock_or_storage_error(&self.upstreams)?;
            let existing = Self::registry_entry_with_live_refcount(&existing, &state, &upstreams);
            return Ok((existing, was_fully_existing));
        }
        if state
            .registry_entries
            .values()
            .any(|existing| existing.name == entry.name)
        {
            return Err(storage_api::StorageError::Conflict {
                message: "live plugin name already exists".to_owned(),
            });
        }

        let record = storage_api::WasmRegistryEntry {
            id: self.next_entity_id(),
            sha256: blob.sha256,
            name: entry.name,
            version: entry.version,
            original_filename: entry.original_filename,
            label: entry.label,
            uploaded_at_unix_secs: entry.uploaded_at_unix_secs,
            uploaded_by_admin_id: entry.uploaded_by_admin_id,
            refcount: 0,
            revision: 0,
            kind: storage_api::BUILTIN_PLUGIN_KIND_FILTER.to_owned(),
            description: entry.description,
            usage: entry.usage,
            hook_metadata: entry.hook_metadata,
            is_builtin: false,
            metadata: None,
            supported_slots: entry.supported_slots,
            schema_hash: entry.schema_hash,
        };
        lock_or_storage_error(&self.plugin_blobs)?.insert(blob.sha256, blob.bytes);
        state.registry_entries.insert(record.id, record.clone());
        Ok((record, false))
    }

    async fn get_blob(
        &self,
        sha256: [u8; 32],
    ) -> storage_api::StorageResult<Option<storage_api::WasmBlobRecord>> {
        Ok(lock_or_storage_error(&self.plugin_blobs)?
            .contains_key(&sha256)
            .then_some(storage_api::WasmBlobRecord { sha256 }))
    }

    async fn get_blob_bytes(
        &self,
        sha256: [u8; 32],
    ) -> storage_api::StorageResult<Option<Vec<u8>>> {
        Ok(lock_or_storage_error(&self.plugin_blobs)?
            .get(&sha256)
            .cloned())
    }

    async fn list_orphan_blobs(&self) -> storage_api::StorageResult<Vec<[u8; 32]>> {
        let registered = lock_or_storage_error(&self.principal_state)?
            .registry_entries
            .values()
            .map(|entry| entry.sha256)
            .collect::<BTreeSet<_>>();
        Ok(lock_or_storage_error(&self.plugin_blobs)?
            .keys()
            .filter(|sha256| !registered.contains(*sha256))
            .copied()
            .collect())
    }

    async fn list_registry(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> storage_api::StorageResult<Vec<storage_api::WasmRegistryEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let state = lock_or_storage_error(&self.principal_state)?;
        let upstreams = lock_or_storage_error(&self.upstreams)?;
        let mut entries = state
            .registry_entries
            .values()
            .filter(|entry| after.is_none_or(|after| entry.id > after))
            .map(|entry| Self::registry_entry_with_live_refcount(entry, &state, &upstreams))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| entry.id);
        entries.truncate(limit);
        Ok(entries)
    }

    async fn get_registry_entry_by_sha(
        &self,
        sha256: [u8; 32],
    ) -> storage_api::StorageResult<Option<storage_api::WasmRegistryEntry>> {
        let state = lock_or_storage_error(&self.principal_state)?;
        let upstreams = lock_or_storage_error(&self.upstreams)?;
        Ok(state
            .registry_entries
            .values()
            .find(|entry| entry.sha256 == sha256)
            .map(|entry| Self::registry_entry_with_live_refcount(entry, &state, &upstreams)))
    }

    async fn get_registry_entry_by_name(
        &self,
        name: &str,
    ) -> storage_api::StorageResult<Option<storage_api::WasmRegistryEntry>> {
        let state = lock_or_storage_error(&self.principal_state)?;
        let upstreams = lock_or_storage_error(&self.upstreams)?;
        Ok(state
            .registry_entries
            .values()
            .find(|entry| entry.name == name)
            .map(|entry| Self::registry_entry_with_live_refcount(entry, &state, &upstreams)))
    }

    async fn get_registry_entry_by_id(
        &self,
        id: Uuid,
    ) -> storage_api::StorageResult<Option<storage_api::WasmRegistryEntry>> {
        let state = lock_or_storage_error(&self.principal_state)?;
        let upstreams = lock_or_storage_error(&self.upstreams)?;
        Ok(state
            .registry_entries
            .get(&id)
            .map(|entry| Self::registry_entry_with_live_refcount(entry, &state, &upstreams)))
    }

    async fn replace_wasm_entry(
        &self,
        _blob: storage_api::WasmBlob,
        _entry: storage_api::WasmRegistryEntryInput,
        _expected_revision: u64,
    ) -> storage_api::StorageResult<storage_api::WasmRegistryEntry> {
        unscripted!(PluginRegistryStore, replace_wasm_entry)
    }

    async fn list_registry_references(
        &self,
        id: Uuid,
    ) -> storage_api::StorageResult<storage_api::WasmRegistryReferences> {
        let state = lock_or_storage_error(&self.principal_state)?;
        let upstreams = lock_or_storage_error(&self.upstreams)?;
        let mut chains = state
            .chain_entries
            .values()
            .filter(|entry| entry.wasm_registry_id == id)
            .collect::<Vec<_>>();
        chains.sort_unstable_by_key(|entry| entry.id);

        let mut references = Vec::with_capacity(chains.len());
        for entry in chains {
            let principal_name = state
                .records
                .get(&entry.principal_id)
                .map(|principal| principal.name.clone())
                .ok_or_else(|| storage_api::StorageError::Corrupted {
                    message: "plugin chain principal is missing".to_owned(),
                })?;
            references.push(storage_api::WasmRegistryReference::PluginChain {
                chain_entry_id: entry.id,
                principal_id: entry.principal_id,
                principal_name,
                slot: entry.slot,
                revision: entry.revision,
            });
        }

        let mut warmups = upstreams
            .values()
            .filter(|upstream| {
                upstream.deleted_at_unix_secs.is_none()
                    && upstream
                        .warmup_dialect_plugin
                        .as_ref()
                        .is_some_and(|plugin| plugin.wasm_registry_id == id)
            })
            .collect::<Vec<_>>();
        warmups.sort_unstable_by_key(|upstream| upstream.id);
        references.reserve(warmups.len());
        for upstream in warmups {
            references.push(storage_api::WasmRegistryReference::UpstreamWarmupDialect {
                upstream_id: upstream.id,
                upstream_name: upstream.name.clone(),
                revision: upstream.revision,
            });
        }

        Ok(storage_api::WasmRegistryReferences::from_references(
            references,
        ))
    }

    async fn cascade_delete_registry_entry(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _expected_references: storage_api::WasmRegistryReferenceFingerprint,
    ) -> storage_api::StorageResult<Option<storage_api::WasmRegistryCascadeDelete>> {
        unscripted!(PluginRegistryStore, cascade_delete_registry_entry)
    }

    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> storage_api::StorageResult<storage_api::WasmRegistryEntry> {
        let mut state = lock_or_storage_error(&self.principal_state)?;
        let updated = {
            let entry = state.registry_entries.get_mut(&id).ok_or_else(|| {
                storage_api::StorageError::Conflict {
                    message: "unknown plugin registry entry".to_owned(),
                }
            })?;
            if entry.revision != expected_revision {
                return Err(storage_api::StorageError::StalePluginRegistryRevision {
                    current: entry.revision,
                });
            }
            entry.label = label;
            entry.revision = entry.revision.saturating_add(1);
            entry.clone()
        };
        let upstreams = lock_or_storage_error(&self.upstreams)?;
        Ok(Self::registry_entry_with_live_refcount(
            &updated, &state, &upstreams,
        ))
    }

    async fn update_supported_slots(
        &self,
        id: Uuid,
        supported_slots: Vec<storage_api::PluginSlotKind>,
    ) -> storage_api::StorageResult<()> {
        if let Some(entry) = lock_or_storage_error(&self.principal_state)?
            .registry_entries
            .get_mut(&id)
        {
            entry.supported_slots = supported_slots;
        }
        Ok(())
    }

    async fn delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> storage_api::StorageResult<Option<storage_api::WasmRegistryEntry>> {
        let mut state = lock_or_storage_error(&self.principal_state)?;
        let Some(entry) = state.registry_entries.get(&id).cloned() else {
            return Ok(None);
        };
        if entry.revision != expected_revision {
            return Err(storage_api::StorageError::StalePluginRegistryRevision {
                current: entry.revision,
            });
        }
        if let Some(reference) = state
            .chain_entries
            .values()
            .filter(|chain| chain.wasm_registry_id == id)
            .min_by_key(|chain| chain.id)
        {
            return Err(storage_api::StorageError::PluginRegistryReferenced {
                id: reference.id.to_string(),
            });
        }
        if let Some(reference) = lock_or_storage_error(&self.upstreams)?
            .values()
            .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
            .find(|upstream| {
                upstream
                    .warmup_dialect_plugin
                    .as_ref()
                    .is_some_and(|plugin| plugin.wasm_registry_id == id)
            })
        {
            return Err(storage_api::StorageError::PluginRegistryReferenced {
                id: reference.id.to_string(),
            });
        }
        state.registry_entries.remove(&id);
        let sha_is_still_referenced = state
            .registry_entries
            .values()
            .any(|remaining| remaining.sha256 == entry.sha256);
        drop(state);
        if !sha_is_still_referenced {
            lock_or_storage_error(&self.plugin_blobs)?.remove(&entry.sha256);
        }
        Ok(Some(entry))
    }

    async fn decrement_blob_refcount_or_delete(
        &self,
        sha256: [u8; 32],
    ) -> storage_api::StorageResult<bool> {
        let is_registered = lock_or_storage_error(&self.principal_state)?
            .registry_entries
            .values()
            .any(|entry| entry.sha256 == sha256);
        if is_registered {
            return Ok(false);
        }
        Ok(lock_or_storage_error(&self.plugin_blobs)?
            .remove(&sha256)
            .is_some())
    }

    async fn insert_chain_entry(
        &self,
        entry: storage_api::PluginChainEntryInput,
    ) -> storage_api::StorageResult<storage_api::PluginChainEntry> {
        let mut state = lock_or_storage_error(&self.principal_state)?;
        if !state.registry_entries.contains_key(&entry.wasm_registry_id) {
            return Err(storage_api::StorageError::PluginRegistryConflict {
                message: "unknown plugin registry entry".to_owned(),
            });
        }
        if !state
            .records
            .get(&entry.principal_id)
            .is_some_and(|principal| principal.deleted_at_unix_secs.is_none())
        {
            return Err(storage_api::StorageError::PrincipalNotFound {
                id: entry.principal_id.to_string(),
            });
        }
        if entry.slot == storage_api::PluginSlotKind::Shape
            && let Some(existing) = state.chain_entries.values().find(|existing| {
                existing.principal_id == entry.principal_id && existing.slot == entry.slot
            })
        {
            return Err(storage_api::StorageError::PluginChainConflict {
                reason: storage_api::PluginChainConflictReason::SlotIsSingleton {
                    existing_entry_id: existing.id,
                },
            });
        }
        let record = storage_api::PluginChainEntry {
            id: self.next_entity_id(),
            principal_id: entry.principal_id,
            slot: entry.slot,
            order: entry.order,
            wasm_registry_id: entry.wasm_registry_id,
            config: entry.config,
            sse_per_event: entry.sse_per_event,
            batched_events_per_flush: entry.batched_events_per_flush,
            batched_flush_ms: entry.batched_flush_ms,
            revision: 0,
        };
        state.chain_entries.insert(record.id, record.clone());
        Ok(record)
    }

    async fn list_chain_for_principal(
        &self,
        principal_id: Uuid,
        slot: storage_api::PluginSlotKind,
    ) -> storage_api::StorageResult<Vec<storage_api::PluginChainEntry>> {
        let mut entries = lock_or_storage_error(&self.principal_state)?
            .chain_entries
            .values()
            .filter(|entry| entry.principal_id == principal_id && entry.slot == slot)
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.order, entry.id));
        Ok(entries)
    }

    async fn update_chain_entry(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: storage_api::PluginChainEntryUpdate,
    ) -> storage_api::StorageResult<Option<storage_api::PluginChainEntry>> {
        unscripted!(PluginRegistryStore, update_chain_entry)
    }

    async fn reorder_chain(
        &self,
        principal_id: Uuid,
        slot: storage_api::PluginSlotKind,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> storage_api::StorageResult<Vec<storage_api::PluginChainEntry>> {
        let mut state = lock_or_storage_error(&self.principal_state)?;
        let mut staged_orders = HashMap::with_capacity(new_orders.len());
        let mut seen_ids = BTreeSet::new();
        for (id, order, expected_revision) in &new_orders {
            if !seen_ids.insert(*id) {
                return Err(storage_api::StorageError::Conflict {
                    message: "duplicate_plugin_chain_entry".to_owned(),
                });
            }
            let entry =
                state
                    .chain_entries
                    .get(id)
                    .ok_or_else(|| storage_api::StorageError::Conflict {
                        message: "unknown plugin chain entry".to_owned(),
                    })?;
            if entry.principal_id != principal_id || entry.slot != slot {
                return Err(storage_api::StorageError::Conflict {
                    message: "plugin chain entry is not in requested chain".to_owned(),
                });
            }
            if entry.revision != *expected_revision {
                return Err(storage_api::StorageError::StalePluginChainRevision {
                    current: entry.revision,
                });
            }
            staged_orders.insert(*id, *order);
        }

        let mut prospective = state
            .chain_entries
            .values()
            .filter(|entry| entry.principal_id == principal_id && entry.slot == slot)
            .cloned()
            .collect::<Vec<_>>();
        for entry in &mut prospective {
            if let Some(order) = staged_orders.get(&entry.id) {
                entry.order = *order;
            }
        }
        prospective.sort_by_key(|entry| (entry.order, entry.id));
        if prospective
            .windows(2)
            .any(|pair| pair[1].order.checked_sub(pair[0].order).unwrap_or(i64::MAX) < 2)
        {
            return Err(storage_api::StorageError::PluginChainConflict {
                reason: storage_api::PluginChainConflictReason::InvalidOrderGap,
            });
        }

        for (id, order, _) in new_orders {
            let entry = state
                .chain_entries
                .get_mut(&id)
                .expect("validated plugin chain entry remains present");
            entry.order = order;
            entry.revision = entry.revision.saturating_add(1);
        }
        let mut entries = state
            .chain_entries
            .values()
            .filter(|entry| entry.principal_id == principal_id && entry.slot == slot)
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.order, entry.id));
        Ok(entries)
    }

    async fn delete_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> storage_api::StorageResult<Option<storage_api::PluginChainEntry>> {
        let mut state = lock_or_storage_error(&self.principal_state)?;
        let Some(entry) = state.chain_entries.get(&id) else {
            return Ok(None);
        };
        if entry.revision != expected_revision {
            return Err(storage_api::StorageError::StalePluginChainRevision {
                current: entry.revision,
            });
        }
        Ok(state.chain_entries.remove(&id))
    }

    async fn rebalance_chain(
        &self,
        principal_id: Uuid,
        slot: storage_api::PluginSlotKind,
    ) -> storage_api::StorageResult<Vec<storage_api::PluginChainEntry>> {
        let entries =
            storage_api::PluginRegistryStore::list_chain_for_principal(self, principal_id, slot)
                .await?;
        let orders = storage_api::sparse_order::rebalance(vec![0; entries.len()]);
        storage_api::PluginRegistryStore::reorder_chain(
            self,
            principal_id,
            slot,
            entries
                .into_iter()
                .zip(orders)
                .map(|(entry, order)| (entry.id, order, entry.revision))
                .collect(),
        )
        .await
    }
}

fn plan_tier_invalid(field: &str, reason: &str) -> storage_api::StorageError {
    storage_api::StorageError::InvalidInput {
        field: field.to_owned(),
        reason: reason.to_owned(),
    }
}

fn plan_tier_conflict(message: &str) -> storage_api::StorageError {
    storage_api::StorageError::Conflict {
        message: message.to_owned(),
    }
}

fn is_known_plan_tier(tier_key: &str) -> bool {
    matches!(
        tier_key,
        "pro" | "team_standard" | "max_5x" | "team_premium" | "max_20x"
    )
}

fn validate_plan_tier_common(
    effective_from_unix_millis: i64,
    provenance: &str,
    created_at_unix_millis: i64,
) -> storage_api::StorageResult<()> {
    if effective_from_unix_millis < 0 {
        return Err(plan_tier_invalid(
            "effective_from_unix_millis",
            "must be non-negative",
        ));
    }
    if provenance.trim().is_empty() {
        return Err(plan_tier_invalid("provenance", "must not be empty"));
    }
    if created_at_unix_millis < 0 {
        return Err(plan_tier_invalid(
            "created_at_unix_millis",
            "must be non-negative",
        ));
    }
    Ok(())
}

fn validate_plan_tier_ratio(
    record: &storage_api::PlanTierRatioRecord,
) -> storage_api::StorageResult<()> {
    if !is_known_plan_tier(&record.tier_key) {
        return Err(plan_tier_invalid("tier_key", "is not a known plan tier"));
    }
    if !record.pro_relative_ratio.is_finite() || record.pro_relative_ratio <= 0.0 {
        return Err(plan_tier_invalid(
            "pro_relative_ratio",
            "must be finite and > 0",
        ));
    }
    validate_plan_tier_common(
        record.effective_from_unix_millis,
        &record.provenance,
        record.created_at_unix_millis,
    )
}

fn validate_metadata_tier_override(
    record: &storage_api::MetadataTierMappingOverrideRecord,
) -> storage_api::StorageResult<()> {
    if !is_known_plan_tier(&record.tier_key) {
        return Err(plan_tier_invalid("tier_key", "is not a known plan tier"));
    }
    validate_plan_tier_common(
        record.effective_from_unix_millis,
        &record.provenance,
        record.created_at_unix_millis,
    )
}

fn validate_upstream_plan_tier(
    record: &storage_api::UpstreamPlanTierRecord,
) -> storage_api::StorageResult<()> {
    match record.resolution_source {
        storage_api::TierResolutionSource::Unknown => {
            if record.tier_key.is_some() {
                return Err(plan_tier_invalid(
                    "tier_key",
                    "must be NULL when resolution_source is unknown",
                ));
            }
            if record.resolved_ratio_snapshot.is_some() {
                return Err(plan_tier_invalid(
                    "resolved_ratio_snapshot",
                    "must be NULL when resolution_source is unknown",
                ));
            }
        }
        storage_api::TierResolutionSource::Override
        | storage_api::TierResolutionSource::Builtin
        | storage_api::TierResolutionSource::Backfill => {
            let tier_key = record.tier_key.as_deref().ok_or_else(|| {
                plan_tier_invalid(
                    "tier_key",
                    "must be set when resolution_source is override, builtin, or backfill",
                )
            })?;
            if !is_known_plan_tier(tier_key) {
                return Err(plan_tier_invalid("tier_key", "is not a known plan tier"));
            }
            let ratio = record.resolved_ratio_snapshot.ok_or_else(|| {
                plan_tier_invalid(
                    "resolved_ratio_snapshot",
                    "must be set when resolution_source is override, builtin, or backfill",
                )
            })?;
            if !ratio.is_finite() || ratio <= 0.0 {
                return Err(plan_tier_invalid(
                    "resolved_ratio_snapshot",
                    "must be finite and > 0",
                ));
            }
        }
    }
    if record.observed_at_unix_millis < 0 {
        return Err(plan_tier_invalid(
            "observed_at_unix_millis",
            "must be non-negative",
        ));
    }
    validate_plan_tier_common(
        record.effective_from_unix_millis,
        &record.provenance,
        record.created_at_unix_millis,
    )
}

fn plan_tier_row_is_effective(from: i64, to: Option<i64>, as_of: i64) -> bool {
    from <= as_of && to.is_none_or(|to| to > as_of)
}

fn upstream_plan_tier_open_matches(
    current: &storage_api::UpstreamPlanTierRecord,
    desired: &storage_api::UpstreamPlanTierRecord,
) -> bool {
    current.tier_key == desired.tier_key
        && current.resolution_source == desired.resolution_source
        && current.organization_type == desired.organization_type
        && current.rate_limit_tier == desired.rate_limit_tier
        && current.seat_tier == desired.seat_tier
}

#[async_trait]
impl storage_api::PlanTierStore for InMemoryStorage {
    async fn upsert_plan_tier_ratio(
        &self,
        record: &storage_api::PlanTierRatioRecord,
    ) -> storage_api::StorageResult<()> {
        validate_plan_tier_ratio(record)?;
        let mut state = lock_or_storage_error(&self.plan_tiers)?;
        if let Some(index) = state.ratios.iter().position(|row| {
            row.tier_key == record.tier_key && row.effective_to_unix_millis.is_none()
        }) {
            let current = &state.ratios[index];
            if current.pro_relative_ratio == record.pro_relative_ratio {
                return Ok(());
            }
            if record.effective_from_unix_millis <= current.effective_from_unix_millis {
                return Err(plan_tier_conflict(
                    "effective_from_unix_millis must increase when closing an open plan-tier row",
                ));
            }
            state.ratios[index].effective_to_unix_millis = Some(record.effective_from_unix_millis);
        }
        let mut opened = record.clone();
        opened.effective_to_unix_millis = None;
        state.ratios.push(opened);
        Ok(())
    }

    async fn list_current_plan_tier_ratios(
        &self,
    ) -> storage_api::StorageResult<Vec<storage_api::PlanTierRatioRecord>> {
        let state = lock_or_storage_error(&self.plan_tiers)?;
        let mut rows = state
            .ratios
            .iter()
            .filter(|row| row.effective_to_unix_millis.is_none())
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.tier_key.cmp(&right.tier_key));
        Ok(rows)
    }

    async fn list_plan_tier_ratios_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> storage_api::StorageResult<Vec<storage_api::PlanTierRatioRecord>> {
        let state = lock_or_storage_error(&self.plan_tiers)?;
        let mut rows = state
            .ratios
            .iter()
            .filter(|row| {
                plan_tier_row_is_effective(
                    row.effective_from_unix_millis,
                    row.effective_to_unix_millis,
                    as_of_unix_millis,
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.tier_key.cmp(&right.tier_key));
        Ok(rows)
    }

    async fn upsert_metadata_tier_override(
        &self,
        record: &storage_api::MetadataTierMappingOverrideRecord,
    ) -> storage_api::StorageResult<()> {
        validate_metadata_tier_override(record)?;
        let mut opened = record.clone();
        opened.organization_type = opened.organization_type.filter(|value| !value.is_empty());
        opened.rate_limit_tier = opened.rate_limit_tier.filter(|value| !value.is_empty());
        opened.seat_tier = opened.seat_tier.filter(|value| !value.is_empty());
        opened.effective_to_unix_millis = None;

        let mut state = lock_or_storage_error(&self.plan_tiers)?;
        if let Some(index) = state.overrides.iter().position(|row| {
            row.organization_type == opened.organization_type
                && row.rate_limit_tier == opened.rate_limit_tier
                && row.seat_tier == opened.seat_tier
                && row.effective_to_unix_millis.is_none()
        }) {
            let current = &state.overrides[index];
            if current.tier_key == opened.tier_key {
                return Ok(());
            }
            if opened.effective_from_unix_millis <= current.effective_from_unix_millis {
                return Err(plan_tier_conflict(
                    "effective_from_unix_millis must increase when closing an open plan-tier row",
                ));
            }
            state.overrides[index].effective_to_unix_millis =
                Some(opened.effective_from_unix_millis);
        }
        state.overrides.push(opened);
        Ok(())
    }

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> storage_api::StorageResult<Vec<storage_api::MetadataTierMappingOverrideRecord>> {
        let state = lock_or_storage_error(&self.plan_tiers)?;
        let mut rows = state
            .overrides
            .iter()
            .filter(|row| row.effective_to_unix_millis.is_none())
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            (
                left.organization_type.as_deref().unwrap_or(""),
                left.rate_limit_tier.as_deref().unwrap_or(""),
                left.seat_tier.as_deref().unwrap_or(""),
            )
                .cmp(&(
                    right.organization_type.as_deref().unwrap_or(""),
                    right.rate_limit_tier.as_deref().unwrap_or(""),
                    right.seat_tier.as_deref().unwrap_or(""),
                ))
        });
        Ok(rows)
    }

    async fn list_metadata_tier_overrides_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> storage_api::StorageResult<Vec<storage_api::MetadataTierMappingOverrideRecord>> {
        let state = lock_or_storage_error(&self.plan_tiers)?;
        let mut rows = state
            .overrides
            .iter()
            .filter(|row| {
                plan_tier_row_is_effective(
                    row.effective_from_unix_millis,
                    row.effective_to_unix_millis,
                    as_of_unix_millis,
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            (
                left.organization_type.as_deref().unwrap_or(""),
                left.rate_limit_tier.as_deref().unwrap_or(""),
                left.seat_tier.as_deref().unwrap_or(""),
            )
                .cmp(&(
                    right.organization_type.as_deref().unwrap_or(""),
                    right.rate_limit_tier.as_deref().unwrap_or(""),
                    right.seat_tier.as_deref().unwrap_or(""),
                ))
        });
        Ok(rows)
    }

    async fn append_upstream_plan_tier(
        &self,
        record: &storage_api::UpstreamPlanTierRecord,
    ) -> storage_api::StorageResult<()> {
        validate_upstream_plan_tier(record)?;
        let mut state = lock_or_storage_error(&self.plan_tiers)?;
        if let Some(index) = state.upstreams.iter().position(|row| {
            row.upstream_id == record.upstream_id && row.effective_to_unix_millis.is_none()
        }) {
            let current = &state.upstreams[index];
            if upstream_plan_tier_open_matches(current, record) {
                return Ok(());
            }
            if record.effective_from_unix_millis <= current.effective_from_unix_millis {
                return Err(plan_tier_conflict(
                    "effective_from_unix_millis must increase when closing an open plan-tier row",
                ));
            }
            state.upstreams[index].effective_to_unix_millis =
                Some(record.effective_from_unix_millis);
        }
        let mut opened = record.clone();
        opened.effective_to_unix_millis = None;
        state.upstreams.push(opened);
        Ok(())
    }

    async fn backfill_upstream_plan_tier_intervals(
        &self,
        upstream_id: Uuid,
        intervals: &[storage_api::UpstreamPlanTierRecord],
        terminal_cap_unix_millis: i64,
        provenance: &str,
    ) -> storage_api::StorageResult<storage_api::BackfillApplyOutcome> {
        let mut state = lock_or_storage_error(&self.plan_tiers)?;
        if state
            .upstreams
            .iter()
            .any(|row| row.upstream_id == upstream_id && row.provenance == provenance)
        {
            return Ok(storage_api::BackfillApplyOutcome::Skipped);
        }

        let cap = state
            .upstreams
            .iter()
            .filter(|row| row.upstream_id == upstream_id)
            .map(|row| row.effective_from_unix_millis)
            .min()
            .map_or(terminal_cap_unix_millis, |cutoff| {
                cutoff.min(terminal_cap_unix_millis)
            });
        let mut counts = storage_api::BackfillApplyCounts {
            inserted: 0,
            skipped_zero_dur: 0,
            capped: 0,
        };
        let mut prepared = Vec::with_capacity(intervals.len());
        for interval in intervals {
            validate_upstream_plan_tier(interval)?;
            if interval.upstream_id != upstream_id {
                return Err(plan_tier_invalid(
                    "upstream_plan_tier.upstream_id",
                    "must match the backfilled upstream_id",
                ));
            }
            if interval.provenance != provenance {
                return Err(plan_tier_invalid(
                    "upstream_plan_tier.provenance",
                    "must match the backfill provenance",
                ));
            }
            if interval.effective_from_unix_millis >= cap {
                counts.skipped_zero_dur += 1;
                continue;
            }
            let effective_to = interval.effective_to_unix_millis.ok_or_else(|| {
                plan_tier_invalid(
                    "upstream_plan_tier.effective_to_unix_millis",
                    "backfill intervals must be closed",
                )
            })?;
            let clamped_to = effective_to.min(cap);
            if clamped_to < effective_to {
                counts.capped += 1;
            }
            if clamped_to <= interval.effective_from_unix_millis {
                counts.skipped_zero_dur += 1;
                continue;
            }
            let mut prepared_interval = interval.clone();
            prepared_interval.effective_to_unix_millis = Some(clamped_to);
            prepared.push(prepared_interval);
        }

        prepared.sort_by_key(|row| {
            (
                row.effective_from_unix_millis,
                row.effective_to_unix_millis.unwrap_or(i64::MAX),
            )
        });
        for pair in prepared.windows(2) {
            if pair[1].effective_from_unix_millis
                < pair[0].effective_to_unix_millis.unwrap_or(i64::MAX)
            {
                return Err(plan_tier_conflict(
                    "upstream plan tier backfill batch contains overlapping intervals",
                ));
            }
        }
        if prepared.iter().any(|candidate| {
            state.upstreams.iter().any(|existing| {
                existing.upstream_id == candidate.upstream_id
                    && existing.effective_from_unix_millis == candidate.effective_from_unix_millis
            })
        }) {
            return Err(plan_tier_conflict(
                "upstream plan tier backfill conflicts with existing row",
            ));
        }

        counts.inserted = u64::try_from(prepared.len()).unwrap_or(u64::MAX);
        state.upstreams.extend(prepared);
        Ok(storage_api::BackfillApplyOutcome::Applied(counts))
    }

    async fn list_current_upstream_plan_tiers(
        &self,
    ) -> storage_api::StorageResult<Vec<storage_api::UpstreamPlanTierRecord>> {
        let state = lock_or_storage_error(&self.plan_tiers)?;
        let mut rows = state
            .upstreams
            .iter()
            .filter(|row| row.effective_to_unix_millis.is_none())
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.upstream_id);
        Ok(rows)
    }

    async fn list_upstream_plan_tiers_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> storage_api::StorageResult<Vec<storage_api::UpstreamPlanTierRecord>> {
        let state = lock_or_storage_error(&self.plan_tiers)?;
        let mut rows = state
            .upstreams
            .iter()
            .filter(|row| {
                plan_tier_row_is_effective(
                    row.effective_from_unix_millis,
                    row.effective_to_unix_millis,
                    as_of_unix_millis,
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.upstream_id);
        Ok(rows)
    }
}

#[async_trait]
impl storage_api::PoolQuotaHistoryStore for InMemoryStorage {
    async fn record_pool_quota_snapshots(
        &self,
        records: &[storage_api::PoolQuotaSnapshotRecord],
    ) -> storage_api::StorageResult<()> {
        let mut snapshots = lock_or_storage_error(&self.pool_quota_snapshots)?;
        for record in records {
            snapshots.insert(
                (record.window, record.snapshot_at_unix_secs),
                record.clone(),
            );
        }
        Ok(())
    }

    async fn list_latest_pool_quota_snapshots(
        &self,
        windows: &[storage_api::SubscriptionQuotaWindow],
    ) -> storage_api::StorageResult<Vec<storage_api::PoolQuotaSnapshotRecord>> {
        let snapshots = lock_or_storage_error(&self.pool_quota_snapshots)?;
        let mut output = Vec::with_capacity(windows.len());
        for window in windows {
            if let Some((_, record)) = snapshots
                .range((*window, i64::MIN)..=(*window, i64::MAX))
                .next_back()
            {
                output.push(record.clone());
            }
        }
        Ok(output)
    }

    async fn list_pool_quota_snapshots_in_range(
        &self,
        windows: &[storage_api::SubscriptionQuotaWindow],
        since_unix_secs: i64,
        until_unix_secs: i64,
    ) -> storage_api::StorageResult<Vec<storage_api::PoolQuotaSnapshotRecord>> {
        if since_unix_secs > until_unix_secs {
            return Ok(Vec::new());
        }
        let snapshots = lock_or_storage_error(&self.pool_quota_snapshots)?;
        let mut output = Vec::new();
        for window in windows {
            output.extend(
                snapshots
                    .range((*window, since_unix_secs)..=(*window, until_unix_secs))
                    .map(|(_, record)| record.clone()),
            );
        }
        Ok(output)
    }

    async fn list_pool_quota_chart_points_in_range(
        &self,
        _windows: &[storage_api::SubscriptionQuotaWindow],
        _since_unix_secs: i64,
        _until_unix_secs: i64,
        _bucket_secs: Option<i64>,
    ) -> storage_api::StorageResult<Vec<storage_api::PoolQuotaChartPointRecord>> {
        unscripted!(PoolQuotaHistoryStore, list_pool_quota_chart_points_in_range)
    }

    async fn delete_pool_quota_snapshots_before(
        &self,
        _cutoff_unix_secs: i64,
        _batch_size: u32,
    ) -> storage_api::StorageResult<u64> {
        unscripted!(PoolQuotaHistoryStore, delete_pool_quota_snapshots_before)
    }
}

#[async_trait]
impl storage_api::PrincipalStore for InMemoryStorage {
    async fn create(
        &self,
        input: storage_api::PrincipalCreate,
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<storage_api::PrincipalRecord> {
        storage_api::validate_identifier("principal.name", &input.name)?;
        let mut state = lock_or_storage_error(&self.principal_state)?;
        if state
            .records
            .values()
            .any(|record| record.deleted_at_unix_secs.is_none() && record.name == input.name)
        {
            return Err(Self::principal_name_conflict(&input.name));
        }

        let id = self.next_entity_id();
        let record = storage_api::PrincipalRecord {
            id,
            name: input.name,
            kind: input.kind,
            allowed_models: input.allowed_models,
            allowed_upstreams: input.allowed_upstreams,
            default_limits: input.default_limits,
            enabled: true,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 0,
            created_at_unix_secs: now_unix_secs,
            updated_at_unix_secs: now_unix_secs,
            router_terminal_strategy: Default::default(),
            cache_keepalive: input.cache_keepalive,
        };
        state.records.insert(id, record.clone());

        let chain = storage_api::PluginChainEntry {
            id: self.next_entity_id(),
            principal_id: id,
            slot: storage_api::PluginSlotKind::Router,
            order: 0,
            wasm_registry_id: storage_api::BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
            config: serde_json::json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
            revision: 0,
        };
        state.chain_entries.insert(chain.id, chain);
        Ok(record)
    }

    async fn get_by_id(
        &self,
        id: Uuid,
    ) -> storage_api::StorageResult<Option<storage_api::PrincipalRecord>> {
        Ok(lock_or_storage_error(&self.principal_state)?
            .records
            .get(&id)
            .cloned())
    }

    async fn get_by_name(
        &self,
        name: &str,
    ) -> storage_api::StorageResult<Option<storage_api::PrincipalRecord>> {
        Ok(lock_or_storage_error(&self.principal_state)?
            .records
            .values()
            .find(|record| record.deleted_at_unix_secs.is_none() && record.name == name)
            .cloned())
    }

    async fn list(
        &self,
        offset: usize,
        limit: usize,
        include_deleted: bool,
    ) -> storage_api::StorageResult<Vec<storage_api::PrincipalRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut records = lock_or_storage_error(&self.principal_state)?
            .records
            .values()
            .filter(|record| include_deleted || record.deleted_at_unix_secs.is_none())
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(records.into_iter().skip(offset).take(limit).collect())
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: storage_api::PrincipalUpdate,
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<Option<storage_api::PrincipalRecord>> {
        if let Some(name) = update.name.as_deref() {
            storage_api::validate_identifier("principal.name", name)?;
        }
        let mut state = lock_or_storage_error(&self.principal_state)?;
        let Some(current) = state.records.get(&id).cloned() else {
            return Ok(None);
        };
        if current.revision != expected_revision {
            return Err(Self::principal_revision_conflict());
        }
        if current.deleted_at_unix_secs.is_none()
            && update.name.as_ref().is_some_and(|name| {
                state.records.values().any(|record| {
                    record.id != id && record.deleted_at_unix_secs.is_none() && record.name == *name
                })
            })
        {
            return Err(Self::principal_name_conflict(
                update.name.as_deref().expect("checked above"),
            ));
        }

        let record = state.records.get_mut(&id).expect("record checked above");
        if let Some(name) = update.name {
            record.name = name;
        }
        if let Some(allowed_models) = update.allowed_models {
            record.allowed_models = allowed_models;
        }
        if let Some(allowed_upstreams) = update.allowed_upstreams {
            record.allowed_upstreams = allowed_upstreams;
        }
        if let Some(default_limits) = update.default_limits {
            record.default_limits = default_limits;
        }
        if let Some(router_terminal_strategy) = update.router_terminal_strategy {
            record.router_terminal_strategy = router_terminal_strategy;
        }
        if let Some(cache_keepalive) = update.cache_keepalive {
            record.cache_keepalive = cache_keepalive;
        }
        record.revision += 1;
        record.updated_at_unix_secs = now_unix_secs;
        Ok(Some(record.clone()))
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<Option<storage_api::PrincipalRecord>> {
        let mut state = lock_or_storage_error(&self.principal_state)?;
        let Some(record) = state.records.get_mut(&id) else {
            return Ok(None);
        };
        if record.revision != expected_revision {
            return Err(Self::principal_revision_conflict());
        }
        record.enabled = enabled;
        record.revision += 1;
        record.updated_at_unix_secs = now_unix_secs;
        Ok(Some(record.clone()))
    }

    async fn soft_delete(
        &self,
        id: Uuid,
        expected_revision: u64,
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<Option<storage_api::PrincipalRecord>> {
        let mut state = lock_or_storage_error(&self.principal_state)?;
        let Some(current) = state.records.get(&id) else {
            return Ok(None);
        };
        if current.revision != expected_revision || current.deleted_at_unix_secs.is_some() {
            return Err(Self::principal_revision_conflict());
        }
        let deleted = {
            let record = state.records.get_mut(&id).expect("record checked above");
            record.deleted_at_unix_secs = Some(now_unix_secs);
            record.updated_at_unix_secs = now_unix_secs;
            record.revision += 1;
            record.clone()
        };
        Self::remove_principal_chains(&mut state, id);
        Ok(Some(deleted))
    }

    async fn hard_delete(&self, id: Uuid) -> storage_api::StorageResult<bool> {
        let principal_id = id.to_string();
        let audit_entries = lock_or_storage_error(&self.audit_entries)?;
        if audit_entries
            .iter()
            .any(|entry| entry.principal_id == principal_id)
        {
            return Err(storage_api::StorageError::Conflict {
                message: "principal is referenced by audit entries".to_owned(),
            });
        }

        let mut state = lock_or_storage_error(&self.principal_state)?;
        Self::remove_principal_chains(&mut state, id);
        Ok(state.records.remove(&id).is_some())
    }

    async fn set_last_apply_error(
        &self,
        id: Uuid,
        error: Option<String>,
        applied_at_unix_secs: u64,
    ) -> storage_api::StorageResult<Option<storage_api::PrincipalRecord>> {
        let mut state = lock_or_storage_error(&self.principal_state)?;
        let Some(record) = state.records.get_mut(&id) else {
            return Ok(None);
        };
        record.last_apply_error = error;
        record.last_apply_at_unix_secs = Some(applied_at_unix_secs);
        Ok(Some(record.clone()))
    }
}

#[async_trait]
impl storage_api::UpstreamStore for InMemoryStorage {
    async fn create(
        &self,
        create: storage_api::UpstreamCreate,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        storage_api::validate_identifier("upstream.name", &create.name)?;
        let now = self.now_unix_secs();
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        if upstreams
            .values()
            .any(|record| record.deleted_at_unix_secs.is_none() && record.name == create.name)
        {
            return Err(Self::upstream_conflict("live upstream name already exists"));
        }
        let record = storage_api::UpstreamRecord {
            id: self.next_entity_id(),
            name: create.name,
            kind: create.kind,
            base_url: create.base_url,
            enabled: true,
            oauth_credentials: None,
            api_key_ciphertext: create.api_key_ciphertext,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            oauth_token_generation: create.oauth_token_generation.unwrap_or(0),
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
            warmup_enabled: create.warmup_enabled,
            warmup_dialect_plugin: create.warmup_dialect_plugin,
            last_warmup_at_unix_secs: None,
        };
        upstreams.insert(record.id, record.clone());
        Ok(record)
    }

    async fn get_by_name(
        &self,
        name: &str,
    ) -> storage_api::StorageResult<Option<storage_api::UpstreamRecord>> {
        Ok(lock_or_storage_error(&self.upstreams)?
            .values()
            .find(|record| record.deleted_at_unix_secs.is_none() && record.name == name)
            .cloned())
    }

    async fn get_by_id(
        &self,
        id: Uuid,
    ) -> storage_api::StorageResult<Option<storage_api::UpstreamRecord>> {
        Ok(lock_or_storage_error(&self.upstreams)?.get(&id).cloned())
    }

    async fn list(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> storage_api::StorageResult<Vec<storage_api::UpstreamRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        Ok(lock_or_storage_error(&self.upstreams)?
            .iter()
            .filter(|(id, record)| {
                record.deleted_at_unix_secs.is_none() && after.is_none_or(|after| **id > after)
            })
            .map(|(_, record)| record.clone())
            .take(limit)
            .collect())
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: storage_api::UpstreamUpdate,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        self.update_upstream_record(id, expected_revision, update, false, true)
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        self.update_upstream_record(
            id,
            expected_revision,
            storage_api::UpstreamUpdate {
                enabled: Some(enabled),
                ..storage_api::UpstreamUpdate::default()
            },
            true,
            false,
        )
    }

    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: storage_api::UpstreamUpdate,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        self.update_upstream_record(id, expected_revision, update, true, false)
    }

    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        let now = self.now_unix_secs();
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        let record = upstreams
            .get_mut(&id)
            .filter(|record| record.deleted_at_unix_secs.is_none())
            .ok_or_else(|| Self::upstream_conflict("upstream not found"))?;
        record.api_key_ciphertext = api_key_ciphertext;
        record.updated_at_unix_secs = now;
        Ok(record.clone())
    }

    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        let now = self.now_unix_secs();
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        let record = upstreams
            .get_mut(&id)
            .filter(|record| record.deleted_at_unix_secs.is_none())
            .ok_or_else(|| Self::upstream_conflict("upstream not found"))?;
        record.oauth_credentials = Some(tokens);
        record.updated_at_unix_secs = now;
        Ok(record.clone())
    }

    async fn set_status(
        &self,
        id: Uuid,
        status: storage_api::UpstreamStatusUpdate,
    ) -> storage_api::StorageResult<()> {
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        let record = upstreams
            .get_mut(&id)
            .filter(|record| record.deleted_at_unix_secs.is_none())
            .ok_or_else(|| Self::upstream_conflict("upstream not found"))?;
        if let Some(value) = status.last_apply_error {
            record.last_apply_error = value;
        }
        if let Some(value) = status.last_apply_at_unix_secs {
            record.last_apply_at_unix_secs = value;
        }
        if let Some(value) = status.last_warmup_at_unix_secs {
            record.last_warmup_at_unix_secs = value;
        }
        Ok(())
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        let now = self.now_unix_secs();
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        let record = upstreams
            .get_mut(&id)
            .filter(|record| record.deleted_at_unix_secs.is_none())
            .ok_or_else(|| Self::upstream_conflict("upstream not found"))?;
        if record.revision != expected_revision {
            return Err(Self::upstream_conflict("stale upstream revision"));
        }
        record.oauth_credentials = Some(tokens);
        record.updated_at_unix_secs = now;
        Ok(record.clone())
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        _holder: Uuid,
        tokens: cc_lb_aead::EncryptedOAuthTokens,
    ) -> storage_api::StorageResult<storage_api::UpstreamRecord> {
        let now = self.now_unix_secs();
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        let record = upstreams
            .get_mut(&id)
            .filter(|record| record.deleted_at_unix_secs.is_none())
            .ok_or_else(|| Self::upstream_conflict("upstream not found"))?;
        record.oauth_credentials = Some(tokens);
        record.oauth_token_generation = record.oauth_token_generation.saturating_add(1);
        record.last_apply_error = None;
        record.updated_at_unix_secs = now;
        Ok(record.clone())
    }

    async fn set_last_apply_error(
        &self,
        id: Uuid,
        error: Option<String>,
    ) -> storage_api::StorageResult<()> {
        let now = self.now_unix_secs();
        storage_api::UpstreamStore::set_status(
            self,
            id,
            storage_api::UpstreamStatusUpdate {
                last_apply_error: Some(error),
                last_apply_at_unix_secs: Some(Some(now)),
                ..storage_api::UpstreamStatusUpdate::default()
            },
        )
        .await
    }

    async fn soft_delete(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> storage_api::StorageResult<()> {
        let now = self.now_unix_secs();
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        let record = upstreams
            .get_mut(&id)
            .filter(|record| record.deleted_at_unix_secs.is_none())
            .ok_or_else(|| Self::upstream_conflict("upstream not found"))?;
        if record.revision != expected_revision {
            return Err(Self::upstream_conflict("stale upstream revision"));
        }
        record.deleted_at_unix_secs = Some(now);
        record.updated_at_unix_secs = now;
        record.revision = record.revision.saturating_add(1);
        Ok(())
    }

    async fn hard_delete(&self, id: Uuid) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.upstreams)?.remove(&id);
        Ok(())
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> storage_api::StorageResult<Option<storage_api::UpstreamRecord>> {
        let now = self.now_unix_secs();
        let mut upstreams = lock_or_storage_error(&self.upstreams)?;
        let Some(record) = upstreams.get_mut(&id).filter(|record| {
            record.deleted_at_unix_secs.is_none() && record.revision == expected_revision
        }) else {
            return Ok(None);
        };
        record.warmup_dialect_plugin = None;
        record.revision = record.revision.saturating_add(1);
        record.updated_at_unix_secs = now;
        Ok(Some(record.clone()))
    }
}

#[async_trait]
impl storage_api::UpstreamAffinityStore for InMemoryStorage {
    async fn resolve_upstream_affinities(
        &self,
        _keys: &[storage_api::UpstreamAffinityKey],
        _now_unix_secs: u64,
        _ttl_secs: u64,
    ) -> storage_api::StorageResult<Vec<storage_api::UpstreamAffinityBinding>> {
        unscripted!(UpstreamAffinityStore, resolve_upstream_affinities)
    }

    async fn bind_upstream_affinities(
        &self,
        _bindings: &[storage_api::UpstreamAffinityBinding],
        _now_unix_secs: u64,
        _ttl_secs: u64,
    ) -> storage_api::StorageResult<()> {
        unscripted!(UpstreamAffinityStore, bind_upstream_affinities)
    }

    async fn purge_expired_upstream_affinities(
        &self,
        _now_unix_secs: u64,
        _ttl_secs: u64,
        _batch_size: usize,
    ) -> storage_api::StorageResult<u64> {
        unscripted!(UpstreamAffinityStore, purge_expired_upstream_affinities)
    }
}

#[async_trait]
impl storage_api::CacheKeepaliveProjectionStore for InMemoryStorage {
    async fn append_cache_keepalive_decision(
        &self,
        decision: &storage_api::CacheKeepaliveDecisionRow,
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.cache_keepalive_decisions)?
            .entry((
                decision.principal_id.clone(),
                decision.source_ref_id.clone(),
            ))
            .or_insert_with(|| storage_api::CacheKeepaliveDecisionRecord {
                source_ref_id: decision.source_ref_id.clone(),
                principal_id: decision.principal_id.clone(),
                session_key_hash: decision.session_key_hash.clone(),
                upstream_id: decision.upstream_id,
                decision: decision.decision.clone(),
                reason: decision.reason.clone(),
                error: decision.error.clone(),
                generation: decision.generation,
                ttl: decision.ttl,
                config_snapshot: decision.config_snapshot.clone(),
                last_message_at_ms: decision.last_message_at_ms,
                ts: decision.ts,
            });
        Ok(())
    }
}

#[async_trait]
impl storage_api::UpstreamRateLimitStateStore for InMemoryStorage {
    async fn put_observation(
        &self,
        record: &storage_api::UpstreamRateLimitObservationRecord,
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.upstream_rate_limit_observations)?.push(record.clone());
        Ok(())
    }

    async fn list_for_upstream_ids(
        &self,
        upstream_ids: &[Uuid],
    ) -> storage_api::StorageResult<Vec<storage_api::UpstreamRateLimitObservationRecord>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }

        let observations = lock_or_storage_error(&self.upstream_rate_limit_observations)?;
        let mut records = observations
            .iter()
            .filter(|record| upstream_ids.contains(&record.upstream_id))
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            left.upstream_id
                .cmp(&right.upstream_id)
                .then_with(|| left.window.cmp(&right.window))
                .then_with(|| left.kind.as_str().cmp(right.kind.as_str()))
        });
        Ok(records)
    }
}

#[async_trait]
impl storage_api::UpstreamSubscriptionQuotaStore for InMemoryStorage {
    async fn record_subscription_quota_samples(
        &self,
        records: &[storage_api::SubscriptionQuotaSample],
    ) -> storage_api::StorageResult<()> {
        {
            let mut state = lock_or_storage_error(&self.subscription_quota)?;
            for record in records {
                let key = (record.upstream_id, record.window, record.source);
                if state.latest.get(&key).is_none_or(|latest| {
                    record.observed_at_unix_millis >= latest.observed_at_unix_millis
                }) {
                    state.latest.insert(key, record.clone());
                }
                if record.sample_kind != storage_api::SubscriptionQuotaSampleKind::Absent {
                    let checkpoint = storage_api::SubscriptionQuotaCheckpointRecord::from(record);
                    insert_subscription_quota_checkpoint_if_changed(&mut state, checkpoint);
                }
            }
        }
        lock_or_storage_error(&self.subscription_quota_sample_batches)?.push(records.to_vec());
        Ok(())
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> storage_api::StorageResult<Vec<storage_api::SubscriptionQuotaLatestRecord>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let state = lock_or_storage_error(&self.subscription_quota)?;
        let mut records = state
            .latest
            .values()
            .filter(|record| upstream_ids.contains(&record.upstream_id))
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            (left.upstream_id, left.window.as_str(), left.source.as_str()).cmp(&(
                right.upstream_id,
                right.window.as_str(),
                right.source.as_str(),
            ))
        });
        Ok(records)
    }

    async fn list_subscription_quota_series(
        &self,
        query: storage_api::SubscriptionQuotaSeriesQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::SubscriptionQuotaSeries>> {
        if query.upstream_ids.is_empty() || query.windows.is_empty() || query.sources.is_empty() {
            return Ok(Vec::new());
        }
        let sources = query
            .sources
            .iter()
            .copied()
            .filter(|source| subscription_quota_source_matches(*source, query.source_merge))
            .collect::<Vec<_>>();
        if sources.is_empty() {
            return Ok(Vec::new());
        }
        let ranges = self.subscription_quota_checkpoint_ranges(
            &storage_api::SubscriptionQuotaCheckpointRangeQuery {
                upstream_ids: query.upstream_ids.clone(),
                windows: query.windows.clone(),
                sources,
                since_unix_millis: query.since_unix_millis,
                until_unix_millis: query.until_unix_millis,
            },
        )?;
        Ok(subscription_quota_series_from_ranges(ranges, &query))
    }

    async fn put_subscription_quota_checkpoints(
        &self,
        records: &[storage_api::SubscriptionQuotaCheckpointRecord],
    ) -> storage_api::StorageResult<usize> {
        let mut state = lock_or_storage_error(&self.subscription_quota)?;
        let mut inserted = 0;
        for record in records {
            if insert_subscription_quota_checkpoint_if_changed(&mut state, record.clone()) {
                inserted += 1;
            }
        }
        Ok(inserted)
    }

    async fn list_latest_subscription_quota_checkpoints_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> storage_api::StorageResult<Vec<storage_api::SubscriptionQuotaCheckpointRecord>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }
        let state = lock_or_storage_error(&self.subscription_quota)?;
        let mut latest = BTreeMap::new();
        for record in state
            .checkpoints
            .iter()
            .filter(|record| upstream_ids.contains(&record.upstream_id))
        {
            let key = (record.upstream_id, record.window, record.source);
            if latest.get(&key).is_none_or(
                |current: &&storage_api::SubscriptionQuotaCheckpointRecord| {
                    (record.changed_at_unix_millis, record.sample_id)
                        > (current.changed_at_unix_millis, current.sample_id)
                },
            ) {
                latest.insert(key, record);
            }
        }
        let mut records = latest.into_values().cloned().collect::<Vec<_>>();
        records.sort_by(|left, right| {
            (left.upstream_id, left.window.as_str(), left.source.as_str()).cmp(&(
                right.upstream_id,
                right.window.as_str(),
                right.source.as_str(),
            ))
        });
        Ok(records)
    }

    async fn list_subscription_quota_checkpoint_ranges(
        &self,
        query: storage_api::SubscriptionQuotaCheckpointRangeQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::SubscriptionQuotaCheckpointRange>> {
        self.subscription_quota_checkpoint_ranges(&query)
    }
}

fn insert_subscription_quota_checkpoint_if_changed(
    state: &mut SubscriptionQuotaState,
    record: storage_api::SubscriptionQuotaCheckpointRecord,
) -> bool {
    let latest = state
        .checkpoints
        .iter()
        .filter(|existing| {
            existing.upstream_id == record.upstream_id
                && existing.window == record.window
                && existing.source == record.source
        })
        .max_by_key(|existing| (existing.changed_at_unix_millis, existing.sample_id));
    if latest.is_some_and(|latest| latest.semantic_fingerprint == record.semantic_fingerprint) {
        return false;
    }
    if state.checkpoints.iter().any(|existing| {
        existing.upstream_id == record.upstream_id
            && existing.window == record.window
            && existing.source == record.source
            && existing.changed_at_unix_millis == record.changed_at_unix_millis
            && existing.sample_id == record.sample_id
    }) {
        return false;
    }
    state.checkpoints.push(record);
    true
}

fn subscription_quota_series_from_ranges(
    ranges: Vec<storage_api::SubscriptionQuotaCheckpointRange>,
    query: &storage_api::SubscriptionQuotaSeriesQuery,
) -> Vec<storage_api::SubscriptionQuotaSeries> {
    let bucket_secs = query.bucket_secs.max(1);
    let mut groups = BTreeMap::<(Uuid, storage_api::SubscriptionQuotaWindow), Vec<_>>::new();
    for range in ranges {
        groups
            .entry((range.upstream_id, range.window))
            .or_default()
            .push(range);
    }

    groups
        .into_iter()
        .map(|((upstream_id, window), group_ranges)| {
            let mut buckets =
                subscription_quota_buckets_from_ranges(group_ranges, bucket_secs, query);
            downsample_subscription_quota_buckets(
                &mut buckets,
                usize::try_from(query.max_points_per_series).unwrap_or(usize::MAX),
            );
            storage_api::SubscriptionQuotaSeries {
                upstream_id,
                window,
                source: query.source_merge,
                buckets,
            }
        })
        .collect()
}

struct SubscriptionQuotaCheckpointStream {
    source: storage_api::SubscriptionQuotaSource,
    checkpoints: Vec<storage_api::SubscriptionQuotaCheckpointRecord>,
    index: usize,
    current: Option<storage_api::SubscriptionQuotaCheckpointRecord>,
}

struct SubscriptionQuotaBucketPoint {
    record: storage_api::SubscriptionQuotaSample,
    is_change: bool,
}

fn subscription_quota_buckets_from_ranges(
    ranges: Vec<storage_api::SubscriptionQuotaCheckpointRange>,
    bucket_secs: u64,
    query: &storage_api::SubscriptionQuotaSeriesQuery,
) -> Vec<storage_api::SubscriptionQuotaBucket> {
    let mut streams = ranges
        .into_iter()
        .map(|range| {
            let mut checkpoints = range.checkpoints;
            checkpoints.sort_by_key(|record| (record.changed_at_unix_millis, record.sample_id));
            SubscriptionQuotaCheckpointStream {
                source: range.source,
                checkpoints,
                index: 0,
                current: range.left_anchor,
            }
        })
        .filter(|stream| stream.current.is_some() || !stream.checkpoints.is_empty())
        .collect::<Vec<_>>();
    streams.sort_by(|left, right| left.source.as_str().cmp(right.source.as_str()));
    if streams.is_empty() {
        return Vec::new();
    }

    let mut bucket_start = subscription_quota_bucket_start(query.since_unix_millis, bucket_secs);
    let end_bucket = subscription_quota_bucket_start(query.until_unix_millis, bucket_secs);
    let mut buckets = Vec::new();
    while bucket_start <= end_bucket {
        let mut points = Vec::new();
        for stream in &mut streams {
            subscription_quota_push_bucket_points(stream, bucket_start, bucket_secs, &mut points);
        }
        if !points.is_empty() {
            buckets.push(subscription_quota_bucket_from_points(bucket_start, &points));
        }
        let Some(next_bucket) = bucket_start.checked_add(bucket_secs) else {
            break;
        };
        bucket_start = next_bucket;
    }
    buckets
}

fn subscription_quota_push_bucket_points(
    stream: &mut SubscriptionQuotaCheckpointStream,
    bucket_start: u64,
    bucket_secs: u64,
    points: &mut Vec<SubscriptionQuotaBucketPoint>,
) {
    let start_index = stream.index;
    while stream.index < stream.checkpoints.len()
        && subscription_quota_bucket_start(
            stream.checkpoints[stream.index].changed_at_unix_millis,
            bucket_secs,
        ) == bucket_start
    {
        let checkpoint = stream.checkpoints[stream.index].clone();
        stream.current = Some(checkpoint.clone());
        points.push(SubscriptionQuotaBucketPoint {
            record: subscription_quota_sample_from_checkpoint(&checkpoint),
            is_change: true,
        });
        stream.index += 1;
    }
    if stream.index == start_index
        && let Some(checkpoint) = &stream.current
    {
        points.push(SubscriptionQuotaBucketPoint {
            record: subscription_quota_sample_from_checkpoint(checkpoint),
            is_change: false,
        });
    }
}

fn subscription_quota_bucket_from_points(
    bucket_start_unix_secs: u64,
    points: &[SubscriptionQuotaBucketPoint],
) -> storage_api::SubscriptionQuotaBucket {
    let mut utilization_count = 0u32;
    let mut utilization_sum = 0.0;
    let mut utilization_min: Option<f64> = None;
    let mut utilization_max: Option<f64> = None;
    let mut last = &points[0].record;
    let mut sources_seen = BTreeSet::new();
    let mut sample_count = 0u32;

    for point in points {
        let record = &point.record;
        sources_seen.insert(record.source);
        if point.is_change {
            sample_count = sample_count.saturating_add(1);
        }
        if record.observed_at_unix_millis >= last.observed_at_unix_millis {
            last = record;
        }
        if let Some(utilization) = record.utilization {
            utilization_count += 1;
            utilization_sum += utilization;
            utilization_min =
                Some(utilization_min.map_or(utilization, |value| value.min(utilization)));
            utilization_max =
                Some(utilization_max.map_or(utilization, |value| value.max(utilization)));
        }
    }

    storage_api::SubscriptionQuotaBucket {
        bucket_start_unix_secs,
        observed: true,
        sample_count,
        utilization_min,
        utilization_avg: (utilization_count > 0)
            .then_some(utilization_sum / f64::from(utilization_count)),
        utilization_max,
        utilization_last: last.utilization,
        status_last: last.status,
        resets_at_unix_secs_last: last.resets_at_unix_secs,
        observed_at_unix_millis_last: Some(last.observed_at_unix_millis),
        sources_seen: sources_seen.into_iter().collect(),
    }
}

fn subscription_quota_sample_from_checkpoint(
    checkpoint: &storage_api::SubscriptionQuotaCheckpointRecord,
) -> storage_api::SubscriptionQuotaSample {
    storage_api::SubscriptionQuotaSample {
        upstream_id: checkpoint.upstream_id,
        window: checkpoint.window,
        source: checkpoint.source,
        sample_kind: checkpoint.sample_kind,
        observed_at_unix_millis: checkpoint.changed_at_unix_millis,
        sample_id: checkpoint.sample_id,
        utilization: checkpoint.utilization,
        status: checkpoint.status,
        resets_at_unix_secs: checkpoint.resets_at_unix_secs,
        surpassed_threshold: checkpoint.surpassed_threshold,
        representative_claim: checkpoint.representative_claim.clone(),
        fallback_percentage: checkpoint.fallback_percentage,
        fallback_available: checkpoint.fallback_available,
        overage_in_use: checkpoint.overage_in_use,
        overage_period_monthly_utilization: checkpoint.overage_period_monthly_utilization,
        upgrade_paths: checkpoint.upgrade_paths.clone(),
        disabled_reason: checkpoint.disabled_reason.clone(),
        extra_usage_enabled: checkpoint.extra_usage_enabled,
        extra_usage_monthly_limit: checkpoint.extra_usage_monthly_limit,
        extra_usage_used_credits: checkpoint.extra_usage_used_credits,
        ingested_at_unix_millis: checkpoint.ingested_at_unix_millis,
    }
}

fn subscription_quota_bucket_start(timestamp_unix_millis: u64, bucket_secs: u64) -> u64 {
    let timestamp_unix_secs = timestamp_unix_millis / 1_000;
    timestamp_unix_secs / bucket_secs * bucket_secs
}

fn subscription_quota_source_matches(
    source: storage_api::SubscriptionQuotaSource,
    source_merge: storage_api::SubscriptionQuotaSourceMerge,
) -> bool {
    match source_merge {
        storage_api::SubscriptionQuotaSourceMerge::Merged => true,
        storage_api::SubscriptionQuotaSourceMerge::Header => {
            source == storage_api::SubscriptionQuotaSource::Header
        }
        storage_api::SubscriptionQuotaSourceMerge::Api => {
            source == storage_api::SubscriptionQuotaSource::Api
        }
    }
}

fn downsample_subscription_quota_buckets<T: Clone>(items: &mut Vec<T>, max_points: usize) {
    if max_points == 0 {
        items.clear();
        return;
    }
    if items.len() <= max_points {
        return;
    }
    if max_points == 1 {
        items.truncate(1);
        return;
    }
    let original = items.clone();
    let last_index = original.len() - 1;
    let mut sampled = Vec::with_capacity(max_points);
    for point in 0..max_points {
        sampled.push(original[point * last_index / (max_points - 1)].clone());
    }
    *items = sampled;
}

#[async_trait]
impl storage_api::UpstreamSubscriptionQuotaAggregateStore for InMemoryStorage {
    async fn list_subscription_quota_slim_checkpoints(
        &self,
        query: storage_api::SubscriptionQuotaCheckpointRangeQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::SubscriptionQuotaSlimCheckpoint>> {
        Ok(self
            .subscription_quota_checkpoint_ranges(&query)?
            .into_iter()
            .flat_map(|range| {
                range
                    .left_anchor
                    .into_iter()
                    .chain(range.checkpoints)
                    .map(|record| storage_api::SubscriptionQuotaSlimCheckpoint {
                        upstream_id: record.upstream_id,
                        window: record.window,
                        source: record.source,
                        changed_at_unix_millis: record.changed_at_unix_millis,
                        sample_id: record.sample_id,
                        utilization: record.utilization,
                        status: record.status,
                        resets_at_unix_secs: record.resets_at_unix_secs,
                    })
            })
            .collect())
    }

    async fn list_subscription_quota_provider_lots(
        &self,
        _query: storage_api::SubscriptionQuotaProviderLotQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::SubscriptionQuotaProviderLot>> {
        unscripted!(
            UpstreamSubscriptionQuotaAggregateStore,
            list_subscription_quota_provider_lots
        )
    }
}

#[async_trait]
impl storage_api::UpstreamSubscriptionMetadataStore for InMemoryStorage {
    async fn put_upstream_subscription_metadata(
        &self,
        record: &storage_api::UpstreamSubscriptionMetadataRecord,
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.upstream_subscription_metadata)?
            .insert(record.upstream_id, record.clone());
        Ok(())
    }

    async fn get_upstream_subscription_metadata(
        &self,
        upstream_id: Uuid,
    ) -> storage_api::StorageResult<Option<storage_api::UpstreamSubscriptionMetadataRecord>> {
        Ok(lock_or_storage_error(&self.upstream_subscription_metadata)?
            .get(&upstream_id)
            .cloned())
    }

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> storage_api::StorageResult<Vec<storage_api::UpstreamSubscriptionMetadataRecord>> {
        Ok(lock_or_storage_error(&self.upstream_subscription_metadata)?
            .values()
            .cloned()
            .collect())
    }
}

#[async_trait]
impl storage_api::UpstreamWarmupAttemptStore for InMemoryStorage {
    async fn insert_warmup_attempt(
        &self,
        _attempt: &storage_api::WarmupAttemptRecord,
    ) -> storage_api::StorageResult<()> {
        unscripted!(UpstreamWarmupAttemptStore, insert_warmup_attempt)
    }

    async fn list_warmup_attempts_for_upstream(
        &self,
        _upstream_id: Uuid,
        _filters: storage_api::WarmupAttemptListFilters,
    ) -> storage_api::StorageResult<Vec<storage_api::WarmupAttemptRecord>> {
        unscripted!(
            UpstreamWarmupAttemptStore,
            list_warmup_attempts_for_upstream
        )
    }

    async fn summarize_recent_warmup_attempts(
        &self,
        _upstream_id: Uuid,
        _cutoff_unix_secs: i64,
    ) -> storage_api::StorageResult<storage_api::WarmupAttemptSummary> {
        unscripted!(UpstreamWarmupAttemptStore, summarize_recent_warmup_attempts)
    }

    async fn latest_warmup_attempt_for_upstream(
        &self,
        _upstream_id: Uuid,
    ) -> storage_api::StorageResult<Option<storage_api::WarmupAttemptRecord>> {
        unscripted!(
            UpstreamWarmupAttemptStore,
            latest_warmup_attempt_for_upstream
        )
    }
}

#[async_trait]
impl storage_api::OrganizationMetadataStore for InMemoryStorage {
    async fn put_organization_metadata(
        &self,
        record: &storage_api::OrganizationMetadataRecord,
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.organization_metadata)?
            .insert(record.organization_uuid.clone(), record.clone());
        Ok(())
    }

    async fn get_organization_metadata(
        &self,
        organization_uuid: &str,
    ) -> storage_api::StorageResult<Option<storage_api::OrganizationMetadataRecord>> {
        Ok(lock_or_storage_error(&self.organization_metadata)?
            .get(organization_uuid)
            .cloned())
    }

    async fn list_organization_metadata(
        &self,
    ) -> storage_api::StorageResult<Vec<storage_api::OrganizationMetadataRecord>> {
        Ok(lock_or_storage_error(&self.organization_metadata)?
            .values()
            .cloned()
            .collect())
    }
}

#[async_trait]
impl storage_api::AnthropicCompatibilityKvStore for InMemoryStorage {
    async fn put_compatibility_kv_value(
        &self,
        _key: &str,
        _value: &str,
        _observed_at_unix_secs: u64,
        _source_url: Option<&str>,
    ) -> storage_api::StorageResult<()> {
        unscripted!(AnthropicCompatibilityKvStore, put_compatibility_kv_value)
    }

    async fn put_compatibility_kv_failure(
        &self,
        _key: &str,
        _attempted_at_unix_secs: u64,
        _error: &str,
    ) -> storage_api::StorageResult<()> {
        unscripted!(AnthropicCompatibilityKvStore, put_compatibility_kv_failure)
    }

    async fn get_compatibility_kv(
        &self,
        _key: &str,
    ) -> storage_api::StorageResult<Option<storage_api::CompatibilityKvRecord>> {
        unscripted!(AnthropicCompatibilityKvStore, get_compatibility_kv)
    }

    async fn list_compatibility_kv(
        &self,
    ) -> storage_api::StorageResult<Vec<storage_api::CompatibilityKvRecord>> {
        unscripted!(AnthropicCompatibilityKvStore, list_compatibility_kv)
    }
}

#[async_trait]
impl storage_api::CacheKeepaliveSessionStore for InMemoryStorage {
    async fn replace_from_real_request(
        &self,
        request: &storage_api::CacheKeepaliveReplaceRequest,
    ) -> storage_api::StorageResult<storage_api::CacheKeepaliveSessionRecord> {
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let existing = sessions.get(&request.session_key_hash);
        let generation = existing
            .map_or(0, |record| record.generation)
            .checked_add(1)
            .ok_or_else(|| storage_api::StorageError::Fatal {
                message: "cache keepalive generation overflow".to_owned(),
            })?;
        let created_at_unix_secs =
            existing.map_or(request.now_unix_secs, |record| record.created_at_unix_secs);
        let record = storage_api::CacheKeepaliveSessionRecord {
            session_key_hash: request.session_key_hash.clone(),
            principal_id: request.principal_id.clone(),
            accounting_key_id: request.accounting_key_id.clone(),
            upstream_id: request.upstream_id,
            generation,
            refresh_count: 0,
            first_scheduled_at_unix_secs: request.cache_anchor_at_unix_secs,
            cache_anchor_at_unix_secs: request.cache_anchor_at_unix_secs,
            run_at_unix_secs: request.run_at_unix_secs,
            ttl: request.ttl,
            status: storage_api::CacheKeepaliveSessionStatus::Active,
            enqueue_state: storage_api::CacheKeepaliveEnqueueState::Pending,
            running_since_unix_secs: None,
            current_job_key: storage_api::cache_keepalive_job_key(
                &request.session_key_hash,
                generation,
            ),
            encrypted_payload: request.encrypted_payload.clone(),
            display_reason: request.display_reason.clone(),
            error: None,
            config_snapshot: Some(request.config_snapshot.clone()),
            terminal_reason: None,
            expires_at_unix_secs: request.expires_at_unix_secs,
            created_at_unix_secs,
            updated_at_unix_secs: request.now_unix_secs,
        };
        sessions.insert(request.session_key_hash.clone(), record.clone());
        Ok(record)
    }

    async fn get_cache_keepalive_session(
        &self,
        session_key_hash: &str,
    ) -> storage_api::StorageResult<Option<storage_api::CacheKeepaliveSessionRecord>> {
        Ok(lock_or_storage_error(&self.cache_keepalive_sessions)?
            .get(session_key_hash)
            .cloned())
    }

    async fn mark_cache_keepalive_enqueued(
        &self,
        session_key_hash: &str,
        generation: u64,
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<bool> {
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let Some(record) = sessions.get_mut(session_key_hash) else {
            return Ok(false);
        };
        if record.generation != generation
            || record.status != storage_api::CacheKeepaliveSessionStatus::Active
            || record.enqueue_state != storage_api::CacheKeepaliveEnqueueState::Pending
        {
            return Ok(false);
        }
        record.enqueue_state = storage_api::CacheKeepaliveEnqueueState::Enqueued;
        record.running_since_unix_secs = None;
        record.updated_at_unix_secs = now_unix_secs;
        Ok(true)
    }

    async fn claim_cache_keepalive_turn(
        &self,
        session_key_hash: &str,
        generation: u64,
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<bool> {
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let Some(record) = sessions.get_mut(session_key_hash) else {
            return Ok(false);
        };
        if record.generation != generation
            || record.status != storage_api::CacheKeepaliveSessionStatus::Active
            || record.enqueue_state != storage_api::CacheKeepaliveEnqueueState::Enqueued
        {
            return Ok(false);
        }
        record.enqueue_state = storage_api::CacheKeepaliveEnqueueState::Running;
        record.running_since_unix_secs = Some(now_unix_secs);
        record.updated_at_unix_secs = now_unix_secs;
        Ok(true)
    }

    async fn update_cache_keepalive_payload(
        &self,
        session_key_hash: &str,
        generation: u64,
        encrypted_payload: &[u8],
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<bool> {
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let Some(record) = sessions.get_mut(session_key_hash) else {
            return Ok(false);
        };
        if record.generation != generation
            || record.status != storage_api::CacheKeepaliveSessionStatus::Active
            || record.enqueue_state != storage_api::CacheKeepaliveEnqueueState::Pending
        {
            return Ok(false);
        }
        record.encrypted_payload = encrypted_payload.to_vec();
        record.updated_at_unix_secs = now_unix_secs;
        Ok(true)
    }

    async fn check_cache_keepalive_generation(
        &self,
        session_key_hash: &str,
    ) -> storage_api::StorageResult<Option<storage_api::CacheKeepaliveGenerationCheck>> {
        Ok(lock_or_storage_error(&self.cache_keepalive_sessions)?
            .get(session_key_hash)
            .map(|record| storage_api::CacheKeepaliveGenerationCheck {
                generation: record.generation,
                status: record.status,
                enqueue_state: record.enqueue_state,
            }))
    }

    async fn reschedule_after_cache_hit(
        &self,
        request: &storage_api::CacheKeepaliveHitRefreshRequest,
    ) -> storage_api::StorageResult<Option<storage_api::CacheKeepaliveSessionRecord>> {
        let next_generation =
            request
                .generation
                .checked_add(1)
                .ok_or_else(|| storage_api::StorageError::Fatal {
                    message: "cache keepalive generation overflow".to_owned(),
                })?;
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let Some(record) = sessions.get_mut(&request.session_key_hash) else {
            return Ok(None);
        };
        if record.generation != request.generation
            || record.status != storage_api::CacheKeepaliveSessionStatus::Active
        {
            return Ok(None);
        }
        record.generation = next_generation;
        record.refresh_count = record.refresh_count.checked_add(1).ok_or_else(|| {
            storage_api::StorageError::Fatal {
                message: "cache keepalive refresh count overflow".to_owned(),
            }
        })?;
        record.cache_anchor_at_unix_secs = request.cache_anchor_at_unix_secs;
        record.run_at_unix_secs = request.run_at_unix_secs;
        record.status = storage_api::CacheKeepaliveSessionStatus::Active;
        record.enqueue_state = storage_api::CacheKeepaliveEnqueueState::Pending;
        record.running_since_unix_secs = None;
        record.current_job_key =
            storage_api::cache_keepalive_job_key(&request.session_key_hash, next_generation);
        if let Some(encrypted_payload) = &request.encrypted_payload {
            record.encrypted_payload.clone_from(encrypted_payload);
        }
        record.terminal_reason = None;
        record.expires_at_unix_secs = request.expires_at_unix_secs;
        record.updated_at_unix_secs = request.now_unix_secs;
        Ok(Some(record.clone()))
    }

    async fn mark_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        generation: u64,
        reason: storage_api::CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<bool> {
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let Some(record) = sessions.get_mut(session_key_hash) else {
            return Ok(false);
        };
        if record.generation != generation
            || record.status != storage_api::CacheKeepaliveSessionStatus::Active
        {
            return Ok(false);
        }
        record.status = storage_api::CacheKeepaliveSessionStatus::Terminal;
        record.terminal_reason = Some(reason);
        record.running_since_unix_secs = None;
        record.encrypted_payload.clear();
        record.updated_at_unix_secs = now_unix_secs;
        Ok(true)
    }

    async fn mark_latest_cache_keepalive_terminal(
        &self,
        session_key_hash: &str,
        reason: storage_api::CacheKeepaliveTerminalReason,
        now_unix_secs: u64,
    ) -> storage_api::StorageResult<bool> {
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let Some(record) = sessions.get_mut(session_key_hash) else {
            return Ok(false);
        };
        if record.status != storage_api::CacheKeepaliveSessionStatus::Active {
            return Ok(false);
        }
        record.status = storage_api::CacheKeepaliveSessionStatus::Terminal;
        record.terminal_reason = Some(reason);
        record.running_since_unix_secs = None;
        record.encrypted_payload.clear();
        record.updated_at_unix_secs = now_unix_secs;
        Ok(true)
    }

    async fn purge_cache_keepalive_expired(
        &self,
        cutoff_unix_secs: u64,
    ) -> storage_api::StorageResult<u64> {
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let before = sessions.len();
        sessions.retain(|_, record| record.expires_at_unix_secs >= cutoff_unix_secs);
        Ok((before - sessions.len()) as u64)
    }

    async fn purge_cache_keepalive_stale_pending(
        &self,
        cutoff_unix_secs: u64,
    ) -> storage_api::StorageResult<u64> {
        let mut sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let before = sessions.len();
        sessions.retain(|_, record| {
            record.status != storage_api::CacheKeepaliveSessionStatus::Active
                || record.enqueue_state != storage_api::CacheKeepaliveEnqueueState::Pending
                || record.updated_at_unix_secs >= cutoff_unix_secs
        });
        Ok((before - sessions.len()) as u64)
    }
}

#[async_trait]
impl storage_api::CacheKeepaliveSessionReadStore for InMemoryStorage {
    async fn list_cache_keepalive_sessions(
        &self,
        query: &storage_api::CacheKeepaliveSessionListQuery,
    ) -> storage_api::StorageResult<storage_api::CacheKeepaliveSessionPage> {
        query.validate_cursor()?;
        if query.limit == 0 {
            return Ok(storage_api::CacheKeepaliveSessionPage {
                rows: Vec::new(),
                next_cursor: None,
            });
        }

        let turns = lock_or_storage_error(&self.cache_keepalive_turns)?;
        let sessions = lock_or_storage_error(&self.cache_keepalive_sessions)?;
        let decisions = lock_or_storage_error(&self.cache_keepalive_decisions)?;
        let mut rows = sessions
            .values()
            .filter(|record| record.principal_id == query.principal_id)
            .map(|record| storage_api::CacheKeepaliveSessionListItem {
                id: record.session_key_hash.clone(),
                source: storage_api::CacheKeepaliveSessionEntrySource::Session,
                session_key_hash: Some(record.session_key_hash.clone()),
                principal_id: record.principal_id.clone(),
                upstream_id: record.upstream_id,
                last_message_at_ms: record.first_scheduled_at_unix_secs.saturating_mul(1_000),
                ttl: record.ttl,
                generation: record.generation,
                refresh_count: Some(record.refresh_count),
                status: Some(record.status),
                enqueue_state: Some(record.enqueue_state),
                terminal_reason: record.terminal_reason,
                decision: None,
                reason: match record.terminal_reason {
                    Some(
                        reason @ (storage_api::CacheKeepaliveTerminalReason::MaxRefreshes
                        | storage_api::CacheKeepaliveTerminalReason::MaxDuration
                        | storage_api::CacheKeepaliveTerminalReason::Expired
                        | storage_api::CacheKeepaliveTerminalReason::DispatchError),
                    ) => reason.display_reason().to_owned(),
                    _ => record.display_reason.clone(),
                },
                error: record.error.clone(),
                config_snapshot: record.config_snapshot.clone(),
            })
            .chain(
                decisions
                    .values()
                    .filter(|decision| {
                        decision.principal_id == query.principal_id
                            && !turns
                                .iter()
                                .any(|turn| turn.source_ref_id == decision.source_ref_id)
                    })
                    .map(|decision| storage_api::CacheKeepaliveSessionListItem {
                        id: decision.source_ref_id.clone(),
                        source: storage_api::CacheKeepaliveSessionEntrySource::Decision,
                        session_key_hash: decision.session_key_hash.clone(),
                        principal_id: decision.principal_id.clone(),
                        upstream_id: decision.upstream_id,
                        last_message_at_ms: decision.last_message_at_ms,
                        ttl: decision.ttl,
                        generation: decision.generation,
                        refresh_count: None,
                        status: None,
                        enqueue_state: None,
                        terminal_reason: None,
                        decision: Some(decision.decision.clone()),
                        reason: decision.reason.clone(),
                        error: decision.error.clone(),
                        config_snapshot: decision.config_snapshot.clone(),
                    }),
            )
            .filter(|item| {
                query
                    .horizon_start_ms
                    .is_none_or(|horizon| item.last_message_at_ms >= horizon)
            })
            .filter(|item| match query.filter {
                storage_api::CacheKeepaliveSessionFilter::All => true,
                storage_api::CacheKeepaliveSessionFilter::Renewed => {
                    item.source == storage_api::CacheKeepaliveSessionEntrySource::Session
                        && item.status == Some(storage_api::CacheKeepaliveSessionStatus::Active)
                        && item.refresh_count.is_some_and(|count| count > 0)
                }
                storage_api::CacheKeepaliveSessionFilter::Scheduled => {
                    item.source == storage_api::CacheKeepaliveSessionEntrySource::Session
                        && item.status == Some(storage_api::CacheKeepaliveSessionStatus::Active)
                        && item.refresh_count == Some(0)
                }
                storage_api::CacheKeepaliveSessionFilter::Capped => matches!(
                    item.terminal_reason,
                    Some(
                        storage_api::CacheKeepaliveTerminalReason::MaxRefreshes
                            | storage_api::CacheKeepaliveTerminalReason::MaxDuration
                    )
                ),
                storage_api::CacheKeepaliveSessionFilter::Expired => {
                    item.terminal_reason == Some(storage_api::CacheKeepaliveTerminalReason::Expired)
                }
                storage_api::CacheKeepaliveSessionFilter::NotTracked => {
                    item.source == storage_api::CacheKeepaliveSessionEntrySource::Decision
                        && item.decision.as_deref() == Some("not_tracked")
                }
                storage_api::CacheKeepaliveSessionFilter::Error => item.error.is_some(),
            })
            .filter(|item| {
                query.cursor.as_ref().is_none_or(|cursor| {
                    item.last_message_at_ms < cursor.last_message_at_ms
                        || (item.last_message_at_ms == cursor.last_message_at_ms
                            && item.source.cursor_entry_id(&item.id) > cursor.entry_id)
                })
            })
            .collect::<Vec<_>>();
        rows.sort_unstable_by(|left, right| {
            right
                .last_message_at_ms
                .cmp(&left.last_message_at_ms)
                .then_with(|| {
                    left.source
                        .cursor_entry_id(&left.id)
                        .cmp(&right.source.cursor_entry_id(&right.id))
                })
        });

        let limit =
            usize::try_from(query.limit).map_err(|_| storage_api::StorageError::InvalidInput {
                field: "cache_keepalive_limit".to_owned(),
                reason: "value cannot be represented as usize".to_owned(),
            })?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_cursor = has_more
            .then(|| {
                let row = rows.last()?;
                Some(storage_api::CacheKeepaliveSessionCursor {
                    principal_id: query.principal_id.clone(),
                    horizon_start_ms: query.horizon_start_ms,
                    filter: query.filter,
                    last_message_at_ms: row.last_message_at_ms,
                    entry_id: row.source.cursor_entry_id(&row.id),
                })
            })
            .flatten();
        Ok(storage_api::CacheKeepaliveSessionPage { rows, next_cursor })
    }

    async fn get_cache_keepalive_session_for_principal(
        &self,
        principal_id: &str,
        session_key_hash: &str,
    ) -> storage_api::StorageResult<Option<storage_api::CacheKeepaliveSessionRecord>> {
        Ok(lock_or_storage_error(&self.cache_keepalive_sessions)?
            .get(session_key_hash)
            .filter(|record| record.principal_id == principal_id)
            .cloned())
    }

    async fn list_cache_keepalive_turns(
        &self,
        principal_id: &str,
        session_key_hash: &str,
    ) -> storage_api::StorageResult<Vec<storage_api::CacheKeepaliveTurnRecord>> {
        let mut turns = lock_or_storage_error(&self.cache_keepalive_turns)?
            .iter()
            .filter(|turn| {
                turn.principal_id == principal_id && turn.session_key_hash == session_key_hash
            })
            .cloned()
            .collect::<Vec<_>>();
        turns.sort_unstable_by(|left, right| {
            right
                .ts
                .cmp(&left.ts)
                .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
        });
        Ok(turns)
    }

    async fn get_cache_keepalive_decision_for_principal(
        &self,
        principal_id: &str,
        source_ref_id: &str,
    ) -> storage_api::StorageResult<Option<storage_api::CacheKeepaliveDecisionRecord>> {
        Ok(lock_or_storage_error(&self.cache_keepalive_decisions)?
            .get(&(principal_id.to_owned(), source_ref_id.to_owned()))
            .cloned())
    }
}

#[async_trait]
impl storage_api::UsageRollupStore for InMemoryStorage {
    async fn rollup_usage_once(&self) -> storage_api::StorageResult<storage_api::UsageRollupRun> {
        unscripted!(UsageRollupStore, rollup_usage_once)
    }

    async fn query_usage_rollups(
        &self,
    ) -> storage_api::StorageResult<Vec<storage_api::UsageRollup>> {
        let mut rollups = lock_or_storage_error(&self.usage_rollups)?.clone();
        rollups.sort_unstable_by_key(|rollup| rollup.bucket_start);
        Ok(rollups)
    }

    async fn query_usage_rollups_in_range(
        &self,
        resolution: storage_api::UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> storage_api::StorageResult<Vec<storage_api::UsageRollup>> {
        if window_end_unix_secs < window_start_unix_secs {
            return Ok(Vec::new());
        }
        let mut rollups = lock_or_storage_error(&self.usage_rollups)?
            .iter()
            .filter(|rollup| {
                rollup.resolution == resolution
                    && rollup.bucket_start >= window_start_unix_secs
                    && rollup.bucket_start < window_end_unix_secs
            })
            .cloned()
            .collect::<Vec<_>>();
        rollups.sort_unstable_by_key(|rollup| rollup.bucket_start);
        Ok(rollups)
    }

    async fn usage_rollup_checkpoint(&self) -> storage_api::StorageResult<Option<u64>> {
        Ok(*lock_or_storage_error(&self.usage_rollup_checkpoint)?)
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        _run: &storage_api::UsageRollupRun,
    ) -> storage_api::StorageResult<()> {
        unscripted!(UsageRollupStore, advance_rollup_checkpoint_and_persist)
    }
}

#[async_trait]
impl storage_api::ApiKeyUsageBucketStore for InMemoryStorage {
    async fn register_api_key_usage_writer(
        &self,
        writer_epoch: Uuid,
        lease_until_unix_secs: u64,
    ) -> storage_api::StorageResult<()> {
        lock_or_storage_error(&self.api_key_usage_writers)?
            .insert(writer_epoch, (lease_until_unix_secs, None));
        Ok(())
    }

    async fn flush_api_key_usage(
        &self,
        flush: &storage_api::ApiKeyUsageFlush,
    ) -> storage_api::StorageResult<storage_api::ApiKeyUsageFlushResult> {
        let mut writers = lock_or_storage_error(&self.api_key_usage_writers)?;
        let Some((lease_until_unix_secs, last_flush_id)) = writers.get_mut(&flush.writer_epoch)
        else {
            return Ok(storage_api::ApiKeyUsageFlushResult::LeaseLost);
        };
        if *lease_until_unix_secs < self.now_unix_secs() {
            return Ok(storage_api::ApiKeyUsageFlushResult::LeaseLost);
        }
        if *last_flush_id == Some(flush.flush_id) {
            return Ok(storage_api::ApiKeyUsageFlushResult::AlreadyApplied);
        }
        if flush
            .deltas
            .iter()
            .any(|delta| delta.key.key_id.is_empty() || delta.key.bucket_width_secs == 0)
        {
            return Err(storage_api::StorageError::InvalidInput {
                field: "api_key_usage_bucket".to_owned(),
                reason: "key_id must be non-empty and bucket_width_secs must be positive"
                    .to_owned(),
            });
        }

        let mut buckets = lock_or_storage_error(&self.api_key_usage_buckets)?;
        for delta in &flush.deltas {
            let usage = buckets
                .entry((flush.writer_epoch, delta.key.clone()))
                .or_default();
            usage.requests += delta.usage.requests;
            usage.input_tokens += delta.usage.input_tokens;
            usage.output_tokens += delta.usage.output_tokens;
            usage.cost_usd_micros += delta.usage.cost_usd_micros;
        }
        *lease_until_unix_secs = flush.lease_until_unix_secs;
        *last_flush_id = Some(flush.flush_id);
        Ok(storage_api::ApiKeyUsageFlushResult::Applied)
    }

    async fn query_api_key_usage_buckets(
        &self,
        query: &storage_api::ApiKeyUsageBucketQuery,
    ) -> storage_api::StorageResult<Vec<storage_api::ApiKeyUsageBucket>> {
        if query.key_ids.is_empty() {
            return Ok(Vec::new());
        }
        let buckets = lock_or_storage_error(&self.api_key_usage_buckets)?;
        let mut aggregated = BTreeMap::<(String, u64, u64), storage_api::ApiKeyUsage>::new();
        for ((writer_epoch, key), usage) in buckets.iter() {
            if *writer_epoch == query.exclude_writer_epoch
                || !query.key_ids.contains(&key.key_id)
                || key
                    .bucket_start_unix_secs
                    .saturating_add(key.bucket_width_secs)
                    <= query.since_unix_secs
                || key.bucket_start_unix_secs > query.until_unix_secs
            {
                continue;
            }
            let aggregate = aggregated
                .entry((
                    key.key_id.clone(),
                    key.bucket_width_secs,
                    key.bucket_start_unix_secs,
                ))
                .or_default();
            aggregate.requests += usage.requests;
            aggregate.input_tokens += usage.input_tokens;
            aggregate.output_tokens += usage.output_tokens;
            aggregate.cost_usd_micros += usage.cost_usd_micros;
        }
        Ok(aggregated
            .into_iter()
            .map(
                |((key_id, bucket_width_secs, bucket_start_unix_secs), usage)| {
                    storage_api::ApiKeyUsageBucket {
                        key: storage_api::ApiKeyUsageBucketKey {
                            key_id,
                            bucket_width_secs,
                            bucket_start_unix_secs,
                        },
                        usage,
                    }
                },
            )
            .collect())
    }

    async fn compact_api_key_usage_buckets(
        &self,
        _writer_inactive_after_secs: u64,
        _retain_for_secs: u64,
        _batch_size: usize,
    ) -> storage_api::StorageResult<storage_api::ApiKeyUsageCompactionRun> {
        unscripted!(ApiKeyUsageBucketStore, compact_api_key_usage_buckets)
    }
}

#[async_trait]
impl storage_api::UsageTokenIntervalStore for InMemoryStorage {
    async fn sum_usage_tokens_for_intervals(
        &self,
        intervals: &[storage_api::UsageTokenInterval],
    ) -> storage_api::StorageResult<Vec<storage_api::UsageTokenIntervalSum>> {
        let rollups = lock_or_storage_error(&self.usage_rollups)?;
        intervals
            .iter()
            .map(|interval| {
                let tokens = rollups
                    .iter()
                    .filter(|rollup| {
                        rollup.resolution == storage_api::UsageRollupResolution::Minute
                            && rollup.upstream_id == interval.upstream_id
                            && rollup.bucket_start >= interval.start_unix_secs
                            && rollup.bucket_start <= interval.end_unix_secs
                    })
                    .try_fold(0_u64, |sum, rollup| {
                        sum.checked_add(rollup.input_tokens)
                            .and_then(|sum| sum.checked_add(rollup.output_tokens))
                            .and_then(|sum| sum.checked_add(rollup.cache_creation_input_tokens))
                            .and_then(|sum| sum.checked_add(rollup.cache_read_input_tokens))
                            .ok_or_else(|| storage_api::StorageError::Fatal {
                                message: "usage token interval sum overflow".to_owned(),
                            })
                    })?;
                Ok(storage_api::UsageTokenIntervalSum {
                    interval_id: interval.interval_id,
                    tokens,
                })
            })
            .collect()
    }
}

#[async_trait]
impl storage_api::PriceCatalogCache for InMemoryStorage {
    async fn put_price_snapshot(
        &self,
        json_bytes: &[u8],
        fetched_at_ms: u64,
    ) -> storage_api::StorageResult<()> {
        use sha2::{Digest, Sha256};

        let digest = Sha256::digest(json_bytes);
        let mut payload_hash = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(payload_hash, "{byte:02x}");
        }
        *lock_or_storage_error(&self.price_catalog_snapshot)? = Some((
            payload_hash,
            storage_api::PriceCatalogSnapshotRecord {
                json_bytes: json_bytes.to_vec(),
                fetched_at_ms,
            },
        ));
        Ok(())
    }

    async fn get_price_snapshot_if_changed(
        &self,
        current_hash: &str,
    ) -> storage_api::StorageResult<storage_api::PriceCatalogSnapshotFetch> {
        let snapshot = lock_or_storage_error(&self.price_catalog_snapshot)?;
        let Some((payload_hash, record)) = snapshot.as_ref() else {
            return Ok(storage_api::PriceCatalogSnapshotFetch::Missing);
        };
        if current_hash == payload_hash {
            return Ok(storage_api::PriceCatalogSnapshotFetch::Unchanged(
                storage_api::PriceCatalogSnapshotMetadata {
                    payload_hash: payload_hash.clone(),
                    fetched_at_ms: record.fetched_at_ms,
                },
            ));
        }
        Ok(storage_api::PriceCatalogSnapshotFetch::Changed(
            record.clone(),
        ))
    }
}

#[async_trait]
impl storage_api::RuntimeChangeNotifier for InMemoryStorage {
    async fn subscribe(
        &self,
    ) -> storage_api::StorageResult<tokio::sync::broadcast::Receiver<storage_api::ChangeEvent>>
    {
        unscripted!(RuntimeChangeNotifier, subscribe)
    }

    async fn run(&self, _cancel: CancellationToken) -> storage_api::StorageResult<()> {
        unscripted!(RuntimeChangeNotifier, run)
    }
}

#[async_trait]
impl storage_api::PluginBlobRepo for InMemoryStorage {
    async fn put_blob(
        &self,
        sha256: &[u8; 32],
        bytes: &[u8],
    ) -> Result<(), storage_api::RepoError> {
        lock_or_storage_error(&self.plugin_blobs)?.insert(*sha256, bytes.to_vec());
        Ok(())
    }

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, storage_api::RepoError> {
        Ok(lock_or_storage_error(&self.plugin_blobs)?
            .get(sha256)
            .cloned())
    }

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), storage_api::RepoError> {
        lock_or_storage_error(&self.plugin_blobs)?.remove(sha256);
        Ok(())
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, storage_api::RepoError> {
        Ok(lock_or_storage_error(&self.plugin_blobs)?
            .keys()
            .copied()
            .collect())
    }
}

#[async_trait]
impl storage_api::PromptCacheObservationStore for InMemoryStorage {
    async fn upsert_observation(
        &self,
        _record: &storage_api::PromptCacheObservationRecord,
    ) -> storage_api::StorageResult<()> {
        unscripted!(PromptCacheObservationStore, upsert_observation)
    }

    async fn list_active_for_upstream(
        &self,
        _upstream_id: Uuid,
        _not_expired_at_unix_secs: u64,
    ) -> storage_api::StorageResult<Vec<storage_api::PromptCacheObservationRecord>> {
        unscripted!(PromptCacheObservationStore, list_active_for_upstream)
    }

    async fn list_active_for_upstream_keys(
        &self,
        _upstream_id: Uuid,
        _not_expired_at_unix_secs: u64,
        _v3_prefix_keys: &[String],
    ) -> storage_api::StorageResult<Vec<storage_api::PromptCacheObservationRecord>> {
        unscripted!(PromptCacheObservationStore, list_active_for_upstream_keys)
    }

    async fn purge_expired_before(&self, _ts_unix_secs: u64) -> storage_api::StorageResult<u64> {
        unscripted!(PromptCacheObservationStore, purge_expired_before)
    }

    async fn count(&self) -> storage_api::StorageResult<u64> {
        unscripted!(PromptCacheObservationStore, count)
    }
}

fn lock_or_storage_error<T>(mutex: &Mutex<T>) -> storage_api::StorageResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| storage_api::StorageError::Fatal {
        message: "InMemoryStorage mutex poisoned".to_owned(),
    })
}

#[allow(non_snake_case)]
#[cfg(test)]
mod tests {
    use super::*;

    fn principal_create(name: &str) -> storage_api::PrincipalCreate {
        storage_api::PrincipalCreate {
            name: name.to_owned(),
            kind: storage_api::PrincipalKind::Machine,
            allowed_models: vec!["claude-*".to_owned()],
            allowed_upstreams: vec![Uuid::from_u128(101)],
            default_limits: vec![storage_api::Limit {
                kind: storage_api::LimitKind::Requests,
                window_secs: 60,
                cap_micros: 100,
            }],
            cache_keepalive: Some(cache_keepalive_config(true)),
        }
    }

    fn cache_keepalive_config(enabled: bool) -> storage_api::CacheKeepaliveConfig {
        storage_api::CacheKeepaliveConfig {
            enabled,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
            classifier: storage_api::ClassifierConfig::default(),
        }
    }

    #[tokio::test]
    async fn principal_live_name_is_reusable_only_after_soft_delete() {
        let storage = InMemoryStorage::default();
        let first =
            storage_api::PrincipalStore::create(&storage, principal_create("shared-name"), 100)
                .await
                .expect("create first principal");

        let duplicate =
            storage_api::PrincipalStore::create(&storage, principal_create("shared-name"), 101)
                .await
                .expect_err("duplicate live name must conflict");
        assert!(matches!(
            duplicate,
            storage_api::StorageError::Conflict { .. }
        ));

        let tombstone =
            storage_api::PrincipalStore::soft_delete(&storage, first.id, first.revision, 102)
                .await
                .expect("soft delete")
                .expect("first principal exists");
        assert_eq!(tombstone.deleted_at_unix_secs, Some(102));
        assert_eq!(tombstone.revision, first.revision + 1);
        assert!(
            storage_api::PrincipalStore::get_by_name(&storage, "shared-name")
                .await
                .expect("lookup by name")
                .is_none()
        );

        let replacement =
            storage_api::PrincipalStore::create(&storage, principal_create("shared-name"), 103)
                .await
                .expect("recreate principal");
        assert_ne!(replacement.id, first.id);
        assert_eq!(
            storage_api::PrincipalStore::get_by_id(&storage, first.id)
                .await
                .expect("lookup tombstone")
                .expect("tombstone remains")
                .deleted_at_unix_secs,
            Some(102)
        );
        assert_eq!(
            storage_api::PrincipalStore::list(&storage, 0, 10, true)
                .await
                .expect("list including deleted")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn principal_update_preserves_optional_fields_and_enforces_revision() {
        let storage = InMemoryStorage::default();
        let created =
            storage_api::PrincipalStore::create(&storage, principal_create("mutable"), 200)
                .await
                .expect("create principal");

        assert_eq!(created.cache_keepalive, Some(cache_keepalive_config(true)));
        assert_eq!(
            created.default_limits[0].kind,
            storage_api::LimitKind::Requests
        );

        let updated = storage_api::PrincipalStore::update(
            &storage,
            created.id,
            created.revision,
            storage_api::PrincipalUpdate {
                allowed_models: Some(vec!["claude-sonnet-*".to_owned()]),
                default_limits: Some(vec![storage_api::Limit {
                    kind: storage_api::LimitKind::InputTokens,
                    window_secs: 300,
                    cap_micros: 900,
                }]),
                ..storage_api::PrincipalUpdate::default()
            },
            201,
        )
        .await
        .expect("update principal")
        .expect("principal exists");
        assert_eq!(updated.revision, 1);
        assert_eq!(updated.updated_at_unix_secs, 201);
        assert_eq!(updated.allowed_models, ["claude-sonnet-*"]);
        assert_eq!(updated.allowed_upstreams, created.allowed_upstreams);
        assert_eq!(
            updated.default_limits[0].kind,
            storage_api::LimitKind::InputTokens
        );
        assert_eq!(updated.cache_keepalive, created.cache_keepalive);

        let replaced = storage_api::PrincipalStore::update(
            &storage,
            created.id,
            updated.revision,
            storage_api::PrincipalUpdate {
                cache_keepalive: Some(Some(cache_keepalive_config(false))),
                ..storage_api::PrincipalUpdate::default()
            },
            202,
        )
        .await
        .expect("replace cache keepalive")
        .expect("principal exists");
        assert_eq!(
            replaced.cache_keepalive,
            Some(cache_keepalive_config(false))
        );
        assert_eq!(replaced.revision, 2);

        let cleared = storage_api::PrincipalStore::update(
            &storage,
            created.id,
            replaced.revision,
            storage_api::PrincipalUpdate {
                cache_keepalive: Some(None),
                ..storage_api::PrincipalUpdate::default()
            },
            203,
        )
        .await
        .expect("clear cache keepalive")
        .expect("principal exists");
        assert_eq!(cleared.cache_keepalive, None);
        assert_eq!(cleared.revision, 3);

        let stale = storage_api::PrincipalStore::set_enabled(
            &storage,
            created.id,
            created.revision,
            false,
            204,
        )
        .await
        .expect_err("stale revision must conflict");
        assert!(matches!(stale, storage_api::StorageError::Conflict { .. }));
    }

    #[tokio::test]
    async fn principal_delete_cascades_owned_plugin_chains() {
        let storage = InMemoryStorage::default();
        let principal =
            storage_api::PrincipalStore::create(&storage, principal_create("with-chains"), 300)
                .await
                .expect("create principal");
        storage_api::PluginRegistryStore::insert_chain_entry(
            &storage,
            storage_api::PluginChainEntryInput {
                principal_id: principal.id,
                slot: storage_api::PluginSlotKind::Shape,
                order: 7,
                wasm_registry_id: storage_api::BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
                config: Default::default(),
                sse_per_event: false,
                batched_events_per_flush: 1,
                batched_flush_ms: 100,
            },
        )
        .await
        .expect("insert shape chain");

        storage_api::PrincipalStore::soft_delete(&storage, principal.id, principal.revision, 301)
            .await
            .expect("soft delete")
            .expect("principal exists");

        for slot in [
            storage_api::PluginSlotKind::Router,
            storage_api::PluginSlotKind::ObservabilityHook,
            storage_api::PluginSlotKind::Shape,
        ] {
            assert!(
                storage_api::PluginRegistryStore::list_chain_for_principal(
                    &storage,
                    principal.id,
                    slot,
                )
                .await
                .expect("list chains")
                .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn request_event_cursor_filters_before_limit() {
        let storage = InMemoryStorage::default();
        for (request_id, principal_id) in [
            ("non-match", "principal-b"),
            ("first-match", "principal-a"),
            ("second-match", "principal-a"),
        ] {
            storage_api::RequestEventStore::append_request_event(
                &storage,
                &storage_api::RequestEvent {
                    request_id: request_id.to_owned(),
                    principal_id: Some(principal_id.to_owned()),
                    ..storage_api::RequestEvent::default()
                },
            )
            .await
            .expect("append request event");
        }

        let rows = storage_api::RequestEventStore::query_request_events_between_cursors(
            &storage,
            0,
            u64::MAX,
            1,
            &storage_api::RequestEventStreamFilters {
                principal_id: Some("principal-a".to_owned()),
                ..storage_api::RequestEventStreamFilters::default()
            },
        )
        .await
        .expect("query filtered request events");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2);
        assert_eq!(rows[0].1.request_id, "first-match");
    }

    #[tokio::test]
    async fn t2__in_memory_storage_provider() {
        let storage = InMemoryStorage::new();
        let provider: Arc<dyn storage_api::Storage> = storage.clone();

        assert_eq!(
            storage_api::MetaStore::backend_kind(provider.as_ref())
                .await
                .expect("read backend kind through Storage trait object"),
            storage_api::BackendKind::Sqlite
        );

        let sha256 = [77; 32];
        storage_api::PluginBlobRepo::put_blob(provider.as_ref(), &sha256, b"provider-contract")
            .await
            .expect("store blob through Storage trait object");
        assert_eq!(
            storage_api::PluginBlobRepo::get_blob(provider.as_ref(), &sha256)
                .await
                .expect("read blob through Storage trait object"),
            Some(b"provider-contract".to_vec())
        );
    }
}
