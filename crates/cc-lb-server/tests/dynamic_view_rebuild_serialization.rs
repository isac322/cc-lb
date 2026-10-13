//! Regression tests for overlapping dynamic-view rebuilds (D3).
//!
//! Every live rebuild-and-install path holds the holder's rebuild lock from
//! the resident-generation read through installation, and the builder samples
//! the revision hash before reading any content. These tests reproduce the
//! overlaps that previously let a committed admin mutation lose its
//! installation, or let a stale build hide a committed change from reconcile.
//!
//! Determinism: pass-through stores park a chosen rebuild inside
//! `build_dynamic_view` after it has read upstream/principal contents
//! (`list_for_upstream_ids` / `list_chains_for_principals`). The second
//! mutation's rebuild is the admin helper's own future, polled once by the
//! test so its resident-generation read (or its wait for the rebuild lock)
//! happens before the parked rebuild is released. No sleeps are used.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::IntoResponse;
use cc_lb_admin::v1::{CC_LB_REBIND_STATUS_HEADER, add_dynamic_rebind_headers};
use cc_lb_admin::{AdminState, DynamicViewRebinder, router};
use cc_lb_aead::AeadService;
use cc_lb_control::api_keys::builtin_authn::{BuiltinAuthError, BuiltinAuthn};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_control::{DynamicView, DynamicViewHolder};
use cc_lb_engine::lifecycle::{RequestKind, build_candidates};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::SubscriptionQuotaCache;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::reconcile::Reconciler;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    MetaStore, PluginChainEntry, PluginChainEntryInput, PluginChainEntryUpdate,
    PluginRegistryStore, PluginSlotKind, PrincipalCreate, PrincipalKind, PrincipalRecord,
    PrincipalStore, Storage as StorageTrait, StorageResult, UpstreamCreate,
    UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore, UpstreamRecord, UpstreamStore,
    WasmBlob, WasmBlobRecord, WasmRegistryCascadeDelete, WasmRegistryEntry, WasmRegistryEntryInput,
    WasmRegistryReferenceFingerprint, WasmRegistryReferences,
};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

const AEAD_KEY: [u8; 32] = [25; 32];
const TOKEN: &str = "rebuild-serialization-token";
const PRINCIPAL: &str = "rebuild-serialization-principal";

// ---------------------------------------------------------------- gates

struct GateSlot {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

struct GateHandle {
    reached: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

fn arm(slots: &Mutex<VecDeque<GateSlot>>) -> GateHandle {
    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    slots.lock().push_back(GateSlot {
        reached: reached_tx,
        release: release_rx,
    });
    GateHandle {
        reached: reached_rx,
        release: release_tx,
    }
}

async fn pass_gate(slots: &Mutex<VecDeque<GateSlot>>) {
    let slot = slots.lock().pop_front();
    if let Some(slot) = slot {
        let _ = slot.reached.send(());
        let _ = slot.release.await;
    }
}

/// Pass-through rate-limit store. Each armed slot parks one
/// `list_for_upstream_ids` call, which the builder issues after reading the
/// upstream list.
struct GatedRateLimits {
    inner: Arc<Storage>,
    slots: Mutex<VecDeque<GateSlot>>,
}

#[async_trait]
impl UpstreamRateLimitStateStore for GatedRateLimits {
    async fn put_observation(
        &self,
        record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        self.inner.put_observation(record).await
    }

    async fn list_for_upstream_ids(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        pass_gate(&self.slots).await;
        self.inner.list_for_upstream_ids(upstream_ids).await
    }
}

/// Pass-through plugin registry store. Each armed slot parks one
/// `list_chains_for_principals` call, which the builder issues after reading
/// the principal list.
struct GatedRegistry {
    inner: Arc<Storage>,
    slots: Mutex<VecDeque<GateSlot>>,
}

#[async_trait]
impl PluginRegistryStore for GatedRegistry {
    async fn persist_wasm_upload(
        &self,
        blob: WasmBlob,
        entry: WasmRegistryEntryInput,
    ) -> StorageResult<(WasmRegistryEntry, bool)> {
        self.inner.persist_wasm_upload(blob, entry).await
    }
    async fn get_blob(&self, sha256: [u8; 32]) -> StorageResult<Option<WasmBlobRecord>> {
        self.inner.get_blob(sha256).await
    }
    async fn get_blob_bytes(&self, sha256: [u8; 32]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get_blob_bytes(sha256).await
    }
    async fn list_orphan_blobs(&self) -> StorageResult<Vec<[u8; 32]>> {
        self.inner.list_orphan_blobs().await
    }
    async fn list_registry(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> StorageResult<Vec<WasmRegistryEntry>> {
        self.inner.list_registry(after, limit).await
    }
    async fn get_registry_entry_by_sha(
        &self,
        sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        self.inner.get_registry_entry_by_sha(sha256).await
    }
    async fn get_registry_entry_by_name(
        &self,
        name: &str,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        self.inner.get_registry_entry_by_name(name).await
    }
    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>> {
        self.inner.get_registry_entry_by_id(id).await
    }
    async fn replace_wasm_entry(
        &self,
        blob: WasmBlob,
        entry: WasmRegistryEntryInput,
        expected_revision: u64,
    ) -> StorageResult<WasmRegistryEntry> {
        self.inner
            .replace_wasm_entry(blob, entry, expected_revision)
            .await
    }
    async fn list_registry_references(&self, id: Uuid) -> StorageResult<WasmRegistryReferences> {
        self.inner.list_registry_references(id).await
    }
    async fn cascade_delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
        expected_references: WasmRegistryReferenceFingerprint,
    ) -> StorageResult<Option<WasmRegistryCascadeDelete>> {
        self.inner
            .cascade_delete_registry_entry(id, expected_revision, expected_references)
            .await
    }
    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry> {
        self.inner
            .update_registry_label(id, expected_revision, label)
            .await
    }
    async fn delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        self.inner
            .delete_registry_entry(id, expected_revision)
            .await
    }
    async fn decrement_blob_refcount_or_delete(&self, sha256: [u8; 32]) -> StorageResult<bool> {
        self.inner.decrement_blob_refcount_or_delete(sha256).await
    }
    async fn insert_chain_entry(
        &self,
        entry: PluginChainEntryInput,
    ) -> StorageResult<PluginChainEntry> {
        self.inner.insert_chain_entry(entry).await
    }
    async fn list_chain_for_principal(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        self.inner
            .list_chain_for_principal(principal_id, slot)
            .await
    }
    async fn list_chains_for_principals(
        &self,
        principal_ids: &[Uuid],
        slots: &[PluginSlotKind],
    ) -> StorageResult<Vec<PluginChainEntry>> {
        pass_gate(&self.slots).await;
        self.inner
            .list_chains_for_principals(principal_ids, slots)
            .await
    }
    async fn update_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PluginChainEntryUpdate,
    ) -> StorageResult<Option<PluginChainEntry>> {
        self.inner
            .update_chain_entry(id, expected_revision, update)
            .await
    }
    async fn reorder_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        self.inner
            .reorder_chain(principal_id, slot, new_orders)
            .await
    }
    async fn delete_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<PluginChainEntry>> {
        self.inner.delete_chain_entry(id, expected_revision).await
    }
    async fn rebalance_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        self.inner.rebalance_chain(principal_id, slot).await
    }
}

// ---------------------------------------------------------------- rebinder

/// Same body as the server's build-only `ServerDynamicViewRebinder`, which is
/// private to app.rs: it builds from the caller's generation and never takes
/// the rebuild lock.
struct ServerLikeRebinder {
    stores: Arc<Stores>,
    runtime: Arc<WasmtimeRuntime>,
    aead: Arc<AeadService>,
    data_dir: std::path::PathBuf,
    quota: Arc<SubscriptionQuotaCache>,
}

#[async_trait]
impl DynamicViewRebinder for ServerLikeRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        Ok(build_dynamic_view(
            &self.stores,
            self.aead.clone(),
            None,
            current_generation,
            &self.runtime,
            &self.data_dir,
            self.quota.clone(),
            30,
            Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
            1800,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await?)
    }
}

// ---------------------------------------------------------------- admin auth

struct StaticToken;

#[async_trait]
impl cc_lb_admin::auth::AdminAuthProvider for StaticToken {
    fn id(&self) -> &str {
        "rebuild-serialization-static"
    }
    fn is_static_token(&self) -> bool {
        true
    }
    async fn authenticate(&self, headers: &HeaderMap) -> cc_lb_admin::auth::ProviderOutcome {
        match headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        {
            None => cc_lb_admin::auth::ProviderOutcome::NotPresent,
            Some(token) if token != TOKEN => {
                cc_lb_admin::auth::ProviderOutcome::Rejected("mismatch")
            }
            Some(_) => {
                cc_lb_admin::auth::ProviderOutcome::Verified(cc_lb_admin::auth::AdminIdentity {
                    authority: "static-token".to_owned(),
                    subject: "rebuild-serialization-static".to_owned(),
                    kind: cc_lb_admin::auth::AdminActorKind::BreakGlass,
                    provider_id: "rebuild-serialization-static".to_owned(),
                    email: None,
                    display_name: None,
                    groups: Vec::new(),
                    expires_at_unix_secs: None,
                })
            }
        }
    }
}

// ---------------------------------------------------------------- fixture

struct Fixture {
    _dir: tempfile::TempDir,
    storage: Arc<Storage>,
    rate_limit_gate: Arc<GatedRateLimits>,
    chain_gate: Arc<GatedRegistry>,
    principal: PrincipalRecord,
    holder: Arc<DynamicViewHolder>,
    reconciler: Arc<Reconciler>,
    state: AdminState,
    app: axum::Router,
    target: UpstreamRecord,
    other: UpstreamRecord,
}

async fn create_upstream(storage: &Storage, name: &str, enabled: bool) -> UpstreamRecord {
    let created = UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("upstream created");
    let ciphertext = AeadService::from_master_key(AEAD_KEY)
        .encrypt(b"sk-ant-fixture-secret", created.id.as_bytes())
        .expect("ciphertext");
    let with_key = UpstreamStore::update_api_key_secret(storage, created.id, Some(ciphertext))
        .await
        .expect("api key stored");
    if with_key.enabled == enabled {
        return with_key;
    }
    UpstreamStore::set_enabled(storage, with_key.id, with_key.revision, enabled)
        .await
        .expect("set enabled")
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!(
        "sqlite://{}",
        dir.path().join("rebuild-serialization.sqlite").display()
    );
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("sqlite"),
    );
    storage.initialize().await.expect("init");

    let principal = PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: PRINCIPAL.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await
    .expect("principal");
    let target = create_upstream(&storage, "u-target", true).await;
    let other = create_upstream(&storage, "u-other", false).await;

    let rate_limit_gate = Arc::new(GatedRateLimits {
        inner: storage.clone(),
        slots: Mutex::new(VecDeque::new()),
    });
    let chain_gate = Arc::new(GatedRegistry {
        inner: storage.clone(),
        slots: Mutex::new(VecDeque::new()),
    });
    let stores = Arc::new(Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: chain_gate.clone(),
        upstream_rate_limits: rate_limit_gate.clone(),
        upstream_subscription_quotas: storage.clone(),
        upstream_subscription_metadata: storage.clone(),
        organization_metadata: storage.clone(),
        plan_tiers: storage.clone(),
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage.clone(),
        audit: None,
    });
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("wasmtime"));
    let aead = Arc::new(AeadService::from_master_key(AEAD_KEY));
    let quota = Arc::new(SubscriptionQuotaCache::new());
    let rebinder = Arc::new(ServerLikeRebinder {
        stores: stores.clone(),
        runtime: runtime.clone(),
        aead: aead.clone(),
        data_dir: dir.path().to_path_buf(),
        quota: quota.clone(),
    });
    let initial = rebinder
        .rebuild_dynamic_view(0)
        .await
        .expect("initial view");
    let holder = Arc::new(DynamicViewHolder::new(initial));
    let reconciler = Arc::new(Reconciler::new(
        stores.clone(),
        holder.clone(),
        runtime.clone(),
        aead.clone(),
        None,
        CancellationToken::new(),
        dir.path().to_path_buf(),
        quota,
        30,
        Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        1800,
        Arc::new(cc_lb_clock::SystemClock),
    ));
    let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let state = AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage.clone() as Arc<dyn StorageTrait>),
        key_store: None,
        aead,
        limit_engine: cc_lb_control::api_keys::limit_engine::LimitEngine::new(
            Arc::new(cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
            clock.clone(),
        ),
        lifecycle: None,
        lazy_refresher: None,
        runtime: Some(runtime),
        data_dir: Some(dir.path().to_path_buf()),
        warmup_dialect_dispatcher: None,
        dynamic_view: holder.clone(),
        config: Arc::new(cc_lb_config::Config::default()),
        dynamic_view_rebinder: Some(rebinder as Arc<dyn DynamicViewRebinder>),
        scheduler: None,
        admin_auth: Arc::new(cc_lb_admin::auth::AdminAuthenticator::new(vec![Arc::new(
            StaticToken,
        )])),
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock,
        anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
            .expect("production Anthropic origin parses"),
    };
    Fixture {
        _dir: dir,
        storage,
        rate_limit_gate,
        chain_gate,
        principal,
        holder,
        reconciler,
        app: router(state.clone()),
        state,
        target,
        other,
    }
}

fn post(path: String, revision: u64) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .header(header::IF_MATCH, format!("W/\"{revision}\""))
        .body(Body::empty())
        .expect("request")
}

fn generation_header(response: &axum::response::Response) -> Option<u64> {
    response
        .headers()
        .get("x-cc-lb-generation")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn candidate_names(view: &DynamicView) -> Vec<String> {
    let mut names: Vec<String> = build_candidates(
        view,
        PRINCIPAL,
        RequestKind::AnthropicMessages,
        &[],
        &HashMap::new(),
        &cc_lb_clock::SystemClock,
    )
    .into_iter()
    .map(|candidate| candidate.name)
    .collect();
    names.sort();
    names
}

fn upstream_enabled_in_view(view: &DynamicView, name: &str) -> Option<bool> {
    view.upstreams_snapshot()
        .iter()
        .find(|upstream| upstream.name == name)
        .map(|upstream| upstream.enabled)
}

/// Runs the admin rebind step for mutation B (whose domain change is already
/// committed) against the parked rebuild A of the admin request `request_a`.
///
/// B's rebind future is polled once before A is released, so on the pre-fix
/// code it has already read the resident generation that A is about to
/// install; with the fix it is queued on the holder's rebuild lock instead.
/// Returns A's response and B's 200 response with its rebind headers.
async fn race_admin_rebind_against_parked_rebuild(
    fx: &Fixture,
    release_a: oneshot::Sender<()>,
    request_a: tokio::task::JoinHandle<Result<axum::response::Response, std::convert::Infallible>>,
) -> (axum::response::Response, axum::response::Response) {
    let mut response_b = StatusCode::OK.into_response();
    let response_a = {
        let mut rebind_b = std::pin::pin!(add_dynamic_rebind_headers(&mut response_b, &fx.state));
        assert!(
            futures_util::poll!(rebind_b.as_mut()).is_pending(),
            "B's rebind must wait while rebuild A is parked"
        );

        release_a.send(()).expect("release rebuild A");
        let response_a = request_a
            .await
            .expect("request A joins")
            .expect("request A responds");
        rebind_b.await;
        response_a
    };
    (response_a, response_b)
}

// ---------------------------------------------------------------- scenarios

/// Q2: admin mutation A (enable u-other) is rebuilding with pre-B contents
/// while admin mutation B (disable u-target) commits and rebinds. B's 200
/// response must only be produced after a view containing B's change is
/// resident, and its generation header must name that view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admin_upstream_disable_is_resident_when_response_returns() {
    let fx = fixture().await;
    let initial_generation = fx.holder.generation();
    assert!(candidate_names(&fx.holder.load()).contains(&"u-target".to_owned()));

    let gate_a = arm(&fx.rate_limit_gate.slots);
    let request_a = tokio::spawn(fx.app.clone().oneshot(post(
        format!("/admin/v1/upstreams/{}/enable", fx.other.id),
        fx.other.revision,
    )));
    gate_a
        .reached
        .await
        .expect("rebuild A parked after reading upstream contents");

    UpstreamStore::set_enabled(fx.storage.as_ref(), fx.target.id, fx.target.revision, false)
        .await
        .expect("mutation B commits");
    let (response_a, response_b) =
        race_admin_rebind_against_parked_rebuild(&fx, gate_a.release, request_a).await;
    assert_eq!(response_a.status(), StatusCode::OK);
    let generation_a = generation_header(&response_a).expect("A generation header");

    assert_eq!(response_b.status(), StatusCode::OK);
    assert!(
        response_b
            .headers()
            .get(CC_LB_REBIND_STATUS_HEADER)
            .is_none(),
        "B's rebind must not be deferred"
    );
    let generation_b = generation_header(&response_b).expect("B generation header");

    let view = fx.holder.load();
    assert!(
        !candidate_names(&view).contains(&"u-target".to_owned()),
        "disabled upstream must not be routable after B's response"
    );
    assert!(candidate_names(&view).contains(&"u-other".to_owned()));
    assert_eq!(upstream_enabled_in_view(&view, "u-target"), Some(false));
    assert_eq!(
        generation_b, view.generation,
        "B's header names the resident view"
    );
    assert_eq!(generation_a, initial_generation + 1);
    assert_eq!(generation_b, initial_generation + 2);

    fx.reconciler.reconcile_once().await.expect("reconcile");
    assert!(!candidate_names(&fx.holder.load()).contains(&"u-target".to_owned()));
}

/// Q3: same overlap with a principal disable. Once B's 200 response is
/// produced, authentication with the principal's still-active key is rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admin_principal_disable_rejects_authentication_when_response_returns() {
    let fx = fixture().await;
    let key_store = Arc::new(KeyStore::new(fx.storage.clone()));
    let (_key_record, key_secret) = key_store
        .create(
            PRINCIPAL,
            CreateParams {
                label: "rebuild-serialization".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("managed key created");
    let authn = BuiltinAuthn::new(key_store, Arc::new(cc_lb_clock::SystemClock));
    let mut auth_headers = HeaderMap::new();
    auth_headers.insert(
        "x-api-key",
        key_secret.expose().parse().expect("api key header value"),
    );
    authn
        .authenticate(&auth_headers, &fx.holder.load().principal_view)
        .await
        .expect("principal authenticates before the disable");

    let gate_a = arm(&fx.chain_gate.slots);
    let request_a = tokio::spawn(fx.app.clone().oneshot(post(
        format!("/admin/v1/upstreams/{}/enable", fx.other.id),
        fx.other.revision,
    )));
    gate_a
        .reached
        .await
        .expect("rebuild A parked after reading principal contents");

    let disabled = PrincipalStore::set_enabled(
        fx.storage.as_ref(),
        fx.principal.id,
        fx.principal.revision,
        false,
        2,
    )
    .await
    .expect("mutation B commits")
    .expect("principal exists");
    assert!(!disabled.enabled);
    let (response_a, response_b) =
        race_admin_rebind_against_parked_rebuild(&fx, gate_a.release, request_a).await;
    assert_eq!(response_a.status(), StatusCode::OK);
    assert_eq!(response_b.status(), StatusCode::OK);
    assert!(
        response_b
            .headers()
            .get(CC_LB_REBIND_STATUS_HEADER)
            .is_none()
    );
    let generation_b = generation_header(&response_b).expect("B generation header");

    let view = fx.holder.load();
    assert_eq!(
        authn
            .authenticate(&auth_headers, &view.principal_view)
            .await
            .map(|success| success.principal_id),
        Err(BuiltinAuthError::PrincipalDisabled),
        "disabled principal must be rejected after B's response"
    );
    assert_eq!(
        generation_b, view.generation,
        "B's header names the resident view"
    );

    fx.reconciler.reconcile_once().await.expect("reconcile");
    assert_eq!(
        authn
            .authenticate(&auth_headers, &fx.holder.load().principal_view)
            .await
            .map(|success| success.principal_id),
        Err(BuiltinAuthError::PrincipalDisabled)
    );
}

/// Q7: a rebuild parked after reading old upstream contents installs while a
/// direct storage disable (with no rebuild of its own) commits in between.
/// The installed view carries the signature sampled before its content reads,
/// so the next reconcile pass detects the drift and removes the candidate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_repairs_view_built_from_contents_older_than_its_signature() {
    let fx = fixture().await;

    // Out-of-band change so the reconciler rebuilds.
    UpstreamStore::set_enabled(fx.storage.as_ref(), fx.other.id, fx.other.revision, true)
        .await
        .expect("out-of-band enable");

    let gate = arm(&fx.rate_limit_gate.slots);
    let reconciler = fx.reconciler.clone();
    let parked = tokio::spawn(async move { reconciler.reconcile_once().await });
    gate.reached
        .await
        .expect("reconcile rebuild parked after reading upstream contents");

    UpstreamStore::set_enabled(fx.storage.as_ref(), fx.target.id, fx.target.revision, false)
        .await
        .expect("direct disable commits without a rebuild");

    gate.release.send(()).expect("release parked rebuild");
    parked
        .await
        .expect("parked reconcile joins")
        .expect("parked reconcile succeeds");
    let stale = fx.holder.load();
    assert!(
        candidate_names(&stale).contains(&"u-target".to_owned()),
        "the parked build read u-target before it was disabled"
    );

    fx.reconciler.reconcile_once().await.expect("reconcile");
    let repaired = fx.holder.load();
    assert!(repaired.generation > stale.generation, "reconcile rebuilt");
    assert!(
        !candidate_names(&repaired).contains(&"u-target".to_owned()),
        "reconcile must remove the directly disabled upstream"
    );
    assert!(candidate_names(&repaired).contains(&"u-other".to_owned()));
    assert_eq!(upstream_enabled_in_view(&repaired, "u-target"), Some(false));
}
