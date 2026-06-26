use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_lb_plugin_wire::limits::{
    SKIP_HANDSHAKE_IF_FRESH_TTL_SECS, STARTUP_HANDSHAKE_PARALLEL_MAX,
    STARTUP_HANDSHAKE_TOTAL_BUDGET_MS,
};
use cc_lb_runtime_extism::{
    handshake::slot_set_from_handshake,
    registry::{PluginRegistry, RegistryError},
};
use cc_lb_storage_api::{
    BUILTIN_CACHE_AFFINITY_ID, BUILTIN_CACHE_AFFINITY_SHA256, BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
    BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256, PluginRegistryRecord, PluginRegistryRepo,
    PluginRegistryStatus, PluginRegistryStore, RepoError, Storage, WasmRegistryEntry,
};
use thiserror::Error;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

pub type ShutdownSignal = watch::Receiver<bool>;

#[derive(Clone, Copy, Debug)]
pub struct StartupHandshakeOpts {
    pub skip_if_fresh: bool,
    pub force: bool,
    pub total_budget: Duration,
    pub parallelism: usize,
}

impl Default for StartupHandshakeOpts {
    fn default() -> Self {
        Self {
            skip_if_fresh: true,
            force: false,
            total_budget: Duration::from_millis(STARTUP_HANDSHAKE_TOTAL_BUDGET_MS),
            parallelism: STARTUP_HANDSHAKE_PARALLEL_MAX,
        }
    }
}

#[derive(Debug, Default)]
pub struct StartupHandshakeReport {
    pub processed: usize,
    pub skipped_fresh: usize,
    pub re_handshaked: usize,
    pub disabled: usize,
    pub errors: Vec<([u8; 32], StartupHandshakeError)>,
}

#[derive(Debug, Error)]
pub enum StartupHandshakeError {
    #[error("plugin registry repository failed: {0}")]
    Repo(#[from] RepoError),
    #[error("plugin registry failed: {0}")]
    Registry(#[from] RegistryError),
    #[error("startup handshake task failed: {0}")]
    Join(String),
    #[error("clock failed: {0}")]
    Clock(String),
}

#[derive(Debug)]
enum RecordOutcome {
    SkippedFresh {
        sha256: [u8; 32],
    },
    ReHandshaked {
        sha256: [u8; 32],
    },
    Failed {
        sha256: [u8; 32],
        error: RegistryError,
    },
}

pub async fn run_startup_handshake(
    registry: &PluginRegistry,
    repo: &dyn PluginRegistryRepo,
    opts: StartupHandshakeOpts,
    shutdown: ShutdownSignal,
) -> StartupHandshakeReport {
    run_startup_handshake_inner(registry, repo, None, opts, shutdown).await
}

pub async fn run_startup_handshake_with_slot_store(
    registry: &PluginRegistry,
    repo: &dyn PluginRegistryRepo,
    slot_store: Arc<dyn Storage>,
    opts: StartupHandshakeOpts,
    shutdown: ShutdownSignal,
) -> StartupHandshakeReport {
    run_startup_handshake_inner(registry, repo, Some(slot_store), opts, shutdown).await
}

#[allow(clippy::manual_clamp)]
async fn run_startup_handshake_inner(
    registry: &PluginRegistry,
    repo: &dyn PluginRegistryRepo,
    slot_store: Option<Arc<dyn Storage>>,
    opts: StartupHandshakeOpts,
    mut shutdown: ShutdownSignal,
) -> StartupHandshakeReport {
    let mut report = StartupHandshakeReport::default();
    let now = match unix_now() {
        Ok(now) => now,
        Err(error) => {
            report.errors.push(([0; 32], error));
            return report;
        }
    };
    let shutdown_marker = match repo.get_shutdown_marker().await {
        Ok(marker) => marker,
        Err(error) => {
            report.errors.push(([0; 32], error.into()));
            return report;
        }
    };
    let records = match repo.list_active().await {
        Ok(records) => records,
        Err(error) => {
            report.errors.push(([0; 32], error.into()));
            return report;
        }
    };

    if shutdown_requested(&shutdown) {
        write_shutdown_marker(repo, &mut report).await;
        return report;
    }

    let parallelism = opts.parallelism.max(1).min(STARTUP_HANDSHAKE_PARALLEL_MAX);
    let semaphore = Arc::new(Semaphore::new(parallelism));
    let mut join_set = JoinSet::new();
    let mut pending = BTreeSet::new();

    for record in records {
        pending.insert(record.sha256);
        let registry = registry.clone();
        let semaphore = semaphore.clone();
        let slot_store = slot_store.clone();
        let force_rehandshake = opts.force || stale_after_shutdown(&record, shutdown_marker);
        join_set.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("startup handshake semaphore is not closed");
            process_record(
                registry,
                record,
                slot_store,
                opts.skip_if_fresh,
                force_rehandshake,
                now,
            )
            .await
        });
    }

    let budget = tokio::time::sleep(opts.total_budget);
    tokio::pin!(budget);

    loop {
        tokio::select! {
            _ = wait_for_shutdown_signal(&mut shutdown) => {
                write_shutdown_marker(repo, &mut report).await;
                abort_remaining(repo, &mut join_set, &mut pending, &mut report, false).await;
                return report;
            }
            _ = &mut budget => {
                abort_remaining(repo, &mut join_set, &mut pending, &mut report, true).await;
                return report;
            }
            joined = join_set.join_next() => {
                let Some(joined) = joined else { break; };
                match joined {
                    Ok(outcome) => apply_outcome(repo, &mut pending, &mut report, outcome).await,
                    Err(error) => report.errors.push(([0; 32], StartupHandshakeError::Join(error.to_string()))),
                }
            }
        }
    }

    if let Err(error) = repo.clear_shutdown_marker().await {
        report.errors.push(([0; 32], error.into()));
    }
    report
}

async fn process_record(
    registry: PluginRegistry,
    record: PluginRegistryRecord,
    slot_store: Option<Arc<dyn Storage>>,
    skip_if_fresh: bool,
    force_rehandshake: bool,
    now: i64,
) -> RecordOutcome {
    if record.sha256 == BUILTIN_CACHE_AFFINITY_SHA256
        || record.sha256 == BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256
    {
        registry.load_record_into_cache(&record);
        return RecordOutcome::SkippedFresh {
            sha256: record.sha256,
        };
    }

    if !force_rehandshake && skip_if_fresh && is_fresh(&record, registry.host_offer_hash(), now) {
        registry.load_record_into_cache(&record);
        return RecordOutcome::SkippedFresh {
            sha256: record.sha256,
        };
    }

    match registry.re_handshake_by_sha256(&record.sha256).await {
        Ok(record) => {
            if let Some(storage) = slot_store.as_deref() {
                reconcile_supported_slots_drift(storage, &record).await;
            }
            RecordOutcome::ReHandshaked {
                sha256: record.sha256,
            }
        }
        Err(error) => RecordOutcome::Failed {
            sha256: record.sha256,
            error,
        },
    }
}

async fn reconcile_supported_slots_drift(storage: &dyn Storage, record: &PluginRegistryRecord) {
    let entry = match storage.get_registry_entry_by_sha(record.sha256).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(target: "cc_lb_server::drift", %error, "supported_slots drift lookup failed");
            return;
        }
    };
    if entry.id == BUILTIN_CACHE_AFFINITY_ID || entry.id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID {
        return;
    }

    let implemented_functions = record
        .augmented_metadata
        .negotiated_functions
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let fresh = slot_set_from_handshake(&implemented_functions);
    let stored = entry.supported_slots;
    if stored == fresh {
        return;
    }

    tracing::warn!(target: "cc_lb_server::drift", stored = ?stored, fresh = ?fresh, "supported_slots drift on re-handshake");
    if let Err(error) = storage.update_supported_slots(entry.id, fresh).await {
        tracing::warn!(target: "cc_lb_server::drift", %error, id = %entry.id, "supported_slots drift update failed");
    }
}

async fn apply_outcome(
    repo: &dyn PluginRegistryRepo,
    pending: &mut BTreeSet<[u8; 32]>,
    report: &mut StartupHandshakeReport,
    outcome: RecordOutcome,
) {
    report.processed += 1;
    match outcome {
        RecordOutcome::SkippedFresh { sha256 } => {
            pending.remove(&sha256);
            report.skipped_fresh += 1;
        }
        RecordOutcome::ReHandshaked { sha256 } => {
            pending.remove(&sha256);
            report.re_handshaked += 1;
        }
        RecordOutcome::Failed { sha256, error } => {
            pending.remove(&sha256);
            disable_failed(repo, report, sha256, error).await;
        }
    }
}

async fn disable_failed(
    repo: &dyn PluginRegistryRepo,
    report: &mut StartupHandshakeReport,
    sha256: [u8; 32],
    error: RegistryError,
) {
    if matches!(error, RegistryError::BlobMissing { .. }) {
        tracing::error!(sha256 = %hex_sha256(&sha256), "plugin registry references missing blob; disabling plugin");
    }
    if let Err(status_error) = repo
        .set_status(&sha256, PluginRegistryStatus::Disabled)
        .await
    {
        report.errors.push((sha256, status_error.into()));
    } else {
        report.disabled += 1;
    }
    report.errors.push((sha256, error.into()));
}

async fn abort_remaining(
    repo: &dyn PluginRegistryRepo,
    join_set: &mut JoinSet<RecordOutcome>,
    pending: &mut BTreeSet<[u8; 32]>,
    report: &mut StartupHandshakeReport,
    disable_pending: bool,
) {
    join_set.abort_all();
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(outcome) => apply_outcome(repo, pending, report, outcome).await,
            Err(error) if error.is_cancelled() => {}
            Err(error) => report
                .errors
                .push(([0; 32], StartupHandshakeError::Join(error.to_string()))),
        }
    }
    if !disable_pending {
        return;
    }
    let remaining = std::mem::take(pending);
    for sha256 in remaining {
        if let Err(error) = repo
            .set_status(&sha256, PluginRegistryStatus::Disabled)
            .await
        {
            report.errors.push((sha256, error.into()));
        } else {
            report.disabled += 1;
        }
    }
}

async fn write_shutdown_marker(repo: &dyn PluginRegistryRepo, report: &mut StartupHandshakeReport) {
    let now = match unix_now() {
        Ok(now) => now,
        Err(error) => {
            report.errors.push(([0; 32], error));
            return;
        }
    };
    if let Err(error) = repo.set_shutdown_marker(now).await {
        report.errors.push(([0; 32], error.into()));
    }
}

fn is_fresh(record: &PluginRegistryRecord, host_offer_hash: [u8; 32], now: i64) -> bool {
    record.host_offer_hash == host_offer_hash
        && record.last_handshake_at >= now.saturating_sub(SKIP_HANDSHAKE_IF_FRESH_TTL_SECS as i64)
}

fn stale_after_shutdown(record: &PluginRegistryRecord, shutdown_marker: Option<i64>) -> bool {
    shutdown_marker.is_some_and(|marker| record.last_handshake_at < marker)
}

fn shutdown_requested(shutdown: &ShutdownSignal) -> bool {
    *shutdown.borrow()
}

async fn wait_for_shutdown_signal(shutdown: &mut ShutdownSignal) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        if shutdown.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

fn unix_now() -> Result<i64, StartupHandshakeError> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StartupHandshakeError::Clock(error.to_string()))?
        .as_secs();
    i64::try_from(seconds).map_err(|error| StartupHandshakeError::Clock(error.to_string()))
}

fn hex_sha256(sha256: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in sha256 {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

const LEGACY_BRIDGE_PAGE_SIZE: usize = 64;

#[derive(Debug, Default)]
pub struct LegacyBridgeReport {
    pub scanned: usize,
    pub already_present: usize,
    pub bridged: usize,
    pub orphan: usize,
    pub failed: Vec<([u8; 32], LegacyBridgeError)>,
}

#[derive(Debug, Error)]
pub enum LegacyBridgeError {
    #[error("legacy wasm store failed: {reason}")]
    LegacyStore { reason: String },
    #[error("plugin registry repository failed: {0}")]
    Repo(#[from] RepoError),
    #[error("plugin registry failed: {0}")]
    Registry(#[from] RegistryError),
}

pub async fn bridge_legacy_wasm_registry(
    plugin_registry: &PluginRegistry,
    legacy_store: &dyn PluginRegistryStore,
) -> LegacyBridgeReport {
    let mut report = LegacyBridgeReport::default();
    let mut cursor: Option<uuid::Uuid> = None;

    loop {
        let page = match legacy_store
            .list_registry(cursor, LEGACY_BRIDGE_PAGE_SIZE)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                report.failed.push((
                    [0; 32],
                    LegacyBridgeError::LegacyStore {
                        reason: error.to_string(),
                    },
                ));
                return report;
            }
        };

        if page.is_empty() {
            break;
        }
        let last_id = page.last().map(|entry| entry.id);

        for entry in page {
            if entry.is_builtin
                || entry.id == BUILTIN_CACHE_AFFINITY_ID
                || entry.id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID
            {
                continue;
            }
            report.scanned += 1;
            bridge_single_entry(plugin_registry, legacy_store, &entry, &mut report).await;
        }

        match last_id {
            Some(id) => cursor = Some(id),
            None => break,
        }
    }

    report
}

async fn bridge_single_entry(
    plugin_registry: &PluginRegistry,
    legacy_store: &dyn PluginRegistryStore,
    entry: &WasmRegistryEntry,
    report: &mut LegacyBridgeReport,
) {
    match plugin_registry.get_by_sha256(&entry.sha256).await {
        Ok(Some(_)) => {
            report.already_present += 1;
            return;
        }
        Ok(None) => {}
        Err(error) => {
            report.failed.push((entry.sha256, error.into()));
            return;
        }
    }

    let blob_bytes = match legacy_store.get_blob_bytes(entry.sha256).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            report.orphan += 1;
            return;
        }
        Err(error) => {
            report.failed.push((
                entry.sha256,
                LegacyBridgeError::LegacyStore {
                    reason: error.to_string(),
                },
            ));
            return;
        }
    };

    match plugin_registry.register_plugin(&blob_bytes).await {
        Ok(_) => report.bridged += 1,
        Err(error) => report.failed.push((entry.sha256, error.into())),
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use cc_lb_plugin_wire::handshake::{
        HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept, HandshakeOffer,
    };
    use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
    use cc_lb_plugin_wire::self_check::{SelfCheckResponse, SelfCheckStatus};
    use cc_lb_runtime_extism::handshake::HandshakeExecutionError;
    use cc_lb_runtime_extism::handshake::build_offer;
    use cc_lb_runtime_extism::registry::RegistryLifecycle;
    use cc_lb_runtime_extism::self_check::SelfCheckExecutionError;
    use cc_lb_storage_api::{PluginBlobRepo, RepoError};
    use serde_json::json;
    use tokio::sync::{Mutex, Notify};

    use super::*;

    #[tokio::test]
    async fn skip_handshake_if_fresh_invalidates_on_offer_change() {
        let repos = Repos::default();
        let old_registry = registry(repos.registry.clone(), repos.blobs.clone(), BTreeSet::new());
        let wasm = plugin_wasm("test-plugin", "1.0.0");
        let sha = sha256(&wasm);
        let mut record = old_registry
            .register_plugin(&wasm)
            .await
            .expect("register succeeds");
        record.host_offer_hash = old_registry.host_offer_hash();
        record.last_handshake_at = unix_now().expect("clock") - 1;
        repos.registry.upsert_record(&record).await.expect("seed");

        let same_offer_registry =
            registry(repos.registry.clone(), repos.blobs.clone(), BTreeSet::new());
        repos.blobs.gets.store(0, Ordering::SeqCst);
        let (_tx, shutdown) = watch::channel(false);
        let fresh_report = run_startup_handshake(
            &same_offer_registry,
            repos.registry.as_ref(),
            StartupHandshakeOpts::default(),
            shutdown,
        )
        .await;
        assert_eq!(fresh_report.processed, 1);
        assert_eq!(fresh_report.skipped_fresh, 1);
        assert_eq!(fresh_report.re_handshaked, 0);
        assert_eq!(repos.blobs.gets.load(Ordering::SeqCst), 0);
        assert!(same_offer_registry.get_metadata(&sha).is_some());

        let new_registry = registry(
            repos.registry.clone(),
            repos.blobs.clone(),
            BTreeSet::from(["new-capability".to_owned()]),
        );
        let (_tx, shutdown) = watch::channel(false);
        let report = run_startup_handshake(
            &new_registry,
            repos.registry.as_ref(),
            StartupHandshakeOpts::default(),
            shutdown,
        )
        .await;

        assert_eq!(report.processed, 1);
        assert_eq!(report.skipped_fresh, 0);
        assert_eq!(report.re_handshaked, 1);
        assert_eq!(repos.blobs.gets.load(Ordering::SeqCst), 1);
        assert!(new_registry.get_metadata(&sha).is_some());
    }

    #[tokio::test]
    async fn mid_startup_shutdown_leaves_recovery_marker() {
        let repos = Repos::default();
        repos.blobs.set_delay(Duration::from_secs(60)).await;
        let registry = registry(repos.registry.clone(), repos.blobs.clone(), BTreeSet::new());
        let wasm = plugin_wasm("test-plugin", "1.0.0");
        registry
            .register_plugin(&wasm)
            .await
            .expect("register succeeds");
        let (tx, shutdown) = watch::channel(false);
        let started = repos.blobs.get_started.clone();
        let started_wait = started.notified();

        let task = tokio::spawn({
            let registry = registry.clone();
            let repo = repos.registry.clone();
            async move {
                run_startup_handshake(
                    &registry,
                    repo.as_ref(),
                    StartupHandshakeOpts {
                        skip_if_fresh: false,
                        parallelism: 1,
                        total_budget: Duration::from_secs(30),
                        force: false,
                    },
                    shutdown,
                )
                .await
            }
        });
        started_wait.await;
        tx.send(true).expect("shutdown sends");
        let report = task.await.expect("task joins");

        assert_eq!(report.disabled, 0);
        assert_eq!(repos.registry.active_count().await, 1);
        assert!(
            repos
                .registry
                .get_shutdown_marker()
                .await
                .expect("marker")
                .is_some()
        );
    }

    #[tokio::test]
    async fn startup_parallel_max_8() {
        let repos = Repos::default();
        repos.blobs.set_delay(Duration::from_millis(50)).await;
        let registry = registry(repos.registry.clone(), repos.blobs.clone(), BTreeSet::new());
        seed_plugins(&registry, 12).await;
        let (_tx, shutdown) = watch::channel(false);

        let report = run_startup_handshake(
            &registry,
            repos.registry.as_ref(),
            StartupHandshakeOpts {
                skip_if_fresh: false,
                parallelism: 32,
                total_budget: Duration::from_secs(60),
                force: false,
            },
            shutdown,
        )
        .await;

        assert_eq!(report.re_handshaked, 12);
        assert!(repos.blobs.max_in_flight.load(Ordering::SeqCst) <= STARTUP_HANDSHAKE_PARALLEL_MAX);
    }

    #[tokio::test]
    async fn startup_budget_exceeded_disables_remaining() {
        let repos = Repos::default();
        repos.blobs.set_delay(Duration::from_secs(60)).await;
        let registry = registry(repos.registry.clone(), repos.blobs.clone(), BTreeSet::new());
        seed_plugins(&registry, 3).await;
        let (_tx, shutdown) = watch::channel(false);

        let report = run_startup_handshake(
            &registry,
            repos.registry.as_ref(),
            StartupHandshakeOpts {
                skip_if_fresh: false,
                parallelism: 2,
                total_budget: Duration::from_millis(20),
                force: false,
            },
            shutdown,
        )
        .await;

        assert_eq!(report.disabled, 3);
        assert_eq!(repos.registry.active_count().await, 0);
    }

    #[tokio::test]
    async fn force_overrides_skip() {
        let repos = Repos::default();
        let registry = registry(repos.registry.clone(), repos.blobs.clone(), BTreeSet::new());
        let wasm = plugin_wasm("test-plugin", "1.0.0");
        registry
            .register_plugin(&wasm)
            .await
            .expect("register succeeds");
        repos.blobs.gets.store(0, Ordering::SeqCst);
        let (_tx, shutdown) = watch::channel(false);

        let report = run_startup_handshake(
            &registry,
            repos.registry.as_ref(),
            StartupHandshakeOpts {
                skip_if_fresh: true,
                force: true,
                ..StartupHandshakeOpts::default()
            },
            shutdown,
        )
        .await;

        assert_eq!(report.processed, 1);
        assert_eq!(report.skipped_fresh, 0);
        assert_eq!(report.re_handshaked, 1);
        assert_eq!(repos.blobs.gets.load(Ordering::SeqCst), 1);
    }

    async fn seed_plugins(registry: &PluginRegistry, count: usize) {
        for index in 0..count {
            let wasm = plugin_wasm(&format!("test-plugin-{index}"), "1.0.0");
            registry
                .register_plugin(&wasm)
                .await
                .expect("register succeeds");
        }
    }

    fn registry(
        registry_repo: Arc<MemoryRegistryRepo>,
        blob_repo: Arc<MemoryBlobRepo>,
        caps: BTreeSet<String>,
    ) -> PluginRegistry {
        PluginRegistry::new_with_lifecycle(
            registry_repo,
            blob_repo,
            build_offer(&caps),
            Arc::new(FastRegistryLifecycle),
        )
        .expect("registry builds")
    }

    struct FastRegistryLifecycle;

    impl RegistryLifecycle for FastRegistryLifecycle {
        fn execute_handshake(
            &self,
            _plugin_bytes: &[u8],
            offer: &HandshakeOffer,
        ) -> Result<HandshakeAccept, HandshakeExecutionError> {
            Ok(HandshakeAccept {
                handshake_schema_version: offer.handshake_schema_version,
                envelope_version: offer.envelope_version,
                chosen_versions: offer
                    .function_versions
                    .iter()
                    .map(|(function, versions)| {
                        let chosen = versions.iter().copied().max().unwrap_or(1);
                        (function.clone(), chosen)
                    })
                    .collect(),
                plugin_supported: offer.function_versions.clone(),
                implemented_functions: offer.function_versions.keys().cloned().collect(),
                required_capabilities: BTreeSet::new(),
            })
        }

        fn execute_self_check(
            &self,
            _plugin_bytes: &[u8],
            _supported_slots: &[cc_lb_storage_api::PluginSlot],
        ) -> Result<SelfCheckResponse, SelfCheckExecutionError> {
            Ok(SelfCheckResponse {
                status: SelfCheckStatus::Success,
                failures: Vec::new(),
                completed_at: 1,
            })
        }
    }

    #[derive(Default)]
    struct Repos {
        registry: Arc<MemoryRegistryRepo>,
        blobs: Arc<MemoryBlobRepo>,
    }

    #[derive(Default)]
    struct MemoryRegistryRepo {
        records: Mutex<BTreeMap<[u8; 32], PluginRegistryRecord>>,
        marker: Mutex<Option<i64>>,
    }

    impl MemoryRegistryRepo {
        async fn active_count(&self) -> usize {
            self.records
                .lock()
                .await
                .values()
                .filter(|record| record.status == PluginRegistryStatus::Active)
                .count()
        }
    }

    #[async_trait]
    impl PluginRegistryRepo for MemoryRegistryRepo {
        async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
            self.records
                .lock()
                .await
                .insert(record.sha256, record.clone());
            Ok(())
        }

        async fn get_by_sha256(
            &self,
            sha256: &[u8; 32],
        ) -> Result<Option<PluginRegistryRecord>, RepoError> {
            Ok(self.records.lock().await.get(sha256).cloned())
        }

        async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
            Ok(self
                .records
                .lock()
                .await
                .values()
                .filter(|record| record.status == PluginRegistryStatus::Active)
                .cloned()
                .collect())
        }

        async fn set_status(
            &self,
            sha256: &[u8; 32],
            status: PluginRegistryStatus,
        ) -> Result<(), RepoError> {
            if let Some(record) = self.records.lock().await.get_mut(sha256) {
                record.status = status;
            }
            Ok(())
        }

        async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
            self.records.lock().await.remove(sha256);
            Ok(())
        }

        async fn count(&self) -> Result<usize, RepoError> {
            Ok(self.records.lock().await.len())
        }

        async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
            Ok(*self.marker.lock().await)
        }

        async fn set_shutdown_marker(&self, unix_secs: i64) -> Result<(), RepoError> {
            *self.marker.lock().await = Some(unix_secs);
            Ok(())
        }

        async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
            *self.marker.lock().await = None;
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryBlobRepo {
        blobs: Mutex<BTreeMap<[u8; 32], Vec<u8>>>,
        delay_get: Mutex<Option<Duration>>,
        gets: AtomicUsize,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        get_started: Arc<Notify>,
    }

    impl MemoryBlobRepo {
        async fn set_delay(&self, delay: Duration) {
            *self.delay_get.lock().await = Some(delay);
        }
    }

    #[async_trait]
    impl PluginBlobRepo for MemoryBlobRepo {
        async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
            self.blobs.lock().await.insert(*sha256, bytes.to_vec());
            Ok(())
        }

        async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            self.get_started.notify_waiters();
            if let Some(delay) = *self.delay_get.lock().await {
                tokio::time::sleep(delay).await;
            }
            let result = self.blobs.lock().await.get(sha256).cloned();
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(result)
        }

        async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
            self.blobs.lock().await.remove(sha256);
            Ok(())
        }

        async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
            Ok(self.blobs.lock().await.keys().copied().collect())
        }
    }

    fn plugin_wasm(plugin_name: &str, plugin_version: &str) -> Vec<u8> {
        let accept = HandshakeAccept {
            handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
            envelope_version: 1,
            chosen_versions: BTreeMap::from([("route".to_owned(), 1)]),
            plugin_supported: BTreeMap::from([("route".to_owned(), vec![1])]),
            implemented_functions: BTreeSet::from(["route".to_owned()]),
            required_capabilities: BTreeSet::new(),
        };
        let handshake_output = serde_json::to_string(&accept).expect("accept serializes");
        let self_check_output =
            json!({"status":"success","failures":[],"completed_at":1}).to_string();
        let wat = format!(
            r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {handshake_helper}
  {self_check_helper}
  (func (export "cc_lb_handshake") (result i32)
    (call $output_set (call $handshake_out) (i64.const {handshake_len}))
    (i32.const 0))
  (func (export "cc_lb_self_check") (result i32)
    (call $output_set (call $self_check_out) (i64.const {self_check_len}))
    (i32.const 0))
  (func (export "route") (result i32)
    (i32.const 0))
)
"#,
            handshake_helper = bytes_helper("handshake_out", handshake_output.as_bytes()),
            self_check_helper = bytes_helper("self_check_out", self_check_output.as_bytes()),
            handshake_len = handshake_output.len(),
            self_check_len = self_check_output.len(),
        );
        let mut wasm = wat::parse_str(&wat).expect("wat parses");
        append_identity_section(&mut wasm, plugin_name, plugin_version);
        wasm
    }

    fn bytes_helper(name: &str, bytes: &[u8]) -> String {
        let mut stores = String::new();
        for (index, byte) in bytes.iter().enumerate() {
            stores.push_str(&format!(
                "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
            ));
        }
        format!(
            r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
            len = bytes.len()
        )
    }

    fn append_identity_section(wasm: &mut Vec<u8>, plugin_name: &str, plugin_version: &str) {
        let payload = json!({
            "magic": CC_LB_PLUGIN_MAGIC,
            "abi_envelope": 1,
            "plugin_name": plugin_name,
            "plugin_version": plugin_version,
        })
        .to_string();
        wasm.push(0);
        let mut section = Vec::new();
        encode_u32(CC_LB_PLUGIN_SECTION_NAME.len() as u32, &mut section);
        section.extend_from_slice(CC_LB_PLUGIN_SECTION_NAME.as_bytes());
        section.extend_from_slice(payload.as_bytes());
        encode_u32(section.len() as u32, wasm);
        wasm.extend_from_slice(&section);
    }

    fn encode_u32(mut value: u32, output: &mut Vec<u8>) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        Sha256::digest(bytes).into()
    }
}
