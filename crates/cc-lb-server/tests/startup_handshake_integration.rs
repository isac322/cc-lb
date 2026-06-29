use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_core::{Clock, TestClock};
use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME, PluginIdentity};
use cc_lb_plugin_wire::limits::{SKIP_HANDSHAKE_IF_FRESH_TTL_SECS, STARTUP_HANDSHAKE_PARALLEL_MAX};
use cc_lb_runtime_extism::handshake::build_offer;
use cc_lb_runtime_extism::registry::PluginRegistry;
use cc_lb_server::startup_handshake::{StartupHandshakeOpts, run_startup_handshake};
use cc_lb_storage_api::{
    BackendKind, PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus,
    RepoError,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify, watch};

static TEST_LOCK: Mutex<()> = Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn skip_if_fresh_fast_path_100_records_executes_zero_handshakes() -> Result<()> {
    let _test_guard = TEST_LOCK.lock().await;
    let repos = TestRepos::new().await?;
    let registry = repos.registry(BTreeSet::new())?;
    let seeded = seed_records(
        &repos,
        100,
        unix_now(repos.clock.as_ref())?.saturating_sub(1),
        registry.host_offer_hash(),
    )
    .await?;
    let (_shutdown_tx, shutdown) = watch::channel(false);

    let report = run_startup_handshake(
        &registry,
        repos.registry_repo.as_ref(),
        StartupHandshakeOpts::default(),
        shutdown,
        repos.clock.as_ref(),
    )
    .await;

    assert!(
        report.errors.is_empty(),
        "unexpected errors: {:?}",
        report.errors
    );
    assert_eq!(report.processed, 100);
    assert_eq!(report.skipped_fresh, 100);
    assert_eq!(report.re_handshaked, 0);
    assert_eq!(repos.blobs.gets(), 0);
    for plugin in seeded {
        assert!(registry.get_metadata(&plugin.sha256).is_some());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn force_handshake_rehandshakes_all_records() -> Result<()> {
    let _test_guard = TEST_LOCK.lock().await;
    let repos = TestRepos::new().await?;
    let registry = repos.registry(BTreeSet::new())?;
    let count = 16;
    seed_records(
        &repos,
        count,
        unix_now(repos.clock.as_ref())?.saturating_sub(120),
        registry.host_offer_hash(),
    )
    .await?;
    let (_shutdown_tx, shutdown) = watch::channel(false);

    let report = run_startup_handshake(
        &registry,
        repos.registry_repo.as_ref(),
        StartupHandshakeOpts {
            force: true,
            ..StartupHandshakeOpts::default()
        },
        shutdown,
        repos.clock.as_ref(),
    )
    .await;

    assert!(
        report.errors.is_empty(),
        "unexpected errors: {:?}",
        report.errors
    );
    assert_eq!(report.processed, count);
    assert_eq!(report.skipped_fresh, 0);
    assert_eq!(report.re_handshaked, count);
    assert_eq!(repos.blobs.gets(), count);
    assert_eq!(repos.registry_repo.list_active().await?.len(), count);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn host_offer_change_rehandshakes_all_records() -> Result<()> {
    let _test_guard = TEST_LOCK.lock().await;
    let repos = TestRepos::new().await?;
    let old_registry = repos.registry(BTreeSet::new())?;
    let new_registry = repos.registry(BTreeSet::from(["changed-host-offer".to_owned()]))?;
    let count = 16;
    let seeded = seed_records(
        &repos,
        count,
        unix_now(repos.clock.as_ref())?.saturating_sub(1),
        old_registry.host_offer_hash(),
    )
    .await?;
    let (_shutdown_tx, shutdown) = watch::channel(false);

    let report = run_startup_handshake(
        &new_registry,
        repos.registry_repo.as_ref(),
        StartupHandshakeOpts::default(),
        shutdown,
        repos.clock.as_ref(),
    )
    .await;

    assert!(
        report.errors.is_empty(),
        "unexpected errors: {:?}",
        report.errors
    );
    assert_eq!(report.processed, count);
    assert_eq!(report.skipped_fresh, 0);
    assert_eq!(report.re_handshaked, count);
    assert_eq!(repos.blobs.gets(), count);
    for plugin in seeded {
        let record = repos
            .registry_repo
            .get_by_sha256(&plugin.sha256)
            .await?
            .expect("record remains present");
        assert_eq!(record.host_offer_hash, new_registry.host_offer_hash());
        assert!(new_registry.get_metadata(&plugin.sha256).is_some());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mid_startup_shutdown_saves_partial_progress_and_marker() -> Result<()> {
    let _test_guard = TEST_LOCK.lock().await;
    let repos = TestRepos::new().await?;
    let registry = repos.registry(BTreeSet::new())?;
    let original_last_handshake_at = unix_now(repos.clock.as_ref())?.saturating_sub(120);
    let seeded = seed_records(
        &repos,
        8,
        original_last_handshake_at,
        registry.host_offer_hash(),
    )
    .await?;
    repos
        .blobs
        .set_delay_after(3, Duration::from_secs(60))
        .await;
    let (shutdown_tx, shutdown) = watch::channel(false);

    let task = tokio::spawn({
        let registry = registry.clone();
        let registry_repo = repos.registry_repo.clone();
        let clock = repos.clock.clone();
        async move {
            run_startup_handshake(
                &registry,
                registry_repo.as_ref(),
                StartupHandshakeOpts {
                    skip_if_fresh: false,
                    force: false,
                    total_budget: Duration::from_secs(30),
                    parallelism: 1,
                },
                shutdown,
                clock.as_ref(),
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(30), repos.blobs.wait_for_gets(4)).await?;
    shutdown_tx.send(true)?;
    let report = tokio::time::timeout(Duration::from_secs(30), task).await??;

    assert!(
        report.errors.is_empty(),
        "unexpected errors: {:?}",
        report.errors
    );
    assert!(
        report.re_handshaked > 0,
        "expected some completed handshakes"
    );
    assert!(
        report.re_handshaked < seeded.len(),
        "expected shutdown to stop remaining work"
    );
    assert_eq!(report.disabled, 0);
    assert!(repos.registry_repo.get_shutdown_marker().await?.is_some());
    assert_eq!(repos.registry_repo.list_active().await?.len(), seeded.len());
    assert_eq!(
        count_records_newer_than(&repos, &seeded, original_last_handshake_at).await?,
        report.re_handshaked
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn recovery_from_mid_shutdown_rehandshakes_stale_records() -> Result<()> {
    let _test_guard = TEST_LOCK.lock().await;
    let repos = TestRepos::new().await?;
    let registry = repos.registry(BTreeSet::new())?;
    let shutdown_marker = unix_now(repos.clock.as_ref())?;
    let seeded = seed_records(
        &repos,
        12,
        shutdown_marker.saturating_sub(120),
        registry.host_offer_hash(),
    )
    .await?;
    repos
        .registry_repo
        .set_shutdown_marker(shutdown_marker)
        .await?;
    let (_shutdown_tx, shutdown) = watch::channel(false);

    let report = run_startup_handshake(
        &registry,
        repos.registry_repo.as_ref(),
        StartupHandshakeOpts::default(),
        shutdown,
        repos.clock.as_ref(),
    )
    .await;

    assert!(
        report.errors.is_empty(),
        "unexpected errors: {:?}",
        report.errors
    );
    assert_eq!(report.processed, seeded.len());
    assert_eq!(report.skipped_fresh, 0);
    assert_eq!(report.re_handshaked, seeded.len());
    assert_eq!(repos.blobs.gets(), seeded.len());
    assert_eq!(repos.registry_repo.get_shutdown_marker().await?, None);
    for plugin in seeded {
        assert!(registry.get_metadata(&plugin.sha256).is_some());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_execution_caps_at_8_and_is_time_bounded() -> Result<()> {
    let _test_guard = TEST_LOCK.lock().await;
    let repos = TestRepos::new().await?;
    let registry = repos.registry(BTreeSet::new())?;
    seed_records(
        &repos,
        12,
        unix_now(repos.clock.as_ref())?.saturating_sub(120),
        registry.host_offer_hash(),
    )
    .await?;
    repos.blobs.set_delay(Duration::from_millis(200)).await;
    let (_shutdown_tx, shutdown) = watch::channel(false);

    let started = Instant::now();
    let report = run_startup_handshake(
        &registry,
        repos.registry_repo.as_ref(),
        StartupHandshakeOpts {
            skip_if_fresh: false,
            force: false,
            total_budget: Duration::from_secs(60),
            parallelism: 32,
        },
        shutdown,
        repos.clock.as_ref(),
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        report.errors.is_empty(),
        "unexpected errors: {:?}",
        report.errors
    );
    assert_eq!(report.re_handshaked, 12);
    assert_eq!(repos.blobs.gets(), 12);
    let observed_max = repos.blobs.max_in_flight();
    assert!(
        observed_max <= STARTUP_HANDSHAKE_PARALLEL_MAX,
        "max_in_flight {observed_max} exceeds semaphore cap {STARTUP_HANDSHAKE_PARALLEL_MAX}"
    );
    assert!(
        observed_max >= 2,
        "max_in_flight {observed_max} is too low to demonstrate parallelism"
    );
    assert!(
        elapsed < Duration::from_secs(60),
        "expected startup handshakes to complete within the CI budget, elapsed {elapsed:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn budget_limit_disables_remaining_records() -> Result<()> {
    let _test_guard = TEST_LOCK.lock().await;
    let repos = TestRepos::new().await?;
    let registry = repos.registry(BTreeSet::new())?;
    let seeded = seed_records(
        &repos,
        100,
        unix_now(repos.clock.as_ref())?.saturating_sub(120),
        registry.host_offer_hash(),
    )
    .await?;
    repos.blobs.set_delay(Duration::from_secs(60)).await;
    let (_shutdown_tx, shutdown) = watch::channel(false);

    let report = run_startup_handshake(
        &registry,
        repos.registry_repo.as_ref(),
        StartupHandshakeOpts {
            skip_if_fresh: false,
            force: false,
            total_budget: Duration::from_millis(20),
            parallelism: 32,
        },
        shutdown,
        repos.clock.as_ref(),
    )
    .await;

    assert!(
        report.errors.is_empty(),
        "unexpected errors: {:?}",
        report.errors
    );
    assert_eq!(report.re_handshaked, 0);
    assert_eq!(report.disabled, seeded.len());
    assert_eq!(repos.registry_repo.list_active().await?.len(), 0);
    assert_eq!(
        count_records_with_status(&repos, &seeded, PluginRegistryStatus::Disabled).await?,
        seeded.len()
    );
    assert!(repos.blobs.max_in_flight() <= STARTUP_HANDSHAKE_PARALLEL_MAX);
    Ok(())
}

struct TestRepos {
    clock: Arc<TestClock>,
    registry_repo: Arc<SqliteStorage>,
    blobs: Arc<InstrumentedBlobRepo>,
}

impl TestRepos {
    async fn new() -> Result<Self> {
        let clock = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let storage = open_sqlite("sqlite::memory:", clock.clone()).await?;
        cc_lb_storage_api::MetaStore::initialize(&storage, BackendKind::Sqlite).await?;
        for record in storage.list_active().await? {
            storage.delete_by_sha256(&record.sha256).await?;
        }
        let storage = Arc::new(storage);
        let blobs = Arc::new(InstrumentedBlobRepo::new(storage.clone()));
        Ok(Self {
            clock,
            registry_repo: storage,
            blobs,
        })
    }

    fn registry(&self, host_caps: BTreeSet<String>) -> Result<PluginRegistry> {
        let registry_repo: Arc<dyn PluginRegistryRepo> = self.registry_repo.clone();
        let blob_repo: Arc<dyn PluginBlobRepo> = self.blobs.clone();
        Ok(PluginRegistry::new(
            registry_repo,
            blob_repo,
            build_offer(&host_caps),
            self.clock.clone(),
        )?)
    }
}

#[derive(Clone, Copy)]
struct SeededPlugin {
    sha256: [u8; 32],
}

async fn seed_records(
    repos: &TestRepos,
    count: usize,
    last_handshake_at: i64,
    host_offer_hash: [u8; 32],
) -> Result<Vec<SeededPlugin>> {
    let mut seeded = Vec::with_capacity(count);
    for index in 0..count {
        let plugin_name = format!("startup-plugin-{index}");
        let wasm = plugin_wasm(&plugin_name, "1.0.0");
        let sha256 = sha256(&wasm);
        repos.blobs.put_blob(&sha256, &wasm).await?;
        let record = plugin_record(
            sha256,
            &plugin_name,
            "1.0.0",
            host_offer_hash,
            last_handshake_at,
        )?;
        repos.registry_repo.upsert_record(&record).await?;
        seeded.push(SeededPlugin { sha256 });
    }
    Ok(seeded)
}

fn plugin_record(
    sha256: [u8; 32],
    plugin_name: &str,
    plugin_version: &str,
    host_offer_hash: [u8; 32],
    last_handshake_at: i64,
) -> Result<PluginRegistryRecord> {
    let identity = PluginIdentity {
        magic: CC_LB_PLUGIN_MAGIC,
        abi_envelope: 1,
        plugin_name: plugin_name.to_owned(),
        plugin_version: plugin_version.to_owned(),
    };
    let augmented_metadata = AugmentedMetadata::from_handshake_and_self_check(
        identity,
        BTreeMap::from([("filter".to_owned(), 1)]),
        BTreeSet::new(),
        last_handshake_at,
        true,
        last_handshake_at,
        SKIP_HANDSHAKE_IF_FRESH_TTL_SECS,
    )?;

    Ok(PluginRegistryRecord {
        sha256,
        plugin_name: plugin_name.to_owned(),
        plugin_version: plugin_version.to_owned(),
        abi_envelope: 1,
        augmented_metadata,
        host_offer_hash,
        handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
        last_handshake_at,
        status: PluginRegistryStatus::Active,
    })
}

async fn count_records_newer_than(
    repos: &TestRepos,
    seeded: &[SeededPlugin],
    timestamp: i64,
) -> Result<usize> {
    let mut count = 0;
    for plugin in seeded {
        let record = repos
            .registry_repo
            .get_by_sha256(&plugin.sha256)
            .await?
            .expect("record remains present");
        if record.last_handshake_at > timestamp {
            count += 1;
        }
    }
    Ok(count)
}

async fn count_records_with_status(
    repos: &TestRepos,
    seeded: &[SeededPlugin],
    status: PluginRegistryStatus,
) -> Result<usize> {
    let mut count = 0;
    for plugin in seeded {
        let record = repos
            .registry_repo
            .get_by_sha256(&plugin.sha256)
            .await?
            .expect("record remains present");
        if record.status == status {
            count += 1;
        }
    }
    Ok(count)
}

struct InstrumentedBlobRepo {
    inner: Arc<SqliteStorage>,
    stats: Arc<BlobStats>,
}

impl InstrumentedBlobRepo {
    fn new(inner: Arc<SqliteStorage>) -> Self {
        Self {
            inner,
            stats: Arc::new(BlobStats::default()),
        }
    }

    async fn set_delay(&self, duration: Duration) {
        *self.stats.delay.lock().await = DelayConfig {
            after_gets: 0,
            duration,
        };
    }

    async fn set_delay_after(&self, after_gets: usize, duration: Duration) {
        *self.stats.delay.lock().await = DelayConfig {
            after_gets,
            duration,
        };
    }

    fn gets(&self) -> usize {
        self.stats.gets.load(Ordering::SeqCst)
    }

    fn max_in_flight(&self) -> usize {
        self.stats.max_in_flight.load(Ordering::SeqCst)
    }

    async fn wait_for_gets(&self, expected: usize) {
        loop {
            if self.gets() >= expected {
                return;
            }
            let notified = self.stats.started.notified();
            if self.gets() >= expected {
                return;
            }
            notified.await;
        }
    }
}

#[async_trait]
impl PluginBlobRepo for InstrumentedBlobRepo {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        self.inner.put_blob(sha256, bytes).await
    }

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        let get_number = self.stats.gets.fetch_add(1, Ordering::SeqCst) + 1;
        let in_flight = self.stats.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.stats
            .max_in_flight
            .fetch_max(in_flight, Ordering::SeqCst);
        self.stats.started.notify_waiters();
        let _in_flight_guard = InFlightGuard {
            stats: self.stats.clone(),
        };
        let delay = *self.stats.delay.lock().await;
        if get_number > delay.after_gets && !delay.duration.is_zero() {
            tokio::time::sleep(delay.duration).await;
        }
        self.inner.get_blob(sha256).await
    }

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        self.inner.delete_blob(sha256).await
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        self.inner.list_blob_keys().await
    }
}

struct BlobStats {
    gets: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    started: Notify,
    delay: Mutex<DelayConfig>,
}

impl Default for BlobStats {
    fn default() -> Self {
        Self {
            gets: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            started: Notify::new(),
            delay: Mutex::new(DelayConfig::default()),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct DelayConfig {
    after_gets: usize,
    duration: Duration,
}

struct InFlightGuard {
    stats: Arc<BlobStats>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.stats.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

fn plugin_wasm(plugin_name: &str, plugin_version: &str) -> Vec<u8> {
    let mut wasm = base_plugin_wasm();
    append_identity_section(&mut wasm, plugin_name, plugin_version);
    wasm
}

fn base_plugin_wasm() -> Vec<u8> {
    static BASE: OnceLock<Vec<u8>> = OnceLock::new();
    BASE.get_or_init(|| {
        let accept = HandshakeAccept {
            handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
            envelope_version: 1,
            chosen_versions: BTreeMap::from([("filter".to_owned(), 1)]),
            plugin_supported: BTreeMap::from([("filter".to_owned(), vec![1])]),
            implemented_functions: BTreeSet::from(["filter".to_owned()]),
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
  (func (export "filter") (result i32)
    (i32.const 0))
)
"#,
            handshake_helper = bytes_helper("handshake_out", handshake_output.as_bytes()),
            self_check_helper = bytes_helper("self_check_out", self_check_output.as_bytes()),
            handshake_len = handshake_output.len(),
            self_check_len = self_check_output.len(),
        );
        wat::parse_str(&wat).expect("wat parses")
    })
    .clone()
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
    Sha256::digest(bytes).into()
}

fn unix_now(clock: &dyn Clock) -> Result<i64> {
    Ok(i64::try_from(
        clock.now().duration_since(UNIX_EPOCH)?.as_secs(),
    )?)
}
