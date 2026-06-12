#![allow(deprecated)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cc_lb_aead::AeadService;
use cc_lb_config::{
    AnthropicOAuthConfig, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind,
};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_core::{DynamicViewHolder, Lifecycle, LifecycleConfig};
use cc_lb_plugin_api::{InternalErrorKind, InternalErrorStage, TerminalStrategy};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    PluginChainEntryInput, PluginRegistryStore, PluginSlot, PrincipalCreate, PrincipalKind,
    PrincipalStore, PrincipalUpdate, RequestEvent, RequestEventStore, Storage as StorageTrait,
    UpstreamCreate, UpstreamStore, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_redb::Storage as RedbStorage;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PRINCIPAL_NAME: &str = "pipeline-principal";
const KEY_ID: &str = "pipeline-e2e-key";
const REQUEST_COUNT: usize = 100;
const TERMINAL_RNG_SEED: [u8; 32] = [0x28; 32];
const MESSAGE_BODY: &[u8] = br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#;

#[tokio::test]
async fn random_terminal_distribution_is_uniform_after_two_filters() -> TestResult<()> {
    let harness = PipelineHarness::new(
        1,
        TerminalStrategy::Random,
        |_| Vec::new(),
        |upstreams| {
            vec![
                filter_plugin("keep-first-3", first_n_ids(upstreams, 3), "keep first 3"),
                filter_plugin("keep-first-2", first_n_ids(upstreams, 2), "keep first 2"),
            ]
        },
        |_| None,
    )
    .await?;
    let survivors = first_n_upstreams(&harness.upstreams, 2);

    for index in 0..REQUEST_COUNT {
        let response = harness.send_message(&format!("random-{index}")).await?;
        assert_eq!(response.status, StatusCode::OK, "{}", response.body_text());
    }

    let events = harness.request_events().await?;
    assert_eq!(events.len(), REQUEST_COUNT);
    let counts = count_upstreams(&events);
    assert_eq!(counts.values().sum::<usize>(), REQUEST_COUNT, "{counts:?}");
    for (name, count) in &counts {
        assert!(
            survivors.iter().any(|upstream| upstream.name == *name),
            "Random terminal selected {name}, outside final survivor set {survivors:?}"
        );
        assert!(
            (40..=60).contains(count),
            "Random terminal selected {name} {count} times, expected 50 +/- 20%: {counts:?}"
        );
    }
    for survivor in &survivors {
        assert!(
            counts.contains_key(&survivor.name),
            "missing survivor {survivor:?}"
        );
    }
    for event in &events {
        assert_trace(
            event,
            &["keep-first-3", "keep-first-2"],
            TerminalStrategy::Random,
        );
        assert!(
            event
                .upstream_id
                .is_some_and(|id| survivors.iter().any(|upstream| upstream.id == id)),
            "event chose outside survivors: {event:?}"
        );
        assert!(
            event.internal_errors.is_empty(),
            "unexpected errors: {event:?}"
        );
    }

    harness.write_evidence(
        "random-distribution",
        format!(
            "100 seeded Random selections after KeepFirst3 -> KeepFirst2: {counts:?}; survivors={:?}",
            survivor_names(&survivors)
        ),
        &events,
    )?;
    Ok(())
}

#[tokio::test]
async fn first_pick_selects_single_candidate_after_two_filters() -> TestResult<()> {
    let harness = PipelineHarness::new(
        2,
        TerminalStrategy::FirstPick,
        |_| Vec::new(),
        |upstreams| {
            vec![
                filter_plugin("keep-first-3", first_n_ids(upstreams, 3), "keep first 3"),
                filter_plugin("keep-first-2", first_n_ids(upstreams, 2), "keep first 2"),
            ]
        },
        |_| None,
    )
    .await?;
    let expected = first_n_upstreams(&harness.upstreams, 1)
        .into_iter()
        .next()
        .expect("one upstream");

    let response = harness.send_message("first-pick").await?;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body_text());
    let events = harness.request_events().await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.upstream_id, Some(expected.id));
    assert_eq!(event.upstream_name.as_deref(), Some(expected.name.as_str()));
    assert_trace(
        event,
        &["keep-first-3", "keep-first-2"],
        TerminalStrategy::FirstPick,
    );
    assert!(
        event.internal_errors.is_empty(),
        "unexpected errors: {event:?}"
    );

    harness.write_evidence(
        "first-pick",
        format!(
            "FirstPick selected deterministic survivor {}",
            expected.name
        ),
        &events,
    )?;
    Ok(())
}

#[tokio::test]
async fn drop_all_chain_returns_503_with_routing_trace() -> TestResult<()> {
    let harness = PipelineHarness::new(
        3,
        TerminalStrategy::Random,
        |_| Vec::new(),
        |_| vec![drop_all_plugin("drop-all-a"), drop_all_plugin("drop-all-b")],
        |_| None,
    )
    .await?;

    let response = harness.send_message("drop-all").await?;
    assert_route_no_upstream_after_filter(&response);
    let events = harness.request_events().await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.status, StatusCode::SERVICE_UNAVAILABLE.as_u16());
    assert_eq!(
        event.error_code.as_deref(),
        Some("route_no_upstream_after_filter")
    );
    assert_trace(
        event,
        &["drop-all-a", "drop-all-b"],
        TerminalStrategy::Random,
    );
    assert_eq!(terminal_upstream_id(event), None);
    assert!(event.internal_errors.iter().any(|error| {
        error.stage == InternalErrorStage::Router
            && error.kind == InternalErrorKind::ConfigError
            && error
                .message
                .as_deref()
                .is_some_and(|message| message.contains("no upstream candidates remain"))
    }));

    harness.write_evidence(
        "drop-all-503",
        format!(
            "DropAll -> DropAll returned 503 body={}",
            response.body_text()
        ),
        &events,
    )?;
    Ok(())
}

#[tokio::test]
async fn trapping_filter_passes_through_to_keep_first_two_then_random() -> TestResult<()> {
    let harness = PipelineHarness::new(
        4,
        TerminalStrategy::Random,
        |_| Vec::new(),
        |upstreams| {
            vec![
                trap_filter_plugin("trapping-filter"),
                filter_plugin("keep-first-2", first_n_ids(upstreams, 2), "keep first 2"),
            ]
        },
        |_| None,
    )
    .await?;
    let survivors = first_n_upstreams(&harness.upstreams, 2);

    for index in 0..REQUEST_COUNT {
        let response = harness.send_message(&format!("trap-{index}")).await?;
        assert_eq!(response.status, StatusCode::OK, "{}", response.body_text());
    }

    let events = harness.request_events().await?;
    assert_eq!(events.len(), REQUEST_COUNT);
    let counts = count_upstreams(&events);
    for (name, count) in &counts {
        assert!(
            survivors.iter().any(|upstream| upstream.name == *name),
            "trapping passthrough selected {name} outside survivors: {counts:?}"
        );
        assert!(*count > 0, "selected count must be positive");
    }
    assert_eq!(counts.values().sum::<usize>(), REQUEST_COUNT);
    for event in &events {
        assert_trace(
            event,
            &["trapping-filter", "keep-first-2"],
            TerminalStrategy::Random,
        );
        assert!(
            event.internal_errors.iter().any(|error| {
                error.stage == InternalErrorStage::Router
                    && error.kind == InternalErrorKind::PluginError
                    && error
                        .message
                        .as_deref()
                        .is_some_and(|message| message.contains("filter trap error"))
            }),
            "expected filter trap internal error: {event:?}"
        );
        assert!(
            event
                .upstream_id
                .is_some_and(|id| survivors.iter().any(|upstream| upstream.id == id)),
            "event chose outside survivors: {event:?}"
        );
    }

    harness.write_evidence(
        "trap-passthrough",
        format!(
            "TrappingFilter passed through five candidates, KeepFirst2 reduced to {:?}, counts={counts:?}",
            survivor_names(&survivors)
        ),
        &events,
    )?;
    Ok(())
}

#[tokio::test]
async fn invalid_unknown_output_passes_through_and_records_invalid_output() -> TestResult<()> {
    let harness = PipelineHarness::new(
        5,
        TerminalStrategy::FirstPick,
        |_| Vec::new(),
        |_| vec![invalid_unknown_plugin("invalid-output-unknown")],
        |_| None,
    )
    .await?;
    let expected = first_n_upstreams(&harness.upstreams, 1)
        .into_iter()
        .next()
        .expect("one upstream");

    let response = harness.send_message("invalid-output").await?;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body_text());
    let events = harness.request_events().await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.upstream_id, Some(expected.id));
    assert_trace(
        event,
        &["invalid-output-unknown"],
        TerminalStrategy::FirstPick,
    );
    let invalid_errors = event
        .internal_errors
        .iter()
        .filter(|error| {
            error.stage == InternalErrorStage::RouterFilter
                && error.kind == InternalErrorKind::InvalidOutput
        })
        .count();
    assert_eq!(invalid_errors, 1, "event={event:?}");
    assert!(
        event
            .routing_trace
            .as_ref()
            .and_then(|trace| trace.stages.first())
            .and_then(|stage| stage.reason.as_deref())
            .is_some_and(|reason| reason.contains("unknown upstream id")),
        "event={event:?}"
    );

    harness.write_evidence(
        "invalid-output",
        format!(
            "InvalidOutput(Unknown) passed through to {} with one InvalidOutput error",
            expected.name
        ),
        &events,
    )?;
    Ok(())
}

#[tokio::test]
async fn shape_trap_raw_forwards_and_records_shape_internal_error() -> TestResult<()> {
    let harness = PipelineHarness::new(
        6,
        TerminalStrategy::FirstPick,
        |_| Vec::new(),
        |_| Vec::new(),
        |_| Some(shape_trap_plugin("shape-trap")),
    )
    .await?;
    let expected = first_n_upstreams(&harness.upstreams, 1)
        .into_iter()
        .next()
        .expect("one upstream");

    let response = harness.send_message("shape-trap").await?;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body_text());
    let last_request = get_json(expected.addr, "/__last_request").await?;
    assert_eq!(last_request["x_api_key"], "sk-ant-test");
    let events = harness.request_events().await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.upstream_id, Some(expected.id));
    assert_trace(event, &[], TerminalStrategy::FirstPick);
    assert!(event.internal_errors.iter().any(|error| {
        error.stage == InternalErrorStage::Shape
            && error.kind == InternalErrorKind::Trap
            && error
                .message
                .as_deref()
                .is_some_and(|message| message.contains("plugin shape failed"))
    }));

    harness.write_evidence(
        "shape-trap",
        format!(
            "Shape trap raw-forwarded to {} with fake-anthropic last_request={last_request}",
            expected.name
        ),
        &events,
    )?;
    Ok(())
}

#[tokio::test]
async fn empty_chain_random_terminal_selects_one_from_five() -> TestResult<()> {
    let harness = PipelineHarness::new(
        7,
        TerminalStrategy::Random,
        |_| Vec::new(),
        |_| Vec::new(),
        |_| None,
    )
    .await?;

    let response = harness.send_message("empty-chain").await?;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body_text());
    let events = harness.request_events().await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_trace(event, &[], TerminalStrategy::Random);
    assert!(
        event
            .upstream_id
            .is_some_and(|id| harness.upstreams.iter().any(|upstream| upstream.id == id)),
        "event chose outside upstream set: {event:?}"
    );
    assert!(
        event.internal_errors.is_empty(),
        "unexpected errors: {event:?}"
    );

    harness.write_evidence(
        "empty-chain",
        format!(
            "Empty chain Random selected {} from five upstreams",
            event.upstream_name.as_deref().unwrap_or("<missing>")
        ),
        &events,
    )?;
    Ok(())
}

#[tokio::test]
async fn allowed_upstreams_prefilter_zero_returns_503() -> TestResult<()> {
    let harness = PipelineHarness::new(
        8,
        TerminalStrategy::Random,
        |_| vec![Uuid::from_u128(0xffff_ffff_ffff_ffff_ffff_ffff_ffff_ff28)],
        |_| Vec::new(),
        |_| None,
    )
    .await?;

    let response = harness.send_message("allowed-zero").await?;
    assert_route_no_upstream_after_filter(&response);
    let events = harness.request_events().await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.status, StatusCode::SERVICE_UNAVAILABLE.as_u16());
    assert_eq!(
        event.error_code.as_deref(),
        Some("route_no_upstream_after_filter")
    );
    assert_trace(event, &[], TerminalStrategy::Random);
    assert_eq!(terminal_upstream_id(event), None);

    harness.write_evidence(
        "allowed-zero-503",
        format!(
            "allowed_upstreams pre-filter yielded zero candidates body={}",
            response.body_text()
        ),
        &events,
    )?;
    Ok(())
}

struct PipelineHarness {
    _dir: tempfile::TempDir,
    storage: Arc<RedbStorage>,
    upstreams: Vec<SeededUpstream>,
    lifecycle: Lifecycle,
}

impl PipelineHarness {
    async fn new<Allowed, RouterPlugins, ShapePlugin>(
        seed: u8,
        terminal: TerminalStrategy,
        allowed_upstreams: Allowed,
        router_plugins: RouterPlugins,
        shape_plugin: ShapePlugin,
    ) -> TestResult<Self>
    where
        Allowed: FnOnce(&[SeededUpstream]) -> Vec<Uuid>,
        RouterPlugins: FnOnce(&[SeededUpstream]) -> Vec<PluginFixture>,
        ShapePlugin: FnOnce(&[SeededUpstream]) -> Option<PluginFixture>,
    {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(RedbStorage::open(
            &dir.path().join("pipeline-e2e.redb"),
            [seed; 32],
        )?);
        let upstreams = spawn_seeded_upstreams(storage.as_ref()).await?;
        let created = PrincipalStore::create(
            storage.as_ref(),
            PrincipalCreate {
                name: PRINCIPAL_NAME.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: allowed_upstreams(&upstreams),
                default_limits: Vec::new(),
            },
            now_secs(),
        )
        .await?;
        PrincipalStore::update(
            storage.as_ref(),
            created.id,
            created.revision,
            PrincipalUpdate {
                router_terminal_strategy: Some(terminal),
                ..PrincipalUpdate::default()
            },
            now_secs(),
        )
        .await?
        .ok_or_else(|| io_err("principal disappeared during terminal update"))?;

        for (index, plugin) in router_plugins(&upstreams).into_iter().enumerate() {
            insert_plugin(
                storage.as_ref(),
                created.id,
                PluginSlot::Router,
                plugin,
                index as i64,
            )
            .await?;
        }
        if let Some(plugin) = shape_plugin(&upstreams) {
            insert_plugin(storage.as_ref(), created.id, PluginSlot::Shape, plugin, 0).await?;
        }

        let stores = stores(Arc::clone(&storage));
        let runtime = ExtismRuntime::new();
        let view = build_dynamic_view(
            &stores,
            &AnthropicOAuthConfig::default(),
            Arc::new(AeadService::from_master_key([seed; 32])),
            None,
            0,
            &runtime,
            dir.path(),
            Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
            1_800,
            &cc_lb_config::Config::default(),
        )
        .await?;
        let authn = Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: PRINCIPAL_NAME.to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            }),
            None,
        ));
        let storage_trait: Arc<dyn StorageTrait> = storage.clone();
        let lifecycle = Lifecycle::new_with_dynamic_view(
            authn,
            Arc::new(DynamicViewHolder::new(view)),
            LifecycleConfig::default(),
        )
        .with_terminal_rng_seed(TERMINAL_RNG_SEED)
        .with_request_event_storage(storage_trait)
        .with_static_limit_subject(
            LimitEngine::new(Arc::new(KeyConcurrencyManager::new())),
            PRINCIPAL_NAME.to_owned(),
            KEY_ID.to_owned(),
            active_record(),
        );

        Ok(Self {
            _dir: dir,
            storage,
            upstreams,
            lifecycle,
        })
    }

    async fn send_message(&self, request_id: &str) -> TestResult<CapturedResponse> {
        let response = self
            .lifecycle
            .handle(
                Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "sk-ant-test")
                    .header("x-request-id", request_id)
                    .header("anthropic-version", "2023-06-01")
                    .header("content-type", "application/json")
                    .body(Bytes::from_static(MESSAGE_BODY))?,
            )
            .await?;
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        Ok(CapturedResponse { status, body })
    }

    async fn request_events(&self) -> TestResult<Vec<RequestEvent>> {
        Ok(
            RequestEventStore::query_request_events(self.storage.as_ref(), 0, u64::MAX, 1_000)
                .await?,
        )
    }

    fn write_evidence(
        &self,
        scenario: &str,
        summary: String,
        events: &[RequestEvent],
    ) -> TestResult<()> {
        let dir = repo_root().join(".omo/evidence");
        fs::create_dir_all(&dir)?;
        fs::write(dir.join(format!("task-28-{scenario}.txt")), summary)?;
        fs::write(
            dir.join(format!("task-28-{scenario}.json")),
            serde_json::to_string_pretty(events)?,
        )?;
        Ok(())
    }
}

struct CapturedResponse {
    status: StatusCode,
    body: Bytes,
}

impl CapturedResponse {
    fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    fn body_json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
}

#[derive(Clone, Debug)]
struct SeededUpstream {
    id: Uuid,
    name: String,
    addr: SocketAddr,
    _server: Arc<JoinHandle<io::Result<()>>>,
}

async fn spawn_seeded_upstreams(storage: &RedbStorage) -> TestResult<Vec<SeededUpstream>> {
    let mut upstreams = Vec::with_capacity(5);
    for index in 0..5 {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = Arc::new(tokio::spawn(async move {
            axum::serve(listener, fake_anthropic_app(AppConfig::default())).await
        }));
        wait_for_listening(addr).await?;
        let record = UpstreamStore::create(
            storage,
            UpstreamCreate {
                name: format!("upstream-{}", index + 1),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: Some(Url::parse(&format!("http://{addr}"))?),
                api_key_ciphertext: Some(vec![1, 2, 3]),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
            },
        )
        .await?;
        upstreams.push(SeededUpstream {
            id: record.id,
            name: record.name,
            addr,
            _server: server,
        });
    }
    Ok(upstreams)
}

async fn wait_for_listening(addr: SocketAddr) -> TestResult<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => return Ok(()),
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => {
                return Err(io_err(format!(
                    "fake-anthropic {addr} did not bind: {error}"
                )));
            }
        }
    }
}

async fn insert_plugin(
    storage: &RedbStorage,
    principal_id: Uuid,
    slot: PluginSlot,
    fixture: PluginFixture,
    order: i64,
) -> TestResult<()> {
    let bytes = wat::parse_str(&fixture.wat)?;
    let sha256 = sha256(&bytes);
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256,
                size_bytes: bytes.len() as u64,
                bytes,
                parse_validated_at_unix_secs: now_secs(),
            },
            WasmRegistryEntryInput {
                name: fixture.name.clone(),
                original_filename: format!("{}.wasm", fixture.name),
                label: None,
                uploaded_at_unix_secs: now_secs(),
                uploaded_by_admin_id: Uuid::new_v4(),
            },
        )
        .await?;
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot,
            order,
            wasm_registry_id: entry.id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
            wire_version: fixture.wire_version,
        })
        .await?;
    Ok(())
}

fn stores(storage: Arc<RedbStorage>) -> Stores {
    Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: storage.clone(),
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage.clone(),
        audit: Some(storage),
        plugin_registry_repo: None,
    }
}

#[derive(Clone)]
struct PluginFixture {
    name: String,
    wat: String,
    wire_version: Option<u8>,
}

fn filter_plugin(name: &str, kept_upstream_ids: Vec<Uuid>, reason: &str) -> PluginFixture {
    let results = kept_upstream_ids
        .iter()
        .map(|upstream_id| {
            json!({
                "upstream_id": upstream_id.to_string(),
                "decision": "accept",
                "reason": reason,
            })
        })
        .collect::<Vec<_>>();
    let output = json!({ "_v": 1, "results": results }).to_string();
    PluginFixture {
        name: name.to_owned(),
        wat: output_wat(name, "filter", &output),
        wire_version: Some(3),
    }
}

fn drop_all_plugin(name: &str) -> PluginFixture {
    filter_plugin(name, Vec::new(), "drop all")
}

fn trap_filter_plugin(name: &str) -> PluginFixture {
    PluginFixture {
        name: name.to_owned(),
        wat: trap_wat(name, "filter"),
        wire_version: Some(3),
    }
}

fn invalid_unknown_plugin(name: &str) -> PluginFixture {
    filter_plugin(
        name,
        vec![Uuid::from_u128(0xffff_ffff_ffff_ffff_ffff_ffff_ffff_2828)],
        "unknown",
    )
}

fn shape_trap_plugin(name: &str) -> PluginFixture {
    PluginFixture {
        name: name.to_owned(),
        wat: trap_wat(name, "shape"),
        wire_version: Some(1),
    }
}

fn output_wat(name: &str, export: &str, output: &str) -> String {
    let bytes = output.as_bytes();
    format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {}
  (func $emit (result i32)
    (local $out i64)
    (local.set $out (call $out_bytes))
    (call $output_set (local.get $out) (i64.const {}))
    (i32.const 0))
  (export "{}" (func $emit))
  (export "marker-{}" (func $emit))
)
"#,
        bytes_helper("out_bytes", bytes),
        bytes.len(),
        export,
        name
    )
}

fn trap_wat(name: &str, export: &str) -> String {
    format!(
        r#"
(module
  (func $trap (result i32)
    unreachable
    (i32.const 0))
  (export "{}" (func $trap))
  (export "marker-{}" (func $trap))
)
"#,
        export, name
    )
}

fn bytes_helper(name: &str, bytes: &[u8]) -> String {
    let mut stores = String::new();
    for (index, byte) in bytes.iter().enumerate() {
        stores.push_str(&format!(
            "    (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
        ));
    }
    format!(
        r#"
  (func ${name} (result i64)
    (local $ptr i64)
    (local.set $ptr (call $alloc (i64.const {})))
{}    (local.get $ptr))
"#,
        bytes.len(),
        stores
    )
}

fn first_n_upstreams(upstreams: &[SeededUpstream], n: usize) -> Vec<SeededUpstream> {
    let mut sorted = upstreams.to_vec();
    sorted.sort_by_key(|upstream| upstream.id);
    sorted.truncate(n);
    sorted
}

fn first_n_ids(upstreams: &[SeededUpstream], n: usize) -> Vec<Uuid> {
    first_n_upstreams(upstreams, n)
        .into_iter()
        .map(|upstream| upstream.id)
        .collect()
}

fn survivor_names(upstreams: &[SeededUpstream]) -> Vec<String> {
    upstreams
        .iter()
        .map(|upstream| upstream.name.clone())
        .collect()
}

fn count_upstreams(events: &[RequestEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        let name = event
            .upstream_name
            .clone()
            .unwrap_or_else(|| "<missing>".to_owned());
        *counts.entry(name).or_insert(0) += 1;
    }
    counts
}

fn assert_trace(event: &RequestEvent, stages: &[&str], terminal: TerminalStrategy) {
    let trace = event
        .routing_trace
        .as_ref()
        .expect("routing trace recorded");
    let actual_stages = trace
        .stages
        .iter()
        .map(|stage| stage.stage_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(actual_stages, stages, "event={event:?}");
    assert_eq!(
        trace
            .terminal_decision
            .as_ref()
            .map(|decision| decision.strategy.clone()),
        Some(terminal),
        "event={event:?}"
    );
}

fn terminal_upstream_id(event: &RequestEvent) -> Option<Uuid> {
    event
        .routing_trace
        .as_ref()
        .and_then(|trace| trace.terminal_decision.as_ref())
        .and_then(|decision| decision.upstream_id)
}

fn assert_route_no_upstream_after_filter(response: &CapturedResponse) {
    assert_eq!(
        response.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        response.body_text()
    );
    let body = response.body_json();
    assert_eq!(
        body.pointer("/error/type"),
        Some(&json!("route_no_upstream_after_filter"))
    );
}

async fn get_json(addr: SocketAddr, path: &str) -> TestResult<Value> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let text = String::from_utf8_lossy(&bytes);
    let (_, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| io_err(format!("malformed response: {text}")))?;
    Ok(serde_json::from_str(body)?)
}

fn active_record() -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: KEY_ID.to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("cc-lb-server lives under crates/")
        .to_path_buf()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn io_err(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
