#![allow(deprecated)]

//! E2E hit-rate comparison: cache-aware vs round-robin router plugins.
//!
//! Run with:
//!   cargo test -p cc-lb-server --release --test e2e_cache_aware_vs_round_robin -- --ignored --nocapture
//!
//! Architecture
//! ------------
//! The test exercises the cache-aware plugin via `cc-lb-runtime-extism` and the
//! production `PromptCacheObservationCache` directly, rather than through
//! `cc_lb_core::Lifecycle::handle`. It now parses the request through the real
//! `cc_lb_core::parse_request_cache_breakpoints` helper, so the `prefix_hash`
//! and `prefix_token_count` driving the cache-aware plugin are produced by
//! production code rather than synthesised.
//!
//! The test exercises:
//!   - production cache-breakpoint extraction + tokenization
//!     (`parse_request_cache_breakpoints`),
//!   - the production prompt cache (`PromptCacheObservationCache`),
//!   - the production extism runtime (`ExtismRuntime`), which compiles and
//!     loads the released wasm artefacts for both router plugins,
//!   - the production wire dispatch (v1 for round-robin, v2 for cache-aware),
//!   - real HTTP traffic to three `fake-anthropic` instances with the T27
//!     `__inject_cache_stats` endpoint controlling which upstream reports a
//!     cache read on its `/v1/messages` responses.
//!
//! For the cache-aware phase, the test upserts a warm observation into the
//! production cache after each response that comes back with
//! `cache_read_input_tokens > 0`. This mirrors what the production response
//! decoder does for the same scenario.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_core::clock::{ClockHandle, TestClock, unix_secs};
use cc_lb_core::parse_request_cache_breakpoints;
use cc_lb_plugin_api::types::{CacheBreakpoint, CacheScore, TtlClass, WarmCacheEntry};
use cc_lb_plugin_api::{
    PluginManifest, PluginRuntime, Principal, PrincipalKind, RequestContext, UpstreamCandidate,
    UpstreamKind,
};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::prompt_cache_observation_cache::PromptCacheObservationCache;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http::{HeaderMap, Method};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const UPSTREAM_NAMES: &[&str] = &["upstream-a", "upstream-b", "upstream-c"];
const REQUEST_COUNT: usize = 100;
const NOW_UNIX_SECS: u64 = 1_700_000_000;
const ONE_HOUR_SECS: u64 = 3_600;
const TEST_MODEL: &str = "claude-sonnet-4-5-20250929";
const INJECTED_CACHE_READ_TOKENS: u64 = 4_096;
const INJECTED_CACHE_CREATION_TOKENS: u64 = 4_096;
const RR_HIT_RATE_MAX: f64 = 0.40;
const CACHE_AWARE_HIT_RATE_MIN: f64 = 0.80;

#[tokio::test]
#[ignore = "release-only e2e: builds wasm32-wasip1 plugins and spins up three fake-anthropic upstreams"]
async fn e2e_cache_aware_beats_round_robin() -> TestResult<()> {
    let Some(round_robin_wasm) = build_plugin_wasm(
        "plugins/router/round-robin/Cargo.toml",
        "cc_lb_router_round_robin",
    )?
    else {
        eprintln!("skipped: wasm32-wasip1 toolchain missing");
        return Ok(());
    };
    let Some(cache_aware_wasm) = build_plugin_wasm(
        "plugins/router/cache-aware/Cargo.toml",
        "cache_aware_router",
    )?
    else {
        eprintln!("skipped: wasm32-wasip1 toolchain missing");
        return Ok(());
    };

    let upstreams = spawn_fake_anthropic_upstreams(UPSTREAM_NAMES.len()).await?;
    let body_value = build_cacheable_request_body();
    let body_bytes = serde_json::to_vec(&body_value)?;
    let signature = canonical_request_signature(&body_value);

    // Inject cache stats so only upstream A returns cache reads/creates for this signature.
    inject_cache_stats(
        upstreams[0].addr,
        &signature,
        INJECTED_CACHE_CREATION_TOKENS,
        INJECTED_CACHE_READ_TOKENS,
    )
    .await?;

    let rr_hits = run_phase(
        "round-robin",
        round_robin_wasm,
        /*wire_version=*/ Some(1),
        &upstreams,
        &body_bytes,
    )
    .await?;
    let rr_hit_rate = rr_hits as f64 / REQUEST_COUNT as f64;

    let cache_aware_hits = run_phase(
        "cache-aware",
        cache_aware_wasm,
        /*wire_version=*/ Some(2),
        &upstreams,
        &body_bytes,
    )
    .await?;
    let cache_aware_hit_rate = cache_aware_hits as f64 / REQUEST_COUNT as f64;

    println!("round_robin={rr_hit_rate:.2} cache_aware={cache_aware_hit_rate:.2}");

    assert!(
        rr_hit_rate <= RR_HIT_RATE_MAX,
        "RR baseline should be ~33%, got {rr_hit_rate}"
    );
    assert!(
        cache_aware_hit_rate >= CACHE_AWARE_HIT_RATE_MIN,
        "cache-aware should be ≥80%, got {cache_aware_hit_rate}"
    );

    Ok(())
}

async fn run_phase(
    label: &str,
    wasm_path: PathBuf,
    wire_version: Option<u8>,
    upstreams: &[Upstream],
    body_bytes: &[u8],
) -> TestResult<usize> {
    // TestClock keeps observation timestamps deterministic; the warm entry uses
    // a 1h expiry from `NOW_UNIX_SECS` so it can't expire while the 100
    // request loop runs.
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS));
    let cache = Arc::new(PromptCacheObservationCache::new_with_debounce(
        clock.clone(),
        /*grace_margin_secs=*/ 30,
        /*warm_set_cap=*/ 32,
        /*refresh_debounce_secs=*/ 60,
    ));

    let manifest = PluginManifest {
        name: format!("{label}-e2e"),
        artifact: wasm_path.to_string_lossy().into_owned(),
        wire_version,
        config: json!({}),
        metadata: BTreeMap::new(),
    };
    let runtime = ExtismRuntime::with_config(
        cc_lb_runtime_extism::ExtismRuntimeConfig::default(),
        std::sync::Arc::new(cc_lb_core::SystemClock),
    );
    let router = runtime
        .instantiate_router(&manifest)
        .map_err(|err| io_err(format!("instantiate {label} router: {err}")))?;

    let request_breakpoints =
        parse_request_cache_breakpoints(&HeaderMap::new(), &Bytes::copy_from_slice(body_bytes));
    assert!(
        !request_breakpoints.is_empty(),
        "production request_cache_metadata returned no breakpoints for cacheable body",
    );
    assert!(
        request_breakpoints
            .iter()
            .all(|bp| bp.prefix_token_count >= 1024),
        "production tokenizer must yield prefix_token_count above the Sonnet 4.5 cache threshold (1024) so observation writes are not gated out: {request_breakpoints:?}",
    );
    let primary_prefix_hash = request_breakpoints[0].prefix_hash.clone();
    let primary_ttl = request_breakpoints[0].requested_ttl;
    let request_breakpoint_hashes: Vec<(String, TtlClass)> = request_breakpoints
        .iter()
        .map(|bp| (bp.prefix_hash.clone(), bp.requested_ttl))
        .collect();

    let principal = test_principal();
    let ctx = test_ctx(body_bytes);

    let mut hits = 0_usize;
    for _ in 0..REQUEST_COUNT {
        let now = unix_secs(clock.now());
        let candidates: Vec<UpstreamCandidate> = upstreams
            .iter()
            .map(|upstream| {
                let warm = cache.snapshot_for_upstream(
                    upstream.id,
                    TEST_MODEL,
                    &request_breakpoint_hashes,
                    now,
                );
                UpstreamCandidate {
                    upstream_id: upstream.id,
                    name: upstream.name.clone(),
                    kind: UpstreamKind::AnthropicApiKey,
                    observed_rate_limits: Vec::new(),
                    subscription_quotas: Vec::new(),
                    observed_at_unix_secs: now,
                    cache_score: build_cache_score(&request_breakpoints, &warm),
                    base_url: None,
                }
            })
            .collect();

        let decision = router
            .route(&ctx, &principal, &candidates)
            .map_err(|err| io_err(format!("{label} route() failed: {err}")))?;
        let picked_upstream_id = decision
            .upstream_id
            .ok_or_else(|| io_err(format!("{label} plugin returned no upstream_id")))?;
        let picked = upstreams
            .iter()
            .find(|upstream| upstream.id == picked_upstream_id)
            .ok_or_else(|| io_err(format!("{label} plugin returned unknown upstream")))?;

        let response = post_json(picked.addr, "/v1/messages", body_bytes).await?;
        let cache_read = response
            .pointer("/usage/cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);

        if cache_read > 0 {
            hits += 1;
            cache.upsert_observation(
                picked.id,
                TEST_MODEL.to_owned(),
                primary_prefix_hash.clone(),
                primary_ttl,
                now + ONE_HOUR_SECS,
                now,
            );
        }
    }

    Ok(hits)
}

/// Mirrors `lifecycle::build_cache_score` without depending on its private
/// visibility. Both call sites compute the same shape: pick the longest warm
/// breakpoint, project its token count as the predicted read amount, and
/// leave creation token predictions empty for the routing phase.
fn build_cache_score(
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &[WarmCacheEntry],
) -> Option<CacheScore> {
    if warm_entries.is_empty() {
        return None;
    }
    let warm_for = |bp: &CacheBreakpoint| {
        warm_entries
            .iter()
            .filter(|entry| entry.prefix_hash == bp.prefix_hash)
            .max_by_key(|entry| entry.expires_at_unix_secs)
    };
    let longest_match = request_breakpoints
        .iter()
        .filter_map(|bp| warm_for(bp).map(|entry| (bp, entry)))
        .max_by_key(|(bp, _)| bp.prefix_token_count);
    Some(CacheScore {
        predicted_cache_read_tokens: longest_match
            .map(|(bp, _)| u32::try_from(bp.prefix_token_count).unwrap_or(u32::MAX))
            .unwrap_or(0),
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: longest_match.map(|(_, entry)| entry.expires_at_unix_secs),
        matched_breakpoint_index: longest_match.map(|(bp, _)| bp.block_index),
        confidence: if longest_match.is_some() { 1.0 } else { 0.0 },
        ambiguity_reason: None,
    })
}

fn test_principal() -> Principal {
    Principal {
        id: "e2e-principal".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    }
}

fn test_ctx(body_bytes: &[u8]) -> RequestContext {
    RequestContext {
        request_id: "e2e-request".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::copy_from_slice(body_bytes),
        cache_breakpoints: Vec::new(),
        canonical_model_id: TEST_MODEL.to_owned(),
    }
}

fn build_cacheable_request_body() -> Value {
    let large_text: String =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(300);
    json!({
        "model": TEST_MODEL,
        "system": [{
            "type": "text",
            "text": large_text,
            "cache_control": { "type": "ephemeral", "ttl": "1h" }
        }],
        "messages": [{
            "role": "user",
            "content": "hi"
        }],
        "max_tokens": 16
    })
}

fn canonical_request_signature(body: &Value) -> String {
    let canonical = serde_json::to_string(body).expect("body serializes");
    let digest = Sha256::digest(canonical.as_bytes());
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest.iter() {
        write!(out, "{byte:02x}").expect("hex write");
    }
    out
}

struct Upstream {
    id: Uuid,
    name: String,
    addr: SocketAddr,
    _server: JoinHandle<io::Result<()>>,
}

async fn spawn_fake_anthropic_upstreams(count: usize) -> TestResult<Vec<Upstream>> {
    let mut upstreams = Vec::with_capacity(count);
    for (index, name) in UPSTREAM_NAMES.iter().take(count).enumerate() {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(listener, fake_anthropic_app(AppConfig::default())).await
        });
        wait_for_listening(addr).await?;
        upstreams.push(Upstream {
            id: Uuid::from_u128(0x0e2e_0000_0000_0000_0000_0000_0000_0001u128 + index as u128),
            name: (*name).to_owned(),
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
            Err(err) => {
                return Err(io_err(format!("fake-anthropic {addr} did not bind: {err}")).into());
            }
        }
    }
}

async fn inject_cache_stats(
    addr: SocketAddr,
    signature: &str,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
) -> TestResult<()> {
    let body = json!({
        "match_request_signature": signature,
        "usage": {
            "cache_creation_input_tokens": cache_creation_input_tokens,
            "cache_read_input_tokens": cache_read_input_tokens
        }
    })
    .to_string();
    let response = post_json(addr, "/__inject_cache_stats", body.as_bytes()).await?;
    if response.get("ok") != Some(&Value::Bool(true)) {
        return Err(io_err(format!("__inject_cache_stats failed: {response}")).into());
    }
    Ok(())
}

async fn post_json(addr: SocketAddr, path: &str, body: &[u8]) -> TestResult<Value> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut request = Vec::with_capacity(256 + body.len());
    request.extend_from_slice(
        format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nx-api-key: sk-ant-test\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\ncontent-length: {len}\r\nConnection: close\r\n\r\n",
            len = body.len()
        )
        .as_bytes(),
    );
    request.extend_from_slice(body);
    stream.write_all(&request).await?;
    stream.flush().await?;

    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    let (_, response_body) = text.split_once("\r\n\r\n").ok_or_else(|| {
        io_err(format!(
            "malformed HTTP response from {addr}{path} ({} bytes): {text:?}",
            text.len()
        ))
    })?;
    serde_json::from_str(response_body).map_err(|err| {
        io_err(format!(
            "decode response body from {addr}{path}: {err}: {response_body}"
        ))
        .into()
    })
}

fn build_plugin_wasm(manifest_path: &str, artifact_name: &str) -> TestResult<Option<PathBuf>> {
    let root = repo_root();
    let status = Command::new("cargo")
        .current_dir(&root)
        .args([
            "build",
            "--manifest-path",
            manifest_path,
            "--target",
            "wasm32-wasip1",
            "--release",
        ])
        .status()?;
    if !status.success() {
        if !wasm32_wasip1_target_installed() {
            return Ok(None);
        }
        return Err(io_err(format!("{manifest_path} wasm build failed: {status}")).into());
    }
    for candidate in wasm_candidates(&root, manifest_path, artifact_name) {
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }
    Err(io_err(format!(
        "{manifest_path} wasm artifact {artifact_name}.wasm was not produced"
    ))
    .into())
}

fn wasm_candidates(root: &Path, manifest_path: &str, artifact_name: &str) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
        candidates.push(
            PathBuf::from(target_dir)
                .join("wasm32-wasip1")
                .join("release")
                .join(format!("{artifact_name}.wasm")),
        );
    }
    candidates.push(
        root.join("target")
            .join("wasm32-wasip1")
            .join("release")
            .join(format!("{artifact_name}.wasm")),
    );
    if let Some(plugin_dir) = Path::new(manifest_path).parent() {
        candidates.push(
            root.join(plugin_dir)
                .join("target")
                .join("wasm32-wasip1")
                .join("release")
                .join(format!("{artifact_name}.wasm")),
        );
    }
    candidates
}

fn wasm32_wasip1_target_installed() -> bool {
    Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line.trim() == "wasm32-wasip1")
        })
        .unwrap_or(false)
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("cc-lb-server lives under crates/")
        .to_path_buf()
}

fn io_err(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
