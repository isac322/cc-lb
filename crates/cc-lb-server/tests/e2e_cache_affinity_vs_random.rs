use std::error::Error;
use std::fmt::Write as _;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_core::builtin_filters::cache_affinity::CacheAffinityFilter;
use cc_lb_core::clock::{ClockHandle, TestClock};
use cc_lb_core::parse_request_cache_breakpoints;
use cc_lb_plugin_api::types::{CacheBreakpoint, CacheScore, TtlClass, WarmCacheEntry};
use cc_lb_plugin_api::{
    FilterPlugin, Principal, PrincipalKind, RequestContext, TerminalStrategy, UpstreamCandidate,
    UpstreamKind,
};
use cc_lb_server::prompt_cache_observation_cache::PromptCacheObservationCache;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http::{HeaderMap, Method};
use rand::{RngExt, SeedableRng, rngs::StdRng};
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
const NO_FILTER_HIT_RATE_MAX: f64 = 0.40;
const CACHE_AFFINITY_HIT_RATE_MIN: f64 = 0.80;
const TERMINAL_RNG_SEED: [u8; 32] = [0x5a; 32];

#[tokio::test]
async fn e2e_cache_affinity_beats_no_filter_random_terminal() -> TestResult<()> {
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

    let no_filter_hits = run_phase(
        "no-filter-random",
        Vec::new(),
        TerminalStrategy::Random,
        &upstreams,
        &body_bytes,
    )
    .await?;
    let no_filter_hit_rate = no_filter_hits as f64 / REQUEST_COUNT as f64;

    let cache_affinity_filter: Arc<dyn FilterPlugin> = Arc::new(CacheAffinityFilter::new());
    let cache_affinity_hits = run_phase(
        "cache-affinity-random",
        vec![cache_affinity_filter],
        TerminalStrategy::Random,
        &upstreams,
        &body_bytes,
    )
    .await?;
    let cache_affinity_hit_rate = cache_affinity_hits as f64 / REQUEST_COUNT as f64;

    println!(
        "no_filter_random={no_filter_hit_rate:.2} cache_affinity_random={cache_affinity_hit_rate:.2}"
    );

    assert!(
        no_filter_hit_rate <= NO_FILTER_HIT_RATE_MAX,
        "no-filter Random baseline should be ~33%, got {no_filter_hit_rate}"
    );
    assert!(
        cache_affinity_hit_rate >= CACHE_AFFINITY_HIT_RATE_MIN,
        "cache-affinity filter + Random terminal should be >=80%, got {cache_affinity_hit_rate}"
    );

    Ok(())
}

async fn run_phase(
    label: &str,
    user_filters: Vec<Arc<dyn FilterPlugin>>,
    terminal: TerminalStrategy,
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
    let mut terminal_rng = StdRng::from_seed(TERMINAL_RNG_SEED);

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
        let now = clock.now_unix_secs();
        let mut candidates: Vec<UpstreamCandidate> = upstreams
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

        for filter in &user_filters {
            let output = filter
                .filter(&ctx, &principal, &candidates)
                .map_err(|err| {
                    io_err(format!(
                        "{label} filter {} failed: {err}",
                        filter.plugin_name()
                    ))
                })?;
            candidates
                .retain(|candidate| output.kept_upstream_ids.contains(&candidate.upstream_id));
        }

        let picked_upstream_id =
            select_terminal_upstream(&mut terminal_rng, &terminal, &candidates)
                .ok_or_else(|| io_err(format!("{label} terminal had no candidates")))?;
        let picked = upstreams
            .iter()
            .find(|upstream| upstream.id == picked_upstream_id)
            .ok_or_else(|| io_err(format!("{label} terminal returned unknown upstream")))?;

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

fn select_terminal_upstream(
    rng: &mut StdRng,
    terminal: &TerminalStrategy,
    candidates: &[UpstreamCandidate],
) -> Option<Uuid> {
    match terminal {
        TerminalStrategy::Random if !candidates.is_empty() => {
            let index = rng.random_range(..candidates.len());
            Some(candidates[index].upstream_id)
        }
        TerminalStrategy::FirstPick | TerminalStrategy::Random => {
            candidates.first().map(|candidate| candidate.upstream_id)
        }
    }
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

fn io_err(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
