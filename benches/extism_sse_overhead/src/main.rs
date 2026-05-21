use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use bytes::Bytes;
use cc_lb_core::{SseBatchConfig, SseRelay};
use cc_lb_plugin_api::{
    DialectError, ObservabilityError, ObservabilityHook, ObserveEvent, PluginManifest,
    PluginRuntime, Principal, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect,
};
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tempfile::TempDir;
use url::Url;

const DEFAULT_DIRECT_EVENTS: usize = 2_048;
const DEFAULT_BATCH_SAMPLES: usize = 1_024;
const DEFAULT_BATCH_SIZE: usize = 32;
const DEFAULT_RELAY_EVENTS: usize = 1_000;
const DEFAULT_RELAY_SAMPLES: usize = 32;
const PASS_THRESHOLD_NS: u128 = 1_000_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse()?;
    let result = run_benchmark(&args).await?;
    let report = result.to_markdown();
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.output, report)?;
    fs::write(args.raw_output(), result.to_json())?;
    println!("report={}", args.output.display());
    println!("raw={}", args.raw_output().display());
    println!("decision={}", result.decision);
    println!("batched_32_p99_ns={}", result.batched_32.cumulative.p99_ns);
    Ok(())
}

async fn run_benchmark(args: &Args) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let direct_samples = measure_direct_per_event(args.direct_events)?;
    let batched_samples = measure_batched_32(args.batch_samples, args.batch_size)?;
    let relay_baseline = measure_relay(
        HookMode::Noop,
        SseBatchConfig {
            max_events: 1,
            max_age: Duration::from_secs(60),
        },
        args.relay_samples,
        args.relay_events,
    )
    .await?;
    let relay_per_event = measure_relay(
        HookMode::Extism { observe_batch: 1 },
        SseBatchConfig {
            max_events: 1,
            max_age: Duration::from_secs(60),
        },
        args.relay_samples,
        args.relay_events,
    )
    .await?;
    let relay_batched = measure_relay(
        HookMode::Extism { observe_batch: 1 },
        SseBatchConfig {
            max_events: args.batch_size,
            max_age: Duration::from_secs(60),
        },
        args.relay_samples,
        args.relay_events,
    )
    .await?;

    let direct = Stats::from_samples(&direct_samples);
    let batched_32 = Stats::from_samples(&batched_samples);
    let decision = if batched_32.p99_ns <= PASS_THRESHOLD_NS {
        "STAY_EXTISM"
    } else {
        "ESCALATE_WASMTIME"
    }
    .to_owned();

    Ok(BenchResult {
        direct_per_event: DirectResult {
            events: args.direct_events,
            stats: direct,
        },
        batched_32: BatchedResult {
            batches: args.batch_samples,
            batch_size: args.batch_size,
            events: args.batch_samples.saturating_mul(args.batch_size),
            cumulative: batched_32,
        },
        relay: RelayResult {
            samples: args.relay_samples,
            events_per_sample: args.relay_events,
            baseline_no_hook: Stats::from_samples(&relay_baseline),
            extism_per_event: Stats::from_samples(&relay_per_event),
            extism_batched_32: Stats::from_samples(&relay_batched),
        },
        threshold_ns: PASS_THRESHOLD_NS,
        decision,
    })
}

fn measure_direct_per_event(events: usize) -> Result<Vec<u128>, Box<dyn std::error::Error>> {
    let fixture = ObserveFixture::new("per-event", 1)?;
    let hook = fixture.hook()?;
    warm_hook(hook.as_ref(), 64)?;

    let mut samples = Vec::with_capacity(events);
    for index in 0..events {
        let started = Instant::now();
        hook.observe(chunk(index as u64, 1, 256))?;
        samples.push(started.elapsed().as_nanos());
    }
    Ok(samples)
}

fn measure_batched_32(
    batches: usize,
    batch_size: usize,
) -> Result<Vec<u128>, Box<dyn std::error::Error>> {
    let fixture = ObserveFixture::new("batched-32", batch_size)?;
    let hook = fixture.hook()?;
    warm_batches(hook.as_ref(), 8, batch_size)?;

    let mut samples = Vec::with_capacity(batches);
    let mut batch_index = 0_u64;
    for _ in 0..batches {
        let started = Instant::now();
        for _ in 0..batch_size {
            hook.observe(chunk(batch_index, 1, 256))?;
            batch_index = batch_index.saturating_add(1);
        }
        samples.push(started.elapsed().as_nanos());
    }
    Ok(samples)
}

async fn measure_relay(
    mode: HookMode,
    batch: SseBatchConfig,
    samples: usize,
    events: usize,
) -> Result<Vec<u128>, Box<dyn std::error::Error>> {
    let mut durations = Vec::with_capacity(samples);
    for _ in 0..samples {
        let _fixture;
        let hook: Arc<dyn ObservabilityHook> = match mode {
            HookMode::Noop => Arc::new(NoopHook),
            HookMode::Extism { observe_batch } => {
                _fixture = ObserveFixture::new("relay", observe_batch)?;
                _fixture.hook()?
            }
        };
        let relay = SseRelay {
            obs: hook,
            dialect: Arc::new(TestDialect),
            batch,
            quota: None,
            principal_id: "bench-principal".to_owned(),
            reservation: None,
            error_normalizer: None,
            upstream_kind: None,
        };
        let started = Instant::now();
        let response = relay.into_response_from_body(body_from_events(events));
        let body = collect_response_body(response).await?;
        if body.is_empty() {
            return Err("relay produced empty body".into());
        }
        durations.push(started.elapsed().as_nanos());
    }
    Ok(durations)
}

fn warm_hook(hook: &dyn ObservabilityHook, events: usize) -> Result<(), ObservabilityError> {
    for index in 0..events {
        hook.observe(chunk(index as u64, 1, 256))?;
    }
    Ok(())
}

fn warm_batches(
    hook: &dyn ObservabilityHook,
    batches: usize,
    batch_size: usize,
) -> Result<(), ObservabilityError> {
    for batch in 0..batches {
        for index in 0..batch_size {
            hook.observe(chunk((batch * batch_size + index) as u64, 1, 256))?;
        }
    }
    Ok(())
}

fn chunk(batch_index: u64, event_count: usize, total_bytes: usize) -> ObserveEvent {
    ObserveEvent::Chunk {
        batch_index,
        event_count,
        total_bytes,
    }
}

#[derive(Clone, Copy)]
enum HookMode {
    Noop,
    Extism { observe_batch: usize },
}

struct ObserveFixture {
    _dir: TempDir,
    runtime: cc_lb_runtime_extism::ExtismRuntime,
    manifest: PluginManifest,
}

impl ObserveFixture {
    fn new(name: &str, observe_batch: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let wasm = wat::parse_str(observe_module())?;
        let artifact = dir.path().join(format!("{name}.wasm"));
        fs::write(&artifact, wasm)?;
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "observe_batch_count".to_owned(),
            Value::from(observe_batch as u64),
        );
        metadata.insert("observe_flush_ms".to_owned(), Value::from(60_000_u64));
        Ok(Self {
            _dir: dir,
            runtime: cc_lb_runtime_extism::ExtismRuntime::new(),
            manifest: PluginManifest {
                name: format!("observe-{name}"),
                artifact: artifact.display().to_string(),
                config: json!({}),
                metadata,
            },
        })
    }

    fn hook(&self) -> Result<Arc<dyn ObservabilityHook>, Box<dyn std::error::Error>> {
        Ok(self.runtime.instantiate_observability(&self.manifest)?)
    }
}

fn observe_module() -> &'static str {
    r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (func $out (result i64)
    (local $ptr i64)
    (local.set $ptr (call $alloc (i64.const 14)))
    (call $store_u8 (local.get $ptr) (i32.const 123))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 1)) (i32.const 34))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 2)) (i32.const 95))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 3)) (i32.const 118))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 4)) (i32.const 101))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 5)) (i32.const 114))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 6)) (i32.const 115))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 7)) (i32.const 105))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 8)) (i32.const 111))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 9)) (i32.const 110))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 10)) (i32.const 34))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 11)) (i32.const 58))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 12)) (i32.const 49))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 13)) (i32.const 125))
    (local.get $ptr))
  (func (export "observe") (result i32)
    (call $output_set (call $out) (i64.const 14))
    (i32.const 0))
)
"#
}

struct NoopHook;

impl ObservabilityHook for NoopHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}

struct TestDialect;

impl UpstreamDialect for TestDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            Url::parse("http://upstream.test/v1/messages")
                .map_err(|source| DialectError::InvalidUrl { source })?,
            http::Method::POST,
            http::HeaderMap::new(),
            Bytes::new(),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

fn body_from_events(count: usize) -> Body {
    let stream = async_stream::stream! {
        for index in 0..count {
            let event = format!("event: content_block_delta\ndata: {{\"index\":{index},\"text\":\"token-{index}\"}}\n\n");
            yield Ok::<Bytes, Infallible>(Bytes::from(event));
        }
    };
    Body::from_stream(stream)
}

async fn collect_response_body(
    response: Response<Body>,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    Ok(response.into_body().collect().await?.to_bytes())
}

struct Args {
    output: PathBuf,
    direct_events: usize,
    batch_samples: usize,
    batch_size: usize,
    relay_events: usize,
    relay_samples: usize,
}

impl Args {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut args = std::env::args().skip(1);
        let mut parsed = Self {
            output: PathBuf::from(".omo/evidence/task-49-bench-report.md"),
            direct_events: DEFAULT_DIRECT_EVENTS,
            batch_samples: DEFAULT_BATCH_SAMPLES,
            batch_size: DEFAULT_BATCH_SIZE,
            relay_events: DEFAULT_RELAY_EVENTS,
            relay_samples: DEFAULT_RELAY_SAMPLES,
        };
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--output" => parsed.output = PathBuf::from(required_value(&flag, &mut args)?),
                "--direct-events" => {
                    parsed.direct_events = required_value(&flag, &mut args)?.parse()?
                }
                "--batch-samples" => {
                    parsed.batch_samples = required_value(&flag, &mut args)?.parse()?
                }
                "--batch-size" => parsed.batch_size = required_value(&flag, &mut args)?.parse()?,
                "--relay-events" => {
                    parsed.relay_events = required_value(&flag, &mut args)?.parse()?
                }
                "--relay-samples" => {
                    parsed.relay_samples = required_value(&flag, &mut args)?.parse()?
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}").into()),
            }
        }
        parsed.batch_size = parsed.batch_size.max(1);
        parsed.direct_events = parsed.direct_events.max(1_000);
        parsed.batch_samples = parsed.batch_samples.max(32);
        parsed.relay_events = parsed.relay_events.max(1_000);
        parsed.relay_samples = parsed.relay_samples.max(1);
        Ok(parsed)
    }

    fn raw_output(&self) -> PathBuf {
        self.output.with_extension("json")
    }
}

fn required_value(
    flag: &str,
    args: &mut impl Iterator<Item = String>,
) -> Result<String, Box<dyn std::error::Error>> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value").into())
}

fn print_help() {
    println!(
        "Usage: cargo run -p extism-sse-overhead --release -- --output .omo/evidence/task-49-bench-report.md [--direct-events N] [--batch-samples N] [--batch-size N] [--relay-events N] [--relay-samples N]"
    );
}

struct BenchResult {
    direct_per_event: DirectResult,
    batched_32: BatchedResult,
    relay: RelayResult,
    threshold_ns: u128,
    decision: String,
}

impl BenchResult {
    fn to_markdown(&self) -> String {
        let batched_per_event = self
            .batched_32
            .cumulative
            .divided_by(self.batched_32.batch_size);
        let relay_per_event_overhead = self
            .relay
            .extism_per_event
            .saturating_sub(&self.relay.baseline_no_hook);
        let relay_batched_overhead = self
            .relay
            .extism_batched_32
            .saturating_sub(&self.relay.baseline_no_hook);
        let escalation = if self.decision == "ESCALATE_WASMTIME" {
            "Batched 32-event p99 exceeded 1 ms. Escalation path: open a follow-up feasibility study for a Wasmtime+Component Model observability adapter, compare the same hook envelope and relay workloads, and keep the current Extism backend unchanged until that study lands."
        } else {
            "Batched 32-event p99 stayed within the 1 ms gate. If a future rerun exceeds the gate, escalate by opening a Wasmtime+Component Model adapter feasibility study only; do not implement a new backend as part of this benchmark task."
        };
        format!(
            r#"# Task 49 Extism SSE Observation Benchmark

Command: `cargo run -p extism-sse-overhead --release -- --output .omo/evidence/task-49-bench-report.md`

Criterion: `cargo bench -p extism-sse-overhead --no-run` compiles the Criterion entry for direct per-event and 32-event batched hook calls; this custom runner remains the evidence generator because it emits reproducible p50/p95/p99 tables and includes the relay-overhead gate in one markdown/json artifact.

DECISION: {decision}

Threshold: batched 32-event cumulative p99 <= {threshold_us:.3} us (1 ms).

## Direct ObservabilityHook Calls

| mode | samples | observed SSE events | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| per-event Extism flush | {direct_samples} | {direct_events} | {direct_p50} | {direct_p95} | {direct_p99} |
| batched Extism flush, cumulative 32 observe calls | {batch_samples} | {batch_events} | {batch_p50} | {batch_p95} | {batch_p99} |
| batched Extism flush, amortized per event | {batch_samples} | {batch_events} | {batch_amort_p50} | {batch_amort_p95} | {batch_amort_p99} |

## Relay End-to-End

Each relay sample streams {relay_events} deterministic SSE events through `SseRelay`; the baseline uses a no-op hook.

| mode | samples | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| no-hook baseline | {relay_samples} | {relay_base_p50} | {relay_base_p95} | {relay_base_p99} |
| Extism per-SSE-event observation | {relay_samples} | {relay_per_p50} | {relay_per_p95} | {relay_per_p99} |
| Extism T22 32-event chunks | {relay_samples} | {relay_batch_p50} | {relay_batch_p95} | {relay_batch_p99} |
| per-event overhead vs baseline | {relay_samples} | {relay_per_over_p50} | {relay_per_over_p95} | {relay_per_over_p99} |
| batched overhead vs baseline | {relay_samples} | {relay_batch_over_p50} | {relay_batch_over_p95} | {relay_batch_over_p99} |

## Caveats

- This is a local deterministic microbenchmark, not a portable hardware-independent latency guarantee; compare trends on the same machine/profile.
- The direct benchmark uses a synthetic Extism `observe` WAT plugin that returns the production JSON envelope `_version: 1` and performs no network or storage calls.
- Relay timings include local SSE parsing, byte forwarding, hook invocation, Tokio scheduling, and body collection; the overhead rows subtract percentile summaries and are approximate.
- {escalation}
"#,
            decision = self.decision,
            threshold_us = ns_to_us(self.threshold_ns),
            direct_samples = self.direct_per_event.stats.samples,
            direct_events = self.direct_per_event.events,
            direct_p50 = self.direct_per_event.stats.p50(),
            direct_p95 = self.direct_per_event.stats.p95(),
            direct_p99 = self.direct_per_event.stats.p99(),
            batch_samples = self.batched_32.cumulative.samples,
            batch_events = self.batched_32.events,
            batch_p50 = self.batched_32.cumulative.p50(),
            batch_p95 = self.batched_32.cumulative.p95(),
            batch_p99 = self.batched_32.cumulative.p99(),
            batch_amort_p50 = batched_per_event.p50(),
            batch_amort_p95 = batched_per_event.p95(),
            batch_amort_p99 = batched_per_event.p99(),
            relay_events = self.relay.events_per_sample,
            relay_samples = self.relay.samples,
            relay_base_p50 = self.relay.baseline_no_hook.p50(),
            relay_base_p95 = self.relay.baseline_no_hook.p95(),
            relay_base_p99 = self.relay.baseline_no_hook.p99(),
            relay_per_p50 = self.relay.extism_per_event.p50(),
            relay_per_p95 = self.relay.extism_per_event.p95(),
            relay_per_p99 = self.relay.extism_per_event.p99(),
            relay_batch_p50 = self.relay.extism_batched_32.p50(),
            relay_batch_p95 = self.relay.extism_batched_32.p95(),
            relay_batch_p99 = self.relay.extism_batched_32.p99(),
            relay_per_over_p50 = relay_per_event_overhead.p50(),
            relay_per_over_p95 = relay_per_event_overhead.p95(),
            relay_per_over_p99 = relay_per_event_overhead.p99(),
            relay_batch_over_p50 = relay_batched_overhead.p50(),
            relay_batch_over_p95 = relay_batched_overhead.p95(),
            relay_batch_over_p99 = relay_batched_overhead.p99(),
            escalation = escalation,
        )
    }

    fn to_json(&self) -> String {
        format!(
            r#"{{
  "decision": "{decision}",
  "threshold_ns": {threshold},
  "direct_per_event": {direct},
  "batched_32": {{"batches": {batches}, "batch_size": {batch_size}, "events": {batch_events}, "cumulative": {batch_stats}}},
  "relay": {{"samples": {relay_samples}, "events_per_sample": {relay_events}, "baseline_no_hook": {relay_base}, "extism_per_event": {relay_per}, "extism_batched_32": {relay_batch}}}
}}
"#,
            decision = self.decision,
            threshold = self.threshold_ns,
            direct = self.direct_per_event.to_json(),
            batches = self.batched_32.batches,
            batch_size = self.batched_32.batch_size,
            batch_events = self.batched_32.events,
            batch_stats = self.batched_32.cumulative.to_json(),
            relay_samples = self.relay.samples,
            relay_events = self.relay.events_per_sample,
            relay_base = self.relay.baseline_no_hook.to_json(),
            relay_per = self.relay.extism_per_event.to_json(),
            relay_batch = self.relay.extism_batched_32.to_json(),
        )
    }
}

struct DirectResult {
    events: usize,
    stats: Stats,
}

impl DirectResult {
    fn to_json(&self) -> String {
        format!(
            r#"{{"events": {}, "stats": {}}}"#,
            self.events,
            self.stats.to_json()
        )
    }
}

struct BatchedResult {
    batches: usize,
    batch_size: usize,
    events: usize,
    cumulative: Stats,
}

struct RelayResult {
    samples: usize,
    events_per_sample: usize,
    baseline_no_hook: Stats,
    extism_per_event: Stats,
    extism_batched_32: Stats,
}

#[derive(Clone)]
struct Stats {
    samples: usize,
    p50_ns: u128,
    p95_ns: u128,
    p99_ns: u128,
}

impl Stats {
    fn from_samples(samples: &[u128]) -> Self {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Self {
            samples: sorted.len(),
            p50_ns: percentile(&sorted, 50),
            p95_ns: percentile(&sorted, 95),
            p99_ns: percentile(&sorted, 99),
        }
    }

    fn divided_by(&self, denominator: usize) -> Self {
        let denominator = denominator.max(1) as u128;
        Self {
            samples: self.samples,
            p50_ns: self.p50_ns / denominator,
            p95_ns: self.p95_ns / denominator,
            p99_ns: self.p99_ns / denominator,
        }
    }

    fn saturating_sub(&self, baseline: &Self) -> Self {
        Self {
            samples: self.samples,
            p50_ns: self.p50_ns.saturating_sub(baseline.p50_ns),
            p95_ns: self.p95_ns.saturating_sub(baseline.p95_ns),
            p99_ns: self.p99_ns.saturating_sub(baseline.p99_ns),
        }
    }

    fn p50(&self) -> String {
        format_duration(self.p50_ns)
    }

    fn p95(&self) -> String {
        format_duration(self.p95_ns)
    }

    fn p99(&self) -> String {
        format_duration(self.p99_ns)
    }

    fn to_json(&self) -> String {
        format!(
            r#"{{"samples": {}, "p50_ns": {}, "p95_ns": {}, "p99_ns": {}}}"#,
            self.samples, self.p50_ns, self.p95_ns, self.p99_ns
        )
    }
}

fn percentile(sorted: &[u128], percentile: u128) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let len = sorted.len() as u128;
    let rank = (percentile * len).div_ceil(100).saturating_sub(1);
    sorted[rank.min(len - 1) as usize]
}

fn format_duration(ns: u128) -> String {
    if ns >= 1_000_000 {
        format!("{:.3} ms", ns_to_ms(ns))
    } else {
        format!("{:.3} us", ns_to_us(ns))
    }
}

fn ns_to_us(ns: u128) -> f64 {
    ns as f64 / 1_000.0
}

fn ns_to_ms(ns: u128) -> f64 {
    ns as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::{percentile, Stats};

    #[test]
    fn percentile_uses_nearest_rank() {
        let samples = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile(&samples, 50), 5);
        assert_eq!(percentile(&samples, 95), 10);
        assert_eq!(percentile(&samples, 99), 10);
    }

    #[test]
    fn stats_subtraction_saturates() {
        let stats = Stats::from_samples(&[10, 20, 30]);
        let baseline = Stats::from_samples(&[20, 30, 40]);
        let diff = stats.saturating_sub(&baseline);
        assert_eq!(diff.p50_ns, 0);
        assert_eq!(diff.p95_ns, 0);
        assert_eq!(diff.p99_ns, 0);
    }
}
