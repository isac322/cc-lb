#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use serde_json::Value;
use tokio::sync::Semaphore;

#[cfg(test)]
use super::analyze_v3_prompt_cache;
use super::{
    CacheBlockRef, PrefixBlockSerializer, SerializationScratch, V3PromptCacheAnalysis,
    V3StructuralBreakpoint, analyze_v3_prompt_cache_structure,
    analyze_v3_prompt_cache_with_tokenize_duration,
    cacheable_breakpoint_prefix_keys_with_tokenize_duration, finish_structural_analysis,
    unanalyzable_prompt_cache,
};

const TOKEN_PREFIX_KEY_DOMAIN: &[u8] = b"cc-lb-token-prefix-v1\0";
const DEFAULT_TOKEN_COUNT_CACHE_CAPACITY: usize = 8_192;

#[cfg(test)]
thread_local! {
    static FORCE_SERIALIZATION_ERROR: Cell<bool> = const { Cell::new(false) };
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct TokenPrefixKey([u8; 32]);

#[derive(Debug, Default)]
struct Flight {
    result: Mutex<Option<Option<u64>>>,
    ready: Condvar,
}

impl Flight {
    fn complete(&self, value: u64) {
        *self.result.lock() = Some(Some(value));
        self.ready.notify_all();
    }

    fn fail(&self) {
        *self.result.lock() = Some(None);
        self.ready.notify_all();
    }

    fn wait(&self) -> Option<u64> {
        let mut result = self.result.lock();
        while result.is_none() {
            self.ready.wait(&mut result);
        }
        result.unwrap_or(None)
    }
}

struct TokenCountCacheState {
    values: HashMap<TokenPrefixKey, u64>,
    insertion_order: VecDeque<TokenPrefixKey>,
    flights: HashMap<TokenPrefixKey, Arc<Flight>>,
}

impl TokenCountCacheState {
    fn new(capacity: usize) -> Self {
        Self {
            values: HashMap::with_capacity(capacity),
            insertion_order: VecDeque::with_capacity(capacity),
            flights: HashMap::new(),
        }
    }
}

struct PromptTokenCountCache {
    capacity: usize,
    state: Mutex<TokenCountCacheState>,
}

enum CacheClaim {
    Hit(u64),
    Leader(Arc<Flight>),
    Follower(Arc<Flight>),
}

impl PromptTokenCountCache {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            state: Mutex::new(TokenCountCacheState::new(capacity)),
        }
    }

    fn claim(&self, key: TokenPrefixKey) -> CacheClaim {
        let mut state = self.state.lock();
        if let Some(value) = state.values.get(&key).copied() {
            return CacheClaim::Hit(value);
        }
        if let Some(flight) = state.flights.get(&key) {
            return CacheClaim::Follower(Arc::clone(flight));
        }
        let flight = Arc::new(Flight::default());
        state.flights.insert(key, Arc::clone(&flight));
        CacheClaim::Leader(flight)
    }

    fn complete(&self, key: TokenPrefixKey, flight: &Arc<Flight>, value: u64) {
        self.insert_value(key, value);
        {
            let mut state = self.state.lock();
            if state
                .flights
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, flight))
            {
                state.flights.remove(&key);
            }
        }
        flight.complete(value);
    }

    fn fail(&self, key: TokenPrefixKey, flight: &Arc<Flight>) {
        let removed = {
            let mut state = self.state.lock();
            if state
                .flights
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, flight))
            {
                state.flights.remove(&key);
                true
            } else {
                false
            }
        };
        if removed {
            flight.fail();
        }
    }

    fn insert_value(&self, key: TokenPrefixKey, value: u64) {
        let mut state = self.state.lock();
        if state.values.contains_key(&key) {
            return;
        }
        while state.values.len() >= self.capacity {
            let Some(oldest) = state.insertion_order.pop_front() else {
                break;
            };
            state.values.remove(&oldest);
        }
        state.values.insert(key, value);
        state.insertion_order.push_back(key);
    }
}

struct LeaderFlightsGuard<'a> {
    cache: &'a PromptTokenCountCache,
    flights: Vec<(TokenPrefixKey, Arc<Flight>)>,
    completed: bool,
}

impl<'a> LeaderFlightsGuard<'a> {
    fn new(cache: &'a PromptTokenCountCache, flights: Vec<(TokenPrefixKey, Arc<Flight>)>) -> Self {
        Self {
            cache,
            flights,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for LeaderFlightsGuard<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        for (key, flight) in &self.flights {
            self.cache.fail(*key, flight);
        }
    }
}

struct ExactPrefixBatch {
    open_prefix_bytes: Vec<u8>,
    breakpoint_offsets: Vec<usize>,
    suffix_bytes: Vec<u8>,
    token_prefix_keys: Vec<TokenPrefixKey>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct PromptCacheAnalysisTimings {
    pub(crate) cache_structure_ms: Option<f64>,
    pub(crate) cache_token_key_ms: Option<f64>,
    pub(crate) cache_count_lookup_ms: Option<f64>,
    pub(crate) cache_tokenizer_queue_ms: Option<f64>,
    pub(crate) cache_serialize_ms: Option<f64>,
    pub(crate) cache_tokenize_ms: Option<f64>,
}

pub(crate) struct PromptCacheAnalysisOutput {
    pub(crate) analysis: V3PromptCacheAnalysis,
    pub(crate) cacheable_breakpoint_prefix_keys: Vec<String>,
    pub(crate) timings: PromptCacheAnalysisTimings,
}

#[derive(Clone, Copy, Debug, Default)]
struct PromptCacheAnalysisDurations {
    structure: Option<Duration>,
    token_key: Option<Duration>,
    count_lookup: Option<Duration>,
    serialize: Option<Duration>,
    tokenize: Option<Duration>,
}

impl PromptCacheAnalysisDurations {
    fn into_timings(self) -> PromptCacheAnalysisTimings {
        PromptCacheAnalysisTimings {
            cache_structure_ms: self.structure.map(duration_ms_f64),
            cache_token_key_ms: self.token_key.map(duration_ms_f64),
            cache_count_lookup_ms: self.count_lookup.map(duration_ms_f64),
            cache_tokenizer_queue_ms: None,
            cache_serialize_ms: self.serialize.map(duration_ms_f64),
            cache_tokenize_ms: self.tokenize.map(duration_ms_f64),
        }
    }
}

fn duration_ms_f64(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn reference_fallback(
    value: &Value,
    canonical_model: &str,
    mut durations: PromptCacheAnalysisDurations,
) -> (
    V3PromptCacheAnalysis,
    PromptCacheAnalysisStats,
    PromptCacheAnalysisDurations,
) {
    let (analysis, tokenization_duration) =
        analyze_v3_prompt_cache_with_tokenize_duration(value, canonical_model);
    durations.count_lookup = Some(Duration::ZERO);
    durations.tokenize = Some(tokenization_duration);
    (analysis, PromptCacheAnalysisStats::default(), durations)
}

#[cfg(test)]
impl ExactPrefixBatch {
    fn owned_serialized_storage(&self) -> (usize, usize) {
        let ExactPrefixBatch {
            open_prefix_bytes,
            breakpoint_offsets: _,
            suffix_bytes,
            token_prefix_keys: _,
        } = self;
        (
            open_prefix_bytes.len().saturating_add(suffix_bytes.len()),
            open_prefix_bytes
                .capacity()
                .saturating_add(suffix_bytes.capacity()),
        )
    }
}
#[derive(Clone, Copy, Debug, Default)]
struct PromptCacheAnalysisStats {
    cache_hits: u64,
    cache_misses: u64,
    cache_coalesced: u64,
}
fn record_analysis_metrics(stats: PromptCacheAnalysisStats, total_duration: Duration) {
    metrics::histogram!(
        "cc_lb_prompt_cache_analysis_duration_seconds",
        "stage" => "total"
    )
    .record(total_duration.as_secs_f64());
    for (result, count) in [
        ("hit", stats.cache_hits),
        ("miss", stats.cache_misses),
        ("coalesced", stats.cache_coalesced),
    ] {
        if count > 0 {
            metrics::counter!(
                "cc_lb_prompt_cache_token_count_cache_total",
                "result" => result
            )
            .increment(count);
        }
    }
}

#[cfg(test)]
fn prepare_exact_prefix_batch(
    canonical_model: &str,
    blocks: &[CacheBlockRef<'_>],
    breakpoints: &[V3StructuralBreakpoint],
) -> serde_json::Result<ExactPrefixBatch> {
    let mut batch = serialize_exact_prefix_batch(canonical_model, blocks, breakpoints)?;
    batch.token_prefix_keys = compute_token_prefix_keys(&batch, &[0; 32])
        .expect("serialized breakpoint offsets are valid and ordered");
    Ok(batch)
}

fn serialize_exact_prefix_batch(
    canonical_model: &str,
    blocks: &[CacheBlockRef<'_>],
    breakpoints: &[V3StructuralBreakpoint],
) -> serde_json::Result<ExactPrefixBatch> {
    #[cfg(test)]
    if FORCE_SERIALIZATION_ERROR.with(|force| force.replace(false)) {
        return Err(serde_json::Error::io(std::io::Error::other(
            "forced exact-prefix serialization failure",
        )));
    }
    if breakpoints.is_empty() {
        return Ok(ExactPrefixBatch {
            open_prefix_bytes: Vec::new(),
            breakpoint_offsets: Vec::new(),
            suffix_bytes: Vec::new(),
            token_prefix_keys: Vec::new(),
        });
    }

    let mut suffix_bytes = b"],\"model\":".to_vec();
    serde_json::to_writer(&mut suffix_bytes, canonical_model)?;
    suffix_bytes.push(b'}');

    let mut positions_by_block = HashMap::<usize, Vec<usize>>::new();
    for (position, breakpoint) in breakpoints.iter().enumerate() {
        positions_by_block
            .entry(breakpoint.block_index as usize)
            .or_default()
            .push(position);
    }

    let mut open_prefix_bytes = b"{\"content_blocks\":[".to_vec();
    let mut breakpoint_offsets = vec![0; breakpoints.len()];
    let token_prefix_keys = Vec::new();
    let deepest = breakpoints
        .iter()
        .map(|breakpoint| breakpoint.block_index as usize)
        .max();

    for (block_index, block) in blocks.iter().enumerate() {
        if deepest.is_some_and(|deepest| block_index > deepest) {
            break;
        }
        if block_index > 0 {
            open_prefix_bytes.push(b',');
        }
        serde_json::to_writer(&mut open_prefix_bytes, &PrefixBlockSerializer(block))?;

        if let Some(positions) = positions_by_block.get(&block_index) {
            for &position in positions {
                breakpoint_offsets[position] = open_prefix_bytes.len();
            }
        }
    }

    Ok(ExactPrefixBatch {
        open_prefix_bytes,
        breakpoint_offsets,
        suffix_bytes,
        token_prefix_keys,
    })
}

fn compute_token_prefix_keys(
    batch: &ExactPrefixBatch,
    cache_scope: &[u8; 32],
) -> Option<Vec<TokenPrefixKey>> {
    if batch
        .breakpoint_offsets
        .iter()
        .any(|&offset| offset > batch.open_prefix_bytes.len())
        || batch
            .breakpoint_offsets
            .windows(2)
            .any(|offsets| offsets[0] > offsets[1])
    {
        return None;
    }

    let mut keys = Vec::with_capacity(batch.breakpoint_offsets.len());
    let mut hasher = blake3::Hasher::new();
    hasher.update(TOKEN_PREFIX_KEY_DOMAIN);
    hasher.update(cache_scope);
    let mut cursor = 0;
    for &offset in &batch.breakpoint_offsets {
        let segment = batch.open_prefix_bytes.get(cursor..offset)?;
        hasher.update(segment);
        cursor = offset;
        let mut prefix_hasher = hasher.clone();
        prefix_hasher.update(&batch.suffix_bytes);
        keys.push(TokenPrefixKey(*prefix_hasher.finalize().as_bytes()));
    }
    Some(keys)
}

fn full_prefix_counts(batch: &ExactPrefixBatch) -> Vec<u64> {
    let suffix_len = batch.suffix_bytes.len() as u64;
    batch
        .breakpoint_offsets
        .iter()
        .map(|&offset| {
            batch
                .open_prefix_bytes
                .get(..offset)
                .map(|prefix| prefix.len() as u64)
                .unwrap_or(0)
                .saturating_add(suffix_len)
        })
        .collect()
}

#[cfg(test)]
fn analyze_v3_prompt_cache_optimized(
    value: &Value,
    canonical_model: &str,
    cache: &PromptTokenCountCache,
) -> (V3PromptCacheAnalysis, PromptCacheAnalysisStats) {
    let (analysis, stats, _) =
        analyze_v3_prompt_cache_optimized_scoped(value, canonical_model, &[0; 32], cache);
    (analysis, stats)
}

fn analyze_v3_prompt_cache_optimized_scoped(
    value: &Value,
    canonical_model: &str,
    cache_scope: &[u8; 32],
    cache: &PromptTokenCountCache,
) -> (
    V3PromptCacheAnalysis,
    PromptCacheAnalysisStats,
    PromptCacheAnalysisDurations,
) {
    let mut durations = PromptCacheAnalysisDurations::default();
    let mut serialization_scratch = SerializationScratch::default();
    let structure_started = Instant::now();
    let structural =
        analyze_v3_prompt_cache_structure(value, canonical_model, &mut serialization_scratch);
    durations.structure = Some(structure_started.elapsed());
    let Some(structural) = structural else {
        return (
            unanalyzable_prompt_cache(),
            PromptCacheAnalysisStats::default(),
            durations,
        );
    };

    // Exact-prefix byte materialization and credential-scoped hashing are
    // deliberately timed separately. Neither interval includes cache access
    // or tokenization.
    let serialize_started = Instant::now();
    let batch =
        serialize_exact_prefix_batch(canonical_model, &structural.blocks, &structural.breakpoints);
    durations.serialize = Some(serialize_started.elapsed());
    let Ok(mut batch) = batch else {
        return reference_fallback(value, canonical_model, durations);
    };
    let token_key_started = Instant::now();
    let token_prefix_keys = compute_token_prefix_keys(&batch, cache_scope);
    durations.token_key = Some(token_key_started.elapsed());
    let Some(token_prefix_keys) = token_prefix_keys else {
        return reference_fallback(value, canonical_model, durations);
    };
    batch.token_prefix_keys = token_prefix_keys;

    let mut stats = PromptCacheAnalysisStats::default();
    let mut count_lookup_duration = Duration::ZERO;
    let mut claims = Vec::with_capacity(batch.token_prefix_keys.len());
    let mut leader_flights = Vec::new();
    for key in &batch.token_prefix_keys {
        let lookup_started = Instant::now();
        let claim = cache.claim(*key);
        count_lookup_duration = count_lookup_duration.saturating_add(lookup_started.elapsed());
        match &claim {
            CacheClaim::Hit(_) => stats.cache_hits += 1,
            CacheClaim::Leader(flight) => {
                stats.cache_misses += 1;
                leader_flights.push((*key, Arc::clone(flight)));
            }
            CacheClaim::Follower(_) => stats.cache_coalesced += 1,
        }
        claims.push(claim);
    }

    let mut leader_guard = LeaderFlightsGuard::new(cache, leader_flights);
    let mut computed_counts = None;
    if !leader_guard.flights.is_empty() {
        let counts = full_prefix_counts(&batch);
        let lookup_started = Instant::now();
        for (position, claim) in claims.iter().enumerate() {
            if let CacheClaim::Leader(flight) = claim {
                cache.complete(batch.token_prefix_keys[position], flight, counts[position]);
            }
        }
        leader_guard.complete();
        count_lookup_duration = count_lookup_duration.saturating_add(lookup_started.elapsed());
        computed_counts = Some(counts);
    }

    let mut prefix_token_counts = Vec::with_capacity(claims.len());
    for (position, claim) in claims.into_iter().enumerate() {
        let value = match claim {
            CacheClaim::Hit(value) => value,
            CacheClaim::Leader(_) => computed_counts
                .as_ref()
                .and_then(|counts| counts.get(position))
                .copied()
                .unwrap_or_else(|| full_prefix_counts(&batch)[position]),
            CacheClaim::Follower(flight) => {
                let lookup_started = Instant::now();
                let waited = flight.wait();
                count_lookup_duration =
                    count_lookup_duration.saturating_add(lookup_started.elapsed());
                match waited {
                    Some(value) => value,
                    None => {
                        let counts = full_prefix_counts(&batch);
                        let value = counts[position];
                        let lookup_started = Instant::now();
                        cache.insert_value(batch.token_prefix_keys[position], value);
                        count_lookup_duration =
                            count_lookup_duration.saturating_add(lookup_started.elapsed());
                        value
                    }
                }
            }
        };
        prefix_token_counts.push(value);
    }

    durations.count_lookup = Some(count_lookup_duration);
    durations.tokenize = Some(Duration::ZERO);
    let analysis = finish_structural_analysis(structural, prefix_token_counts)
        .unwrap_or_else(unanalyzable_prompt_cache);
    (analysis, stats, durations)
}

#[cfg(test)]
struct AnalysisTestGate {
    release_state: Arc<AnalysisTestGateReleaseState>,
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    runtime_worker: std::thread::ThreadId,
}

#[cfg(test)]
struct AnalysisTestGateReleaseState {
    released: Mutex<bool>,
    release_changed: Condvar,
}

#[cfg(test)]
impl AnalysisTestGate {
    fn new(
        runtime_worker: std::thread::ThreadId,
    ) -> (
        Arc<Self>,
        tokio::sync::oneshot::Receiver<()>,
        AnalysisTestGateRelease,
    ) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let release_state = Arc::new(AnalysisTestGateReleaseState {
            released: Mutex::new(false),
            release_changed: Condvar::new(),
        });
        let gate = Arc::new(Self {
            release_state: Arc::clone(&release_state),
            started: Mutex::new(Some(started_tx)),
            runtime_worker,
        });
        let release = AnalysisTestGateRelease {
            release_state: Some(release_state),
        };
        (gate, started_rx, release)
    }

    fn wait_for_release(&self) {
        let mut released = self.release_state.released.lock();
        let started = self
            .started
            .lock()
            .take()
            .expect("analysis test gate may only be entered once");
        let _ = started.send(());
        assert_ne!(
            std::thread::current().id(),
            self.runtime_worker,
            "blocking analysis body ran on the Tokio runtime worker instead of spawn_blocking"
        );
        while !*released {
            self.release_state.release_changed.wait(&mut released);
        }
    }
}

#[cfg(test)]
struct AnalysisTestGateRelease {
    release_state: Option<Arc<AnalysisTestGateReleaseState>>,
}

#[cfg(test)]
impl AnalysisTestGateRelease {
    fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        let Some(release_state) = self.release_state.take() else {
            return;
        };
        let mut released = release_state.released.lock();
        *released = true;
        drop(released);
        release_state.release_changed.notify_all();
    }
}

#[cfg(test)]
impl Drop for AnalysisTestGateRelease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[cfg(test)]
tokio::task_local! {
    static PROMPT_CACHE_ANALYSIS_TEST_GATE: Arc<AnalysisTestGate>;
}

#[cfg(test)]
#[derive(Clone, Default)]
struct TestExecutionGate {
    started: Arc<tokio::sync::Notify>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(test)]
impl TestExecutionGate {
    fn block(&self) {
        self.started.notify_one();
        let (released, ready) = &*self.released;
        let mut released = released.lock();
        while !*released {
            ready.wait(&mut released);
        }
    }

    async fn wait_until_started(&self) {
        self.started.notified().await;
    }

    fn release(&self) {
        let (released, ready) = &*self.released;
        *released.lock() = true;
        ready.notify_all();
    }
}

fn analysis_output(
    analysis: V3PromptCacheAnalysis,
    canonical_model: &str,
    token_threshold: Option<usize>,
    mut durations: PromptCacheAnalysisDurations,
    tokenize_ambiguous: bool,
) -> PromptCacheAnalysisOutput {
    let (cacheable_breakpoint_prefix_keys, threshold_tokenization_duration) = token_threshold
        .map(|threshold| {
            cacheable_breakpoint_prefix_keys_with_tokenize_duration(
                &analysis,
                canonical_model,
                threshold,
                tokenize_ambiguous,
            )
        })
        .unwrap_or_default();
    if token_threshold.is_some() {
        durations.tokenize = Some(
            durations
                .tokenize
                .unwrap_or_default()
                .saturating_add(threshold_tokenization_duration),
        );
    }
    PromptCacheAnalysisOutput {
        analysis,
        cacheable_breakpoint_prefix_keys,
        timings: durations.into_timings(),
    }
}

#[derive(Clone)]
pub(crate) struct PromptCacheAnalysisExecutor {
    semaphore: Arc<Semaphore>,
    cache: Arc<PromptTokenCountCache>,
    #[cfg(test)]
    test_delay: Duration,
    #[cfg(test)]
    test_panic: bool,
    #[cfg(test)]
    test_gate: Option<TestExecutionGate>,
}

impl Default for PromptCacheAnalysisExecutor {
    fn default() -> Self {
        let concurrency = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get().saturating_sub(1).max(1))
            .unwrap_or(1);
        Self {
            semaphore: Arc::new(Semaphore::new(concurrency)),
            cache: Arc::new(PromptTokenCountCache::new(
                DEFAULT_TOKEN_COUNT_CACHE_CAPACITY,
            )),
            #[cfg(test)]
            test_delay: Duration::ZERO,
            #[cfg(test)]
            test_panic: false,
            #[cfg(test)]
            test_gate: None,
        }
    }
}

impl PromptCacheAnalysisExecutor {
    pub(crate) async fn analyze(
        &self,
        value: Arc<Value>,
        canonical_model: String,
        cache_scope: [u8; 32],
        token_threshold: Option<usize>,
    ) -> PromptCacheAnalysisOutput {
        let queue_started = Instant::now();
        let permit = Arc::clone(&self.semaphore).acquire_owned().await;
        let queue_duration = queue_started.elapsed();
        metrics::histogram!(
            "cc_lb_prompt_cache_analysis_duration_seconds",
            "stage" => "queue"
        )
        .record(queue_duration.as_secs_f64());
        let Ok(permit) = permit else {
            let (analysis, _, durations) = reference_fallback(
                &value,
                &canonical_model,
                PromptCacheAnalysisDurations::default(),
            );
            let mut output =
                analysis_output(analysis, &canonical_model, token_threshold, durations, false);
            output.timings.cache_tokenizer_queue_ms =
                Some(duration_ms_f64(queue_duration));
            return output;
        };

        let cache = Arc::clone(&self.cache);
        let fallback_value = Arc::clone(&value);
        let fallback_model = canonical_model.clone();
        #[cfg(test)]
        let test_delay = self.test_delay;
        #[cfg(test)]
        let test_panic = self.test_panic;
        #[cfg(test)]
        let analysis_test_gate = PROMPT_CACHE_ANALYSIS_TEST_GATE.try_with(Arc::clone).ok();
        #[cfg(test)]
        let test_gate = self.test_gate.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            #[cfg(test)]
            if let Some(analysis_test_gate) = analysis_test_gate {
                analysis_test_gate.wait_for_release();
            }
            #[cfg(test)]
            if test_panic {
                panic!("forced prompt-cache analysis worker failure");
            }
            #[cfg(test)]
            if !test_delay.is_zero() {
                std::thread::sleep(test_delay);
            }
            #[cfg(test)]
            if let Some(gate) = test_gate {
                gate.block();
            }
            let total_started = Instant::now();
            let (analysis, stats, durations) = analyze_v3_prompt_cache_optimized_scoped(
                &value,
                &canonical_model,
                &cache_scope,
                &cache,
            );
            let output = analysis_output(analysis, &canonical_model, token_threshold);
            record_analysis_metrics(stats, total_started.elapsed());
            let mut output =
                analysis_output(analysis, &canonical_model, token_threshold, durations, true);
            output.timings.cache_tokenizer_queue_ms = Some(duration_ms_f64(queue_duration));
            output
        });

        match task.await {
            Ok(output) => output,
            Err(error) => {
                tracing::error!(error = %error, "prompt-cache analysis worker failed; using exact synchronous fallback");
                metrics::counter!("cc_lb_prompt_cache_analysis_worker_failed_total").increment(1);
                let (analysis, _, durations) = reference_fallback(
                    &fallback_value,
                    &fallback_model,
                    PromptCacheAnalysisDurations::default(),
                );
                let mut output =
                    analysis_output(analysis, &fallback_model, token_threshold, durations, false);
                output.timings.cache_tokenizer_queue_ms =
                    Some(duration_ms_f64(queue_duration));
                output
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use metrics::{
        Counter, CounterFn, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
        SharedString, Unit,
    };
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    const MODEL: &str = "claude-sonnet-4-5-20250929";
    const PRODUCTION_FIXTURE_MESSAGE_COUNT: usize = 182;
    const PRODUCTION_FIXTURE_BREAKPOINT_MESSAGE_INDICES: [usize; 4] = [44, 90, 136, 180];
    const PRODUCTION_FIXTURE_PAYLOAD_REPEATS: usize = 170;
    const PRODUCTION_FIXTURE_SERIALIZED_BYTES: usize = 4_065_868;
    const PRODUCTION_FIXTURE_UNIT: &str = " boundary=\"quoted\" slash=\\ end=}], comma=,, newline=\n tab=\t carriage=\r 한글=캐시 emoji=👩‍💻🙂 whitespace=   \u{00a0}\n";
    const _: () = assert!(
        PRODUCTION_FIXTURE_SERIALIZED_BYTES >= 4_000_000
            && PRODUCTION_FIXTURE_SERIALIZED_BYTES <= 4_800_000
    );
    #[derive(Clone, Default)]
    struct RecordingMetrics {
        counters: Arc<StdMutex<HashMap<String, u64>>>,
        histograms: Arc<StdMutex<HashMap<String, Vec<f64>>>>,
    }

    struct RecordingCounter {
        counters: Arc<StdMutex<HashMap<String, u64>>>,
        key: String,
    }

    impl CounterFn for RecordingCounter {
        fn increment(&self, value: u64) {
            *self
                .counters
                .lock()
                .expect("counter recorder lock")
                .entry(self.key.clone())
                .or_default() += value;
        }

        fn absolute(&self, value: u64) {
            self.counters
                .lock()
                .expect("counter recorder lock")
                .insert(self.key.clone(), value);
        }
    }

    struct RecordingHistogram {
        histograms: Arc<StdMutex<HashMap<String, Vec<f64>>>>,
        key: String,
    }

    impl HistogramFn for RecordingHistogram {
        fn record(&self, value: f64) {
            self.histograms
                .lock()
                .expect("histogram recorder lock")
                .entry(self.key.clone())
                .or_default()
                .push(value);
        }
    }

    impl Recorder for RecordingMetrics {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(RecordingCounter {
                counters: Arc::clone(&self.counters),
                key: key.to_string(),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::from_arc(Arc::new(RecordingHistogram {
                histograms: Arc::clone(&self.histograms),
                key: key.to_string(),
            }))
        }
    }

    fn four_breakpoint_request(text: &str) -> Value {
        json!({
            "model": MODEL,
            "system": [
                {"type":"text","text":format!("a-{text}"),"cache_control":{"type":"ephemeral"}},
                {"type":"text","text":format!("b-{text}"),"cache_control":{"type":"ephemeral"}},
                {"type":"text","text":format!("c-{text}"),"cache_control":{"type":"ephemeral"}},
                {"type":"text","text":format!("d-{text}"),"cache_control":{"type":"ephemeral"}}
            ],
            "messages": [{"role":"user","content":"hello"}]
        })
    }
    fn four_part_request(parts: [&str; 4]) -> Value {
        json!({
            "model": MODEL,
            "system": [
                {"type":"text","text":parts[0],"cache_control":{"type":"ephemeral"}},
                {"type":"text","text":parts[1],"cache_control":{"type":"ephemeral"}},
                {"type":"text","text":parts[2],"cache_control":{"type":"ephemeral"}},
                {"type":"text","text":parts[3],"cache_control":{"type":"ephemeral"}}
            ],
            "messages": [{"role":"user","content":"hello"}]
        })
    }

    fn production_size_prompt_cache_request() -> Value {
        let mut messages = Vec::with_capacity(PRODUCTION_FIXTURE_MESSAGE_COUNT);
        for message_index in 0..PRODUCTION_FIXTURE_MESSAGE_COUNT {
            let text = format!(
                "message-{message_index:03}:{}",
                PRODUCTION_FIXTURE_UNIT.repeat(PRODUCTION_FIXTURE_PAYLOAD_REPEATS)
            );
            let mut content_block = json!({"type": "text", "text": text});
            let ttl = match message_index {
                44 | 90 => Some("1h"),
                136 | 180 => Some("5m"),
                _ => None,
            };
            if let Some(ttl) = ttl {
                content_block["cache_control"] = json!({"type": "ephemeral", "ttl": ttl});
            }
            messages.push(json!({
                "role": if message_index % 2 == 0 { "user" } else { "assistant" },
                "content": [content_block]
            }));
        }
        json!({
            "model": MODEL,
            "max_tokens": 1024,
            "messages": messages
        })
    }

    #[test]
    fn metric_emission_uses_fixed_cache_labels() {
        let recorder = RecordingMetrics::default();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        record_analysis_metrics(
            PromptCacheAnalysisStats::default(),
            Duration::from_millis(1),
        );
        record_analysis_metrics(
            PromptCacheAnalysisStats {
                cache_hits: 2,
                cache_misses: 1,
                cache_coalesced: 3,
            },
            Duration::from_millis(5),
        );

        let counters = recorder.counters.lock().expect("counter recorder lock");
        assert_eq!(
            counters
                .iter()
                .find(|(key, _)| key.contains("result = hit"))
                .map(|(_, value)| *value),
            Some(2)
        );
        assert_eq!(
            counters
                .iter()
                .find(|(key, _)| key.contains("result = miss"))
                .map(|(_, value)| *value),
            Some(1)
        );
        assert_eq!(
            counters
                .iter()
                .find(|(key, _)| key.contains("result = coalesced"))
                .map(|(_, value)| *value),
            Some(3)
        );
        drop(counters);

        let histograms = recorder.histograms.lock().expect("histogram recorder lock");
        assert_eq!(
            histograms
                .iter()
                .find(|(key, _)| key.contains("stage = total"))
                .map(|(_, values)| values.len()),
            Some(2)
        );
    }

    #[test]
    fn optimized_analysis_matches_reference() {
        let request = four_breakpoint_request(&"boundary }],\n한글🙂 ".repeat(2_000));
        let reference = analyze_v3_prompt_cache(&request, MODEL);
        let cache = PromptTokenCountCache::new(32);
        let (optimized, _) = analyze_v3_prompt_cache_optimized(&request, MODEL, &cache);
        assert_eq!(optimized, reference);
    }

    #[test]
    fn token_prefix_keys_reject_invalid_offsets_without_scope_free_sentinels() {
        let valid = ExactPrefixBatch {
            open_prefix_bytes: b"abc".to_vec(),
            breakpoint_offsets: vec![1, 3],
            suffix_bytes: b"-suffix".to_vec(),
            token_prefix_keys: Vec::new(),
        };
        let first_scope = compute_token_prefix_keys(&valid, &[1; 32]).expect("valid offsets");
        let second_scope = compute_token_prefix_keys(&valid, &[2; 32]).expect("valid offsets");
        assert_ne!(first_scope, second_scope);
        assert!(
            first_scope
                .iter()
                .chain(&second_scope)
                .all(|key| *key != TokenPrefixKey([0; 32]))
        );

        let descending = ExactPrefixBatch {
            breakpoint_offsets: vec![3, 1],
            ..valid
        };

        assert!(compute_token_prefix_keys(&descending, &[1; 32]).is_none());
        let out_of_range = ExactPrefixBatch {
            open_prefix_bytes: b"abc".to_vec(),
            breakpoint_offsets: vec![4],
            suffix_bytes: b"-suffix".to_vec(),
            token_prefix_keys: Vec::new(),
        };
        assert!(compute_token_prefix_keys(&out_of_range, &[1; 32]).is_none());
    }
    #[test]
    fn reference_fallback_records_zero_tokenizer_time_and_zero_lookup() {
        let request = four_breakpoint_request("serialization-fallback");
        let reference = analyze_v3_prompt_cache(&request, MODEL);
        let durations = PromptCacheAnalysisDurations {
            structure: Some(Duration::from_micros(1)),
            serialize: Some(Duration::from_micros(2)),
            ..PromptCacheAnalysisDurations::default()
        };

        let (analysis, stats, durations) = reference_fallback(&request, MODEL, durations);

        assert_eq!(analysis, reference);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 0);
        assert_eq!(durations.count_lookup, Some(Duration::ZERO));
        assert_eq!(durations.tokenize, Some(Duration::ZERO));
        assert_eq!(durations.structure, Some(Duration::from_micros(1)));
        assert_eq!(durations.serialize, Some(Duration::from_micros(2)));
    }

    #[test]
    fn exact_batch_prefix_bytes_match_reference_serializer() {
        let request = four_breakpoint_request("bytes }], 한글🙂");
        let mut structural_scratch = SerializationScratch::default();
        let structural =
            analyze_v3_prompt_cache_structure(&request, MODEL, &mut structural_scratch).unwrap();
        let batch =
            prepare_exact_prefix_batch(MODEL, &structural.blocks, &structural.breakpoints).unwrap();
        let mut reference_scratch = SerializationScratch::default();
        for (position, breakpoint) in structural.breakpoints.iter().enumerate() {
            let expected = super::super::serialized_prefix(
                MODEL,
                &structural.blocks[..=breakpoint.block_index as usize],
                &mut reference_scratch,
            )
            .to_vec();
            let offset = batch.breakpoint_offsets[position];
            let mut actual = batch.open_prefix_bytes[..offset].to_vec();
            actual.extend_from_slice(&batch.suffix_bytes);
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn serialization_error_fallback_records_zero_tokenizer_time() {
        let request = four_breakpoint_request("forced-serialization-error");
        let reference = analyze_v3_prompt_cache(&request, MODEL);
        FORCE_SERIALIZATION_ERROR.with(|force| force.set(true));
        let cache = PromptTokenCountCache::new(32);

        let (analysis, stats, durations) =
            analyze_v3_prompt_cache_optimized_scoped(&request, MODEL, &[9; 32], &cache);

        assert_eq!(analysis, reference);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 0);
        assert_eq!(durations.count_lookup, Some(Duration::ZERO));
        assert_eq!(durations.tokenize, Some(Duration::ZERO));
    }
    #[test]
    fn production_size_prompt_cache_regression_gate() {
        let request = production_size_prompt_cache_request();
        let serialized_request = serde_json::to_vec(&request).expect("serialize fixture");
        assert_eq!(
            serialized_request.len(),
            PRODUCTION_FIXTURE_SERIALIZED_BYTES,
            "production fixture shape changed"
        );

        let messages = request["messages"].as_array().expect("fixture messages");
        assert_eq!(messages.len(), PRODUCTION_FIXTURE_MESSAGE_COUNT);
        let breakpoint_message_indices = messages
            .iter()
            .enumerate()
            .filter_map(|(message_index, message)| {
                message["content"][0]
                    .get("cache_control")
                    .map(|_| message_index)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            breakpoint_message_indices,
            PRODUCTION_FIXTURE_BREAKPOINT_MESSAGE_INDICES
        );
        let first_text = messages[0]["content"][0]["text"]
            .as_str()
            .expect("fixture text");
        for boundary in [
            "\"quoted\"",
            "slash=\\",
            "}],",
            "\n",
            "\t",
            "\r",
            "한글=캐시",
            "👩‍💻🙂",
            "\u{00a0}",
        ] {
            assert!(
                first_text.contains(boundary),
                "fixture lost serializer/tokenizer boundary {boundary:?}"
            );
        }

        let reference = analyze_v3_prompt_cache(&request, MODEL);
        assert_eq!(reference.blocks.len(), PRODUCTION_FIXTURE_MESSAGE_COUNT);
        assert_eq!(reference.breakpoints.len(), 4);

        let mut structural_scratch = SerializationScratch::default();
        let structural =
            analyze_v3_prompt_cache_structure(&request, MODEL, &mut structural_scratch)
                .expect("fixture remains analyzable");
        assert_eq!(structural.blocks.len(), PRODUCTION_FIXTURE_MESSAGE_COUNT);
        assert_eq!(structural.breakpoints.len(), 4);
        let batch = prepare_exact_prefix_batch(MODEL, &structural.blocks, &structural.breakpoints)
            .expect("prepare exact prefix batch");

        let mut reference_scratch = SerializationScratch::default();
        for (position, breakpoint) in structural.breakpoints.iter().enumerate() {
            let expected = super::super::serialized_prefix(
                MODEL,
                &structural.blocks[..=breakpoint.block_index as usize],
                &mut reference_scratch,
            );
            let offset = batch.breakpoint_offsets[position];
            assert_eq!(expected.len(), offset + batch.suffix_bytes.len());
            assert_eq!(&expected[..offset], &batch.open_prefix_bytes[..offset]);
            assert_eq!(&expected[offset..], batch.suffix_bytes.as_slice());
        }

        let expected_ttls = ["1h", "1h", "5m", "5m"];
        for (position, breakpoint) in reference.breakpoints.iter().enumerate() {
            assert_eq!(
                breakpoint.message_index,
                Some(PRODUCTION_FIXTURE_BREAKPOINT_MESSAGE_INDICES[position] as u64)
            );
            assert_eq!(breakpoint.ttl.as_deref(), Some(expected_ttls[position]));
            assert_eq!(breakpoint.lookback_prefixes.len(), 20);
            assert_eq!(
                breakpoint
                    .lookback_prefixes
                    .iter()
                    .map(|prefix| prefix.lookback_distance)
                    .collect::<Vec<_>>(),
                (0..20).collect::<Vec<_>>()
            );
        }

        let deepest_prefix_bytes = batch
            .breakpoint_offsets
            .last()
            .copied()
            .expect("deepest breakpoint")
            .saturating_add(batch.suffix_bytes.len()) as u64;
        let (owned_serialized_bytes, owned_serialized_capacity) = batch.owned_serialized_storage();
        assert_eq!(owned_serialized_bytes as u64, deepest_prefix_bytes);
        assert!(
            (owned_serialized_capacity as u64).saturating_mul(4)
                <= deepest_prefix_bytes.saturating_mul(9),
            "exact-prefix batch retained too much serialized capacity: owned={owned_serialized_capacity}, deepest={deepest_prefix_bytes}"
        );

        let cache = PromptTokenCountCache::new(32);
        let cache_scope = [0x5a; 32];
        let (cold, cold_stats, _cold_durations) =
            analyze_v3_prompt_cache_optimized_scoped(&request, MODEL, &cache_scope, &cache);
        for (position, (actual, expected)) in cold
            .breakpoints
            .iter()
            .zip(&reference.breakpoints)
            .enumerate()
        {
            assert_eq!(
                actual.prefix_token_count, expected.prefix_token_count,
                "breakpoint {position} prefix token count drift"
            );
            assert_eq!(
                actual.prefix_key, expected.prefix_key,
                "breakpoint {position} prefix hash drift"
            );
            assert_eq!(actual.ttl, expected.ttl, "breakpoint {position} TTL drift");
            assert_eq!(
                actual.lookback_prefixes, expected.lookback_prefixes,
                "breakpoint {position} lookback metadata drift"
            );
        }
        assert_eq!(cold, reference);
        assert_eq!(cold_stats.cache_hits, 0);
        assert_eq!(cold_stats.cache_misses, 4);
        assert_eq!(cold_stats.cache_coalesced, 0);

        let (warm, warm_stats, _warm_durations) =
            analyze_v3_prompt_cache_optimized_scoped(&request, MODEL, &cache_scope, &cache);
        assert_eq!(warm, reference);
        assert_eq!(warm_stats.cache_hits, 4);
        assert_eq!(warm_stats.cache_misses, 0);
        assert_eq!(warm_stats.cache_coalesced, 0);

        println!(
            "prompt-cache-production-regression fixture_bytes={} messages={} breakpoints={} deepest_prefix_bytes={} warm_hits={} owned_serialized_bytes={} owned_serialized_capacity={}",
            serialized_request.len(),
            messages.len(),
            reference.breakpoints.len(),
            deepest_prefix_bytes,
            warm_stats.cache_hits,
            owned_serialized_bytes,
            owned_serialized_capacity
        );
    }
    #[test]
    fn special_token_literal_matches_exact_reference() {
        let request = four_breakpoint_request("<|endoftext|>");
        let reference = analyze_v3_prompt_cache(&request, MODEL);
        let cache = PromptTokenCountCache::new(32);
        let (optimized, _) = analyze_v3_prompt_cache_optimized(&request, MODEL, &cache);
        assert_eq!(optimized, reference);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn arbitrary_nested_prefixes_match_reference(
            first in any::<String>(),
            second in any::<String>(),
            third in any::<String>(),
            fourth in any::<String>(),
        ) {
            let request = four_part_request([&first, &second, &third, &fourth]);
            let reference = analyze_v3_prompt_cache(&request, MODEL);
            let cache = PromptTokenCountCache::new(32);
            let (optimized, _) = analyze_v3_prompt_cache_optimized(&request, MODEL, &cache);
            prop_assert_eq!(optimized, reference);
        }
    }

    #[test]
    fn exact_token_key_keeps_cache_control_bytes() {
        let implicit = json!({
            "model": MODEL,
            "messages":[{"role":"user","content":[{
                "type":"text","text":"stable","cache_control":{"type":"ephemeral"}
            }]}]
        });
        let explicit = json!({
            "model": MODEL,
            "messages":[{"role":"user","content":[{
                "type":"text","text":"stable","cache_control":{"type":"ephemeral","ttl":"5m"}
            }]}]
        });
        let mut scratch = SerializationScratch::default();
        let a = analyze_v3_prompt_cache_structure(&implicit, MODEL, &mut scratch).unwrap();
        let a_batch = prepare_exact_prefix_batch(MODEL, &a.blocks, &a.breakpoints).unwrap();
        let b = analyze_v3_prompt_cache_structure(&explicit, MODEL, &mut scratch).unwrap();
        let b_batch = prepare_exact_prefix_batch(MODEL, &b.blocks, &b.breakpoints).unwrap();
        assert_eq!(a.breakpoints[0].prefix_key, b.breakpoints[0].prefix_key);
        assert_ne!(a_batch.token_prefix_keys[0], b_batch.token_prefix_keys[0]);
    }

    #[test]
    fn abandoned_leader_releases_followers_for_exact_recomputation() {
        let cache = PromptTokenCountCache::new(4);
        let key = TokenPrefixKey([7; 32]);
        let leader = match cache.claim(key) {
            CacheClaim::Leader(flight) => flight,
            _ => panic!("first claim must lead"),
        };
        let follower = match cache.claim(key) {
            CacheClaim::Follower(flight) => flight,
            _ => panic!("second claim must follow"),
        };
        drop(LeaderFlightsGuard::new(
            &cache,
            vec![(key, Arc::clone(&leader))],
        ));
        assert_eq!(follower.wait(), None);
        assert!(matches!(cache.claim(key), CacheClaim::Leader(_)));
    }

    #[test]
    fn repeated_analysis_uses_exact_count_cache() {
        let request = four_breakpoint_request("cache");
        let cache = PromptTokenCountCache::new(32);
        let (first, first_stats) = analyze_v3_prompt_cache_optimized(&request, MODEL, &cache);
        let (second, second_stats) = analyze_v3_prompt_cache_optimized(&request, MODEL, &cache);
        assert_eq!(first, second);
        assert_eq!(first_stats.cache_misses, 4);
        assert_eq!(second_stats.cache_hits, 4);
    }

    #[test]
    fn token_count_cache_is_scoped_by_downstream_identity() {
        let request = four_breakpoint_request("scoped-cache");
        let cache = PromptTokenCountCache::new(32);
        let (_, first_scope, _) =
            analyze_v3_prompt_cache_optimized_scoped(&request, MODEL, &[1; 32], &cache);
        let (_, second_scope, _) =
            analyze_v3_prompt_cache_optimized_scoped(&request, MODEL, &[2; 32], &cache);
        let (_, first_scope_again, _) =
            analyze_v3_prompt_cache_optimized_scoped(&request, MODEL, &[1; 32], &cache);
        assert_eq!(first_scope.cache_misses, 4);
        assert_eq!(second_scope.cache_misses, 4);
        assert_eq!(first_scope_again.cache_hits, 4);
    }

    #[test]
    fn concurrent_identical_analysis_single_flights() {
        let request = Arc::new(four_breakpoint_request(&"x".repeat(40_000)));
        let cache = Arc::new(PromptTokenCountCache::new(32));
        let threads = (0..64)
            .map(|_| {
                let request = Arc::clone(&request);
                let cache = Arc::clone(&cache);
                std::thread::spawn(move || {
                    analyze_v3_prompt_cache_optimized(&request, MODEL, &cache)
                })
            })
            .collect::<Vec<_>>();
        let results = threads
            .into_iter()
            .map(|thread| thread.join().expect("analysis thread"))
            .collect::<Vec<_>>();
        let expected = &results[0].0;
        assert!(results.iter().all(|(analysis, _)| analysis == expected));
        assert_eq!(
            results
                .iter()
                .map(|(_, stats)| stats.cache_misses)
                .sum::<u64>(),
            4
        );
    }

    #[tokio::test]
    async fn executor_computes_threshold_eligibility_only_when_requested() {
        let executor = PromptCacheAnalysisExecutor::default();
        let request = Arc::new(four_breakpoint_request(&"large-prefix ".repeat(2_000)));

        let without_threshold = executor
            .analyze(Arc::clone(&request), MODEL.to_owned(), [1; 32], None)
            .await;
        assert!(
            without_threshold
                .cacheable_breakpoint_prefix_keys
                .is_empty()
        );

        let with_threshold = executor
            .analyze(request, MODEL.to_owned(), [1; 32], Some(1_024))
            .await;
        assert_eq!(
            with_threshold.cacheable_breakpoint_prefix_keys.len(),
            with_threshold.analysis.breakpoints.len()
        );
    }

    // One worker makes its thread ID authoritative and runtime progress causally meaningful.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn executor_keeps_async_runtime_progressing() {
        let executor = PromptCacheAnalysisExecutor {
            semaphore: Arc::new(Semaphore::new(1)),
            cache: Arc::new(PromptTokenCountCache::new(32)),
            ..PromptCacheAnalysisExecutor::default()
        };
        let runtime_worker = tokio::spawn(async { std::thread::current().id() })
            .await
            .expect("runtime worker task");
        let (analysis_gate, analysis_started, release_analysis) =
            AnalysisTestGate::new(runtime_worker);
        let request = Arc::new(four_breakpoint_request("runtime-progress"));
        let analysis_task = {
            let executor = executor.clone();
            tokio::spawn(
                PROMPT_CACHE_ANALYSIS_TEST_GATE.scope(analysis_gate, async move {
                    executor
                        .analyze(request, MODEL.to_owned(), [1; 32], None)
                        .await
                }),
            )
        };

        analysis_started
            .await
            .expect("spawn-blocking analysis closure started");
        let (runtime_progress_tx, runtime_progress_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = runtime_progress_tx.send(());
        });
        runtime_progress_rx
            .await
            .expect("Tokio task progressed while blocking analysis was gated");
        assert!(
            !analysis_task.is_finished(),
            "blocking analysis completed before the test released its gate"
        );

        release_analysis.release();
        let analysis = analysis_task.await.expect("analysis task");
        assert_eq!(analysis.analysis.breakpoints.len(), 4);
    }

    #[test]
    fn request_without_breakpoints_performs_no_tokenization() {
        let request = json!({
            "model": MODEL,
            "messages": [{"role":"user","content":"no cache marker"}]
        });
        let reference = analyze_v3_prompt_cache(&request, MODEL);
        let cache = PromptTokenCountCache::new(4);
        let (optimized, stats) = analyze_v3_prompt_cache_optimized(&request, MODEL, &cache);
        assert_eq!(optimized, reference);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 0);
    }

    #[test]
    fn cache_eviction_only_causes_exact_recomputation() {
        let first_request = four_breakpoint_request("first");
        let second_request = four_breakpoint_request("second");
        let first_reference = analyze_v3_prompt_cache(&first_request, MODEL);
        let cache = PromptTokenCountCache::new(1);
        let _ = analyze_v3_prompt_cache_optimized(&first_request, MODEL, &cache);
        let _ = analyze_v3_prompt_cache_optimized(&second_request, MODEL, &cache);
        let (recomputed, stats) = analyze_v3_prompt_cache_optimized(&first_request, MODEL, &cache);
        assert_eq!(recomputed, first_reference);
        assert!(stats.cache_misses > 0);
    }

    #[tokio::test]
    async fn executor_reports_non_overlapping_request_timings_without_threshold_tokenization() {
        let executor = PromptCacheAnalysisExecutor {
            semaphore: Arc::new(Semaphore::new(1)),
            cache: Arc::new(PromptTokenCountCache::new(32)),
            ..PromptCacheAnalysisExecutor::default()
        };
        let request = Arc::new(four_breakpoint_request("timed-analysis"));
        let cold_started = Instant::now();
        let cold = executor
            .analyze(Arc::clone(&request), MODEL.to_owned(), [3; 32], None)
            .await;
        let cold_outer_ms = duration_ms_f64(cold_started.elapsed());
        let warm_started = Instant::now();
        let warm = executor
            .analyze(request, MODEL.to_owned(), [3; 32], None)
            .await;
        let warm_outer_ms = duration_ms_f64(warm_started.elapsed());

        assert_eq!(cold.analysis.breakpoints.len(), 4);
        assert_eq!(warm.analysis, cold.analysis);
        for value in [
            cold.timings.cache_structure_ms,
            cold.timings.cache_token_key_ms,
            cold.timings.cache_count_lookup_ms,
            cold.timings.cache_tokenizer_queue_ms,
            cold.timings.cache_serialize_ms,
            cold.timings.cache_tokenize_ms,
            warm.timings.cache_structure_ms,
            warm.timings.cache_token_key_ms,
            warm.timings.cache_count_lookup_ms,
            warm.timings.cache_tokenizer_queue_ms,
            warm.timings.cache_serialize_ms,
            warm.timings.cache_tokenize_ms,
        ] {
            let value = value.expect("completed prompt-cache timing");
            assert!(value.is_finite());
            assert!(value >= 0.0);
        }
        for (timings, outer_ms) in [(cold.timings, cold_outer_ms), (warm.timings, warm_outer_ms)] {
            let measured_sum = [
                timings.cache_tokenizer_queue_ms,
                timings.cache_structure_ms,
                timings.cache_serialize_ms,
                timings.cache_token_key_ms,
                timings.cache_count_lookup_ms,
                timings.cache_tokenize_ms,
            ]
            .into_iter()
            .flatten()
            .sum::<f64>();
            assert!(
                measured_sum <= outer_ms,
                "nested setup timings double-counted: measured={measured_sum} outer={outer_ms}"
            );
        }
        assert_eq!(cold.timings.cache_tokenize_ms, Some(0.0));
        assert!(
            warm.timings
                .cache_count_lookup_ms
                .is_some_and(|value| value >= 0.0)
        );
        assert_eq!(warm.timings.cache_tokenize_ms, Some(0.0));
    }

    #[tokio::test]
    async fn closed_semaphore_fallback_skips_threshold_tokenization() {
        let semaphore = Arc::new(Semaphore::new(1));
        semaphore.close();
        let executor = PromptCacheAnalysisExecutor {
            semaphore,
            cache: Arc::new(PromptTokenCountCache::new(32)),
            ..PromptCacheAnalysisExecutor::default()
        };
        let request = Arc::new(four_breakpoint_request("closed-semaphore-fallback"));
        let reference = analyze_v3_prompt_cache(&request, MODEL);

        let output = executor
            .analyze(request, MODEL.to_owned(), [7; 32], Some(1_024))
            .await;

        assert_eq!(output.analysis, reference);
        assert_eq!(output.timings.cache_count_lookup_ms, Some(0.0));
        assert!(output.cacheable_breakpoint_prefix_keys.is_empty());
        assert_eq!(output.timings.cache_tokenize_ms, Some(0.0));
    }

    #[tokio::test]
    async fn join_failure_fallback_skips_threshold_tokenization() {
        let executor = PromptCacheAnalysisExecutor {
            semaphore: Arc::new(Semaphore::new(1)),
            cache: Arc::new(PromptTokenCountCache::new(32)),
            test_panic: true,
            ..PromptCacheAnalysisExecutor::default()
        };
        let request = Arc::new(four_breakpoint_request("join-failure-fallback"));
        let reference = analyze_v3_prompt_cache(&request, MODEL);

        let output = executor
            .analyze(request, MODEL.to_owned(), [8; 32], Some(1_024))
            .await;

        assert_eq!(output.analysis, reference);
        assert_eq!(output.timings.cache_count_lookup_ms, Some(0.0));
        assert!(output.cacheable_breakpoint_prefix_keys.is_empty());
        assert_eq!(output.timings.cache_tokenize_ms, Some(0.0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_request_does_not_poison_executor_or_cache() {
        let gate = TestExecutionGate::default();
        let executor = PromptCacheAnalysisExecutor {
            semaphore: Arc::new(Semaphore::new(1)),
            cache: Arc::new(PromptTokenCountCache::new(32)),
            test_gate: Some(gate.clone()),
            ..PromptCacheAnalysisExecutor::default()
        };
        let request = Arc::new(four_breakpoint_request(&"cancel-safe ".repeat(80_000)));
        let first = {
            let executor = executor.clone();
            let request = Arc::clone(&request);
            tokio::spawn(async move {
                executor
                    .analyze(request, MODEL.to_owned(), [1; 32], None)
                    .await
            })
        };
        gate.wait_until_started().await;
        first.abort();
        let _ = first.await;
        gate.release();

        let output = tokio::time::timeout(
            Duration::from_secs(5),
            executor.analyze(request, MODEL.to_owned(), [1; 32], None),
        )
        .await
        .expect("executor remains available after caller cancellation");
        assert_eq!(output.analysis.breakpoints.len(), 4);
        assert_eq!(executor.cache.state.lock().values.len(), 4);
    }
}
