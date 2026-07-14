//! Process-wide bounded cache for immutable compiled Wasm artifacts.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use wasmtime::InstancePre;

use crate::engine::HostState;
use crate::error::WasmtimeRuntimeError;

pub(crate) const MAX_COMPILED_MODULE_CACHE_BYTES: usize = 48 * 1024 * 1024;
const MAX_COMPILED_MODULE_CACHE_ENTRIES: usize = 64;

static PROCESS_WIDE_COMPILED_MODULE_CACHE: LazyLock<Arc<CompiledModuleCache>> =
    LazyLock::new(|| Arc::new(CompiledModuleCache::new(CacheLimits::production())));

#[derive(Clone, Copy)]
struct CacheLimits {
    max_bytes: usize,
    max_entries: usize,
}

impl CacheLimits {
    const fn production() -> Self {
        Self {
            max_bytes: MAX_COMPILED_MODULE_CACHE_BYTES,
            max_entries: MAX_COMPILED_MODULE_CACHE_ENTRIES,
        }
    }
}

struct CacheEntry {
    instance_pre: Arc<InstancePre<HostState>>,
    compiled_bytes: usize,
    last_used: u64,
}

struct CacheState {
    entries: HashMap<[u8; 32], CacheEntry>,
    warm_keys: HashSet<[u8; 32]>,
    total_bytes: usize,
    next_use: u64,
}

/// Bounded, mutex-protected cache for immutable compiled module artifacts.
pub(crate) struct CompiledModuleCache {
    limits: CacheLimits,
    state: Mutex<CacheState>,
}

impl CompiledModuleCache {
    fn new(limits: CacheLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(CacheState {
                entries: HashMap::new(),
                warm_keys: HashSet::new(),
                total_bytes: 0,
                next_use: 0,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
        Self::new(CacheLimits::production())
    }

    pub(crate) fn get_or_compile<F>(
        &self,
        key: [u8; 32],
        compile: F,
    ) -> Result<Arc<InstancePre<HostState>>, WasmtimeRuntimeError>
    where
        F: FnOnce() -> Result<(Arc<InstancePre<HostState>>, usize), WasmtimeRuntimeError>,
    {
        let mut state = self.state.lock();
        let next_use = bump_use(&mut state.next_use);
        if let Some(entry) = state.entries.get_mut(&key) {
            entry.last_used = next_use;
            return Ok(Arc::clone(&entry.instance_pre));
        }

        let (instance_pre, compiled_bytes) = compile()?;
        state.total_bytes = state.total_bytes.saturating_add(compiled_bytes);
        state.entries.insert(
            key,
            CacheEntry {
                instance_pre: Arc::clone(&instance_pre),
                compiled_bytes,
                last_used: next_use,
            },
        );
        evict_to_limits(&mut state, self.limits);
        Ok(instance_pre)
    }

    /// Keep compiled artifacts backing active reconcile slots from being
    /// selected before colder entries during size or entry-count eviction.
    pub(crate) fn retain_warm_content(&self, warm_keys: HashSet<[u8; 32]>) {
        let mut state = self.state.lock();
        state.warm_keys = warm_keys;
        evict_to_limits(&mut state, self.limits);
    }
}

pub(crate) fn process_wide_compiled_module_cache() -> Arc<CompiledModuleCache> {
    Arc::clone(&PROCESS_WIDE_COMPILED_MODULE_CACHE)
}

fn bump_use(next_use: &mut u64) -> u64 {
    *next_use = next_use.wrapping_add(1);
    *next_use
}

fn evict_to_limits(state: &mut CacheState, limits: CacheLimits) {
    while state.entries.len() > limits.max_entries || state.total_bytes > limits.max_bytes {
        let eviction_key = state
            .entries
            .iter()
            .min_by_key(|(key, entry)| (state.warm_keys.contains(*key), entry.last_used))
            .map(|(key, _)| *key);
        let Some(eviction_key) = eviction_key else {
            return;
        };
        if let Some(entry) = state.entries.remove(&eviction_key) {
            state.total_bytes = state.total_bytes.saturating_sub(entry.compiled_bytes);
        }
        state.warm_keys.remove(&eviction_key);
    }
}
