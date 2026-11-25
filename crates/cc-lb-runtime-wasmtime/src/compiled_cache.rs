//! Process-wide bounded cache for immutable compiled Wasm artifacts.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use parking_lot::{Condvar, Mutex};
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

struct InFlight {
    done: Mutex<bool>,
    ready: Condvar,
}

impl InFlight {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            done: Mutex::new(false),
            ready: Condvar::new(),
        })
    }

    fn wait(&self) {
        let mut done = self.done.lock();
        while !*done {
            self.ready.wait(&mut done);
        }
    }

    fn finish(&self) {
        let mut done = self.done.lock();
        *done = true;
        self.ready.notify_all();
    }
}

struct InFlightGuard<'a> {
    state: &'a Mutex<CacheState>,
    key: [u8; 32],
    flight: Arc<InFlight>,
    armed: bool,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        {
            let mut state = self.state.lock();
            state.in_flight.remove(&self.key);
        }
        self.flight.finish();
    }
}

struct CacheState {
    entries: HashMap<[u8; 32], CacheEntry>,
    in_flight: HashMap<[u8; 32], Arc<InFlight>>,
    warm_keys: HashSet<[u8; 32]>,
    total_bytes: usize,
    next_use: u64,
}

/// Bounded, single-flight cache for immutable compiled module artifacts.
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
                in_flight: HashMap::new(),
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
        let (leader_guard, leader_use) = loop {
            enum LookupResult {
                Hit(Arc<InstancePre<HostState>>),
                Wait(Arc<InFlight>),
                Leader(Arc<InFlight>, u64),
            }

            let lookup = {
                let mut state = self.state.lock();
                if state.entries.contains_key(&key) {
                    let next_use = bump_use(&mut state.next_use);
                    let instance_pre = {
                        let entry = state.entries.get_mut(&key).expect("checked contains_key");
                        entry.last_used = next_use;
                        Arc::clone(&entry.instance_pre)
                    };
                    metrics::counter!("cc_lb_compiled_module_cache_hits_total").increment(1);
                    record_cache_size_metrics(&state);
                    LookupResult::Hit(instance_pre)
                } else if let Some(flight) = state.in_flight.get(&key) {
                    LookupResult::Wait(Arc::clone(flight))
                } else {
                    let flight = InFlight::new();
                    let next_use = bump_use(&mut state.next_use);
                    state.in_flight.insert(key, Arc::clone(&flight));
                    metrics::counter!("cc_lb_compiled_module_cache_misses_total").increment(1);
                    LookupResult::Leader(flight, next_use)
                }
            };

            match lookup {
                LookupResult::Hit(instance_pre) => return Ok(instance_pre),
                LookupResult::Wait(flight) => {
                    flight.wait();
                }
                LookupResult::Leader(flight, next_use) => {
                    break (
                        InFlightGuard {
                            state: &self.state,
                            key,
                            flight,
                            armed: true,
                        },
                        next_use,
                    );
                }
            }
        };

        let (instance_pre, compiled_bytes) = compile()?;

        {
            let mut state = self.state.lock();
            state.in_flight.remove(&key);
            if let Some(prev) = state.entries.insert(
                key,
                CacheEntry {
                    instance_pre: Arc::clone(&instance_pre),
                    compiled_bytes,
                    last_used: leader_use,
                },
            ) {
                state.total_bytes = state.total_bytes.saturating_sub(prev.compiled_bytes);
            }
            state.total_bytes = state.total_bytes.saturating_add(compiled_bytes);
            evict_to_limits(&mut state, self.limits);
            record_cache_size_metrics(&state);
        }

        let mut guard = leader_guard;
        guard.armed = false;
        guard.flight.finish();
        Ok(instance_pre)
    }

    /// Keep compiled artifacts backing active reconcile slots from being
    /// selected before colder entries during size or entry-count eviction.
    pub(crate) fn retain_warm_content(&self, warm_keys: HashSet<[u8; 32]>) {
        let mut state = self.state.lock();
        state.warm_keys = warm_keys;
        evict_to_limits(&mut state, self.limits);
        record_cache_size_metrics(&state);
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
            metrics::counter!("cc_lb_compiled_module_cache_evictions_total").increment(1);
        }
        state.warm_keys.remove(&eviction_key);
    }
}

fn record_cache_size_metrics(state: &CacheState) {
    metrics::gauge!("cc_lb_compiled_module_cache_entries").set(state.entries.len() as f64);
    metrics::gauge!("cc_lb_compiled_module_cache_bytes").set(state.total_bytes as f64);
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use wasmtime::Linker;

    use super::*;
    use crate::engine::{HotEngineConfig, build_hot_engine};

    fn compile_trivial(
        engine: &wasmtime::Engine,
        linker: &Linker<HostState>,
    ) -> Result<(Arc<InstancePre<HostState>>, usize), WasmtimeRuntimeError> {
        let module =
            wasmtime::Module::new(engine, b"\0asm\x01\x00\x00\x00").expect("trivial module");
        let pre = linker.instantiate_pre(&module).expect("instantiate_pre");
        Ok((Arc::new(pre), 64))
    }

    #[test]
    fn get_or_compile_does_not_hold_lock_across_compile_and_coalesces_same_key() {
        let engine = build_hot_engine(&HotEngineConfig::default()).expect("engine");
        let linker = Linker::new(&engine);
        let cache = Arc::new(CompiledModuleCache::new_for_test());
        let k0 = [0u8; 32];
        let k1 = [1u8; 32];
        let k2 = [2u8; 32];

        cache
            .get_or_compile(k0, || compile_trivial(&engine, &linker))
            .expect("prewarm k0");

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let compiles_k1 = Arc::new(AtomicUsize::new(0));

        let c_leader = Arc::clone(&cache);
        let entered_l = Arc::clone(&entered);
        let release_l = Arc::clone(&release);
        let cnt_l = Arc::clone(&compiles_k1);
        let eng_l = engine.clone();
        let lnk_l = linker.clone();
        let t_leader = std::thread::spawn(move || {
            c_leader.get_or_compile(k1, || {
                cnt_l.fetch_add(1, Ordering::SeqCst);
                entered_l.wait();
                release_l.wait();
                compile_trivial(&eng_l, &lnk_l)
            })
        });

        entered.wait();

        // Coalesced waiter on k1 waits while unrelated k0 hit & k2 compile finish BEFORE releasing k1.
        let c_waiter = Arc::clone(&cache);
        let cnt_w = Arc::clone(&compiles_k1);
        let eng_w = engine.clone();
        let lnk_w = linker.clone();
        let t_waiter = std::thread::spawn(move || {
            c_waiter.get_or_compile(k1, || {
                cnt_w.fetch_add(1, Ordering::SeqCst);
                compile_trivial(&eng_w, &lnk_w)
            })
        });

        cache
            .get_or_compile(k0, || unreachable!("k0 must hit without waiting for k1"))
            .expect("k0 hit");
        cache
            .get_or_compile(k2, || compile_trivial(&engine, &linker))
            .expect("k2 compiles concurrently while k1 is blocked");

        release.wait();
        let a = t_leader.join().unwrap().expect("leader k1 ok");
        let b = t_waiter.join().unwrap().expect("waiter k1 ok");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(compiles_k1.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn get_or_compile_error_and_panic_wake_waiters_without_deadlock() {
        let engine = build_hot_engine(&HotEngineConfig::default()).expect("engine");
        let linker = Linker::new(&engine);
        let cache = Arc::new(CompiledModuleCache::new_for_test());

        let err_key = [3u8; 32];
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let c1 = Arc::clone(&cache);
        let e1 = Arc::clone(&entered);
        let r1 = Arc::clone(&release);
        let t1 = std::thread::spawn(move || {
            c1.get_or_compile(err_key, || {
                e1.wait();
                r1.wait();
                Err(WasmtimeRuntimeError::ModuleRejected {
                    reason: "boom".into(),
                })
            })
        });
        entered.wait();
        let c2 = Arc::clone(&cache);
        let eng2 = engine.clone();
        let lnk2 = linker.clone();
        let t2 = std::thread::spawn(move || {
            c2.get_or_compile(err_key, || compile_trivial(&eng2, &lnk2))
        });
        release.wait();
        assert!(t1.join().unwrap().is_err());
        assert!(t2.join().unwrap().is_ok());

        let panic_key = [4u8; 32];
        let c_p = Arc::clone(&cache);
        let p_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = c_p.get_or_compile(panic_key, || panic!("simulated compile panic"));
        }));
        assert!(p_res.is_err());
        cache
            .get_or_compile(panic_key, || compile_trivial(&engine, &linker))
            .expect("in_flight entry cleared by Drop guard after panic");
    }
}
