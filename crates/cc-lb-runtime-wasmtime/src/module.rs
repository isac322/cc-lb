//! Wasm module admission pipeline.
//!
//!   `wasm bytes -> inspect_wasm (imports/exports/schema fingerprints) -> CompiledModuleCache::get_or_compile (Engine::precompile_module -> Module::deserialize -> InstancePre) -> probe`
//!
//! Inspection happens against the raw `.wasm` because
//! `Module::custom_sections` does not round-trip through
//! `precompile_module → deserialize`.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use wasmtime::{Engine, Linker, Module};

use crate::engine::HostState;
use crate::engine::HotEngineConfig;
use crate::error::WasmtimeRuntimeError;
use cc_lb_plugin_wire::schema::HookKind;

use crate::inspect::{ModuleInspection, inspect_wasm, inspect_wasm_agnostic};
use crate::probe::probe_hook_dispatch;

// Bumpable on validation-policy change (import allowlist, schema-hash
// algorithm, per-hook export shape). `register()` mixes this into
// `PluginCell::content_hash`, so bumping it forces every registered
// plugin to recompile on next reconcile even when the wasm bytes are
// byte-identical — guarantees a policy tightening cannot be silently
// carried by a stale short-circuited slot.
pub(crate) const VALIDATION_POLICY_VERSION: u32 = 1;

const WASMTIME_CACHE_COMPATIBILITY_VERSION: &str = "wasmtime-46.0.1";

pub(crate) fn compute_content_hash(
    engine: &Engine,
    config: &HotEngineConfig,
    wasm_bytes: &[u8],
) -> [u8; 32] {
    compute_content_hash_with_policy_version(engine, config, wasm_bytes, VALIDATION_POLICY_VERSION)
}

fn compute_content_hash_with_policy_version(
    engine: &Engine,
    config: &HotEngineConfig,
    wasm_bytes: &[u8],
    validation_policy_version: u32,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cc-lb-compiled-wasm-v1");
    hasher.update(WASMTIME_CACHE_COMPATIBILITY_VERSION.as_bytes());
    hasher.update(&validation_policy_version.to_le_bytes());
    let mut compatibility_hasher = std::collections::hash_map::DefaultHasher::new();
    engine
        .precompile_compatibility_hash()
        .hash(&mut compatibility_hasher);
    hasher.update(&compatibility_hasher.finish().to_le_bytes());
    hasher.update(&config.memory_max_pages.to_le_bytes());
    hasher.update(&config.max_wasm_stack.to_le_bytes());
    hasher.update(&config.pool_total_memories.to_le_bytes());
    hasher.update(&config.pool_total_core_instances.to_le_bytes());
    hasher.update(&config.memory_reservation_bytes.to_le_bytes());
    hasher.update(&config.memory_guard_bytes.to_le_bytes());
    hasher.update(&config.wire_bounds.output_body_bytes.to_le_bytes());
    hasher.update(&config.wire_bounds.max_headers.to_le_bytes());
    hasher.update(&config.wire_bounds.max_header_value_bytes.to_le_bytes());
    hasher.update(&config.wire_bounds.reason_bytes.to_le_bytes());
    hasher.update(&[config.cookie_redaction.into()]);
    hasher.update(wasm_bytes);
    *hasher.finalize().as_bytes()
}

/// Compile raw `.wasm` bytes into an [`InstancePre`](wasmtime::InstancePre) bound to `engine`.
///
/// `Module::deserialize` is documented `unsafe` because tampered compiled
/// artifacts can execute arbitrary code. This function only ever feeds it
/// bytes produced in-memory by `Engine::precompile_module(wasm_bytes)`
/// in the same process, so the cwasm never crosses a trust boundary.
#[allow(unsafe_code)]
fn compile_instance_pre(
    engine: &Engine,
    linker: &Linker<HostState>,
    wasm_bytes: &[u8],
) -> Result<(Arc<wasmtime::InstancePre<HostState>>, usize), WasmtimeRuntimeError> {
    let cwasm = engine
        .precompile_module(wasm_bytes)
        .map_err(|e| WasmtimeRuntimeError::ModuleCompile(anyhow::Error::from(e)))?;
    let compiled_bytes = cwasm.len();

    // SAFETY: cwasm bytes were just produced by `engine.precompile_module`
    // in this process — they are not attacker-controlled.
    let module = unsafe {
        Module::deserialize(engine, &cwasm)
            .map_err(|e| WasmtimeRuntimeError::ModuleCompile(anyhow::Error::from(e)))?
    };

    let instance_pre = linker
        .instantiate_pre(&module)
        .map_err(|e| WasmtimeRuntimeError::InstantiateFailed(anyhow::Error::from(e)))?;

    Ok((Arc::new(instance_pre), compiled_bytes))
}

pub fn admit_wasm(
    engine: &Engine,
    linker: &Linker<HostState>,
    kind: HookKind,
    wasm_bytes: &[u8],
    config: &HotEngineConfig,
) -> Result<(Arc<wasmtime::InstancePre<HostState>>, ModuleInspection), WasmtimeRuntimeError> {
    let inspection = inspect_wasm(kind, wasm_bytes)?;
    admit_inspected_wasm(engine, linker, wasm_bytes, config, inspection)
}

/// Compile and probe every metadata-declared hook without selecting a slot.
pub fn admit_wasm_agnostic(
    engine: &Engine,
    linker: &Linker<HostState>,
    wasm_bytes: &[u8],
    config: &HotEngineConfig,
) -> Result<(Arc<wasmtime::InstancePre<HostState>>, ModuleInspection), WasmtimeRuntimeError> {
    let inspection = inspect_wasm_agnostic(wasm_bytes)?;
    admit_inspected_wasm(engine, linker, wasm_bytes, config, inspection)
}

fn admit_inspected_wasm(
    engine: &Engine,
    linker: &Linker<HostState>,
    wasm_bytes: &[u8],
    config: &HotEngineConfig,
    inspection: ModuleInspection,
) -> Result<(Arc<wasmtime::InstancePre<HostState>>, ModuleInspection), WasmtimeRuntimeError> {
    let content_hash = compute_content_hash(engine, config, wasm_bytes);
    let compiled_modules = crate::compiled_cache::process_wide_compiled_module_cache();
    let instance_pre = compiled_modules.get_or_compile(content_hash, || {
        compile_instance_pre(engine, linker, wasm_bytes)
    })?;
    for (hook, wire_version) in &inspection.hook_versions {
        probe_hook_dispatch(
            Arc::clone(&instance_pre),
            *hook,
            *wire_version,
            &inspection.metadata,
            config.memory_max_pages,
        )?;
    }

    Ok((instance_pre, inspection))
}

#[cfg(test)]
mod agnostic_tests;
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use wasmtime::Linker;

    use super::{compile_instance_pre, compute_content_hash_with_policy_version};
    use crate::compiled_cache::CompiledModuleCache;
    use crate::engine::{HostState, HotEngineConfig, build_hot_engine};

    #[test]
    fn compiled_module_cache_reuses_identical_bytes_without_recompiling() {
        // Given: one engine, one bounded cache, and one module identity.
        let engine = build_hot_engine(&HotEngineConfig::default()).expect("engine");
        let linker = Linker::<HostState>::new(&engine);
        let cache = CompiledModuleCache::new_for_test();
        let wasm = wat::parse_str("(module)").expect("valid wasm");
        let key = compute_content_hash_with_policy_version(
            &engine,
            &HotEngineConfig::default(),
            &wasm,
            1,
        );
        let compile_count = AtomicUsize::new(0);

        // When: the same content is compiled through two independent slots.
        let first = cache
            .get_or_compile(key, || {
                compile_count.fetch_add(1, Ordering::Relaxed);
                compile_instance_pre(&engine, &linker, &wasm)
            })
            .expect("first compilation");
        let second = cache
            .get_or_compile(key, || {
                compile_count.fetch_add(1, Ordering::Relaxed);
                compile_instance_pre(&engine, &linker, &wasm)
            })
            .expect("cached compilation");

        // Then: the compiled artifact is reused and Cranelift work happened once.
        assert!(std::sync::Arc::ptr_eq(&first, &second));
        assert_eq!(compile_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn compiled_module_cache_misses_for_different_bytes_or_validation_policy() {
        // Given: one cache and distinct content identities.
        let config = HotEngineConfig::default();
        let engine = build_hot_engine(&config).expect("engine");
        let linker = Linker::<HostState>::new(&engine);
        let cache = CompiledModuleCache::new_for_test();
        let first_wasm = wat::parse_str("(module)").expect("valid first wasm");
        let changed_wasm = wat::parse_str("(module (func))").expect("valid changed wasm");
        let first_key = compute_content_hash_with_policy_version(&engine, &config, &first_wasm, 1);
        let changed_bytes_key =
            compute_content_hash_with_policy_version(&engine, &config, &changed_wasm, 1);
        let changed_policy_key =
            compute_content_hash_with_policy_version(&engine, &config, &first_wasm, 2);
        let compile_count = AtomicUsize::new(0);

        // When: bytes or the validation policy identity changes.
        cache
            .get_or_compile(first_key, || {
                compile_count.fetch_add(1, Ordering::Relaxed);
                compile_instance_pre(&engine, &linker, &first_wasm)
            })
            .expect("first compilation");
        cache
            .get_or_compile(changed_bytes_key, || {
                compile_count.fetch_add(1, Ordering::Relaxed);
                compile_instance_pre(&engine, &linker, &changed_wasm)
            })
            .expect("changed bytes compilation");
        cache
            .get_or_compile(changed_policy_key, || {
                compile_count.fetch_add(1, Ordering::Relaxed);
                compile_instance_pre(&engine, &linker, &first_wasm)
            })
            .expect("changed policy compilation");

        // Then: each distinct identity reaches the compiler.
        assert_eq!(compile_count.load(Ordering::Relaxed), 3);
    }
}
