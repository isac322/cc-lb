use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, PluginIdentity};
use cc_lb_plugin_wire::v1::build_signer::BuildSignerFn;
use cc_lb_plugin_wire::v1::normalize_error::NormalizeErrorFn;
use cc_lb_plugin_wire::v1::observe::ObserveFn;
use cc_lb_plugin_wire::v1::on_unauthorized::OnUnauthorizedFn;
use cc_lb_plugin_wire::v1::route::RouteFn;
use cc_lb_plugin_wire::v1::shape::ShapeFn;
use cc_lb_plugin_wire::v1::sign::SignFn;
use cc_lb_plugin_wire::wire_function::{FallbackPolicy, WireFunction};
use cc_lb_runtime_extism::ExtismRuntimeConfig;
use cc_lb_runtime_extism::dispatch::{DispatchOutcome, dispatch_wire_call};
use extism::{Manifest, PluginBuilder, Wasm};

#[test]
fn extism_runtime_config_has_no_fallback_policy_field() {
    let ExtismRuntimeConfig {
        memory_max_pages,
        fuel_max,
        max_call_duration,
        storage_quota_bytes,
        observe_batch_count,
        observe_flush_interval,
    } = ExtismRuntimeConfig::default();

    assert!(memory_max_pages > 0);
    assert!(fuel_max > 0);
    assert!(max_call_duration > Duration::ZERO);
    assert!(storage_quota_bytes > 0);
    assert!(observe_batch_count > 0);
    assert!(observe_flush_interval > Duration::ZERO);
}

#[test]
fn dispatch_missing_version_uses_each_wire_function_const() {
    let metadata = metadata_without_negotiated_functions();
    let mut plugin = inert_plugin();

    assert_dispatch_fallback::<SignFn>(&mut plugin, &metadata, FallbackPolicy::FailRequest);
    assert_dispatch_fallback::<BuildSignerFn>(&mut plugin, &metadata, FallbackPolicy::FailRequest);
    assert_dispatch_fallback::<ShapeFn>(&mut plugin, &metadata, FallbackPolicy::FailRequest);
    assert_dispatch_fallback::<ObserveFn>(&mut plugin, &metadata, FallbackPolicy::SilentSkip);
    assert_dispatch_fallback::<RouteFn>(&mut plugin, &metadata, FallbackPolicy::UseDefault);
    assert_dispatch_fallback::<NormalizeErrorFn>(
        &mut plugin,
        &metadata,
        FallbackPolicy::PassThrough,
    );
    assert_dispatch_fallback::<OnUnauthorizedFn>(
        &mut plugin,
        &metadata,
        FallbackPolicy::PassThrough,
    );
}

#[test]
fn runtime_sources_do_not_define_fallback_policy_config_or_maps() {
    let forbidden_literals = [
        "\"fallback_policy\"",
        "\"fallback_policies\"",
        "\"fallback-policy\"",
        "\"fallback-policies\"",
        "metadata.get(\"fallback",
        "config.get(\"fallback",
    ];
    let forbidden_compact_patterns = [
        "HashMap<String,FallbackPolicy>",
        "HashMap<&str,FallbackPolicy>",
        "HashMap<&'staticstr,FallbackPolicy>",
        "BTreeMap<String,FallbackPolicy>",
        "BTreeMap<&str,FallbackPolicy>",
        "BTreeMap<&'staticstr,FallbackPolicy>",
    ];

    for (path, source) in runtime_source_files() {
        for pattern in forbidden_literals {
            assert!(
                !source.contains(pattern),
                "{} must not load fallback policy from runtime config pattern {pattern}",
                path.display()
            );
        }

        let compact = compact_source(&source);
        for pattern in forbidden_compact_patterns {
            assert!(
                !compact.contains(pattern),
                "{} must not define dynamic fallback policy map pattern {pattern}",
                path.display()
            );
        }
    }
}

#[test]
fn runtime_fallback_helpers_return_wire_function_const() {
    let dispatch_source = compact_source(include_str!("../src/dispatch.rs"));
    let plugin_wrap_source = compact_source(include_str!("../src/plugin_wrap.rs"));

    assert!(dispatch_source.contains("DispatchOutcome::Fallback(F::FALLBACK)"));
    assert!(plugin_wrap_source.contains("DispatchOutcome::Fallback(F::FALLBACK)"));
}

fn assert_dispatch_fallback<F>(
    plugin: &mut extism::Plugin,
    metadata: &AugmentedMetadata,
    expected: FallbackPolicy,
) where
    F: WireFunction,
    F::Response: std::fmt::Debug + PartialEq,
{
    let outcome = dispatch_wire_call::<F>(plugin, metadata, F::dry_run_request());

    assert_eq!(outcome, DispatchOutcome::Fallback(expected));
}

fn metadata_without_negotiated_functions() -> AugmentedMetadata {
    AugmentedMetadata {
        identity: PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: "fallback-test".to_owned(),
            plugin_version: "1.0.0".to_owned(),
        },
        negotiated_functions: BTreeMap::new(),
        negotiated_capabilities: BTreeSet::new(),
        handshake_completed_at: 1,
        self_check_passed: true,
        self_check_completed_at: 1,
        expires_at: 2,
    }
}

fn inert_plugin() -> extism::Plugin {
    let wasm = wat::parse_str("(module)").expect("inert module wat parses");
    let manifest = Manifest::new([Wasm::data(wasm)]).disallow_all_hosts();
    PluginBuilder::new(&manifest)
        .with_wasi(false)
        .with_cache_disabled()
        .build()
        .expect("inert extism plugin builds")
}

fn runtime_source_files() -> Vec<(PathBuf, String)> {
    let mut files = Vec::new();
    collect_rust_sources(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    files
}

fn collect_rust_sources(dir: &Path, files: &mut Vec<(PathBuf, String)>) {
    for entry in fs::read_dir(dir).expect("runtime src dir is readable") {
        let path = entry.expect("runtime src entry is readable").path();
        if path.is_dir() {
            collect_rust_sources(&path, files);
        } else if path.extension() == Some(OsStr::new("rs")) {
            let source = fs::read_to_string(&path).expect("runtime source file is UTF-8");
            files.push((path, source));
        }
    }
}

fn compact_source(source: &str) -> String {
    source.chars().filter(|ch| !ch.is_whitespace()).collect()
}
