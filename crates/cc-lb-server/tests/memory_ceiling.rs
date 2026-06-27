use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;

use cc_lb_plugin_api::PluginManifest;
use cc_lb_runtime_extism::{ExtismRuntime, StagedSlot};
use serde_json::json;
use uuid::Uuid;

const PRINCIPAL_COUNT: usize = 200;
const PLUGINS_PER_PRINCIPAL: usize = 3;
const EXPECTED_SLOT_COUNT: usize = PRINCIPAL_COUNT * PLUGINS_PER_PRINCIPAL;
const RSS_DELTA_CEILING_KIB: u64 = 512 * 1024;

#[test]
fn per_principal_plugin_slots_stay_under_memory_ceiling() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = StubWasms::new()?;
    let (_baseline_runtime, baseline_rss_kib) = boot_and_measure(&fixture, false)?;
    let (runtime, loaded_rss_kib) = boot_and_measure(&fixture, true)?;

    let slot_count = runtime.registered_slot_keys().len();
    assert_eq!(slot_count, EXPECTED_SLOT_COUNT);

    let delta_kib = loaded_rss_kib.saturating_sub(baseline_rss_kib);
    eprintln!(
        "memory_ceiling: baseline_rss_kib={baseline_rss_kib} loaded_rss_kib={loaded_rss_kib} delta_kib={delta_kib} ceiling_kib={RSS_DELTA_CEILING_KIB}"
    );
    // 2026-05-28 local measurement for this stub-WAT fixture: baseline 20,096 KiB,
    // loaded 391,920 KiB, delta 371,824 KiB. The 512 MiB ceiling leaves headroom for
    // allocator and CI variance while still catching a per-slot memory regression.
    assert!(
        delta_kib <= RSS_DELTA_CEILING_KIB,
        "RSS delta {delta_kib} KiB exceeds ceiling {RSS_DELTA_CEILING_KIB} KiB"
    );

    Ok(())
}

fn boot_and_measure(
    fixture: &StubWasms,
    include_plugins: bool,
) -> Result<(ExtismRuntime, u64), Box<dyn std::error::Error>> {
    let runtime = ExtismRuntime::with_config(
        cc_lb_runtime_extism::ExtismRuntimeConfig::default(),
        std::sync::Arc::new(cc_lb_core::SystemClock),
    );
    if include_plugins {
        let filter_manifest = fixture.filter_manifest();
        let observe_manifest = fixture.observe_manifest();
        let mut staged = Vec::<StagedSlot>::with_capacity(EXPECTED_SLOT_COUNT);
        for principal_index in 0..PRINCIPAL_COUNT {
            let principal = format!("principal_{principal_index:03}");
            let (_filter, filter_staged) = runtime.instantiate_filter_for(
                &principal,
                Uuid::new_v4(),
                "filter",
                &filter_manifest,
            )?;
            staged.push(filter_staged);
            let (_observe_a, observe_a_staged) = runtime.instantiate_observability_for(
                &principal,
                "observe-a",
                &observe_manifest,
            )?;
            staged.push(observe_a_staged);
            let (_observe_b, observe_b_staged) = runtime.instantiate_observability_for(
                &principal,
                "observe-b",
                &observe_manifest,
            )?;
            staged.push(observe_b_staged);
        }
        runtime.commit_staged(staged)?;
    }
    Ok((runtime, vmrss_kib()?))
}

struct StubWasms {
    _dir: tempfile::TempDir,
    filter_path: PathBuf,
    observe_path: PathBuf,
}

impl StubWasms {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let filter_path = dir.path().join("filter.wasm");
        let observe_path = dir.path().join("observe.wasm");
        fs::write(&filter_path, wat::parse_str(filter_module())?)?;
        fs::write(&observe_path, wat::parse_str(observe_module())?)?;
        Ok(Self {
            _dir: dir,
            filter_path,
            observe_path,
        })
    }

    fn filter_manifest(&self) -> PluginManifest {
        PluginManifest {
            name: "filter".to_owned(),
            artifact: self.filter_path.to_string_lossy().into_owned(),
            wire_version: None,
            config: json!({}),
            metadata: BTreeMap::new(),
        }
    }

    fn observe_manifest(&self) -> PluginManifest {
        PluginManifest {
            name: "observe".to_owned(),
            artifact: self.observe_path.to_string_lossy().into_owned(),
            wire_version: None,
            config: json!({}),
            metadata: BTreeMap::new(),
        }
    }
}

fn vmrss_kib() -> io::Result<u64> {
    let status = fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix("VmRSS:")?.trim();
            value.split_whitespace().next()?.parse::<u64>().ok()
        })
        .ok_or_else(|| io::Error::other("VmRSS missing from /proc/self/status"))
}

fn filter_module() -> &'static str {
    r#"(module (func (export "filter") (result i32) (i32.const 0)))"#
}

fn observe_module() -> &'static str {
    r#"(module (func (export "observe") (result i32) (i32.const 0)))"#
}
