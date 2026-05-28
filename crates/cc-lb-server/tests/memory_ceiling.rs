mod reload_common;

use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::reload::ConfigWatcher;

const PRINCIPAL_COUNT: usize = 200;
const PLUGINS_PER_PRINCIPAL: usize = 3;
const EXPECTED_SLOT_COUNT: usize = PRINCIPAL_COUNT * PLUGINS_PER_PRINCIPAL;
const RSS_DELTA_CEILING_KIB: u64 = 512 * 1024;

#[test]
fn per_principal_plugin_slots_stay_under_memory_ceiling() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = StubWasms::new()?;
    let baseline_dir = tempfile::tempdir()?;
    let baseline_path = baseline_dir.path().join("cc-lb-baseline.toml");
    write_config(&baseline_path, &fixture, false)?;
    let (_baseline_runtime, baseline_rss_kib) = boot_and_measure(&baseline_path)?;

    let loaded_dir = tempfile::tempdir()?;
    let loaded_path = loaded_dir.path().join("cc-lb-loaded.toml");
    write_config(&loaded_path, &fixture, true)?;
    let (runtime, loaded_rss_kib) = boot_and_measure(&loaded_path)?;

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
    config_path: &Path,
) -> Result<(Arc<ExtismRuntime>, u64), Box<dyn std::error::Error>> {
    let config = reload_common::load_config(config_path);
    let runtime = Arc::new(ExtismRuntime::new());
    let principal_view = Arc::new(ArcSwap::from(PrincipalView::from_config(
        &config,
        HashMap::new(),
    )?));
    let watcher = ConfigWatcher::new_with_principal_view(
        config_path,
        config,
        runtime.clone(),
        Some(principal_view),
    );
    watcher.reload_now()?;
    Ok((runtime, vmrss_kib()?))
}

struct StubWasms {
    _dir: tempfile::TempDir,
    router_path: PathBuf,
    observe_path: PathBuf,
}

impl StubWasms {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let router_path = dir.path().join("router.wasm");
        let observe_path = dir.path().join("observe.wasm");
        fs::write(&router_path, wat::parse_str(router_module())?)?;
        fs::write(&observe_path, wat::parse_str(observe_module())?)?;
        Ok(Self {
            _dir: dir,
            router_path,
            observe_path,
        })
    }
}

fn write_config(path: &Path, fixture: &StubWasms, include_plugins: bool) -> io::Result<()> {
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().expect("proxy addr parses");
    let mut config = format!(
        r#"[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "127.0.0.1:19090"
metrics_addr = "127.0.0.1:19091"

[body]
messages_cap_bytes = 1048576
files_cap_bytes = 1048576

"#
    );

    let router_path = reload_common::toml_path(&fixture.router_path);
    let observe_path = reload_common::toml_path(&fixture.observe_path);
    for principal_index in 0..PRINCIPAL_COUNT {
        config.push_str(&format!(
            r#"[principals.principal_{principal_index:03}]
allowed_models = ["*"]

"#
        ));
        if include_plugins {
            config.push_str(&format!(
                r#"[principals.principal_{principal_index:03}.router_plugin]
name = "router"
wasm_path = "{router_path}"

[[principals.principal_{principal_index:03}.observability_hooks]]
name = "observe-a"
wasm_path = "{observe_path}"

[[principals.principal_{principal_index:03}.observability_hooks]]
name = "observe-b"
wasm_path = "{observe_path}"

"#
            ));
        }
    }

    fs::write(path, config)
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

fn router_module() -> &'static str {
    r#"(module (func (export "route") (result i32) (i32.const 0)))"#
}

fn observe_module() -> &'static str {
    r#"(module (func (export "observe") (result i32) (i32.const 0)))"#
}
