use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use cc_lb_plugin_api::{PluginManifest, PluginRuntime, PrincipalKind, RequestContext};
use cc_lb_runtime_extism::ExtismRuntime;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use serde_json::json;

const REPEATED_INVOKES: usize = 1_000;
const RESOURCE_WARMUP_INVOKES: usize = 64;
const RSS_GROWTH_LIMIT_KIB: u64 = 1_024;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn example_plugins_load_invoke_reload_drop_without_leaks() {
    let artifacts = build_example_plugins();
    let mut reports = Vec::new();

    for scenario in scenarios(&artifacts) {
        let runtime = Arc::new(ExtismRuntime::new());
        let runtime_weak = Arc::downgrade(&runtime);
        let authn = runtime
            .instantiate(&scenario.before_manifest())
            .expect("example plugin loads");
        let authn_weak = Arc::downgrade(&authn);

        let init = authn
            .authenticate(&scenario.before_ctx())
            .await
            .expect("init authenticate succeeds");
        assert_eq!(init.principal.id, scenario.before_principal);
        assert_eq!(init.principal.kind, scenario.principal_kind);

        invoke_concurrent(authn.clone(), scenario.clone(), RESOURCE_WARMUP_INVOKES).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let before_resources = ResourceSnapshot::capture();
        let latencies = invoke_repeated(authn.as_ref(), &scenario, REPEATED_INVOKES).await;
        let after_resources = ResourceSnapshot::capture();
        let resource_delta = ResourceDelta::between(before_resources, after_resources);
        resource_delta.assert_within_limits(scenario.name);

        runtime
            .reload_manifest(&scenario.after_manifest())
            .expect("reload with updated example plugin config succeeds");
        let after_reload = authn
            .authenticate(&scenario.after_ctx())
            .await
            .expect("new call after reload succeeds");
        assert_eq!(after_reload.principal.id, scenario.after_principal);
        assert_eq!(after_reload.principal.kind, scenario.principal_kind);

        drop(authn);
        let wrapper_released = authn_weak.upgrade().is_none();
        drop(runtime);
        let runtime_released = runtime_weak.upgrade().is_none();
        assert!(
            wrapper_released,
            "{} wrapper still has references",
            scenario.name
        );
        assert!(
            runtime_released,
            "{} runtime still has references",
            scenario.name
        );

        let (mean_us, p99_us) = latency_summary_us(latencies);
        reports.push(format!(
            "plugin={} init_principal={} invokes={} mean_us={} p99_us={} reload_new_principal={} resource_metrics={} rss_growth_kib={} rss_threshold_kib={} fd_delta={} thread_delta={} wrapper_released={} runtime_released={}",
            scenario.name,
            init.principal.id,
            REPEATED_INVOKES,
            mean_us,
            p99_us,
            after_reload.principal.id,
            resource_delta.metrics_kind,
            resource_delta.rss_growth_kib,
            RSS_GROWTH_LIMIT_KIB,
            resource_delta.fd_delta,
            resource_delta.thread_delta,
            wrapper_released,
            runtime_released,
        ));
    }

    println!(
        "task_45_example_plugin_lifecycle PASSED plugins=3 wat_only=false {}",
        reports.join(" | ")
    );
}

async fn invoke_repeated(
    authn: &dyn cc_lb_plugin_api::AuthnPlugin,
    scenario: &PluginScenario,
    count: usize,
) -> Vec<Duration> {
    let mut latencies = Vec::with_capacity(count);
    for call_index in 0..count {
        let started = Instant::now();
        let outcome = authn
            .authenticate(&scenario.before_ctx())
            .await
            .unwrap_or_else(|error| {
                panic!("{} invoke {call_index} failed: {error}", scenario.name)
            });
        let elapsed = started.elapsed();
        assert_eq!(outcome.principal.id, scenario.before_principal);
        assert_eq!(outcome.principal.kind, scenario.principal_kind);
        latencies.push(elapsed);
    }
    latencies
}

async fn invoke_concurrent(
    authn: Arc<dyn cc_lb_plugin_api::AuthnPlugin>,
    scenario: PluginScenario,
    count: usize,
) {
    let mut handles = Vec::with_capacity(count);
    for call_index in 0..count {
        let authn = authn.clone();
        let scenario = scenario.clone();
        handles.push(tokio::spawn(async move {
            let outcome = authn
                .authenticate(&scenario.before_ctx())
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "{} warmup invoke {call_index} failed: {error}",
                        scenario.name
                    )
                });
            assert_eq!(outcome.principal.id, scenario.before_principal);
            assert_eq!(outcome.principal.kind, scenario.principal_kind);
        }));
    }
    for handle in handles {
        handle.await.expect("warmup task joins");
    }
}

fn build_example_plugins() -> ExampleArtifacts {
    let workspace = workspace_root();
    let packages = [
        "plugin-api-key",
        "plugin-oauth-anthropic-pro",
        "plugin-internal-key-mapping",
    ];
    let output = Command::new("cargo")
        .current_dir(&workspace)
        .args(["build", "--target", "wasm32-wasip1", "--release"])
        .args(packages.iter().flat_map(|package| ["-p", *package]))
        .output()
        .expect("cargo build for example plugins starts");
    assert!(
        output.status.success(),
        "example plugin wasm build failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let target_dir = target_dir(&workspace);
    ExampleArtifacts {
        api_key: wasm_artifact(&target_dir, "plugin_api_key.wasm"),
        oauth: wasm_artifact(&target_dir, "plugin_oauth_anthropic_pro.wasm"),
        internal: wasm_artifact(&target_dir, "plugin_internal_key_mapping.wasm"),
    }
}

fn wasm_artifact(target_dir: &Path, file_name: &str) -> PathBuf {
    let path = target_dir
        .join("wasm32-wasip1")
        .join("release")
        .join(file_name);
    assert!(path.exists(), "missing wasm artifact at {}", path.display());
    path
}

fn target_dir(workspace: &Path) -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(value) => {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                path
            } else {
                workspace.join(path)
            }
        }
        None => workspace.join("target"),
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("plugin-lifecycle package is under tests/plugin-lifecycle")
        .to_path_buf()
}

fn scenarios(artifacts: &ExampleArtifacts) -> Vec<PluginScenario> {
    vec![
        PluginScenario {
            name: "plugin-api-key",
            artifact: artifacts.api_key.clone(),
            config_key: "keys",
            before_config: r#"{"alice":"sk-ant-lifecycle-before"}"#,
            after_config: r#"{"bob":"sk-ant-lifecycle-after"}"#,
            before_headers: vec![("x-api-key", "sk-ant-lifecycle-before")],
            after_headers: vec![("x-api-key", "sk-ant-lifecycle-after")],
            before_principal: "alice",
            after_principal: "bob",
            principal_kind: PrincipalKind::ApiKey,
        },
        PluginScenario {
            name: "plugin-oauth-anthropic-pro",
            artifact: artifacts.oauth.clone(),
            config_key: "config",
            before_config: r#"{"client_id":"id","principals":{"alice":{"refresh_token_storage_key":"alice:anthropic_oauth","token_prefix":"sk-ant-oat01-LC-before"}}}"#,
            after_config: r#"{"client_id":"id","principals":{"bob":{"refresh_token_storage_key":"bob:anthropic_oauth","token_prefix":"sk-ant-oat01-LC-after"}}}"#,
            before_headers: vec![("authorization", "Bearer sk-ant-oat01-LC-before-token")],
            after_headers: vec![("authorization", "Bearer sk-ant-oat01-LC-after-token")],
            before_principal: "alice",
            after_principal: "bob",
            principal_kind: PrincipalKind::SubscriptionBearer,
        },
        PluginScenario {
            name: "plugin-internal-key-mapping",
            artifact: artifacts.internal.clone(),
            config_key: "tokens",
            before_config: r#"{"ck-internal-lifecycle-before":{"principal_id":"alice","real_credential_kind":"anthropic_api_key","real_credential_storage_key":"alice:real_anthropic_api_key"}}"#,
            after_config: r#"{"ck-internal-lifecycle-after":{"principal_id":"bob","real_credential_kind":"anthropic_api_key","real_credential_storage_key":"bob:real_anthropic_api_key"}}"#,
            before_headers: vec![("authorization", "Bearer ck-internal-lifecycle-before")],
            after_headers: vec![("authorization", "Bearer ck-internal-lifecycle-after")],
            before_principal: "alice",
            after_principal: "bob",
            principal_kind: PrincipalKind::InternalKey,
        },
    ]
}

struct ExampleArtifacts {
    api_key: PathBuf,
    oauth: PathBuf,
    internal: PathBuf,
}

#[derive(Clone)]
struct PluginScenario {
    name: &'static str,
    artifact: PathBuf,
    config_key: &'static str,
    before_config: &'static str,
    after_config: &'static str,
    before_headers: Vec<(&'static str, &'static str)>,
    after_headers: Vec<(&'static str, &'static str)>,
    before_principal: &'static str,
    after_principal: &'static str,
    principal_kind: PrincipalKind,
}

impl PluginScenario {
    fn before_manifest(&self) -> PluginManifest {
        self.manifest(self.before_config)
    }

    fn after_manifest(&self) -> PluginManifest {
        self.manifest(self.after_config)
    }

    fn manifest(&self, config_value: &'static str) -> PluginManifest {
        PluginManifest {
            name: self.name.to_owned(),
            artifact: self.artifact.to_string_lossy().into_owned(),
            config: json!({ self.config_key: config_value }),
            metadata: BTreeMap::new(),
        }
    }

    fn before_ctx(&self) -> RequestContext {
        request_context(&self.before_headers)
    }

    fn after_ctx(&self) -> RequestContext {
        request_context(&self.after_headers)
    }
}

fn request_context(headers: &[(&str, &str)]) -> RequestContext {
    let mut header_map = HeaderMap::new();
    for (name, value) in headers {
        header_map.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("test header name is valid"),
            HeaderValue::from_str(value).expect("test header value is valid"),
        );
    }

    RequestContext {
        request_id: "req-plugin-lifecycle".to_owned(),
        downstream_headers: header_map,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
    }
}

fn latency_summary_us(mut latencies: Vec<Duration>) -> (u128, u128) {
    latencies.sort_unstable();
    let total_us = latencies.iter().map(Duration::as_micros).sum::<u128>();
    let mean_us = total_us / latencies.len() as u128;
    let p99_index = ((latencies.len() * 99) / 100).min(latencies.len() - 1);
    (mean_us, latencies[p99_index].as_micros())
}

#[derive(Clone, Copy)]
struct ResourceSnapshot {
    metrics_kind: &'static str,
    rss_kib: Option<u64>,
    fd_count: Option<usize>,
    thread_count: Option<usize>,
}

impl ResourceSnapshot {
    fn capture() -> Self {
        Self {
            metrics_kind: metrics_kind(),
            rss_kib: current_rss_kib(),
            fd_count: current_fd_count(),
            thread_count: current_thread_count(),
        }
    }
}

#[derive(Clone, Copy)]
struct ResourceDelta {
    metrics_kind: &'static str,
    rss_growth_kib: u64,
    fd_delta: isize,
    thread_delta: isize,
}

impl ResourceDelta {
    fn between(before: ResourceSnapshot, after: ResourceSnapshot) -> Self {
        Self {
            metrics_kind: after.metrics_kind,
            rss_growth_kib: growth(before.rss_kib, after.rss_kib),
            fd_delta: signed_delta(before.fd_count, after.fd_count),
            thread_delta: signed_delta(before.thread_count, after.thread_count),
        }
    }

    fn assert_within_limits(&self, plugin_name: &str) {
        if self.metrics_kind == "unsupported" {
            return;
        }
        assert!(
            self.rss_growth_kib < RSS_GROWTH_LIMIT_KIB,
            "{plugin_name} RSS growth {} KiB exceeded {} KiB",
            self.rss_growth_kib,
            RSS_GROWTH_LIMIT_KIB,
        );
        assert_eq!(
            self.fd_delta, 0,
            "{plugin_name} file descriptor count changed"
        );
        assert_eq!(self.thread_delta, 0, "{plugin_name} thread count changed");
    }
}

fn growth(before: Option<u64>, after: Option<u64>) -> u64 {
    match (before, after) {
        (Some(before), Some(after)) => after.saturating_sub(before),
        _ => 0,
    }
}

fn signed_delta(before: Option<usize>, after: Option<usize>) -> isize {
    match (before, after) {
        (Some(before), Some(after)) => after as isize - before as isize,
        _ => 0,
    }
}

#[cfg(target_os = "linux")]
fn metrics_kind() -> &'static str {
    "linux_proc"
}

#[cfg(not(target_os = "linux"))]
fn metrics_kind() -> &'static str {
    "unsupported"
}

#[cfg(target_os = "linux")]
fn current_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?;
        value.split_whitespace().next()?.parse().ok()
    })
}

#[cfg(not(target_os = "linux"))]
fn current_rss_kib() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn current_fd_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd").ok().map(Iterator::count)
}

#[cfg(not(target_os = "linux"))]
fn current_fd_count() -> Option<usize> {
    None
}

#[cfg(target_os = "linux")]
fn current_thread_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(Iterator::count)
}

#[cfg(not(target_os = "linux"))]
fn current_thread_count() -> Option<usize> {
    None
}
