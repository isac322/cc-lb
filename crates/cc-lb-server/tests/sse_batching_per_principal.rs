use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::sync::{Arc, Mutex};

use cc_lb_plugin_api::{ObservabilityHook, ObserveEvent, PluginManifest};
use cc_lb_runtime_extism::ExtismRuntime;
use serde_json::{Value, json};
use tempfile::TempDir;
use tracing_subscriber::fmt::MakeWriter;

const FLUSH_LOG: &str = "cc-lb-test-sse-flush";
const OBSERVE_FLUSH_MS: u64 = 60_000;

#[test]
fn same_plugin_name_with_different_principals_keeps_sse_batches_independent() {
    let logs = install_log_capture();

    let fixture = wasm_fixture("audit", logging_observe_module());
    let principal_a = HookConfig {
        principal_id: "principal-a",
        plugin_name: "audit",
        sse_per_event: true,
        observe_batch_count: 1,
    };
    let principal_b = HookConfig {
        principal_id: "principal-b",
        plugin_name: "audit",
        sse_per_event: false,
        observe_batch_count: 5,
    };

    assert!(principal_a.sse_per_event);
    assert!(!principal_b.sse_per_event);

    let runtime = ExtismRuntime::with_config(
        cc_lb_runtime_extism::ExtismRuntimeConfig::default(),
        std::sync::Arc::new(cc_lb_core::SystemClock),
    );
    let (hook_a, staged_a) = runtime
        .instantiate_observability_for(
            principal_a.principal_id,
            principal_a.plugin_name,
            &manifest_for(&fixture, &principal_a),
        )
        .expect("principal A audit hook stages");
    let (hook_b, staged_b) = runtime
        .instantiate_observability_for(
            principal_b.principal_id,
            principal_b.plugin_name,
            &manifest_for(&fixture, &principal_b),
        )
        .expect("principal B audit hook stages");
    runtime
        .commit_staged(vec![staged_a, staged_b])
        .expect("staged principal hooks commit");

    let mut registered = runtime.registered_slot_keys();
    registered.sort();
    assert_eq!(
        registered,
        vec![
            ("principal-a".to_owned(), "audit".to_owned()),
            ("principal-b".to_owned(), "audit".to_owned()),
        ]
    );

    observe_chunks(hook_a.as_ref(), 0..3);
    assert_eq!(logs.flush_count(), 3);

    observe_chunks(hook_b.as_ref(), 0..3);
    assert_eq!(logs.flush_count(), 3);

    drop(hook_a);
    assert_eq!(logs.flush_count(), 3);

    observe_chunks(hook_b.as_ref(), 3..4);
    assert_eq!(logs.flush_count(), 3);

    observe_chunks(hook_b.as_ref(), 4..5);
    assert_eq!(logs.flush_count(), 4);

    drop(hook_b);
    assert_eq!(logs.flush_count(), 4);
}

fn observe_chunks(hook: &dyn ObservabilityHook, range: std::ops::Range<u64>) {
    for batch_index in range {
        hook.observe(ObserveEvent::Chunk {
            batch_index,
            event_count: 1,
            total_bytes: 16,
        })
        .expect("observe event succeeds");
    }
}

struct HookConfig {
    principal_id: &'static str,
    plugin_name: &'static str,
    sse_per_event: bool,
    observe_batch_count: u64,
}

struct WasmFixture {
    _dir: TempDir,
    artifact: String,
}

fn wasm_fixture(name: &str, wat: String) -> WasmFixture {
    let dir = tempfile::tempdir().expect("tempdir is created");
    let wasm = wat::parse_str(&wat).expect("wat parses");
    let artifact = dir.path().join(format!("{name}.wasm"));
    fs::write(&artifact, wasm).expect("wasm fixture is written");
    WasmFixture {
        _dir: dir,
        artifact: artifact.to_string_lossy().into_owned(),
    }
}

fn manifest_for(fixture: &WasmFixture, config: &HookConfig) -> PluginManifest {
    PluginManifest {
        name: config.plugin_name.to_owned(),
        artifact: fixture.artifact.clone(),
        wire_version: None,
        config: json!({}),
        metadata: metadata(&[
            ("observe_batch_count", config.observe_batch_count),
            ("observe_flush_ms", OBSERVE_FLUSH_MS),
        ]),
    }
}

fn metadata(pairs: &[(&str, u64)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), Value::from(*value)))
        .collect()
}

fn logging_observe_module() -> String {
    module(&format!(
        r#"
{level}
{message}
{response}
(func (export "observe") (result i32)
  (call $cc_lb_log (call $level) (call $message))
  (call $output_set (call $response) (i64.const {response_len}))
  (i32.const 0))
"#,
        level = bytes_helper("level", b"info"),
        message = bytes_helper("message", FLUSH_LOG.as_bytes()),
        response = bytes_helper("response", br#"{"_version":1}"#),
        response_len = br#"{"_version":1}"#.len()
    ))
}

fn module(functions: &str) -> String {
    format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (import "extism:host/user" "cc_lb_log" (func $cc_lb_log (param i64 i64)))
  {functions}
)
"#
    )
}

fn bytes_helper(name: &str, bytes: &[u8]) -> String {
    let mut stores = String::new();
    for (index, byte) in bytes.iter().enumerate() {
        stores.push_str(&format!(
            "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
        ));
    }
    format!(
        r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
        len = bytes.len()
    )
}

#[derive(Clone, Default)]
struct LogSink {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl LogSink {
    fn flush_count(&self) -> usize {
        let bytes = self.bytes.lock().expect("log sink lock").clone();
        String::from_utf8_lossy(&bytes).matches(FLUSH_LOG).count()
    }
}

struct LogWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .expect("log sink lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for LogSink {
    type Writer = LogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        LogWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

fn install_log_capture() -> LogSink {
    let logs = LogSink::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(logs.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("install tracing subscriber");
    logs
}
