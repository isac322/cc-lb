use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use cc_lb_core::TestClock;
use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
use cc_lb_plugin_wire::limits::SELF_CHECK_OUTPUT_MAX_BYTES;
use cc_lb_plugin_wire::self_check::SelfCheckStatus;
use cc_lb_runtime_extism::handshake::build_offer;
use cc_lb_runtime_extism::registry::{PluginRegistry, RegistryError};
use cc_lb_runtime_extism::self_check::{SelfCheckExecutionError, execute_self_check};
use cc_lb_storage_api::{
    PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus, PluginSlot,
    RepoError,
};
use serde_json::json;
use tokio::sync::Mutex;

#[test]
fn handler_panic_does_not_affect_self_check() {
    let wasm = self_check_module(SelfCheckModule {
        output: success_response().as_bytes(),
        import_user_host: false,
        include_self_check: true,
        shape_panics: true,
        verify_checked_functions: true,
        infinite_loop: false,
    });

    let clock = TestClock::new_at_secs(1_800_000_000);

    let response = execute_self_check(&wasm, &[PluginSlot::Shape], &clock)
        .expect("self-check succeeds without calling shape");

    assert_eq!(response.status, SelfCheckStatus::Success);
    assert!(response.failures.is_empty());
}

#[test]
fn host_fn_call_during_self_check_is_rejected() {
    let output = success_response();
    let wasm = self_check_module(SelfCheckModule {
        output: output.as_bytes(),
        import_user_host: true,
        include_self_check: true,
        shape_panics: false,
        verify_checked_functions: false,
        infinite_loop: false,
    });

    let clock = TestClock::new_at_secs(1_800_000_000);

    let error = execute_self_check(&wasm, &[PluginSlot::Shape], &clock)
        .expect_err("host import is rejected or traps");

    match error {
        SelfCheckExecutionError::Instantiate { .. } | SelfCheckExecutionError::Call { .. } => {}
        other => panic!("expected host-call purity rejection, got {other:?}"),
    }
}

#[test]
fn self_check_timeout_is_rejected() {
    let wasm = self_check_module(SelfCheckModule {
        output: b"",
        import_user_host: false,
        include_self_check: true,
        shape_panics: false,
        verify_checked_functions: false,
        infinite_loop: true,
    });

    let clock = TestClock::new_at_secs(1_800_000_000);

    let error = execute_self_check(&wasm, &[PluginSlot::Shape], &clock)
        .expect_err("infinite loop is rejected");

    match error {
        SelfCheckExecutionError::Call { reason } => {
            let reason = reason.to_ascii_lowercase();
            assert!(
                reason.contains("timeout")
                    || reason.contains("fuel")
                    || reason.contains("interrupt"),
                "expected timeout/fuel failure, got: {reason}"
            );
        }
        other => panic!("expected timeout/fuel call failure, got {other:?}"),
    }
}

#[test]
fn self_check_output_too_large_is_rejected() {
    let output = "x".repeat(SELF_CHECK_OUTPUT_MAX_BYTES + 1);
    let wasm = self_check_module(SelfCheckModule {
        output: output.as_bytes(),
        import_user_host: false,
        include_self_check: true,
        shape_panics: false,
        verify_checked_functions: false,
        infinite_loop: false,
    });

    let clock = TestClock::new_at_secs(1_800_000_000);

    let error = execute_self_check(&wasm, &[PluginSlot::Shape], &clock)
        .expect_err("oversized output is rejected");

    match error {
        SelfCheckExecutionError::OutputTooLarge { bytes, max } => {
            assert_eq!(bytes, SELF_CHECK_OUTPUT_MAX_BYTES + 1);
            assert_eq!(max, SELF_CHECK_OUTPUT_MAX_BYTES);
        }
        other => panic!("expected output-too-large, got {other:?}"),
    }
}

#[test]
fn missing_self_check_export_is_rejected() {
    let wasm = self_check_module(SelfCheckModule {
        output: b"",
        import_user_host: false,
        include_self_check: false,
        shape_panics: false,
        verify_checked_functions: false,
        infinite_loop: false,
    });

    let clock = TestClock::new_at_secs(1_800_000_000);

    let error = execute_self_check(&wasm, &[PluginSlot::Shape], &clock)
        .expect_err("missing export is rejected");

    match error {
        SelfCheckExecutionError::MissingSelfCheckExport => {}
        other => panic!("expected missing self-check export, got {other:?}"),
    }
}

#[tokio::test]
async fn self_check_failure_status_rejects_registration() {
    let repos = Repos::default();
    let registry = PluginRegistry::new(
        repos.registry.clone(),
        repos.blobs.clone(),
        build_offer(&BTreeSet::new()),
        Arc::new(TestClock::new_at_secs(1_800_000_000)),
    )
    .expect("registry builds");
    let output = json!({
        "status": "failure",
        "failures": [{"stage": "wire_function_test", "message": "round-trip failed"}],
        "completed_at": 1,
    })
    .to_string();
    let wasm = registry_plugin_wasm(&output);

    let error = registry
        .register_plugin(&wasm)
        .await
        .expect_err("failure status rejects registration");

    match error {
        RegistryError::SelfCheck(SelfCheckExecutionError::FailureStatus { failures }) => {
            assert_eq!(failures, 1)
        }
        other => panic!("expected registry self-check failure, got {other:?}"),
    }
    assert_eq!(repos.registry.upserts.load(Ordering::SeqCst), 0);
    assert_eq!(repos.blobs.puts.load(Ordering::SeqCst), 0);
}

struct SelfCheckModule<'a> {
    output: &'a [u8],
    import_user_host: bool,
    include_self_check: bool,
    shape_panics: bool,
    verify_checked_functions: bool,
    infinite_loop: bool,
}

fn self_check_module(options: SelfCheckModule<'_>) -> Vec<u8> {
    let output_helper = bytes_helper("self_check_out", options.output);
    let user_import = if options.import_user_host {
        r#"(import "extism:host/user" "cc_lb_log" (func $cc_lb_log (param i64 i64)))"#
    } else {
        ""
    };
    let user_call = if options.import_user_host {
        "  (call $cc_lb_log (call $self_check_out) (call $self_check_out))"
    } else {
        ""
    };
    let shape_body = if options.shape_panics {
        "unreachable"
    } else {
        "i32.const 0"
    };
    let checked_helpers = if options.verify_checked_functions {
        contains_helper("contains_shape", br#""shape""#)
    } else {
        String::new()
    };
    let checked_verification = if options.verify_checked_functions {
        r#"
    (if (i32.eqz (call $contains_shape)) (then unreachable))"#
    } else {
        ""
    };
    let self_check_export = if options.include_self_check {
        if options.infinite_loop {
            r#"
  (func (export "cc_lb_self_check") (result i32)
    (loop $again
      br $again)
    (i32.const 0))"#
                .to_owned()
        } else {
            format!(
                r#"
  (func (export "cc_lb_self_check") (result i32)
{user_call}
{checked_verification}
    (call $output_set (call $self_check_out) (i64.const {len}))
    (i32.const 0))"#,
                len = options.output.len()
            )
        }
    } else {
        String::new()
    };

    let wat = format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (import "extism:host/env" "input_length" (func $input_length (result i64)))
  (import "extism:host/env" "input_load_u8" (func $input_load_u8 (param i64) (result i32)))
  {user_import}
  {output_helper}
  {checked_helpers}
  {self_check_export}
  (func (export "shape") (result i32)
    {shape_body}))
"#,
    );
    wat::parse_str(&wat).expect("self-check wat parses")
}

fn registry_plugin_wasm(self_check_output: &str) -> Vec<u8> {
    let accept = HandshakeAccept {
        handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
        envelope_version: 1,
        chosen_versions: BTreeMap::from([("shape".to_owned(), 1)]),
        plugin_supported: BTreeMap::from([("shape".to_owned(), vec![1])]),
        implemented_functions: BTreeSet::from(["shape".to_owned()]),
        required_capabilities: BTreeSet::new(),
    };
    let handshake_output = serde_json::to_string(&accept).expect("accept serializes");
    let wat = format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {handshake_helper}
  {self_check_helper}
  (func (export "cc_lb_handshake") (result i32)
    (call $output_set (call $handshake_out) (i64.const {handshake_len}))
    (i32.const 0))
  (func (export "cc_lb_self_check") (result i32)
    (call $output_set (call $self_check_out) (i64.const {self_check_len}))
    (i32.const 0))
  (func (export "shape") (result i32)
    (i32.const 0)))
"#,
        handshake_helper = bytes_helper("handshake_out", handshake_output.as_bytes()),
        self_check_helper = bytes_helper("self_check_out", self_check_output.as_bytes()),
        handshake_len = handshake_output.len(),
        self_check_len = self_check_output.len(),
    );
    let mut wasm = wat::parse_str(&wat).expect("registry plugin wat parses");
    append_identity_section(&mut wasm, "self-check-failure", "1.0.0");
    wasm
}

fn success_response() -> String {
    json!({"status": "success", "failures": [], "completed_at": 1}).to_string()
}

fn contains_helper(name: &str, marker: &[u8]) -> String {
    let mut comparisons = String::new();
    for (offset, byte) in marker.iter().enumerate() {
        comparisons.push_str(&format!(
            r#"
      (if (i32.ne (call $input_load_u8 (i64.add (local.get $i) (i64.const {offset}))) (i32.const {byte}))
        (then (local.set $matched (i32.const 0))))
"#,
        ));
    }
    format!(
        r#"
(func ${name} (result i32)
  (local $input_len i64)
  (local $i i64)
  (local $matched i32)
  (local.set $input_len (call $input_length))
  (if (i64.lt_u (local.get $input_len) (i64.const {marker_len}))
    (then (return (i32.const 0))))
  (loop $scan
    (local.set $matched (i32.const 1))
{comparisons}
    (if (local.get $matched)
      (then (return (i32.const 1))))
    (local.set $i (i64.add (local.get $i) (i64.const 1)))
    (br_if $scan (i64.le_u (i64.add (local.get $i) (i64.const {marker_len})) (local.get $input_len))))
  (i32.const 0))
"#,
        marker_len = marker.len()
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

fn append_identity_section(wasm: &mut Vec<u8>, plugin_name: &str, plugin_version: &str) {
    let payload = json!({
        "magic": CC_LB_PLUGIN_MAGIC,
        "abi_envelope": 1,
        "plugin_name": plugin_name,
        "plugin_version": plugin_version,
    })
    .to_string();
    wasm.push(0);
    let mut section = Vec::new();
    encode_u32(CC_LB_PLUGIN_SECTION_NAME.len() as u32, &mut section);
    section.extend_from_slice(CC_LB_PLUGIN_SECTION_NAME.as_bytes());
    section.extend_from_slice(payload.as_bytes());
    encode_u32(section.len() as u32, wasm);
    wasm.extend_from_slice(&section);
}

fn encode_u32(mut value: u32, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

#[derive(Default)]
struct Repos {
    registry: Arc<MemoryRegistryRepo>,
    blobs: Arc<MemoryBlobRepo>,
}

#[derive(Default)]
struct MemoryRegistryRepo {
    records: Mutex<BTreeMap<[u8; 32], PluginRegistryRecord>>,
    upserts: AtomicUsize,
}

#[async_trait]
impl PluginRegistryRepo for MemoryRegistryRepo {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
        self.upserts.fetch_add(1, Ordering::SeqCst);
        self.records
            .lock()
            .await
            .insert(record.sha256, record.clone());
        Ok(())
    }

    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        Ok(self.records.lock().await.get(sha256).cloned())
    }

    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        Ok(self
            .records
            .lock()
            .await
            .values()
            .filter(|record| record.status == PluginRegistryStatus::Active)
            .cloned()
            .collect())
    }

    async fn set_status(
        &self,
        sha256: &[u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        if let Some(record) = self.records.lock().await.get_mut(sha256) {
            record.status = status;
        }
        Ok(())
    }

    async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        self.records.lock().await.remove(sha256);
        Ok(())
    }

    async fn count(&self) -> Result<usize, RepoError> {
        Ok(self.records.lock().await.len())
    }

    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        Ok(None)
    }

    async fn set_shutdown_marker(&self, _unix_secs: i64) -> Result<(), RepoError> {
        Ok(())
    }

    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        Ok(())
    }
}

#[derive(Default)]
struct MemoryBlobRepo {
    blobs: Mutex<BTreeMap<[u8; 32], Vec<u8>>>,
    puts: AtomicUsize,
}

#[async_trait]
impl PluginBlobRepo for MemoryBlobRepo {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.blobs.lock().await.insert(*sha256, bytes.to_vec());
        Ok(())
    }

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        Ok(self.blobs.lock().await.get(sha256).cloned())
    }

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        self.blobs.lock().await.remove(sha256);
        Ok(())
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        Ok(self.blobs.lock().await.keys().copied().collect())
    }
}
