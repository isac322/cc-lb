use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME, PluginIdentity};
use cc_lb_runtime_extism::handshake::build_offer;
use cc_lb_runtime_extism::registry::PluginRegistry;
use cc_lb_server::startup_handshake::{
    StartupHandshakeOpts, run_startup_handshake_with_slot_store,
};
use cc_lb_storage_api::{
    BackendKind, PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus,
    PluginRegistryStore, PluginSlot, Storage as StorageTrait, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_redb::RedbStorage;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use uuid::Uuid;

#[tokio::test(flavor = "current_thread")]
async fn startup_rehandshake_updates_supported_slots_and_warns_on_drift() -> Result<()> {
    let storage = Arc::new(RedbStorage::open_in_memory([77; 32])?);
    storage.initialize(BackendKind::Redb)?;
    let registry = PluginRegistry::new(
        storage.clone() as Arc<dyn PluginRegistryRepo>,
        storage.clone() as Arc<dyn PluginBlobRepo>,
        build_offer(&BTreeSet::new()),
    )?;

    let wasm = plugin_wasm("drift-plugin", "1.0.0", &["filter", "shape"]);
    let sha256 = sha256(&wasm);
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256,
                size_bytes: wasm.len() as u64,
                bytes: wasm.clone(),
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                name: "drift-plugin".to_owned(),
                original_filename: "drift-plugin.wasm".to_owned(),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                wire_version: 1,
                supported_slots: vec![PluginSlot::Router],
            },
        )
        .await?;
    storage.put_blob(&sha256, &wasm).await?;
    storage
        .upsert_record(&plugin_record(
            sha256,
            "drift-plugin",
            "1.0.0",
            registry.host_offer_hash(),
            unix_now()? - 120,
            &["filter"],
        )?)
        .await?;

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_target(true)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let (_tx, shutdown) = watch::channel(false);

    let report = run_startup_handshake_with_slot_store(
        &registry,
        storage.as_ref(),
        storage.clone() as Arc<dyn StorageTrait>,
        StartupHandshakeOpts {
            skip_if_fresh: false,
            force: true,
            ..StartupHandshakeOpts::default()
        },
        shutdown,
    )
    .await;

    assert!(
        report.errors.is_empty(),
        "unexpected startup errors: {:?}",
        report.errors
    );
    assert_eq!(report.re_handshaked, 1);
    let after = storage
        .get_registry_entry_by_id(entry.id)
        .await?
        .expect("registry entry remains present");
    assert_eq!(
        after.supported_slots,
        vec![PluginSlot::Router, PluginSlot::Shape]
    );
    let output = logs.contents();
    assert!(
        output.contains("cc_lb_server::drift"),
        "expected drift target in logs:\n{output}"
    );
    assert!(
        output.contains("supported_slots drift on re-handshake"),
        "expected drift message in logs:\n{output}"
    );

    Ok(())
}

fn plugin_record(
    sha256: [u8; 32],
    plugin_name: &str,
    plugin_version: &str,
    host_offer_hash: [u8; 32],
    last_handshake_at: i64,
    functions: &[&str],
) -> Result<PluginRegistryRecord> {
    let identity = PluginIdentity {
        magic: CC_LB_PLUGIN_MAGIC,
        abi_envelope: 1,
        plugin_name: plugin_name.to_owned(),
        plugin_version: plugin_version.to_owned(),
    };
    let augmented_metadata = AugmentedMetadata::from_handshake_and_self_check(
        identity,
        function_versions(functions),
        BTreeSet::new(),
        last_handshake_at,
        true,
        last_handshake_at,
        60,
    )?;

    Ok(PluginRegistryRecord {
        sha256,
        plugin_name: plugin_name.to_owned(),
        plugin_version: plugin_version.to_owned(),
        abi_envelope: 1,
        augmented_metadata,
        host_offer_hash,
        handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
        last_handshake_at,
        status: PluginRegistryStatus::Active,
    })
}

fn plugin_wasm(plugin_name: &str, plugin_version: &str, functions: &[&str]) -> Vec<u8> {
    let accept = HandshakeAccept {
        handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
        envelope_version: 1,
        chosen_versions: function_versions(functions),
        plugin_supported: functions
            .iter()
            .map(|function| ((*function).to_owned(), vec![1]))
            .collect(),
        implemented_functions: functions
            .iter()
            .map(|function| (*function).to_owned())
            .collect(),
        required_capabilities: BTreeSet::new(),
    };
    let handshake_output = serde_json::to_string(&accept).expect("accept serializes");
    let self_check_output =
        serde_json::json!({"status":"success","failures":[],"completed_at":1}).to_string();
    let exports = functions
        .iter()
        .map(|function| {
            format!(
                r#"  (func (export "{function}") (result i32)
    (i32.const 0))
"#
            )
        })
        .collect::<String>();
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
{exports})
"#,
        handshake_helper = bytes_helper("handshake_out", handshake_output.as_bytes()),
        self_check_helper = bytes_helper("self_check_out", self_check_output.as_bytes()),
        handshake_len = handshake_output.len(),
        self_check_len = self_check_output.len(),
    );
    let mut wasm = wat::parse_str(&wat).expect("wat parses");
    append_identity_section(&mut wasm, plugin_name, plugin_version);
    wasm
}

fn function_versions(functions: &[&str]) -> BTreeMap<String, u32> {
    functions
        .iter()
        .map(|function| ((*function).to_owned(), 1))
        .collect()
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
    let payload = serde_json::json!({
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

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn unix_now() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    )?)
}

#[derive(Clone, Default)]
struct CapturedLogs {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        let bytes = self.inner.lock().expect("captured logs lock");
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

struct CapturedWriter {
    logs: CapturedLogs,
}

impl Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.logs
            .inner
            .lock()
            .expect("captured logs lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWriter { logs: self.clone() }
    }
}
