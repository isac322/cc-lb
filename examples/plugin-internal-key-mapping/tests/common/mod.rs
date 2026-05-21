#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use extism::{Manifest, Plugin, PluginBuilder, Wasm};
use serde_json::{json, Value};

pub fn build_wasm() -> PathBuf {
    let workspace = workspace_root();
    let output = Command::new("cargo")
        .current_dir(&workspace)
        .args([
            "build",
            "--target",
            "wasm32-wasip1",
            "--release",
            "-p",
            "plugin-internal-key-mapping",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "wasm build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let wasm = workspace
        .join("target")
        .join("wasm32-wasip1")
        .join("release")
        .join("plugin_internal_key_mapping.wasm");
    assert!(wasm.exists(), "missing wasm at {}", wasm.display());
    wasm
}

pub fn plugin_with_tokens(wasm: &Path, tokens_json: &str) -> Plugin {
    let bytes = std::fs::read(wasm).unwrap();
    let manifest = Manifest::new([Wasm::data(bytes)]).with_config_key("tokens", tokens_json);
    PluginBuilder::new(&manifest)
        .with_wasi(true)
        .with_cache_disabled()
        .build()
        .unwrap()
}

pub fn authenticate(
    plugin: &mut Plugin,
    headers: Vec<(&str, &str)>,
) -> Result<Value, extism::Error> {
    let input = json!({
        "_version": 1,
        "request": {
            "request_id": "req-plugin-internal-key-test",
            "headers": headers.into_iter().map(|(name, value)| {
                json!({"name": name, "value_base64": BASE64.encode(value.as_bytes())})
            }).collect::<Vec<_>>(),
            "method": "POST",
            "path": "/v1/messages",
            "query": null,
            "body_base64": ""
        }
    });
    let output = plugin.call::<String, String>("authenticate", input.to_string())?;
    Ok(serde_json::from_str(&output).unwrap())
}

pub fn alice_tokens() -> &'static str {
    r#"{"ck-internal-alice-X":{"principal_id":"alice","real_credential_kind":"anthropic_api_key","real_credential_storage_key":"alice:real_anthropic_api_key"}}"#
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf()
}
