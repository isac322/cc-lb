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
            "plugin-oauth-anthropic-pro",
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
        .join("plugin_oauth_anthropic_pro.wasm");
    assert!(wasm.exists(), "missing wasm at {}", wasm.display());
    wasm
}

pub fn plugin_with_config(wasm: &Path, config_json: &str) -> Plugin {
    let bytes = std::fs::read(wasm).unwrap();
    let manifest = Manifest::new([Wasm::data(bytes)]).with_config_key("config", config_json);
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
            "request_id": "req-plugin-oauth-test",
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

pub fn alice_config() -> &'static str {
    r#"{"client_id":"id","principals":{"alice":{"refresh_token_storage_key":"alice:anthropic_oauth","token_prefix":"sk-ant-oat01-MOCK-alice"}}}"#
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf()
}
