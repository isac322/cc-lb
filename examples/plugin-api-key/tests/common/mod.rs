#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use extism::{Manifest, Plugin, PluginBuilder, Wasm};
use serde_json::{json, Value};

pub fn build_wasm() -> PathBuf {
    let workspace = workspace_root();
    let mut failures = Vec::new();
    for target in ["wasm32-wasi", "wasm32-wasip1"] {
        let output = Command::new("cargo")
            .current_dir(&workspace)
            .args([
                "build",
                "--target",
                target,
                "--release",
                "-p",
                "plugin-api-key",
            ])
            .output()
            .unwrap();
        if output.status.success() {
            let wasm = workspace
                .join("target")
                .join(target)
                .join("release")
                .join("plugin_api_key.wasm");
            assert!(wasm.exists(), "missing wasm at {}", wasm.display());
            return wasm;
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("target may not be installed") && rustup_available() {
            let add = Command::new("rustup")
                .args(["target", "add", target])
                .output()
                .unwrap();
            assert!(
                add.status.success(),
                "rustup target add {target} failed: {}",
                String::from_utf8_lossy(&add.stderr)
            );
            continue;
        }

        failures.push(format!(
            "target={target} status={} stderr={}",
            output.status, stderr
        ));
    }
    panic!("wasm build failed:\n{}", failures.join("\n"));
}

pub fn plugin_with_keys(wasm: &Path, keys_json: &str) -> Plugin {
    let bytes = std::fs::read(wasm).unwrap();
    let manifest = Manifest::new([Wasm::data(bytes)]).with_config_key("keys", keys_json);
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
            "request_id": "req-plugin-api-key-test",
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

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf()
}

fn rustup_available() -> bool {
    Command::new("rustup").arg("--version").output().is_ok()
}
