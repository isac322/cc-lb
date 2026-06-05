#![allow(dead_code)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[path = "src/types.rs"]
mod types;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src/types.rs");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .ok_or("cannot resolve workspace root")?;
    let schema = schemars::schema_for!(types::Config);
    let schema_json = serde_json::to_string_pretty(&schema)?;
    let schema_path = workspace_root.join("config-schema.json");
    let schema_json = format!("{schema_json}\n");
    if fs::read_to_string(&schema_path).ok().as_deref() != Some(schema_json.as_str()) {
        fs::write(schema_path, schema_json)?;
    }
    Ok(())
}
