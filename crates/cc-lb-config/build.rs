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
    fs::write(
        workspace_root.join("config-schema.json"),
        format!("{schema_json}\n"),
    )?;
    Ok(())
}
