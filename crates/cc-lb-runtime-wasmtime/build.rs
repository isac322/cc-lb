//! Pre-build the wasm32 fixture artifacts the integration tests load.
//!
//! Without this, `cargo test -p cc-lb-runtime-wasmtime` silently skips
//! `shape_round_trip` / `observe_drain` / `cache_aware_wasmtime_e2e`
//! when no human pre-built the wasm — letting broken plugins ship as
//! "tests pass". Stage 1 protocol audit (S2 blocker) flagged this.
//!
//! Strategy: spawn `cargo build --target wasm32-unknown-unknown
//! --release -p <fixture>` for each fixture crate. Failure here turns
//! a missing/broken fixture into a hard build failure, which is what
//! the audit demanded.

use std::path::{Path, PathBuf};
use std::process::Command;

struct Fixture {
    crate_name: &'static str,
    crate_path: &'static str,
}

const FIXTURE_CRATES: &[Fixture] = &[
    Fixture {
        crate_name: "wasmtime-shape-passthrough",
        crate_path: "../../plugins/test-fixtures/wasmtime-shape-passthrough",
    },
    Fixture {
        crate_name: "wasmtime-observe-noop",
        crate_path: "../../plugins/test-fixtures/wasmtime-observe-noop",
    },
    Fixture {
        crate_name: "wasmtime-filter-service-tier",
        crate_path: "../../plugins/test-fixtures/wasmtime-filter-service-tier",
    },
    Fixture {
        crate_name: "cache-aware-wasmtime",
        crate_path: "../../plugins/router/cache-aware-wasmtime",
    },
];

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CC_LB_SKIP_WASM_FIXTURE_BUILD");
    if std::env::var("CC_LB_SKIP_WASM_FIXTURE_BUILD").as_deref() == Ok("1") {
        return;
    }
    emit_rerun_directives(&manifest_dir);

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root from cc-lb-runtime-wasmtime crate path");

    let nested_target_dir = PathBuf::from(&out_dir).join("wasm-fixture-target");

    let mut args = vec![
        "build".to_string(),
        "--locked".to_string(),
        "--target".to_string(),
        "wasm32-unknown-unknown".to_string(),
        "--release".to_string(),
        "--target-dir".to_string(),
        nested_target_dir.to_string_lossy().into_owned(),
    ];
    for f in FIXTURE_CRATES {
        args.push("-p".to_string());
        args.push(f.crate_name.to_string());
    }

    let status = Command::new(&cargo)
        .args(&args)
        .current_dir(workspace_root)
        .env_remove("CARGO_TARGET_DIR")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("RUSTC_WRAPPER")
        .status()
        .expect("failed to spawn cargo for wasm fixture build");

    if !status.success() {
        panic!(
            "wasm fixture build failed (exit {:?}). \
             Run `cargo build --locked --target wasm32-unknown-unknown --release {}` \
             from the workspace root to reproduce. \
             Set CC_LB_SKIP_WASM_FIXTURE_BUILD=1 to bypass when the \
             integration tests are not in scope (e.g. release builds of \
             cc-lb-server).",
            status.code(),
            FIXTURE_CRATES
                .iter()
                .map(|f| format!("-p {}", f.crate_name))
                .collect::<Vec<_>>()
                .join(" "),
        );
    }

    let canonical_dir = workspace_root.join("target/wasm32-unknown-unknown/release");
    std::fs::create_dir_all(&canonical_dir).unwrap_or_else(|e| {
        panic!(
            "failed to create canonical wasm fixture dir {}: {}",
            canonical_dir.display(),
            e
        )
    });
    let nested_release_dir = nested_target_dir.join("wasm32-unknown-unknown/release");
    for f in FIXTURE_CRATES {
        let file = format!("{}.wasm", f.crate_name.replace('-', "_"));
        let src = nested_release_dir.join(&file);
        let dst = canonical_dir.join(&file);
        std::fs::copy(&src, &dst).unwrap_or_else(|e| {
            panic!(
                "failed to copy wasm fixture {} -> {}: {}",
                src.display(),
                dst.display(),
                e
            )
        });
    }
}

fn emit_rerun_directives(manifest_dir: &Path) {
    for path in ["../../Cargo.toml", "../../Cargo.lock"] {
        emit_rerun_tree(manifest_dir, Path::new(path));
    }
    for fixture in FIXTURE_CRATES {
        emit_crate_rerun_directives(manifest_dir, Path::new(fixture.crate_path));
    }
    for crate_path in [
        "../../crates/cc-lb-pdk-wasmtime",
        "../../crates/cc-lb-pdk-wasmtime-macros",
        "../../crates/cc-lb-plugin-wire",
    ] {
        emit_crate_rerun_directives(manifest_dir, Path::new(crate_path));
    }
}

fn emit_crate_rerun_directives(manifest_dir: &Path, crate_path: &Path) {
    emit_rerun_tree(manifest_dir, &crate_path.join("Cargo.toml"));
    emit_rerun_tree(manifest_dir, &crate_path.join("src"));

    let build_script = crate_path.join("build.rs");
    if manifest_dir.join(&build_script).is_file() {
        emit_rerun_tree(manifest_dir, &build_script);
    }
}

fn emit_rerun_tree(manifest_dir: &Path, relative_path: &Path) {
    let path = manifest_dir.join(relative_path);
    println!("cargo:rerun-if-changed={}", relative_path.display());

    if path.is_file() {
        return;
    }
    if !path.exists() {
        return;
    }
    if !path.is_dir() {
        panic!(
            "wasm fixture build input is not a file or directory: {}",
            path.display()
        );
    }

    let mut entries = std::fs::read_dir(&path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read wasm fixture build input directory {}: {error}",
                path.display()
            )
        })
        .map(|entry| {
            entry.unwrap_or_else(|error| {
                panic!(
                    "failed to read an entry in wasm fixture build input directory {}: {error}",
                    path.display()
                )
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        emit_rerun_tree(manifest_dir, &relative_path.join(entry.file_name()));
    }
}
