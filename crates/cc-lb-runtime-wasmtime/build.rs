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

use std::path::PathBuf;
use std::process::Command;

struct Fixture {
    crate_name: &'static str,
    src_path: &'static str,
}

const FIXTURE_CRATES: &[Fixture] = &[
    Fixture {
        crate_name: "wasmtime-shape-passthrough",
        src_path: "../../plugins/test-fixtures/wasmtime-shape-passthrough/src/lib.rs",
    },
    Fixture {
        crate_name: "wasmtime-observe-noop",
        src_path: "../../plugins/test-fixtures/wasmtime-observe-noop/src/lib.rs",
    },
    Fixture {
        crate_name: "wasmtime-filter-v2",
        src_path: "../../plugins/test-fixtures/wasmtime-filter-v2/src/lib.rs",
    },
    Fixture {
        crate_name: "cache-aware-wasmtime",
        src_path: "../../plugins/router/cache-aware-wasmtime/src/lib.rs",
    },
];

fn main() {
    for f in FIXTURE_CRATES {
        println!("cargo:rerun-if-changed={}", f.src_path);
    }
    println!("cargo:rerun-if-changed=build.rs");

    if std::env::var_os("CC_LB_SKIP_WASM_FIXTURE_BUILD").is_some() {
        return;
    }

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let workspace_root = std::path::Path::new(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root from cc-lb-runtime-wasmtime crate path");

    let nested_target_dir = PathBuf::from(&out_dir).join("wasm-fixture-target");

    let mut args = vec![
        "build".to_string(),
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
             Run `cargo build --target wasm32-unknown-unknown --release {}` \
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
