use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

struct Fixture {
    dir: &'static str,
    env_var: &'static str,
    wasm_file: &'static str,
}

const FIXTURES: &[Fixture] = &[
    Fixture {
        dir: "tests/fixtures/conformance-plugin-shape",
        env_var: "CONFORMANCE_FIXTURE_SHAPE_WASM",
        wasm_file: "conformance_plugin_shape.wasm",
    },
    Fixture {
        dir: "tests/fixtures/conformance-plugin-router",
        env_var: "CONFORMANCE_FIXTURE_ROUTER_WASM",
        wasm_file: "conformance_plugin_router.wasm",
    },
    Fixture {
        dir: "tests/fixtures/conformance-plugin-observe",
        env_var: "CONFORMANCE_FIXTURE_OBSERVE_WASM",
        wasm_file: "conformance_plugin_observe.wasm",
    },
];

fn main() {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .expect("Cargo must provide CARGO_MANIFEST_DIR to build scripts"),
    );

    for fixture in FIXTURES {
        println!("cargo:rerun-if-changed={}/src/lib.rs", fixture.dir);
    }

    for fixture in FIXTURES {
        let fixture_dir = manifest_dir.join(fixture.dir);
        build_fixture(&fixture_dir);

        let wasm_path = fixture_dir
            .join("target")
            .join("wasm32-unknown-unknown")
            .join("release")
            .join(fixture.wasm_file);
        let wasm_path = wasm_path.canonicalize().unwrap_or_else(|err| {
            panic!(
                "expected fixture wasm at {} after cargo build: {err}",
                wasm_path.display()
            )
        });

        println!(
            "cargo:rustc-env={}={}",
            fixture.env_var,
            wasm_path.display()
        );
    }
}

fn build_fixture(fixture_dir: &Path) {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let output = Command::new(cargo)
        .args(["build", "--target", "wasm32-unknown-unknown", "--release"])
        .current_dir(fixture_dir)
        .env("CARGO_TARGET_DIR", fixture_dir.join("target"))
        .env("CARGO_ENCODED_RUSTFLAGS", "")
        .env("RUSTFLAGS", "")
        .output()
        .unwrap_or_else(|err| panic!("failed to run cargo for {}: {err}", fixture_dir.display()));

    if !output.status.success() {
        eprintln!("fixture build failed in {}", fixture_dir.display());
        eprintln!("status: {}", output.status);
        eprintln!("stdout:\n{}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr:\n{}", String::from_utf8_lossy(&output.stderr));
        panic!("fixture build failed");
    }
}
