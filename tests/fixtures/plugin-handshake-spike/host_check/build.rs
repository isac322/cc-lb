use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .expect("Cargo must provide CARGO_MANIFEST_DIR to build scripts"),
    );
    let fixture_dir = manifest_dir
        .parent()
        .expect("host_check has a parent fixture directory");

    println!(
        "cargo:rerun-if-changed={}",
        fixture_dir.join("src/lib.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        fixture_dir.join("Cargo.toml").display()
    );

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

    let wasm_path = fixture_dir
        .join("target")
        .join("wasm32-unknown-unknown")
        .join("release")
        .join("plugin_handshake_spike.wasm");
    let wasm_path = wasm_path.canonicalize().unwrap_or_else(|err| {
        panic!(
            "expected fixture wasm at {} after cargo build: {err}",
            wasm_path.display()
        )
    });
    println!(
        "cargo:rustc-env=PLUGIN_HANDSHAKE_SPIKE_WASM={}",
        wasm_path.display()
    );
}
