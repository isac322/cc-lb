fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("Cargo must provide CARGO_MANIFEST_DIR to build scripts");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rustc-env=CC_LB_PLUGIN_HANDSHAKE_SPIKE_REMAP_FROM={manifest_dir}");
    println!(
        "cargo:rustc-env=CC_LB_PLUGIN_HANDSHAKE_SPIKE_REMAP_TO=/workspace/tests/fixtures/plugin-handshake-spike"
    );
}
