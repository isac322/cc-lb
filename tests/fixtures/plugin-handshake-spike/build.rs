const REMAP_TO: &str = "/workspace/tests/fixtures/plugin-handshake-spike";

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("Cargo must provide CARGO_MANIFEST_DIR to build scripts");
    let remap_from = if std::env::var_os("CI").is_some() {
        REMAP_TO.to_owned()
    } else {
        manifest_dir
    };

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-env-changed=CI");
    println!("cargo:rustc-env=CC_LB_PLUGIN_HANDSHAKE_SPIKE_REMAP_FROM={remap_from}");
    println!("cargo:rustc-env=CC_LB_PLUGIN_HANDSHAKE_SPIKE_REMAP_TO={REMAP_TO}");
}
