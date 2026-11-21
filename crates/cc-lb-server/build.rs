use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    emit("CC_LB_VERSION", env!("CARGO_PKG_VERSION").to_owned());
    emit("CC_LB_RUSTC", rustc_version());
    emit("CC_LB_FEATURES", features());
    emit(
        "CC_LB_TARGET",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned()),
    );
    emit(
        "CC_LB_PROFILE",
        std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned()),
    );
}

fn emit(name: &str, value: String) {
    println!("cargo:rustc-env={name}={value}");
}

fn rustc_version() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    Command::new(&rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| output.status.success().then_some(output.stdout))
        .and_then(|stdout| String::from_utf8(stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or(rustc)
}

fn features() -> String {
    let mut features = std::env::vars()
        .filter_map(|(name, _)| name.strip_prefix("CARGO_FEATURE_").map(feature_name))
        .collect::<Vec<_>>();
    features.sort();
    if features.is_empty() {
        "unknown".to_owned()
    } else {
        features.join(",")
    }
}

fn feature_name(value: &str) -> String {
    value.to_ascii_lowercase().replace('_', "-")
}
