use std::path::PathBuf;

pub fn required_wasm(file_name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release")
        .join(file_name);

    std::fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "required wasm fixture is missing at {}: {error}; build the fixture before running runtime contract tests",
            path.display()
        )
    })
}
