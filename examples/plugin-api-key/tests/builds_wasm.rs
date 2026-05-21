#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn builds_wasm() {
    let wasm = common::build_wasm();
    assert_eq!(wasm.file_name().unwrap(), "plugin_api_key.wasm");
}
