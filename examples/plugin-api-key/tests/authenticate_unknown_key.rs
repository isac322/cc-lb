#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn authenticate_unknown_key_returns_error() {
    let wasm = common::build_wasm();
    let mut plugin = common::plugin_with_keys(&wasm, r#"{"alice":"sk-ant-alice"}"#);
    let error = common::authenticate(&mut plugin, vec![("x-api-key", "sk-ant-NOPE")])
        .expect_err("unknown key should be rejected by explicit-map mode");

    assert!(error.to_string().contains("unknown api key"));
}
