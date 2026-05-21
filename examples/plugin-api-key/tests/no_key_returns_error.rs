#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn no_key_returns_error() {
    let wasm = common::build_wasm();
    let mut plugin = common::plugin_with_keys(&wasm, r#"{"alice":"sk-ant-alice"}"#);
    let error =
        common::authenticate(&mut plugin, Vec::new()).expect_err("missing key should be rejected");

    assert!(error.to_string().contains("no api key"));
}
