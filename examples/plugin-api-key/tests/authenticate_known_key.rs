#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn authenticate_known_key() {
    let wasm = common::build_wasm();
    let mut plugin = common::plugin_with_keys(&wasm, r#"{"alice":"sk-ant-alice"}"#);
    let output = common::authenticate(&mut plugin, vec![("x-api-key", "sk-ant-alice")]).unwrap();

    assert_eq!(output["_version"], 1);
    assert_eq!(output["principal"]["id"], "alice");
    assert_eq!(output["principal"]["kind"], "api_key");
    assert_eq!(output["signer_factory_ref"], "anthropic-key");
    assert_eq!(output["quotas"]["requests_per_window"], 1000);
    assert_eq!(output["quotas"]["window_ms"], 60000);
}
