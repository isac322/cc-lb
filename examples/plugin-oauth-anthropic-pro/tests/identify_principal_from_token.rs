#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn identify_principal_from_token() {
    let wasm = common::build_wasm();
    let mut plugin = common::plugin_with_config(&wasm, common::alice_config());
    let output = common::authenticate(
        &mut plugin,
        vec![("Authorization", "Bearer sk-ant-oat01-MOCK-alice-xyz")],
    )
    .unwrap();

    assert_eq!(output["_version"], 1);
    assert_eq!(output["principal"]["id"], "alice");
    assert_eq!(output["principal"]["kind"], "subscription_bearer");
    assert_eq!(output["signer_factory_ref"], "anthropic-oauth");
    assert_eq!(
        output["principal"]["claims"]["refresh_token_storage_key"],
        "alice:anthropic_oauth"
    );
}
