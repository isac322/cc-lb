#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn maps_to_real_credential_via_storage_key() {
    let wasm = common::build_wasm();
    let mut plugin = common::plugin_with_tokens(&wasm, common::alice_tokens());
    let output = common::authenticate(
        &mut plugin,
        vec![("Authorization", "Bearer ck-internal-alice-X")],
    )
    .unwrap();
    let storage_key = output["principal"]["claims"]["real_credential_storage_key"]
        .as_str()
        .unwrap();
    assert_eq!(
        storage_key, "alice:real_anthropic_api_key",
        "host signer storage lookup is covered by cc-lb-signer-anthropic-key storage-key tests and T35 live evidence"
    );
    assert_eq!(output["signer_factory_ref"], "anthropic-key");
}
