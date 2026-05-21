#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn internal_token_recognized() {
    let wasm = common::build_wasm();
    let mut plugin = common::plugin_with_tokens(&wasm, common::alice_tokens());
    let output = common::authenticate(
        &mut plugin,
        vec![("Authorization", "Bearer ck-internal-alice-X")],
    )
    .unwrap();

    assert_eq!(output["_version"], 1);
    assert_eq!(output["principal"]["id"], "alice");
    assert_eq!(output["principal"]["kind"], "internal_key");
    assert_eq!(
        output["principal"]["claims"]["real_credential_storage_key"],
        "alice:real_anthropic_api_key"
    );
    assert_eq!(
        output["principal"]["claims"]["real_credential_kind"],
        "anthropic_api_key"
    );
    assert_eq!(output["signer_factory_ref"], "anthropic-key");
    assert_eq!(output["quotas"]["requests_per_window"], 1000);
    assert_eq!(output["quotas"]["input_tokens_per_window"], 1_000_000);
    assert_eq!(output["quotas"]["output_tokens_per_window"], 200_000);
    assert_eq!(output["quotas"]["window_ms"], 60_000);
}
