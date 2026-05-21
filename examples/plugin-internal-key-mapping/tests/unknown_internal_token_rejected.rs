#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn unknown_internal_token_rejected() {
    let wasm = common::build_wasm();
    let mut plugin = common::plugin_with_tokens(&wasm, common::alice_tokens());

    let unknown = common::authenticate(
        &mut plugin,
        vec![("Authorization", "Bearer ck-internal-bob-Y")],
    )
    .expect_err("unknown internal token must be rejected");
    assert!(unknown.to_string().contains("unknown internal token"));

    let not_internal = common::authenticate(
        &mut plugin,
        vec![("Authorization", "Bearer sk-ant-not-internal")],
    )
    .expect_err("Anthropic-shaped bearer must not fall back through this plugin");
    assert!(not_internal.to_string().contains("not an internal token"));

    let missing = common::authenticate(&mut plugin, Vec::new())
        .expect_err("missing authorization header must be rejected");
    assert!(missing.to_string().contains("missing bearer token"));
}
