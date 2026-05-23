#![cfg(not(target_arch = "wasm32"))]

mod common;

#[test]
fn unknown_token_rejected() {
    let wasm = common::build_wasm();
    let mut plugin = common::plugin_with_config(&wasm, common::alice_config());

    let unknown = common::authenticate(
        &mut plugin,
        vec![("Authorization", "Bearer sk-ant-oat01-UNKNOWN-bob")],
    )
    .expect_err("unknown OAuth bearer must be rejected");
    assert!(
        unknown
            .to_string()
            .contains("unknown Anthropic OAuth bearer")
    );

    let wrong_prefix = common::authenticate(
        &mut plugin,
        vec![("Authorization", "Bearer sk-ant-WRONGPREFIX-xxx")],
    )
    .expect_err("wrong token prefix must be rejected");
    assert!(
        wrong_prefix
            .to_string()
            .contains("invalid Anthropic OAuth token prefix")
    );
}
