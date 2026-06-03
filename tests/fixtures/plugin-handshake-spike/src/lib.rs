use extism_pdk::{FnResult, plugin_fn};

const HANDSHAKE_PAYLOAD: &str = r#"{"magic":"cc-lb-plugin","abi_envelope":1,"plugin_name":"plugin-handshake-spike","plugin_version":"0.1.0"}"#;

#[used]
#[unsafe(link_section = "cc_lb.plugin.v1")]
static CC_LB_PLUGIN_METADATA: [u8; 105] = *br#"{"magic":"cc-lb-plugin","abi_envelope":1,"plugin_name":"plugin-handshake-spike","plugin_version":"0.1.0"}"#;

#[plugin_fn]
pub fn route(input: String) -> FnResult<String> {
    Ok(input)
}

#[plugin_fn]
pub fn cc_lb_handshake(_input: String) -> FnResult<String> {
    Ok(HANDSHAKE_PAYLOAD.to_owned())
}

#[plugin_fn]
pub fn cc_lb_self_check(_input: String) -> FnResult<String> {
    Ok(r#"{"ok":true}"#.to_owned())
}
