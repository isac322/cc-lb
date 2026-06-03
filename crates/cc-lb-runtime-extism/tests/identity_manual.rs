use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
use cc_lb_runtime_extism::identity::read_identity;
use serde_json::json;

#[test]
fn public_read_identity_surface_reads_custom_section() {
    let payload = json!({
        "magic": CC_LB_PLUGIN_MAGIC,
        "abi_envelope": 1,
        "plugin_name": "manual-plugin",
        "plugin_version": "1.0.0",
    })
    .to_string();
    let wasm = wasm_with_custom_section(CC_LB_PLUGIN_SECTION_NAME, payload.as_bytes());

    let identity = read_identity(&wasm).expect("identity is read through public API");

    assert_eq!(identity.plugin_name, "manual-plugin");
}

fn wasm_with_custom_section(name: &str, data: &[u8]) -> Vec<u8> {
    let mut wasm = Vec::from([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
    wasm.push(0);
    let mut payload = Vec::new();
    encode_u32(name.len() as u32, &mut payload);
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);
    encode_u32(payload.len() as u32, &mut wasm);
    wasm.extend_from_slice(&payload);
    wasm
}

fn encode_u32(mut value: u32, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}
