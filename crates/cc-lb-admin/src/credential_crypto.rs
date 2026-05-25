#[allow(dead_code)]
pub(crate) fn anthropic_api_key_aad(storage_key: &str) -> Vec<u8> {
    format!("anthropic-api-key:{storage_key}").into_bytes()
}
