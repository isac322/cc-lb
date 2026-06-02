use cc_lb_plugin_wire::identity::{IdentityError, PluginIdentity, CC_LB_PLUGIN_SECTION_NAME};
use cc_lb_plugin_wire::limits;
use serde_json::Value;
use thiserror::Error;
use wasmparser::{Parser, Payload};

pub fn read_identity(wasm_bytes: &[u8]) -> Result<PluginIdentity, IdentityReadError> {
    let mut identity = None;

    for payload in Parser::new(0).parse_all(wasm_bytes) {
        let payload = payload?;
        let Payload::CustomSection(section) = payload else {
            continue;
        };
        if section.name() != CC_LB_PLUGIN_SECTION_NAME {
            continue;
        }
        if identity.is_some() {
            return Err(IdentityReadError::DuplicateSection);
        }
        identity = Some(read_section_payload(section.data())?);
    }

    identity.ok_or(IdentityReadError::MissingSection)
}

fn read_section_payload(data: &[u8]) -> Result<PluginIdentity, IdentityReadError> {
    if data.len() > limits::CUSTOM_SECTION_MAX_SIZE {
        return Err(IdentityReadError::SectionTooLarge {
            actual: data.len(),
            max: limits::CUSTOM_SECTION_MAX_SIZE,
        });
    }

    let value: Value = serde_json::from_slice(data)?;
    let actual = value.as_object().map_or(0, |object| object.len());
    if actual != limits::CUSTOM_SECTION_FIELD_COUNT {
        return Err(IdentityReadError::UnexpectedFieldCount {
            actual,
            expected: limits::CUSTOM_SECTION_FIELD_COUNT,
        });
    }

    let identity: PluginIdentity = serde_json::from_value(value)?;
    identity.validate()?;
    Ok(identity)
}

#[derive(Debug, Error)]
pub enum IdentityReadError {
    #[error("invalid wasm: {0}")]
    InvalidWasm(#[from] wasmparser::BinaryReaderError),
    #[error("missing {CC_LB_PLUGIN_SECTION_NAME} custom section")]
    MissingSection,
    #[error("duplicate {CC_LB_PLUGIN_SECTION_NAME} custom section")]
    DuplicateSection,
    #[error("{CC_LB_PLUGIN_SECTION_NAME} custom section is {actual} bytes, max {max}")]
    SectionTooLarge { actual: usize, max: usize },
    #[error("invalid {CC_LB_PLUGIN_SECTION_NAME} JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("{CC_LB_PLUGIN_SECTION_NAME} custom section has {actual} fields, expected {expected}")]
    UnexpectedFieldCount { actual: usize, expected: usize },
    #[error("invalid plugin identity: {0}")]
    InvalidIdentity(#[from] IdentityError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_plugin_wire::{identity::CC_LB_PLUGIN_MAGIC, limits};
    use serde_json::json;

    #[test]
    fn reads_identity_from_custom_section() {
        let wasm = wasm_with_custom_sections(&[(
            CC_LB_PLUGIN_SECTION_NAME,
            valid_identity_payload().as_bytes(),
        )]);

        let identity = read_identity(&wasm).expect("identity is read");

        assert_eq!(identity.magic, CC_LB_PLUGIN_MAGIC);
        assert_eq!(identity.abi_envelope, 1);
        assert_eq!(identity.plugin_name, "test-plugin");
        assert_eq!(identity.plugin_version, "1.0.0");
    }

    #[test]
    fn rejects_missing_identity_section() {
        let wasm = wasm_with_custom_sections(&[]);

        let error = read_identity(&wasm).expect_err("identity section is required");

        assert!(matches!(error, IdentityReadError::MissingSection));
    }

    #[test]
    fn rejects_duplicate_identity_sections() {
        let payload = valid_identity_payload();
        let wasm = wasm_with_custom_sections(&[
            (CC_LB_PLUGIN_SECTION_NAME, payload.as_bytes()),
            (CC_LB_PLUGIN_SECTION_NAME, payload.as_bytes()),
        ]);

        let error = read_identity(&wasm).expect_err("duplicate section is rejected");

        assert!(matches!(error, IdentityReadError::DuplicateSection));
    }

    #[test]
    fn rejects_section_over_size_limit() {
        let oversized = vec![b' '; limits::CUSTOM_SECTION_MAX_SIZE + 1];
        let wasm = wasm_with_custom_sections(&[(CC_LB_PLUGIN_SECTION_NAME, &oversized)]);

        let error = read_identity(&wasm).expect_err("oversized section is rejected");

        assert!(matches!(
            error,
            IdentityReadError::SectionTooLarge {
                actual,
                max: limits::CUSTOM_SECTION_MAX_SIZE,
            } if actual == limits::CUSTOM_SECTION_MAX_SIZE + 1
        ));
    }

    #[test]
    fn rejects_bad_eight_byte_magic() {
        let payload = identity_payload(json!([0, 0, 0, 0, 0, 0, 0, 0]));
        let wasm = wasm_with_custom_sections(&[(CC_LB_PLUGIN_SECTION_NAME, payload.as_bytes())]);

        let error = read_identity(&wasm).expect_err("bad magic is rejected");

        assert!(matches!(
            error,
            IdentityReadError::InvalidIdentity(IdentityError::MagicMismatch)
        ));
    }

    #[test]
    fn rejects_four_byte_ascii_magic() {
        let payload = identity_payload(json!([67, 67, 76, 66]));
        let wasm = wasm_with_custom_sections(&[(CC_LB_PLUGIN_SECTION_NAME, payload.as_bytes())]);

        let error = read_identity(&wasm).expect_err("4-byte magic is rejected");

        assert!(matches!(error, IdentityReadError::InvalidJson(_)));
    }

    #[test]
    fn rejects_extra_identity_field() {
        let payload = json!({
            "magic": CC_LB_PLUGIN_MAGIC,
            "abi_envelope": 1,
            "plugin_name": "test-plugin",
            "plugin_version": "1.0.0",
            "extra": true,
        })
        .to_string();
        let wasm = wasm_with_custom_sections(&[(CC_LB_PLUGIN_SECTION_NAME, payload.as_bytes())]);

        let error = read_identity(&wasm).expect_err("extra field is rejected");

        assert!(matches!(
            error,
            IdentityReadError::UnexpectedFieldCount {
                actual: 5,
                expected: limits::CUSTOM_SECTION_FIELD_COUNT,
            }
        ));
    }

    #[test]
    fn rejects_invalid_wasm_bytes() {
        let error = read_identity(b"not wasm").expect_err("invalid wasm is rejected");

        assert!(matches!(error, IdentityReadError::InvalidWasm(_)));
    }

    fn valid_identity_payload() -> String {
        identity_payload(json!(CC_LB_PLUGIN_MAGIC))
    }

    fn identity_payload(magic: Value) -> String {
        json!({
            "magic": magic,
            "abi_envelope": 1,
            "plugin_name": "test-plugin",
            "plugin_version": "1.0.0",
        })
        .to_string()
    }

    fn wasm_with_custom_sections(sections: &[(&str, &[u8])]) -> Vec<u8> {
        let mut wasm = Vec::from([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
        for (name, data) in sections {
            wasm.push(0);
            let mut payload = Vec::new();
            encode_u32(name.len() as u32, &mut payload);
            payload.extend_from_slice(name.as_bytes());
            payload.extend_from_slice(data);
            encode_u32(payload.len() as u32, &mut wasm);
            wasm.extend_from_slice(&payload);
        }
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
}
