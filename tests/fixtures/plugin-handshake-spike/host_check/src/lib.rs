#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    use extism::{Manifest, PluginBuilder, Wasm};
    use serde_json::Value;
    use wasmparser::{ExternalKind, Parser, Payload};

    const CUSTOM_SECTION_NAME: &str = "cc_lb.plugin.v1";
    const REQUIRED_EXPORTS: [&str; 3] = ["route", "cc_lb_handshake", "cc_lb_self_check"];

    #[test]
    fn custom_section_contains_handshake_payload() {
        let wasm = wasm_bytes();
        let payload = custom_section_payload(&wasm).expect("handshake custom section is present");
        let metadata: Value =
            serde_json::from_slice(payload).expect("custom section is valid JSON");

        assert_eq!(metadata["magic"], "cc-lb-plugin");
        assert_eq!(metadata["abi_envelope"], 1);
        assert_eq!(metadata["plugin_name"], "plugin-handshake-spike");
        assert_eq!(metadata["plugin_version"], "0.1.0");
    }

    #[test]
    fn wasmparser_sees_three_required_exports() {
        let wasm = wasm_bytes();
        let exports = exported_functions(&wasm);

        for name in REQUIRED_EXPORTS {
            assert!(exports.contains(name), "missing wasm export {name}");
        }
    }

    #[test]
    fn extism_plugin_reports_three_required_exports() {
        let wasm = wasm_bytes();
        let manifest = Manifest::new([Wasm::data(wasm)]);
        let plugin = PluginBuilder::new(manifest)
            .with_wasi(false)
            .with_cache_disabled()
            .build()
            .expect("Extism can instantiate handshake spike plugin");

        for name in REQUIRED_EXPORTS {
            assert!(
                plugin.function_exists(name),
                "Extism cannot find export {name}"
            );
        }
    }

    fn wasm_bytes() -> Vec<u8> {
        let path = wasm_artifact_path();
        fs::read(&path).unwrap_or_else(|source| {
            panic!(
                "failed to read {}: {source}; build it first with `cargo build -p plugin-handshake-spike --target wasm32-unknown-unknown --release`",
                path.display()
            )
        })
    }

    fn wasm_artifact_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../target/wasm32-unknown-unknown/release/plugin_handshake_spike.wasm")
    }

    fn custom_section_payload(wasm: &[u8]) -> Option<&[u8]> {
        for payload in Parser::new(0).parse_all(wasm) {
            let payload = payload.expect("wasm payload parses");
            if let Payload::CustomSection(section) = payload {
                if section.name() == CUSTOM_SECTION_NAME {
                    return Some(section.data());
                }
            }
        }
        None
    }

    fn exported_functions(wasm: &[u8]) -> BTreeSet<&str> {
        let mut exports = BTreeSet::new();
        for payload in Parser::new(0).parse_all(wasm) {
            let payload = payload.expect("wasm payload parses");
            if let Payload::ExportSection(section) = payload {
                for export in section {
                    let export = export.expect("export entry parses");
                    if export.kind == ExternalKind::Func {
                        exports.insert(export.name);
                    }
                }
            }
        }
        exports
    }
}
