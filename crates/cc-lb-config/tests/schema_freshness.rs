use std::path::PathBuf;

use cc_lb_config::Config;

#[test]
fn committed_config_schema_matches_generated_schema() {
    let schema_path = schema_path();
    let generated = generated_schema_json();

    if std::env::var_os("UPDATE_CONFIG_SCHEMA").is_some() {
        std::fs::write(&schema_path, generated).expect("write config-schema.json");
        return;
    }

    let committed = std::fs::read_to_string(&schema_path).expect("read config-schema.json");
    assert_eq!(
        committed, generated,
        "run `UPDATE_CONFIG_SCHEMA=1 cargo test -p cc-lb-config committed_config_schema_matches_generated_schema` to refresh config-schema.json"
    );
}

#[test]
fn request_diagnostics_log_key_is_boolean_schema_property_defaulting_false() {
    let schema = serde_json::to_value(Config::json_schema()).expect("serialize config schema");
    let expected = serde_json::json!({ "type": "boolean", "default": false });

    assert_eq!(
        schema["$defs"]["ObservabilityConfig"]["properties"]["request_diagnostics_log"],
        expected
    );
    assert_eq!(
        schema["properties"]["observability"]["default"]["request_diagnostics_log"],
        serde_json::json!(false)
    );
}

fn schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config-schema.json")
}

fn generated_schema_json() -> String {
    let schema = Config::json_schema();
    let schema_json = serde_json::to_string_pretty(&schema).expect("serialize config schema");
    format!("{schema_json}\n")
}
