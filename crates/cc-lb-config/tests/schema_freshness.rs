use cc_lb_config::Config;

#[test]
fn committed_config_schema_matches_generated_schema() {
    let generated = generated_schema_json();
    let committed = include_str!("../../../config-schema.json");

    assert_eq!(
        committed, generated,
        "config-schema.json is stale; regenerate it from Config::json_schema()"
    );
}

fn generated_schema_json() -> String {
    let schema = Config::json_schema();
    let schema_json = serde_json::to_string_pretty(&schema).expect("serialize config schema");
    format!("{schema_json}\n")
}
