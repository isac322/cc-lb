#[test]
fn v2_mirrors_v1_byte_compat() {
    let v1_candidate = cc_lb_plugin_wire::v1::common::CandidateWire {
        upstream_id: String::from("11111111-1111-1111-1111-111111111111"),
        name: String::from("byte-compatible-upstream"),
        kind: String::from("anthropic_api_key"),
        observed_rate_limits: Vec::new(),
        subscription_quotas: Vec::new(),
        observed_at_unix_secs: 1_717_171_717,
    };

    let v2_candidate = cc_lb_plugin_wire::v2::common::CandidateWire {
        upstream_id: String::from("11111111-1111-1111-1111-111111111111"),
        name: String::from("byte-compatible-upstream"),
        kind: String::from("anthropic_api_key"),
        observed_rate_limits: Vec::new(),
        subscription_quotas: Vec::new(),
        observed_at_unix_secs: 1_717_171_717,
    };

    let v1_json = serde_json::to_string(&v1_candidate).unwrap();
    let v2_json = serde_json::to_string(&v2_candidate).unwrap();

    assert_eq!(v1_json.as_bytes(), v2_json.as_bytes());
}
