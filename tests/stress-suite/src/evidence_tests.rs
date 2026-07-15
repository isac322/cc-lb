use serde_json::Value;

#[test]
fn evidence_schema_redacts_secrets() {
    // Given: a fixture that contains a provider-shaped API key at nested paths.
    let fixture: Value = serde_json::from_str(include_str!("../fixtures/secret-evidence.json"))
        .expect("secret evidence fixture parses");

    // When: the shared evidence serializer prepares the document for disk.
    let serialized = crate::evidence_redaction::serialize_redacted_value(fixture)
        .expect("redacted evidence serializes");

    // Then: no provider-shaped secret can reach the serialized artifact.
    assert!(!serialized.contains("sk-ant-SECRET"));
    assert!(!serialized.contains("fixture-access-token"));
}

#[test]
fn metrics_percentiles_and_dispersion() {
    // Given: an intentionally non-uniform latency sample.
    let samples = [10, 20, 30, 40, 100];

    // When: the numeric evidence collector summarizes it.
    let summary = crate::evidence_metrics::summarize_samples(&samples);

    // Then: percentiles and robust dispersion are both reported.
    assert_eq!(summary.p95, 100);
    assert_eq!(summary.dispersion.iqr, 20);
    assert_eq!(summary.dispersion.mad, 10);
}

#[test]
fn rps_and_recovery_fields_present() {
    // Given: the deterministic evidence self-check input.
    let document = crate::evidence_self_check::synthetic_document()
        .expect("synthetic evidence document builds");

    // When: it is represented as the external JSON contract.
    let json = serde_json::to_value(document).expect("synthetic evidence serializes");

    // Then: rate accounting and recovery measurements remain queryable per wave.
    assert!(json["waves"][0]["rps"]["attempted"].is_u64());
    assert!(json["waves"][0]["rps"]["completed"].is_u64());
    assert!(json["waves"][0]["recovery_ms"].is_u64());
}

#[test]
fn qdisc_stats_recorded() {
    // Given: a captured tc qdisc output with packet counters.
    let output = "qdisc netem 1: root refcnt 2 limit 1000 delay 10ms\n Sent 480 bytes 4 pkt (dropped 2, overlimits 0 requeues 0)\n backlog 0b 0p requeues 0\n";

    // When: the qdisc collector records an edge capture.
    let stat = crate::evidence_qdisc::record_qdisc(crate::evidence_qdisc::QdiscCapture {
        edge: "probe-to-cc-lb",
        device: "eth0",
        output,
        scheduled_at_unix_ms: 100,
        command_started_at_unix_ms: 100,
        command_completed_at_unix_ms: 120,
    })
    .expect("qdisc output parses");

    // Then: counters, command timestamps, and schedule drift are retained.
    assert_eq!(stat.packets, 4);
    assert_eq!(stat.dropped, 2);
    assert_eq!(stat.command.started_at_unix_ms, 100);
    assert_eq!(stat.command.completed_at_unix_ms, 120);
    assert_eq!(stat.schedule_drift_ms, 20);
}
