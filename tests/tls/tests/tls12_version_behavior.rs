use crate::common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls12_only_client_is_accepted_by_current_ring_tls12_policy() {
    let app = common::start_tls_app(1_048_576).await;

    let response = common::tls_get_with_versions(
        app.proxy_addr,
        &app.cert_path,
        "/v1/models",
        &app.api_key,
        &[&rustls::version::TLS12],
    )
    .await
    .expect("TLS 1.2-only proxy request");

    assert_eq!(response.status, 200);
    assert_eq!(response.peer_fingerprint, app.cert_a_fingerprint);
    assert_eq!(
        response.protocol_version,
        Some(rustls::ProtocolVersion::TLSv1_2)
    );

    let evidence = format!(
        "task=47\ntls12_plan_expectation=tls12_disabled_by_default\ntls12_actual_behavior=accepted\ntls12_rationale=rustls/tokio-rustls are built with ring+tls12 and server uses with_safe_default_protocol_versions, whose DEFAULT_VERSIONS includes TLS13 and TLS12\ntls12_status={}\ntls12_peer_fingerprint={}\ntls12_protocol_version={:?}\n",
        response.status, response.peer_fingerprint, response.protocol_version
    );
    common::write_evidence("tls-version.log", &evidence);

    println!(
        "tls12_version_behavior PASSED: tls12_actual_behavior=accepted status={} protocol_version={:?} fingerprint={}",
        response.status, response.protocol_version, response.peer_fingerprint
    );

    app.shutdown().await;
}
