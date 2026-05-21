mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_cert_reload_keeps_previous_certificate_serving() {
    let app = common::start_tls_app(1_048_576).await;
    let before = common::tls_get(app.proxy_addr, &app.cert_path, "/v1/models")
        .await
        .expect("pre-reload request");
    assert_eq!(before.status, 200);
    assert_eq!(before.peer_fingerprint, app.cert_a_fingerprint);

    std::fs::copy(common::fixture("cert-bad.pem"), &app.cert_path).expect("write bad cert");
    common::send_sighup().await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    std::fs::copy(&app.cert_a_path, &app.cert_path)
        .expect("restore trusted cert file for client roots");
    let after = common::tls_get(app.proxy_addr, &app.cert_path, "/v1/models")
        .await
        .expect("post-failed-reload request");
    assert_eq!(after.status, 200);
    assert_eq!(after.peer_fingerprint, app.cert_a_fingerprint);

    println!(
        "bad_cert_reload_rejected PASSED: before_fp={} after_failed_reload_fp={} previous_cert_still_serving=true",
        before.peer_fingerprint, after.peer_fingerprint
    );

    app.shutdown().await;
}
