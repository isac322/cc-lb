mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handshake_self_signed_proxy_tls_only() {
    let app = common::start_tls_app(1_048_576).await;

    let proxy = common::tls_get(app.proxy_addr, &app.cert_path, "/v1/models")
        .await
        .expect("proxy TLS request");
    assert_eq!(proxy.status, 200);
    assert_eq!(proxy.peer_fingerprint, app.cert_a_fingerprint);

    common::wait_plain_status(app.admin_addr, "/admin/health", 200).await;

    println!(
        "handshake_self_signed PASSED: proxy_status={} proxy_peer_fingerprint={} admin_plain_http=true metrics_addr={}",
        proxy.status, proxy.peer_fingerprint, app.metrics_addr
    );

    app.shutdown().await;
}
