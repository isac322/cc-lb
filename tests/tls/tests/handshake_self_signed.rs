use crate::common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handshake_self_signed_proxy_tls_only() {
    let app = common::start_tls_app(1_048_576).await;

    let proxy = common::tls_get(app.proxy_addr, &app.cert_path, "/v1/models", &app.api_key)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_tls_handshake_does_not_block_other_clients_and_times_out() {
    use std::time::{Duration, Instant};
    use tokio::io::AsyncReadExt as _;
    use tokio::net::TcpStream;

    let app = common::start_tls_app_with_handshake_timeout(1_048_576, 1).await;

    // Hold a raw TCP socket open without sending a TLS ClientHello.
    let mut stalled = TcpStream::connect(app.proxy_addr)
        .await
        .expect("connect raw TCP socket");

    // Concurrent legitimate TLS client must complete well before the 1s handshake timeout.
    let started = Instant::now();
    let proxy = common::tls_get(app.proxy_addr, &app.cert_path, "/v1/models", &app.api_key)
        .await
        .expect("concurrent TLS request must not be head-of-line blocked");
    assert_eq!(proxy.status, 200);
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "concurrent TLS request waited on stalled ClientHello: {:?}",
        started.elapsed()
    );

    // Server must drop the stalled TLS connection once handshake_timeout_secs=1 elapses.
    let mut buf = [0_u8; 16];
    let read_res = tokio::time::timeout(Duration::from_secs(4), stalled.read(&mut buf))
        .await
        .expect("stalled TLS socket should close within timeout");
    assert!(
        matches!(read_res, Ok(0) | Err(_)),
        "expected stalled TLS socket closure, got {read_res:?}"
    );

    app.shutdown().await;
}
