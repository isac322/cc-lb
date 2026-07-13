use crate::common;

use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cert_reload_mid_traffic_keeps_in_flight_stream_alive() {
    let app = common::start_tls_app(8_192).await;

    let before = common::tls_get(app.proxy_addr, &app.cert_path, "/v1/models")
        .await
        .expect("pre-reload request");
    assert_eq!(before.status, 200);
    assert_eq!(before.peer_fingerprint, app.cert_a_fingerprint);

    let mut stream = common::start_streaming_post(app.proxy_addr, &app.cert_path)
        .await
        .expect("start in-flight stream");
    assert_eq!(stream.peer_fingerprint, app.cert_a_fingerprint);
    let stream_peer_fingerprint = stream.peer_fingerprint.clone();
    let partial = stream
        .read_until("content_block_delta", Duration::from_secs(10))
        .await;
    assert!(partial.contains(" 200 "));

    common::overwrite_pair(&app.cert_path, &app.key_path, "cert-b.pem", "key-b.pem");
    common::send_sighup().await;

    let after =
        common::wait_for_reloaded_cert(app.proxy_addr, &app.cert_path, &app.cert_b_fingerprint)
            .await;
    assert_eq!(after.status, 200);
    assert_eq!(after.peer_fingerprint, app.cert_b_fingerprint);

    let completed = stream.read_to_end(Duration::from_secs(20)).await;
    assert!(completed.contains("message_stop"));

    let evidence = format!(
        "task=47\npre_reload_status={}\npre_reload_peer_fingerprint={}\nin_flight_started_with_cert_a={}\nin_flight_completed_with_cert_a={}\nnew_conn_status={}\nnew_conn_peer_fingerprint={}\nnew_conn_used_cert_b={}\nold_new_fingerprints_differ={}\n",
        before.status,
        before.peer_fingerprint,
        stream_peer_fingerprint == app.cert_a_fingerprint,
        completed.contains("message_stop") && stream_peer_fingerprint == app.cert_a_fingerprint,
        after.status,
        after.peer_fingerprint,
        after.peer_fingerprint == app.cert_b_fingerprint,
        app.cert_a_fingerprint != app.cert_b_fingerprint,
    );
    common::write_evidence("task-47-tls-reload.log", &evidence);

    println!(
        "cert_reload_mid_traffic PASSED: in_flight_completed_with_cert_a=true new_conn_used_cert_b=true old_fp={} new_fp={}",
        app.cert_a_fingerprint, app.cert_b_fingerprint
    );

    app.shutdown().await;
}
