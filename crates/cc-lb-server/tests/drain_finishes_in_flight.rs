#![cfg(any())]

mod drain_common;

use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_finishes_in_flight() {
    let app = drain_common::start_app(10, drain_common::slow_fake_config()).await;
    let mut handles = Vec::new();

    for _ in 0..25 {
        let proxy_addr = app.proxy_addr;
        handles.push(tokio::spawn(async move {
            drain_common::http_post(
                proxy_addr,
                "/v1/messages",
                drain_common::STREAM_BODY,
                &[("accept", "text/event-stream"), ("x-fake-mode", "slow")],
            )
            .await
            .expect("stream response")
        }));
    }

    drain_common::wait_for_in_flight(&app.controller, 25).await;
    app.signals.start_shutdown();

    let mut completed = 0_u64;
    for handle in handles {
        let response = handle.await.expect("join stream task");
        assert_eq!(response.status, 200);
        assert!(response.body.contains("message_stop"));
        completed += 1;
    }

    tokio::time::timeout(Duration::from_secs(5), app.server)
        .await
        .expect("server exits")
        .expect("server join")
        .expect("server ok");

    assert_eq!(app.controller.in_flight(), 0);
    assert_eq!(app.controller.force_closed_total(), 0);
    println!(
        "T28 in-flight streams completed count={completed} in_flight={} force_closed_total={}",
        app.controller.in_flight(),
        app.controller.force_closed_total()
    );
}
