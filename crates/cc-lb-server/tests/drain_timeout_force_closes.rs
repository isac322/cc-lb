mod drain_common;

use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_timeout_force_closes() {
    let app = drain_common::start_app(1, drain_common::very_slow_fake_config()).await;
    let proxy_addr = app.proxy_addr;
    let client = tokio::spawn(async move {
        drain_common::http_post(
            proxy_addr,
            "/v1/messages",
            drain_common::STREAM_BODY,
            &[("accept", "text/event-stream"), ("x-fake-mode", "slow")],
        )
        .await
    });

    drain_common::wait_for_in_flight(&app.controller, 1).await;
    app.signals.start_shutdown();
    let timed_out = app
        .controller
        .await_drained(Duration::from_millis(500))
        .await;
    assert!(timed_out);

    drain_common::wait_for_force_closed(&app.controller).await;
    tokio::time::timeout(Duration::from_secs(5), app.server)
        .await
        .expect("server exits")
        .expect("server join")
        .expect("server ok");

    let client_result = tokio::time::timeout(Duration::from_secs(5), client)
        .await
        .expect("client released")
        .expect("client join");
    match client_result {
        Ok(response) => {
            assert!(!response.body.contains("message_stop"));
            println!(
                "T28 force-close sample await_timed_out={timed_out} force_closed_total={} client_status={} complete_message_stop=false",
                app.controller.force_closed_total(),
                response.status
            );
        }
        Err(source) => {
            println!(
                "T28 force-close sample await_timed_out={timed_out} force_closed_total={} client_error={source}",
                app.controller.force_closed_total()
            );
        }
    }
}
