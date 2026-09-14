use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::any;
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::support::{
    ADMIN_TOKEN, ChildProcess, EXIT_TIMEOUT, Fixture, MASTER_KEY_HEX, READY_TIMEOUT, ReservedAddrs,
    ServerAddrs, Signal, base_server_config, http_request, raw_http, serve_command,
    wait_http_status,
};

const PRINCIPAL_ID: &str = "t5-drain-principal";
const KEY_ENV: &str = "CC_LB_T5_DRAIN_KEY";
const MESSAGES_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#;
const UPSTREAM_BODY: &str = r#"{"id":"msg_t5_drain","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":7,"output_tokens":3}}"#;

#[tokio::test]
async fn t5__process_drain_rejects_new_and_finishes_inflight() {
    let fixture = Fixture::new("drain");
    let mut upstream = SlowUpstream::spawn().await;
    let reserved = ReservedAddrs::bind().expect("reserve cc-lb listener addresses");
    let addrs = ServerAddrs {
        proxy: reserved.proxy.addr(),
        admin: reserved.admin.addr(),
        metrics: reserved.metrics.addr(),
    };
    let storage_path = fixture.path("storage.sqlite");
    let data_dir = fixture.path("runtime");
    std::fs::create_dir_all(&data_dir).expect("create runtime data directory");
    let config = base_server_config(addrs, &storage_path, &data_dir, KEY_ENV, PRINCIPAL_ID);
    let config_path = fixture.write("cc-lb.toml", config);
    reserved.release();

    let mut command = serve_command(&config_path);
    command
        .env(KEY_ENV, MASTER_KEY_HEX)
        .env("CC_LB_ADMIN_TOKEN", ADMIN_TOKEN);
    let mut child = ChildProcess::spawn(command).expect("spawn cc-lb serve process");

    let health = http_request("GET", addrs.admin, "/admin/health", &[], "");
    wait_http_status(&mut child, addrs.admin, &health, 200, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("admin listener readiness: {error}"));
    seed_proxy_state(&mut child, addrs, upstream.addr).await;

    let proxy_request = http_request(
        "POST",
        addrs.proxy,
        "/v1/messages",
        &[
            ("x-api-key", "sk-ant-test"),
            ("anthropic-version", "2023-06-01"),
            ("content-type", "application/json"),
        ],
        MESSAGES_BODY,
    );
    let in_flight = tokio::spawn(async move {
        raw_http(addrs.proxy, &proxy_request, READY_TIMEOUT)
            .await
            .expect("in-flight proxy request completes")
    });
    upstream.wait_until_entered().await;

    child
        .send_signal(Signal::Terminate)
        .expect("deliver SIGTERM to cc-lb");
    wait_for_proxy_refusal(&mut child, addrs.proxy, Duration::from_secs(5)).await;

    let rejected = raw_http(
        addrs.proxy,
        &http_request(
            "POST",
            addrs.proxy,
            "/v1/messages",
            &[
                ("x-api-key", "sk-ant-test"),
                ("anthropic-version", "2023-06-01"),
                ("content-type", "application/json"),
            ],
            MESSAGES_BODY,
        ),
        Duration::from_secs(1),
    )
    .await
    .expect_err("proxy listener must reject a new connection during process drain");
    assert_eq!(
        rejected.kind(),
        io::ErrorKind::ConnectionRefused,
        "unexpected post-SIGTERM proxy error: {rejected}"
    );
    child.assert_running("child must remain alive for the in-flight request");

    upstream.release();
    let completed = in_flight.await.expect("join in-flight proxy request");
    assert_eq!(completed.status, 200, "body={}", completed.body);
    assert!(
        completed.body.contains(r#""id":"msg_t5_drain""#),
        "body={}",
        completed.body
    );

    let output = child
        .wait_for_exit(EXIT_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("SIGTERM drain exit: {error}"));
    assert_eq!(
        output.status.code(),
        Some(0),
        "status={} stdout={} stderr={}",
        output.status,
        output.stdout,
        output.stderr
    );
    assert!(
        !output.stderr.contains("graceful drain deadline elapsed")
            && !output.stderr.contains("panicked at")
            && !output.stderr.contains("thread panicked"),
        "stderr={}",
        output.stderr
    );
}

async fn seed_proxy_state(child: &mut ChildProcess, addrs: ServerAddrs, upstream: SocketAddr) {
    let admin_headers = [
        (
            "authorization",
            concat!("Bearer ", "t5-process-admin-token"),
        ),
        ("content-type", "application/json"),
    ];
    let upstream_body = json!({
        "name": "t5_drain_upstream",
        "kind": "anthropic_api_key",
        "base_url": format!("http://{upstream}/"),
        "api_key_value": "sk-ant-test"
    })
    .to_string();
    let create_upstream = http_request(
        "POST",
        addrs.admin,
        "/admin/v1/upstreams",
        &admin_headers,
        &upstream_body,
    );
    wait_http_status(child, addrs.admin, &create_upstream, 201, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("admin upstream seed: {error}"));

    let principal_body = json!({
        "name": PRINCIPAL_ID,
        "kind": "machine",
        "allowed_models": [],
        "allowed_upstreams": [],
        "default_limits": []
    })
    .to_string();
    let create_principal = http_request(
        "POST",
        addrs.admin,
        "/admin/v1/principals",
        &admin_headers,
        &principal_body,
    );
    wait_http_status(child, addrs.admin, &create_principal, 201, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("admin principal seed: {error}"));
}

async fn wait_for_proxy_refusal(child: &mut ChildProcess, addr: SocketAddr, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(10));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        child.assert_running("wait for proxy listener shutdown while draining");
        match TcpStream::connect(addr).await {
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => return,
            // A connection accepted while the listener closes can reset before
            // the next connect observes the terminal refusal state.
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            Err(error) => panic!(
                "unexpected proxy listener shutdown error ({:?}): {error}",
                error.kind()
            ),
            Ok(stream) => drop(stream),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "proxy listener continued accepting connections for {timeout:?} after SIGTERM"
        );
        poll.tick().await;
    }
}

struct SlowUpstream {
    addr: SocketAddr,
    entered: Option<oneshot::Receiver<()>>,
    release: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl SlowUpstream {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind slow upstream");
        let addr = listener.local_addr().expect("slow upstream address");
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered_tx)));
        let release = Arc::new(Mutex::new(Some(release_rx)));
        let router = Router::new().fallback(any(move || {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            async move {
                entered
                    .lock()
                    .expect("lock upstream entered channel")
                    .take()
                    .expect("slow upstream receives one request")
                    .send(())
                    .expect("drain test waits for upstream entry");
                let release = release
                    .lock()
                    .expect("lock upstream release channel")
                    .take()
                    .expect("slow upstream receives one request");
                release.await.expect("drain test releases upstream request");
                (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    UPSTREAM_BODY,
                )
                    .into_response()
            }
        }));
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("serve slow upstream");
        });
        Self {
            addr,
            entered: Some(entered_rx),
            release: Some(release_tx),
            task,
        }
    }

    async fn wait_until_entered(&mut self) {
        tokio::time::timeout(
            READY_TIMEOUT,
            self.entered.take().expect("wait for upstream entry once"),
        )
        .await
        .expect("proxy request reaches slow upstream before timeout")
        .expect("slow upstream entry sender remains open");
    }

    fn release(&mut self) {
        self.release
            .take()
            .expect("release slow upstream once")
            .send(())
            .expect("slow upstream request remains in flight");
    }
}

impl Drop for SlowUpstream {
    fn drop(&mut self) {
        let _ = self.release.take().map(|sender| sender.send(()));
        self.task.abort();
    }
}
