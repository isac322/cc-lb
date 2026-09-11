#![cfg(feature = "postgres")]

use std::path::Path;

use serde_json::json;
use uuid::Uuid;

use super::support::{
    ADMIN_TOKEN, ChildProcess, EXIT_TIMEOUT, Fixture, MASTER_KEY_HEX, PostgresConnectionProxy,
    READY_TIMEOUT, ReservedAddrs, ServerAddrs, Signal, base_server_config, http_request,
    serve_command, spawn_fake_anthropic, wait_http_status,
};

const KEY_ENV: &str = "CC_LB_T5_DB_RESTART_KEY";
const MESSAGES_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#;

#[tokio::test]
async fn t5__process_db_recovery_after_restart() {
    let database_fixture = cc_lb_storage_conformance::postgres_fixture()
        .await
        .expect("create isolated Postgres fixture");
    let postgres_proxy = PostgresConnectionProxy::spawn_for_schema(
        database_fixture.database_url(),
        database_fixture.schema_name(),
    )
    .await
    .expect("start Postgres connection proxy");

    let fixture = Fixture::new("db-restart");
    let upstream = spawn_fake_anthropic().await;
    let suffix = Uuid::new_v4().simple().to_string();
    let principal_id = format!("t5-db-restart-{suffix}");
    let upstream_name = format!("t5_db_restart_{suffix}");
    let reserved = ReservedAddrs::bind().expect("reserve cc-lb listener addresses");
    let addrs = ServerAddrs {
        proxy: reserved.proxy.addr(),
        admin: reserved.admin.addr(),
        metrics: reserved.metrics.addr(),
    };
    let data_dir = fixture.path("runtime");
    std::fs::create_dir_all(&data_dir).expect("create runtime data directory");
    let config = postgres_config(
        addrs,
        fixture.path("unused.sqlite").as_path(),
        &data_dir,
        &principal_id,
        postgres_proxy.database_url(),
    );
    let config_path = fixture.write("cc-lb.toml", config);
    reserved.release();

    let mut command = serve_command(&config_path);
    command
        .env(KEY_ENV, MASTER_KEY_HEX)
        .env("CC_LB_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("CC_LB_CLUSTER_TOKEN", "t5-db-restart-cluster-token");
    let mut child = ChildProcess::spawn(command).expect("spawn cc-lb serve process");

    let health = http_request("GET", addrs.admin, "/admin/health", &[], "");
    wait_http_status(&mut child, addrs.admin, &health, 200, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("admin listener readiness: {error}"));
    seed_proxy_state(
        &mut child,
        addrs,
        upstream.addr,
        &principal_id,
        &upstream_name,
    )
    .await;

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
    let before = wait_http_status(&mut child, addrs.proxy, &proxy_request, 200, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("proxy request before database restart: {error}"));
    assert!(
        before.body.contains(r#""type":"message""#),
        "body={}",
        before.body
    );

    postgres_proxy
        .set_available(false)
        .await
        .expect("make Postgres unavailable");
    child.assert_running("cc-lb remains alive while Postgres is unavailable");
    postgres_proxy
        .set_available(true)
        .await
        .expect("restore Postgres availability");

    let after = wait_http_status(&mut child, addrs.proxy, &proxy_request, 200, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("proxy request after database restart: {error}"));
    assert!(
        after.body.contains(r#""type":"message""#),
        "body={}",
        after.body
    );

    child
        .send_signal(Signal::Terminate)
        .expect("deliver SIGTERM to cc-lb");
    let output = child
        .wait_for_exit(EXIT_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("SIGTERM exit after database recovery: {error}"));
    assert_eq!(
        output.status.code(),
        Some(0),
        "status={} stdout={} stderr={}",
        output.status,
        output.stdout,
        output.stderr
    );
    assert!(
        !output.stderr.contains("panicked at") && !output.stderr.contains("thread panicked"),
        "stderr={}",
        output.stderr
    );
    postgres_proxy
        .shutdown()
        .await
        .expect("stop Postgres connection proxy");
    database_fixture
        .teardown()
        .await
        .expect("drop isolated Postgres fixture");
}

fn postgres_config(
    addrs: ServerAddrs,
    unused_storage_path: &Path,
    data_dir: &Path,
    principal_id: &str,
    database_url: &str,
) -> String {
    let base = base_server_config(addrs, unused_storage_path, data_dir, KEY_ENV, principal_id);
    let sqlite_storage = format!(
        "[storage]\nkind = \"sqlite\"\npath = \"{}\"",
        unused_storage_path.display()
    );
    assert!(
        base.contains(&sqlite_storage),
        "base config storage block changed"
    );
    base.replace(
        &sqlite_storage,
        &format!(
            "[storage]\nkind = \"postgres\"\nurl = \"{database_url}\"\n\n[cluster]\ninstance_url = \"http://127.0.0.1:1\"\ntoken_env = \"CC_LB_CLUSTER_TOKEN\""
        ),
    )
}

async fn seed_proxy_state(
    child: &mut ChildProcess,
    addrs: ServerAddrs,
    upstream: std::net::SocketAddr,
    principal_id: &str,
    upstream_name: &str,
) {
    let admin_headers = [
        (
            "authorization",
            concat!("Bearer ", "t5-process-admin-token"),
        ),
        ("content-type", "application/json"),
    ];
    let upstream_body = json!({
        "name": upstream_name,
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
    let created_upstream =
        wait_http_status(child, addrs.admin, &create_upstream, 201, READY_TIMEOUT)
            .await
            .unwrap_or_else(|error| panic!("admin upstream seed: {error}"));
    let upstream_id = serde_json::from_str::<serde_json::Value>(&created_upstream.body)
        .expect("admin upstream response is JSON")["id"]
        .as_str()
        .expect("admin upstream response has id")
        .to_owned();

    let principal_body = json!({
        "name": principal_id,
        "kind": "machine",
        "allowed_models": [],
        "allowed_upstreams": [upstream_id],
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
