use std::process::Command;

use crate::common;

#[tokio::test]
async fn admin_separate_listener() {
    let server = common::spawn_test_server().await;
    let admin = common::http_get(server.admin_addr, "/admin/health")
        .await
        .expect("admin health");
    let proxy = common::proxy_get(
        server.proxy_addr,
        "/admin/health",
        &server.managed_key.plaintext,
    )
    .await
    .expect("proxy admin path");

    assert_eq!(admin.status, 200);
    assert!(admin.body.contains(r#""status":"ok""#));
    assert_ne!(proxy.status, 200);
}

#[tokio::test]
async fn server_and_admin_build_metadata_agree() {
    let server = common::spawn_test_server().await;
    let proxy_health = common::http_get(server.proxy_addr, "/healthz")
        .await
        .expect("proxy health");
    let admin_health = common::http_get(server.admin_addr, "/admin/health")
        .await
        .expect("admin health");
    let admin_status = common::admin_get(server.admin_addr, "/admin/v1/status", "admin-token")
        .await
        .expect("admin status");
    let scheduler_status =
        common::admin_get(server.admin_addr, "/admin/scheduler/status", "admin-token")
            .await
            .expect("scheduler status");

    assert_eq!(proxy_health.status, 200);
    assert_eq!(admin_health.status, 200);
    assert_eq!(admin_status.status, 200);
    assert_eq!(scheduler_status.status, 200);

    let proxy_json: serde_json::Value =
        serde_json::from_str(&proxy_health.body).expect("proxy health JSON");
    let health_json: serde_json::Value =
        serde_json::from_str(&admin_health.body).expect("admin health JSON");
    let status_json: serde_json::Value =
        serde_json::from_str(&admin_status.body).expect("admin status JSON");
    let scheduler_json: serde_json::Value =
        serde_json::from_str(&scheduler_status.body).expect("scheduler status JSON");
    assert_eq!(
        proxy_json["build"]["git_sha"], status_json["git_sha"],
        "git_sha differs between proxy health and admin status"
    );
    assert_eq!(health_json["status"], "ok");
    assert!(health_json["uptime_secs"].is_u64());
    let profile = proxy_json["build"]["profile"]
        .as_str()
        .expect("build profile");
    assert!(
        matches!(profile, "debug" | "release"),
        "unexpected Cargo build profile: {profile}"
    );
    let rustc = proxy_json["build"]["rustc"]
        .as_str()
        .expect("rustc provenance");
    assert!(
        rustc.starts_with("rustc "),
        "unexpected rustc provenance: {rustc}"
    );
    let target = proxy_json["build"]["target"]
        .as_str()
        .expect("target provenance");
    assert!(
        target.contains(std::env::consts::ARCH),
        "target does not match compiled architecture: {target}"
    );

    for (proxy_key, status_key) in [
        ("rustc", "rust_version"),
        ("profile", "profile"),
        ("target", "target"),
    ] {
        assert_eq!(
            proxy_json["build"][proxy_key], status_json["build"][status_key],
            "{proxy_key} differs between proxy health and admin status"
        );
    }
    assert_eq!(health_json["git_sha"], proxy_json["build"]["git_sha"]);
    assert_eq!(status_json["version"], proxy_json["build"]["version"]);
    assert_eq!(health_json["version"], proxy_json["build"]["version"]);
    assert_eq!(
        scheduler_json["schedule_version"],
        proxy_json["build"]["git_sha"]
    );

    let cli = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .arg("--version")
        .output()
        .expect("CLI version");
    assert!(cli.status.success());
    assert_eq!(
        String::from_utf8(cli.stdout)
            .expect("UTF-8 CLI version")
            .trim(),
        format!(
            "cc-lb {} ({})",
            proxy_json["build"]["version"]
                .as_str()
                .expect("build version"),
            proxy_json["build"]["git_sha"].as_str().expect("build SHA"),
        )
    );

    for route in ["/admin/v1/status", "/admin/scheduler/status"] {
        let response = common::http_get(server.admin_addr, route)
            .await
            .expect("unauthenticated admin request");
        assert_eq!(response.status, 401, "{route} must remain protected");
    }
}
