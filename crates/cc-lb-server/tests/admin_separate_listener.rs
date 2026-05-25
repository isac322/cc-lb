mod common;

#[tokio::test]
async fn admin_separate_listener() {
    let server = common::spawn_test_server().await;
    let admin = common::http_get(server.admin_addr, "/admin/health")
        .await
        .expect("admin health");
    let proxy = common::http_get(server.proxy_addr, "/admin/health")
        .await
        .expect("proxy admin path");

    assert_eq!(admin.status, 200);
    assert!(admin.body.contains(r#""status":"ok""#));
    assert_ne!(proxy.status, 200);
}
