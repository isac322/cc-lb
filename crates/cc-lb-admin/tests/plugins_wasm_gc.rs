use axum::http::StatusCode;
use cc_lb_storage_api::PluginBlobRepo;
use serde_json::json;

use crate::admin_test_common::spawn_admin_server;

#[tokio::test]
async fn t2__plugins_wasm_gc_removes_unreferenced_blobs() {
    let server = spawn_admin_server().await;
    let orphan_sha = [0xa5; 32];
    server
        .storage
        .put_blob(&orphan_sha, b"orphan wasm bytes")
        .await
        .expect("orphan blob is stored");

    let (status, _, body) = server
        .client
        .post_json("/admin/v1/plugins/wasm/gc", json!({}))
        .await;

    let expected_sha = orphan_sha
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "removed": [expected_sha], "count": 1 }));
    assert_eq!(
        server
            .storage
            .get_blob(&orphan_sha)
            .await
            .expect("blob lookup succeeds"),
        None
    );
}
