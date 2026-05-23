mod config_admin_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_storage_redb::Storage;
use config_admin_common::{app, authed_json, minimal_config, put_body, test_state};
use serde_json::json;

#[tokio::test]
async fn draft_persists_after_storage_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.redb");
    let draft = json!({
        "quotas": {
            "default_requests_per_window": 77,
            "default_window_secs": 60
        }
    });

    {
        let storage = Arc::new(Storage::open(&db_path, [0; 32]).unwrap());
        let app = app(test_state(minimal_config(), Some(storage)));
        let (status, _, json, _) = authed_json(
            app,
            "PUT",
            "/admin/config/draft",
            Some(put_body(draft.clone(), 0)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["revision"], 1);
    }

    let storage = Arc::new(Storage::open(&db_path, [0; 32]).unwrap());
    let app = app(test_state(minimal_config(), Some(storage)));
    let (status, _, json, _) = authed_json(app, "GET", "/admin/config/draft", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["revision"], 1);
    assert_eq!(json["draft"], draft);
}
