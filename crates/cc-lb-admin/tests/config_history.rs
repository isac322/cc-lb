use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_storage_api::ConfigStore;

#[tokio::test]
async fn config_history_returns_metadata_only_entries() {
    let (dir, storage) = config_admin_common::temp_storage().await;
    storage.append_config_history(7, 1234).await.unwrap();
    let config = config_admin_common::minimal_config();
    let path = config_admin_common::write_config_file(dir.path(), &config);
    let app = config_admin_common::app(config_admin_common::test_state_with_config_path(
        config,
        Some(storage),
        path,
    ));

    let (status, _, json, _) =
        config_admin_common::authed_json(app, "GET", "/admin/v1/config/history?limit=1", None)
            .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json,
        serde_json::json!({
            "entries": [{
                "revision": 7,
                "saved_at_unix_secs": 1234,
            }],
        })
    );
    assert!(!json.to_string().contains("toml"));
    assert!(!json.to_string().contains("storage.url"));
}
