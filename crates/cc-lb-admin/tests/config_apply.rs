use crate::config_admin_common::{
    TestReloader, app, apply_state, authed_json, config_value, minimal_config, put_body,
    temp_storage,
};

use std::sync::Arc;

use axum::http::StatusCode;

#[tokio::test]
async fn t2__config_apply_audit_records_static_token_actor_identity() {
    let (_, storage) = temp_storage().await;
    let config_path = std::path::PathBuf::new();
    let reloader = Arc::new(TestReloader::new(config_path.clone(), minimal_config()));
    let app = app(apply_state(storage, config_path, reloader).await);

    let (status, _, draft, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(config_value(180), 0)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(draft["revision"], 1);

    let (status, _, applied, _) =
        authed_json(app.clone(), "POST", "/admin/v1/config/apply", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(applied["status"], "applied");

    let (status, _, audit, _) = authed_json(app, "GET", "/admin/v1/audit", None).await;
    assert_eq!(status, StatusCode::OK);
    let entry = audit["entries"]
        .as_array()
        .expect("audit entries array")
        .iter()
        .find(|entry| entry["admin_action"] == "config_apply")
        .expect("config apply audit entry");
    assert_eq!(entry["actor_authority"], "static-token");
    assert_eq!(entry["actor_subject"], "legacy");
    assert_eq!(entry["actor_kind"], "break_glass");
}
