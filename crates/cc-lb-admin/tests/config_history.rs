use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{ConfigStore, HistorySummary};

#[tokio::test]
async fn t2__config_history_route_returns_applied_history() {
    let (_dir, storage) = config_admin_common::temp_storage().await;
    let config = Config::default();
    storage
        .append_config_history(
            7,
            toml::to_string_pretty(&config).unwrap(),
            1234,
            HistorySummary {
                upstreams: 0,
                principals: 0,
                plugin_count: 0,
                tls_enabled: false,
            },
        )
        .await
        .unwrap();
    let app = config_admin_common::app(config_admin_common::test_state(config, Some(storage)));

    let (status, _, json, _) =
        config_admin_common::authed_json(app, "GET", "/admin/config/history?limit=1", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["history"][0]["revision"], 7);
    assert_eq!(json["history"][0]["applied_at_unix_secs"], 1234);
}
