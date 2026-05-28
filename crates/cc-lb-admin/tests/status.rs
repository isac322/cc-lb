use std::{collections::HashMap, path::PathBuf, sync::Arc};

use arc_swap::ArcSwap;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, CurrentConfig, LastReloadStatus, ReloadOutcome, router};
use cc_lb_config::{Config, PluginRef, PluginsConfig, PrincipalSpec};
use cc_lb_core::api_keys::{
    concurrent_guard::KeyConcurrencyManager, limit_engine::LimitEngine,
    principal_view::PrincipalView,
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

struct StaticConfig {
    config: Arc<Config>,
    last_reload_status: Option<LastReloadStatus>,
}

impl CurrentConfig for StaticConfig {
    fn current_config(&self) -> Arc<Config> {
        self.config.clone()
    }

    fn last_reload_status(&self) -> Option<LastReloadStatus> {
        self.last_reload_status.clone()
    }
}

fn test_state(config: Config, last_reload_status: Option<LastReloadStatus>) -> AdminState {
    let principal_view = Arc::new(ArcSwap::from(
        PrincipalView::from_config(&config, HashMap::new()).expect("principal view builds"),
    ));
    AdminState {
        storage: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            principal_view.clone(),
        ),
        lifecycle: None,
        audit_sink: None,
        principal_view,
        config: Arc::new(StaticConfig {
            config: Arc::new(config),
            last_reload_status,
        }),
        admin_token: Some("test-token".to_owned()),
        start_time: std::time::Instant::now(),
    }
}

fn plugin_ref(name: &str, wasm_path: &str, config: Value) -> PluginRef {
    PluginRef {
        name: name.to_owned(),
        wasm_path: Some(PathBuf::from(wasm_path)),
        config,
        ..Default::default()
    }
}

async fn get_status(config: Config, last_reload_status: Option<LastReloadStatus>) -> Value {
    let response = router(test_state(config, last_reload_status))
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/status")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

fn config_hash(config: &Value) -> String {
    let serialized = serde_json::to_vec(config).unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &serialized);
    digest.as_ref()[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn has_config_key(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key("config") || object.values().any(has_config_key)
        }
        Value::Array(values) => values.iter().any(has_config_key),
        _ => false,
    }
}

#[tokio::test]
async fn admin_status_includes_per_principal_plugins_with_redacted_config() {
    let router_config = json!({ "secret": "router-token", "mode": "strict" });
    let hook_config = json!({ "api_key": "audit-token", "sample_rate": 1 });
    let mut config = Config {
        plugins: PluginsConfig {
            router_plugin: Some(plugin_ref(
                "global-router",
                "/plugins/global-router.wasm",
                json!({ "global": true }),
            )),
            observability_hooks: vec![plugin_ref(
                "global-hook",
                "/plugins/global-hook.wasm",
                json!({ "global_hook": true }),
            )],
        },
        ..Default::default()
    };
    config.principals.insert(
        "alice".to_owned(),
        PrincipalSpec {
            router_plugin: Some(plugin_ref(
                "alice-router",
                "/plugins/alice-router.wasm",
                router_config.clone(),
            )),
            observability_hooks: Some(vec![plugin_ref(
                "audit",
                "/plugins/audit.wasm",
                hook_config.clone(),
            )]),
            ..Default::default()
        },
    );
    config
        .principals
        .insert("bob".to_owned(), PrincipalSpec::default());

    let json = get_status(config, None).await;

    assert!(json["plugins"].is_array());
    assert_eq!(json["plugins"][0]["name"], json!("global-router"));
    assert_eq!(json["plugins"][1]["name"], json!("global-hook"));
    assert_eq!(
        json["principals"]["alice"]["router_plugin"]["name"],
        json!("alice-router")
    );
    assert_eq!(
        json["principals"]["alice"]["router_plugin"]["wasm_path"],
        json!("/plugins/alice-router.wasm")
    );
    assert_eq!(
        json["principals"]["alice"]["router_plugin"]["config_hash"],
        json!(config_hash(&router_config))
    );
    assert_eq!(
        json["principals"]["alice"]["observability_hooks"][0]["name"],
        json!("audit")
    );
    assert_eq!(
        json["principals"]["alice"]["observability_hooks"][0]["config_hash"],
        json!(config_hash(&hook_config))
    );
    assert_eq!(json["principals"]["bob"]["router_plugin"], Value::Null);
    assert_eq!(json["principals"]["bob"]["observability_hooks"], json!([]));
    assert_eq!(json["last_reload_status"], Value::Null);
    assert!(
        !has_config_key(&json),
        "status response leaked raw config: {json}"
    );
}

#[tokio::test]
async fn admin_status_zero_principal_config_keeps_legacy_plugins_and_empty_principals() {
    let config = Config {
        plugins: PluginsConfig {
            router_plugin: Some(plugin_ref(
                "global-router",
                "/plugins/global-router.wasm",
                json!({ "global": true }),
            )),
            observability_hooks: vec![plugin_ref(
                "global-hook",
                "/plugins/global-hook.wasm",
                json!({ "global_hook": true }),
            )],
        },
        ..Default::default()
    };

    let json = get_status(config, None).await;

    assert_eq!(json["plugins"].as_array().unwrap().len(), 2);
    assert_eq!(json["plugins"][0]["slot"], json!("router"));
    assert_eq!(json["plugins"][1]["slot"], json!("observability"));
    assert_eq!(json["principals"], json!({}));
    assert_eq!(json["last_reload_status"], Value::Null);
}

#[tokio::test]
async fn admin_status_includes_last_reload_status_from_current_config() {
    let last_reload_status = LastReloadStatus {
        timestamp_unix_secs: 1_717_171_717,
        outcome: ReloadOutcome::Success,
        config_path: Some("/etc/cc-lb/config.toml".to_owned()),
    };

    let json = get_status(Config::default(), Some(last_reload_status)).await;

    assert_eq!(
        json["last_reload_status"]["timestamp_unix_secs"],
        json!(1_717_171_717_u64)
    );
    assert_eq!(json["last_reload_status"]["outcome"], json!("Success"));
    assert_eq!(
        json["last_reload_status"]["config_path"],
        json!("/etc/cc-lb/config.toml")
    );
}
