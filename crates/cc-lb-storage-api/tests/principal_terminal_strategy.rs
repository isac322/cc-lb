use cc_lb_domain::TerminalStrategy;
use cc_lb_storage_api::{PrincipalKind, PrincipalRecord};
use serde_json::json;
use uuid::Uuid;

#[test]
fn principal_record_router_terminal_strategy_defaults_to_first_pick() {
    let record = PrincipalRecord {
        id: Uuid::nil(),
        name: "test".to_string(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["gpt-4".to_string()],
        allowed_upstreams: vec![],
        default_limits: vec![],
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 1000,
        updated_at_unix_secs: 1000,
        router_terminal_strategy: TerminalStrategy::FirstPick,
        cache_keepalive: None,
        smart_routing_enabled: true,
    };

    assert_eq!(record.router_terminal_strategy, TerminalStrategy::FirstPick);
}

#[test]
fn principal_record_router_terminal_strategy_deserializes_with_default() {
    let json_str = r#"{
        "id": "00000000-0000-0000-0000-000000000000",
        "name": "test",
        "kind": "machine",
        "allowed_models": ["gpt-4"],
        "allowed_upstreams": [],
        "default_limits": [],
        "enabled": true,
        "last_apply_error": null,
        "last_apply_at_unix_secs": null,
        "deleted_at_unix_secs": null,
        "revision": 1,
        "created_at_unix_secs": 1000,
        "updated_at_unix_secs": 1000
    }"#;

    let record: PrincipalRecord = serde_json::from_str(json_str).expect("should deserialize");
    assert_eq!(record.router_terminal_strategy, TerminalStrategy::FirstPick);
}

#[test]
fn principal_record_router_terminal_strategy_serialization_roundtrip() {
    let original = PrincipalRecord {
        id: Uuid::new_v4(),
        name: "test-principal".to_string(),
        kind: PrincipalKind::Human,
        allowed_models: vec!["claude-3".to_string()],
        allowed_upstreams: vec![Uuid::new_v4()],
        default_limits: vec![],
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 42,
        created_at_unix_secs: 1000,
        updated_at_unix_secs: 2000,
        router_terminal_strategy: TerminalStrategy::Random,
        cache_keepalive: None,
        smart_routing_enabled: true,
    };

    let json_str = serde_json::to_string(&original).expect("should serialize");
    let deserialized: PrincipalRecord =
        serde_json::from_str(&json_str).expect("should deserialize");

    assert_eq!(original, deserialized);
    assert_eq!(
        deserialized.router_terminal_strategy,
        TerminalStrategy::Random
    );
}

#[test]
fn principal_record_router_terminal_strategy_all_variants() {
    let strategies = vec![TerminalStrategy::FirstPick, TerminalStrategy::Random];

    for strategy in strategies {
        let record = PrincipalRecord {
            id: Uuid::nil(),
            name: "test".to_string(),
            kind: PrincipalKind::Admin,
            allowed_models: vec![],
            allowed_upstreams: vec![],
            default_limits: vec![],
            enabled: true,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            created_at_unix_secs: 1000,
            updated_at_unix_secs: 1000,
            router_terminal_strategy: strategy.clone(),
            cache_keepalive: None,
            smart_routing_enabled: true,
        };

        let json_str = serde_json::to_string(&record).expect("should serialize");
        let deserialized: PrincipalRecord =
            serde_json::from_str(&json_str).expect("should deserialize");

        assert_eq!(deserialized.router_terminal_strategy, strategy);
    }
}

#[test]
fn principal_record_router_terminal_strategy_json_format() {
    let json_value = json!({
        "id": "00000000-0000-0000-0000-000000000001",
        "name": "prod-principal",
        "kind": "machine",
        "allowed_models": ["gpt-4"],
        "allowed_upstreams": [],
        "default_limits": [],
        "enabled": true,
        "last_apply_error": null,
        "last_apply_at_unix_secs": null,
        "deleted_at_unix_secs": null,
        "revision": 1,
        "created_at_unix_secs": 1000,
        "updated_at_unix_secs": 1000,
        "router_terminal_strategy": "random"
    });

    let record: PrincipalRecord = serde_json::from_value(json_value).expect("should deserialize");
    assert_eq!(record.router_terminal_strategy, TerminalStrategy::Random);
}
