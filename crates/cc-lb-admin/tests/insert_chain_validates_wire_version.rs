use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, PluginRegistryStore, PluginSlotKind, PrincipalCreate,
    PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
};
use config_admin_common::{app, authed_json, temp_storage, test_state};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn insert_chain_slot_validation_table() {
    enum Target {
        Registry {
            seed: u8,
            name: &'static str,
            wire_version: u8,
        },
        Builtin,
    }

    enum ExpectedBody {
        UnsupportedSlot,
        NullWireVersion,
        BuiltinSubscriptionPreference,
    }

    struct Case {
        case: &'static str,
        principal_name: &'static str,
        target: Target,
        slot: &'static str,
        expected_status: StatusCode,
        expected_body: ExpectedBody,
    }

    let cases = [
        Case {
            case: "insert_chain_rejects_unsupported_slot_from_registry_metadata",
            principal_name: "principal-wire-too-new",
            target: Target::Registry {
                seed: 41,
                name: "wire-v1-plugin",
                wire_version: 1,
            },
            slot: "Shape",
            expected_status: StatusCode::BAD_REQUEST,
            expected_body: ExpectedBody::UnsupportedSlot,
        },
        Case {
            case: "insert_chain_accepts_registry_entry",
            principal_name: "principal-wire-equal",
            target: Target::Registry {
                seed: 42,
                name: "wire-v1-plugin-equal",
                wire_version: 1,
            },
            slot: "Router",
            expected_status: StatusCode::CREATED,
            expected_body: ExpectedBody::NullWireVersion,
        },
        Case {
            case: "insert_chain_accepts_unspecified_metadata",
            principal_name: "principal-wire-unspecified",
            target: Target::Registry {
                seed: 43,
                name: "wire-default-plugin",
                wire_version: 1,
            },
            slot: "Router",
            expected_status: StatusCode::CREATED,
            expected_body: ExpectedBody::NullWireVersion,
        },
        Case {
            case: "insert_chain_accepts_builtin_subscription_preference",
            principal_name: "principal-wire-builtin",
            target: Target::Builtin,
            slot: "Router",
            expected_status: StatusCode::CREATED,
            expected_body: ExpectedBody::BuiltinSubscriptionPreference,
        },
    ];

    for case in cases {
        let (_dir, storage) = temp_storage().await;
        let principal_id = seed_principal(&storage, case.principal_name).await;
        let wasm_registry_id = match case.target {
            Target::Registry {
                seed,
                name,
                wire_version,
            } => {
                seed_registry_with_wire_version(&storage, seed, name, wire_version)
                    .await
                    .id
            }
            Target::Builtin => BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
        };
        let app = app(test_state(Config::default(), Some(storage)));

        let (status, _, body, _) = authed_json(
            app,
            "POST",
            &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
            Some(json!({
                "slot": case.slot,
                "wasm_registry_id": wasm_registry_id,
            })),
        )
        .await;

        assert_eq!(status, case.expected_status, "case={}", case.case);
        match case.expected_body {
            ExpectedBody::UnsupportedSlot => {
                assert_eq!(body["error"], "unsupported_slot", "case={}", case.case);
            }
            ExpectedBody::NullWireVersion => {
                assert!(
                    body.get("wire_version")
                        .is_none_or(serde_json::Value::is_null),
                    "case={}",
                    case.case
                );
            }
            ExpectedBody::BuiltinSubscriptionPreference => {
                assert_eq!(
                    body["wasm_registry_id"],
                    BUILTIN_SUBSCRIPTION_PREFERENCE_ID.to_string(),
                    "case={}",
                    case.case
                );
            }
        }
    }
}

async fn seed_principal(storage: &cc_lb_testkit::InMemoryStorage, name: &str) -> Uuid {
    storage
        .create(
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1_800_000_000,
        )
        .await
        .unwrap()
        .id
}

async fn seed_registry_with_wire_version(
    storage: &cc_lb_testkit::InMemoryStorage,
    seed: u8,
    name: &str,
    wire_version: u8,
) -> cc_lb_storage_api::WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed; seed as usize],
                size_bytes: seed as u64,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                schema_hash: None,
                name: name.to_owned(),
                version: None,
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                description: format!("{name} description"),
                usage: format!("wire v{wire_version} fixture"),
                hook_metadata: Default::default(),
                supported_slots: vec![PluginSlotKind::Router],
            },
        )
        .await
        .unwrap();
    entry
}
