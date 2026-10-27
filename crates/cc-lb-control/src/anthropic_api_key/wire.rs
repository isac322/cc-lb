//! Admin wire JSON for API-key upstreams. Must equal the types in
//! `crates/cc-lb-admin/web/src/lib/api.ts`; the contract fixtures under
//! `web/src/lib/__contract__/` pin both sides.

use std::fmt;

use cc_lb_storage_api::{
    ApiKeyClass, ApiKeyOrgCapability, ApiKeyResolution, ApiKeyType, ApiKeyWorkspace,
    ApiUsageAttribution, RemediationCode, UpstreamKind,
};
use serde::{Deserialize, Serialize};

use super::billing::BillingStopKind;

/// `POST /admin/v1/upstreams/preflight` body. Unknown fields are ignored and
/// `Debug` never prints the key.
#[derive(Clone, Default, Deserialize)]
pub struct PreflightRequest {
    #[serde(default)]
    pub api_key_value: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub kind: Option<UpstreamKind>,
}

impl fmt::Debug for PreflightRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreflightRequest")
            .field(
                "api_key_value",
                &self.api_key_value.as_ref().map(|_| "[REDACTED]"),
            )
            .field("workspace_id", &self.workspace_id)
            .field("base_url", &self.base_url)
            .field("kind", &self.kind)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreflightVerdict {
    Ok,
    NeedsWorkspace,
    Rejected,
    Unreachable,
}

/// Inference capability on the wire; `ok_limited` means `count_tokens`
/// answered 429, so the key works.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireInference {
    Ok,
    OkLimited,
    Denied,
    Unverified,
}

/// One workspace from Anthropic's org Admin API.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AnthropicWorkspace {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightClassification {
    pub key_class: ApiKeyClass,
    pub needs_workspace: bool,
    pub workspace: ApiKeyWorkspace,
    pub key_type: Option<ApiKeyType>,
}

/// `items` is filled only when `state` is `ok`; otherwise it is empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightWorkspaces {
    pub state: ApiKeyOrgCapability,
    pub items: Vec<AnthropicWorkspace>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightCapabilities {
    pub inference: WireInference,
    pub workspace_list: ApiKeyOrgCapability,
    pub usage_report: ApiKeyOrgCapability,
    pub api_key_resolution: ApiKeyResolution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Remediation {
    pub code: RemediationCode,
}

/// Preflight/Re-check envelope. Always HTTP 200 with
/// `Cache-Control: no-store`; the verdict is data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightWireResponse {
    pub verdict: PreflightVerdict,
    pub key_hint: Option<String>,
    pub classification: PreflightClassification,
    pub workspaces: PreflightWorkspaces,
    pub capabilities: PreflightCapabilities,
    pub remediation: Vec<Remediation>,
    /// Unix seconds.
    pub checked_at: u64,
}

/// Nested failure body `{"error":{"code":…,"message":…}}` for every failure
/// that carries a remediation code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemediationErrorBody {
    pub error: RemediationErrorInner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemediationErrorInner {
    pub code: RemediationCode,
    pub message: String,
}

/// `source` of `GET /usage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyUsageSource {
    Real,
    Estimated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyUsageTotals {
    pub cost_usd_micros: u64,
    /// Always `true`: cost is computed at list price.
    pub cost_is_estimate: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyBillingStop {
    pub kind: BillingStopKind,
    pub observed_at_unix_secs: u64,
}

/// Calendar month (UTC) strip of `GET /usage`, independent of `range`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyUsageMonth {
    pub start_unix_secs: u64,
    pub end_unix_secs: u64,
    pub tokens: u64,
    pub cost_usd_micros: u64,
    pub billing_stop: Option<KeyBillingStop>,
}

/// Fields `GET /usage` adds to the flattened dashboard usage response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyUsageEnvelope {
    pub source: KeyUsageSource,
    pub attribution: ApiUsageAttribution,
    /// The workspace a `workspace` attribution sums; `None` for `api_key`.
    pub attribution_workspace_id: Option<String>,
    pub stale: bool,
    /// When the poller last got a usable report (Unix seconds); `None`
    /// when no poll has succeeded. Independent of `range`.
    pub last_reported_unix_secs: Option<u64>,
    pub totals: KeyUsageTotals,
    pub fix: Option<Remediation>,
    pub month: KeyUsageMonth,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use cc_lb_storage_api::{ApiKeyWorkspaceMode, RemediationCode};

    use super::*;

    const UPDATE_ENV: &str = "UPDATE_API_KEY_CONTRACT";

    fn contract_path(file: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../cc-lb-admin/web/src/lib/__contract__")
            .join(file)
    }

    /// Compares `actual` with the committed fixture after serde
    /// normalization, or rewrites the fixture under `UPDATE_API_KEY_CONTRACT=1`.
    fn assert_contract(file: &str, actual: serde_json::Value) {
        let path = contract_path(file);
        if std::env::var(UPDATE_ENV).is_ok_and(|value| value == "1") {
            let rendered = serde_json::to_string_pretty(&actual).map(|text| text + "\n");
            assert!(rendered.is_ok(), "fixture renders");
            let written = rendered.map(|text| std::fs::write(&path, text));
            assert!(matches!(written, Ok(Ok(()))), "write {}", path.display());
            return;
        }
        let committed = std::fs::read_to_string(&path)
            .map_err(|error| error.to_string())
            .and_then(|text| {
                serde_json::from_str::<serde_json::Value>(&text).map_err(|error| error.to_string())
            });
        assert_eq!(
            committed,
            Ok(actual),
            "{} drifted; regenerate with {UPDATE_ENV}=1",
            path.display()
        );
    }

    fn preflight_ok_org() -> PreflightWireResponse {
        PreflightWireResponse {
            verdict: PreflightVerdict::Ok,
            key_hint: Some("sk-ant-api03-...AbCd".to_owned()),
            classification: PreflightClassification {
                key_class: ApiKeyClass::OrgScoped,
                needs_workspace: true,
                workspace: ApiKeyWorkspace {
                    mode: ApiKeyWorkspaceMode::Selected,
                    id: Some("wrkspc_01Prod".to_owned()),
                    name: Some("Production".to_owned()),
                },
                key_type: Some(ApiKeyType::ServiceAccount),
            },
            workspaces: PreflightWorkspaces {
                state: ApiKeyOrgCapability::Ok,
                items: vec![
                    AnthropicWorkspace {
                        id: "wrkspc_01Prod".to_owned(),
                        name: "Production".to_owned(),
                    },
                    AnthropicWorkspace {
                        id: "wrkspc_02Stage".to_owned(),
                        name: "Staging".to_owned(),
                    },
                ],
            },
            capabilities: PreflightCapabilities {
                inference: WireInference::OkLimited,
                workspace_list: ApiKeyOrgCapability::Ok,
                usage_report: ApiKeyOrgCapability::Ok,
                api_key_resolution: ApiKeyResolution::Resolved,
            },
            remediation: Vec::new(),
            checked_at: 1_760_000_000,
        }
    }

    /// CTR-01: the Rust vocabulary is exactly the 16 codes in the shared fixture.
    #[test]
    fn ctr_01_remediation_vocabulary_matches_contract() {
        let codes: Vec<&str> = RemediationCode::ALL
            .iter()
            .map(|code| code.as_str())
            .collect();
        assert_eq!(codes.len(), 16);
        let serialized = serde_json::to_value(RemediationCode::ALL).ok();
        assert_eq!(serialized, Some(serde_json::json!(codes)));
        assert_contract("remediation_codes.json", serde_json::json!(codes));
    }

    /// PF-01: the preflight serializer matches the shared wire fixture.
    #[test]
    fn pf_01_preflight_wire_contract() {
        let value = serde_json::to_value(preflight_ok_org()).ok();
        assert!(value.is_some());
        let Some(value) = value else { return };
        let mut keys: Vec<&str> = value
            .as_object()
            .map(|object| object.keys().map(String::as_str).collect())
            .unwrap_or_default();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "capabilities",
                "checked_at",
                "classification",
                "key_hint",
                "remediation",
                "verdict",
                "workspaces"
            ]
        );
        let round_trip = serde_json::from_value::<PreflightWireResponse>(value.clone()).ok();
        assert_eq!(round_trip, Some(preflight_ok_org()));
        assert_contract("preflight_ok_org.json", value);
    }

    #[test]
    fn pf_01_rejected_preflight_serializes_nulls_and_codes() {
        let response = PreflightWireResponse {
            verdict: PreflightVerdict::Rejected,
            key_hint: None,
            classification: PreflightClassification {
                key_class: ApiKeyClass::Admin,
                needs_workspace: false,
                workspace: ApiKeyWorkspace {
                    mode: ApiKeyWorkspaceMode::Required,
                    id: None,
                    name: None,
                },
                key_type: None,
            },
            workspaces: PreflightWorkspaces {
                state: ApiKeyOrgCapability::NotApplicable,
                items: Vec::new(),
            },
            capabilities: PreflightCapabilities {
                inference: WireInference::Unverified,
                workspace_list: ApiKeyOrgCapability::NotApplicable,
                usage_report: ApiKeyOrgCapability::NotApplicable,
                api_key_resolution: ApiKeyResolution::NotApplicable,
            },
            remediation: vec![Remediation {
                code: RemediationCode::KeyAdmin,
            }],
            checked_at: 7,
        };
        assert_eq!(
            serde_json::to_value(response).ok(),
            Some(serde_json::json!({
                "verdict": "rejected",
                "key_hint": null,
                "classification": {
                    "key_class": "admin",
                    "needs_workspace": false,
                    "workspace": { "mode": "required" },
                    "key_type": null,
                },
                "workspaces": { "state": "not_applicable", "items": [] },
                "capabilities": {
                    "inference": "unverified",
                    "workspace_list": "not_applicable",
                    "usage_report": "not_applicable",
                    "api_key_resolution": "not_applicable",
                },
                "remediation": [{ "code": "key_admin" }],
                "checked_at": 7,
            }))
        );
    }

    #[test]
    fn remediation_error_body_is_nested() {
        let body = RemediationErrorBody {
            error: RemediationErrorInner {
                code: RemediationCode::WorkspaceRequired,
                message: "choose".to_owned(),
            },
        };
        assert_eq!(
            serde_json::to_value(body).ok(),
            Some(serde_json::json!({
                "error": { "code": "workspace_required", "message": "choose" }
            }))
        );
    }

    #[test]
    fn preflight_request_ignores_unknown_fields() {
        let parsed: Result<PreflightRequest, _> = serde_json::from_str(
            r#"{"api_key_value":"k","api_key_env":"ANTHROPIC_API_KEY","extra":1}"#,
        );
        assert!(parsed.is_ok_and(|request| request.api_key_value.as_deref() == Some("k")));
    }

    /// SEC-09: Debug of `PreflightRequest` never prints the key.
    #[test]
    fn sec_09_debug_redacts_preflight_request() {
        let request = PreflightRequest {
            api_key_value: Some("sk-ant-api03-SECRETVALUE1234".to_owned()),
            workspace_id: Some("wrkspc_1".to_owned()),
            base_url: None,
            kind: None,
        };
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("SECRETVALUE"), "{rendered}");
        assert!(rendered.contains("[REDACTED]"), "{rendered}");
        assert!(rendered.contains("wrkspc_1"), "{rendered}");
    }
}
