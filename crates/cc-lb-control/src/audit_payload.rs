use std::fmt;

use serde::Serialize;
use uuid::Uuid;

use cc_lb_storage_api::{AuditActorFields, AuditEntry};

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum AuditPayload {
    PrincipalCreate {
        principal_id: String,
        principal_kind: String,
    },
    PrincipalUpdate {
        principal_id: String,
        fields_changed: Vec<&'static str>,
    },
    PrincipalDelete {
        principal_id: String,
    },
    /// `key_hint`, `verdict`, and `remediation` are set for API-key upstreams;
    /// `verdict` is `None` when the create skipped the preflight.
    UpstreamCreate {
        upstream_id: String,
        kind: String,
        key_hint: Option<String>,
        verdict: Option<String>,
        remediation: Vec<&'static str>,
    },
    /// `key_hint`, `verdict`, and `remediation` are set when the update
    /// re-probed or replaced an API key; `old_host`/`new_host` when
    /// `base_url` changed (`None` = the default Anthropic host).
    UpstreamUpdate {
        upstream_id: String,
        fields_changed: Vec<&'static str>,
        key_hint: Option<String>,
        verdict: Option<String>,
        remediation: Vec<&'static str>,
        old_host: Option<String>,
        new_host: Option<String>,
    },
    /// Candidate-key preflight: nothing is persisted but this row.
    UpstreamPreflight {
        key_hint: Option<String>,
        verdict: String,
        remediation: Vec<&'static str>,
    },
    /// Stored-key Re-check.
    UpstreamRecheck {
        upstream_id: String,
        key_hint: Option<String>,
        verdict: String,
        remediation: Vec<&'static str>,
    },
    UpstreamDelete {
        upstream_id: String,
    },
    UpstreamEnable {
        upstream_id: String,
    },
    UpstreamDisable {
        upstream_id: String,
    },
    UpstreamOauthStart {
        upstream_id: String,
        upstream_name: String,
    },
    UpstreamOauthComplete {
        upstream_id: String,
        upstream_name: String,
        expires_at_unix_secs: u64,
        access_token_fingerprint: String,
    },
    UpstreamOauthRefreshSuccess {
        upstream_id: String,
        upstream_name: String,
        expires_at_unix_secs: u64,
        access_token_fingerprint: String,
        replica_id: Uuid,
    },
    UpstreamOauthRefreshFailure {
        upstream_id: String,
        upstream_name: String,
        reason: String,
        replica_id: Uuid,
    },
    PluginRegistryUpload {
        sha256: String,
        size_bytes: u64,
        original_filename: String,
    },
    PluginRegistryDelete {
        sha256: String,
    },
    PluginChainUpdate {
        principal_id: String,
        slots_changed: Vec<&'static str>,
        wasm_registry_id: String,
        sha256_hex: String,
        supported_slots: Vec<String>,
    },
    PluginRegistryLabelUpdate {
        wasm_registry_id: String,
        label: Option<String>,
    },
    PluginWasmGc {
        removed_count: usize,
    },
}

impl fmt::Display for AuditPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuditPayload::PrincipalCreate {
                principal_id,
                principal_kind,
            } => write!(
                f,
                "principal_create(id={}, kind={})",
                principal_id, principal_kind
            ),
            AuditPayload::PrincipalUpdate {
                principal_id,
                fields_changed,
            } => write!(
                f,
                "principal_update(id={}, fields={})",
                principal_id,
                fields_changed.join(",")
            ),
            AuditPayload::PrincipalDelete { principal_id } => {
                write!(f, "principal_delete(id={})", principal_id)
            }
            AuditPayload::UpstreamCreate {
                upstream_id,
                kind,
                key_hint,
                verdict,
                remediation,
            } => {
                write!(f, "upstream_create(id={}, kind={}", upstream_id, kind)?;
                write_key_check(f, key_hint.as_deref(), verdict.as_deref(), remediation)?;
                f.write_str(")")
            }
            AuditPayload::UpstreamUpdate {
                upstream_id,
                fields_changed,
                key_hint,
                verdict,
                remediation,
                old_host,
                new_host,
            } => {
                write!(
                    f,
                    "upstream_update(id={}, fields={}",
                    upstream_id,
                    fields_changed.join(",")
                )?;
                write_key_check(f, key_hint.as_deref(), verdict.as_deref(), remediation)?;
                if fields_changed.contains(&"base_url") {
                    write!(
                        f,
                        ", old_host={}, new_host={}",
                        old_host.as_deref().unwrap_or("default"),
                        new_host.as_deref().unwrap_or("default")
                    )?;
                }
                f.write_str(")")
            }
            AuditPayload::UpstreamPreflight {
                key_hint,
                verdict,
                remediation,
            } => {
                f.write_str("upstream_preflight(")?;
                write!(f, "key_hint={}", key_hint.as_deref().unwrap_or("none"))?;
                write!(
                    f,
                    ", verdict={}, remediation={})",
                    verdict,
                    remediation.join(",")
                )
            }
            AuditPayload::UpstreamRecheck {
                upstream_id,
                key_hint,
                verdict,
                remediation,
            } => {
                write!(f, "upstream_recheck(id={}", upstream_id)?;
                write_key_check(f, key_hint.as_deref(), Some(verdict), remediation)?;
                f.write_str(")")
            }
            AuditPayload::UpstreamDelete { upstream_id } => {
                write!(f, "upstream_delete(id={})", upstream_id)
            }
            AuditPayload::UpstreamEnable { upstream_id } => {
                write!(f, "upstream_enable(id={})", upstream_id)
            }
            AuditPayload::UpstreamDisable { upstream_id } => {
                write!(f, "upstream_disable(id={})", upstream_id)
            }
            AuditPayload::UpstreamOauthStart {
                upstream_id,
                upstream_name,
            } => {
                write!(
                    f,
                    "upstream_oauth_start(id={}, name={})",
                    upstream_id, upstream_name
                )
            }
            AuditPayload::UpstreamOauthComplete {
                upstream_id,
                upstream_name,
                expires_at_unix_secs,
                access_token_fingerprint,
            } => write!(
                f,
                "upstream_oauth_complete(id={}, name={}, expires={}, fingerprint={})",
                upstream_id, upstream_name, expires_at_unix_secs, access_token_fingerprint
            ),
            AuditPayload::UpstreamOauthRefreshSuccess {
                upstream_id,
                upstream_name,
                expires_at_unix_secs,
                access_token_fingerprint,
                replica_id,
            } => write!(
                f,
                "upstream_oauth_refresh_success(id={}, name={}, expires={}, fingerprint={}, replica={})",
                upstream_id,
                upstream_name,
                expires_at_unix_secs,
                access_token_fingerprint,
                replica_id
            ),
            AuditPayload::UpstreamOauthRefreshFailure {
                upstream_id,
                upstream_name,
                reason,
                replica_id,
            } => write!(
                f,
                "upstream_oauth_refresh_failure(id={}, name={}, reason={}, replica={})",
                upstream_id, upstream_name, reason, replica_id
            ),
            AuditPayload::PluginRegistryUpload {
                sha256,
                size_bytes,
                original_filename,
            } => write!(
                f,
                "plugin_registry_upload(sha256={}, size={}, filename={})",
                sha256, size_bytes, original_filename
            ),
            AuditPayload::PluginRegistryDelete { sha256 } => {
                write!(f, "plugin_registry_delete(sha256={})", sha256)
            }
            AuditPayload::PluginChainUpdate {
                principal_id,
                slots_changed,
                ..
            } => write!(
                f,
                "plugin_chain_update(principal={}, slots={})",
                principal_id,
                slots_changed.join(",")
            ),
            AuditPayload::PluginRegistryLabelUpdate {
                wasm_registry_id, ..
            } => write!(f, "plugin_registry_label_update(id={})", wasm_registry_id),
            AuditPayload::PluginWasmGc { removed_count } => {
                write!(f, "plugin_wasm_gc(removed={})", removed_count)
            }
        }
    }
}

/// Appends the API-key check facts. Never prints anything but the hint.
fn write_key_check(
    f: &mut fmt::Formatter<'_>,
    key_hint: Option<&str>,
    verdict: Option<&str>,
    remediation: &[&'static str],
) -> fmt::Result {
    if let Some(key_hint) = key_hint {
        write!(f, ", key_hint={key_hint}")?;
    }
    if let Some(verdict) = verdict {
        write!(f, ", verdict={verdict}")?;
    }
    if !remediation.is_empty() {
        write!(f, ", remediation={}", remediation.join(","))?;
    }
    Ok(())
}

impl From<AuditPayload> for AuditEntry {
    fn from(payload: AuditPayload) -> Self {
        let AuditActorFields {
            actor,
            authority,
            subject,
            kind,
            email,
        } = AuditActorFields::system("control");
        AuditEntry {
            admin_action: Some(payload.to_string()),
            actor: Some(actor),
            actor_authority: Some(authority),
            actor_subject: Some(subject),
            actor_kind: Some(kind),
            actor_email: email,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn audit_payload_serializes_without_secret_fields() {
        let payload = AuditPayload::UpstreamOauthComplete {
            upstream_id: "test-upstream".to_string(),
            upstream_name: "test-upstream".to_string(),
            expires_at_unix_secs: 1234567890,
            access_token_fingerprint: "abc12345".to_string(),
        };

        let display_str = format!("{}", payload);

        assert!(!display_str.contains("refresh_token"));
        assert!(!display_str.contains("access_token="));
        assert!(display_str.contains("fingerprint=abc12345"));
        assert!(display_str.contains("upstream_oauth_complete"));
    }

    #[test]
    fn oauth_complete_emits_fingerprint_only() {
        let payload = AuditPayload::UpstreamOauthComplete {
            upstream_id: "primary".to_string(),
            upstream_name: "primary".to_string(),
            expires_at_unix_secs: 1700000000,
            access_token_fingerprint: "deadbeef".to_string(),
        };

        let audit_entry: AuditEntry = payload.into();
        let admin_action = audit_entry.admin_action.unwrap();

        assert!(admin_action.contains("deadbeef"));
        assert!(!admin_action.contains("sk-ant"));
        assert!(!admin_action.contains("token="));
    }

    #[test]
    fn display_format_stable() {
        let payload = AuditPayload::PrincipalCreate {
            principal_id: "user-1".to_string(),
            principal_kind: "api_key".to_string(),
        };

        let display_str = format!("{}", payload);

        assert_eq!(display_str, "principal_create(id=user-1, kind=api_key)");
    }

    #[test]
    fn plugin_chain_update_serializes_registry_metadata() {
        let payload = AuditPayload::PluginChainUpdate {
            principal_id: "principal-1".to_owned(),
            slots_changed: vec!["router", "shape"],
            wasm_registry_id: "registry-1".to_owned(),
            sha256_hex: "abc123".to_owned(),
            supported_slots: vec!["router".to_owned(), "shape".to_owned()],
        };

        let value = serde_json::to_value(payload).unwrap();
        let payload = value
            .get("PluginChainUpdate")
            .and_then(|value| value.as_object())
            .expect("plugin chain update payload serializes as an object");

        assert_eq!(payload.get("principal_id"), Some(&json!("principal-1")));
        assert_eq!(
            payload.get("slots_changed"),
            Some(&json!(["router", "shape"]))
        );
        assert_eq!(payload.get("wasm_registry_id"), Some(&json!("registry-1")));
        assert_eq!(payload.get("sha256_hex"), Some(&json!("abc123")));
        assert_eq!(
            payload.get("supported_slots"),
            Some(&json!(["router", "shape"]))
        );
        assert_eq!(payload.len(), 5);
    }

    #[test]
    fn plugin_label_update_and_wasm_gc_display_stable() {
        let label_update = AuditPayload::PluginRegistryLabelUpdate {
            wasm_registry_id: "registry-1".to_owned(),
            label: Some("a, b=c)".to_owned()),
        };
        assert_eq!(
            label_update.to_string(),
            "plugin_registry_label_update(id=registry-1)"
        );

        let gc = AuditPayload::PluginWasmGc { removed_count: 0 };
        assert_eq!(gc.to_string(), "plugin_wasm_gc(removed=0)");
    }

    /// AUTH-07/SEC-03 (T0): API-key audit text names the hint, verdict,
    /// codes, fields, and hosts.
    #[test]
    fn api_key_audit_payloads_render_check_facts() {
        let create = AuditPayload::UpstreamCreate {
            upstream_id: "u1".to_owned(),
            kind: "anthropic_api_key".to_owned(),
            key_hint: Some("sk-ant-api03-...AAAA".to_owned()),
            verdict: Some("ok".to_owned()),
            remediation: vec!["non_anthropic_base_url"],
        };
        assert_eq!(
            create.to_string(),
            "upstream_create(id=u1, kind=anthropic_api_key, key_hint=sk-ant-api03-...AAAA, verdict=ok, remediation=non_anthropic_base_url)"
        );
        let update = AuditPayload::UpstreamUpdate {
            upstream_id: "u1".to_owned(),
            fields_changed: vec!["base_url"],
            key_hint: None,
            verdict: None,
            remediation: Vec::new(),
            old_host: None,
            new_host: Some("https://proxy.example.com".to_owned()),
        };
        assert_eq!(
            update.to_string(),
            "upstream_update(id=u1, fields=base_url, old_host=default, new_host=https://proxy.example.com)"
        );
        let preflight = AuditPayload::UpstreamPreflight {
            key_hint: None,
            verdict: "rejected".to_owned(),
            remediation: vec!["key_admin"],
        };
        assert_eq!(
            preflight.to_string(),
            "upstream_preflight(key_hint=none, verdict=rejected, remediation=key_admin)"
        );
        let recheck = AuditPayload::UpstreamRecheck {
            upstream_id: "u1".to_owned(),
            key_hint: Some("sk-ant-api03-...AAAA".to_owned()),
            verdict: "ok".to_owned(),
            remediation: Vec::new(),
        };
        assert_eq!(
            recheck.to_string(),
            "upstream_recheck(id=u1, key_hint=sk-ant-api03-...AAAA, verdict=ok)"
        );
    }
}
