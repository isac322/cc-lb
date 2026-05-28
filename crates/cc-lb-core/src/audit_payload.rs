use std::fmt;
use uuid::Uuid;

use crate::AuditEntry;

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
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
    UpstreamCreate {
        upstream_id: String,
        kind: String,
    },
    UpstreamUpdate {
        upstream_id: String,
        fields_changed: Vec<&'static str>,
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
        expires_at_unix_secs: u64,
        access_token_fingerprint: String,
        replica_id: Uuid,
    },
    UpstreamOauthRefreshFailure {
        upstream_id: String,
        reason_class: &'static str,
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
    },
    KillswitchOn,
    KillswitchOff,
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
            AuditPayload::UpstreamCreate { upstream_id, kind } => {
                write!(f, "upstream_create(id={}, kind={})", upstream_id, kind)
            }
            AuditPayload::UpstreamUpdate {
                upstream_id,
                fields_changed,
            } => write!(
                f,
                "upstream_update(id={}, fields={})",
                upstream_id,
                fields_changed.join(",")
            ),
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
                expires_at_unix_secs,
                access_token_fingerprint,
                replica_id,
            } => write!(
                f,
                "upstream_oauth_refresh_success(id={}, expires={}, fingerprint={}, replica={})",
                upstream_id, expires_at_unix_secs, access_token_fingerprint, replica_id
            ),
            AuditPayload::UpstreamOauthRefreshFailure {
                upstream_id,
                reason_class,
                replica_id,
            } => write!(
                f,
                "upstream_oauth_refresh_failure(id={}, reason={}, replica={})",
                upstream_id, reason_class, replica_id
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
            } => write!(
                f,
                "plugin_chain_update(principal={}, slots={})",
                principal_id,
                slots_changed.join(",")
            ),
            AuditPayload::KillswitchOn => write!(f, "killswitch_on"),
            AuditPayload::KillswitchOff => write!(f, "killswitch_off"),
        }
    }
}

impl From<AuditPayload> for AuditEntry {
    fn from(payload: AuditPayload) -> Self {
        AuditEntry {
            admin_action: Some(payload.to_string()),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
