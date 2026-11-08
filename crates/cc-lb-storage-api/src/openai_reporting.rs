//! Provider-neutral OpenAI organization-reporting credential vocabulary.
//!
//! The credential is deployment-scoped rather than attached to an upstream.
//! Storage receives only ciphertext produced by the existing deployment AEAD
//! path. Public records intentionally contain metadata only: plaintext and
//! ciphertext are never part of a serialized reporting snapshot.

use serde::{Deserialize, Serialize};

/// Additional authenticated data for the deployment-scoped OpenAI
/// organization reporting credential.
pub const OPENAI_ORG_REPORTING_CREDENTIAL_AAD: &[u8] =
    b"openai-organization-reporting-credential:v1:deployment";

/// Persisted status of the deployment's optional OpenAI organization
/// reporting credential.
///
/// No row means that no Admin credential has been configured yet. A row is
/// present only after a credential replacement and may retain a denied or
/// failed status so the admin surface can explain why reporting is disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenAiOrganizationReportingCredentialStatus {
    Configured,
    Denied,
    Unavailable,
    Failed,
}

impl OpenAiOrganizationReportingCredentialStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configured => "configured",
            Self::Denied => "denied",
            Self::Unavailable => "unavailable",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "configured" => Some(Self::Configured),
            "denied" => Some(Self::Denied),
            "unavailable" => Some(Self::Unavailable),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Redacted deployment-scoped snapshot of the OpenAI organization Admin
/// credential. The encrypted credential bytes are deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenAiOrganizationReportingCredentialRecord {
    /// Stable singleton scope. This is always `deployment` for this record.
    pub scope: String,
    /// Operator-visible hint only; never an authorization header or secret.
    pub redacted_hint: String,
    pub status: OpenAiOrganizationReportingCredentialStatus,
    pub revision: u64,
    pub updated_at_unix_secs: u64,
}

/// Replacement of the deployment-scoped OpenAI organization credential.
///
/// `encrypted_secret_ciphertext` MUST already be sealed with the deployment's
/// existing AEAD/AAD path. It is intentionally not serializable and its
/// `Debug` representation is redacted. `expected_revision` is `None` when a
/// caller is creating the singleton row and `Some(current_revision)` for a
/// conditional replacement; a mismatch returns `StorageError::Conflict`.
#[derive(Clone, PartialEq, Eq)]
pub struct OpenAiOrganizationReportingCredentialWrite {
    pub encrypted_secret_ciphertext: Vec<u8>,
    pub redacted_hint: String,
    pub status: OpenAiOrganizationReportingCredentialStatus,
    pub expected_revision: Option<u64>,
    pub updated_at_unix_secs: u64,
}

impl std::fmt::Debug for OpenAiOrganizationReportingCredentialWrite {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiOrganizationReportingCredentialWrite")
            .field(
                "encrypted_secret_ciphertext",
                &format_args!(
                    "<{} bytes redacted>",
                    self.encrypted_secret_ciphertext.len()
                ),
            )
            .field("redacted_hint", &self.redacted_hint)
            .field("status", &self.status)
            .field("expected_revision", &self.expected_revision)
            .field("updated_at_unix_secs", &self.updated_at_unix_secs)
            .finish()
    }
}
