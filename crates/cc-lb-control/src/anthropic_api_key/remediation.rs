//! Operator text and HTTP status for each remediation code.

use cc_lb_storage_api::RemediationCode;
use http::StatusCode;

/// Operator-facing sentence carried as `error.message` in nested remediation
/// bodies. It never contains key material.
pub const fn operator_message(code: RemediationCode) -> &'static str {
    match code {
        RemediationCode::KeyAdmin => {
            "This is an Admin key; it manages the org and can't call models. Use a standard API key."
        }
        RemediationCode::KeyEnterpriseScoped => {
            "This is an Enterprise management key; it can't call models. Use a Console API key."
        }
        RemediationCode::KeyOat => {
            "That is an OAuth token, not an API key; it expires and cc-lb cannot refresh it. Sign in with Claude instead."
        }
        RemediationCode::KeyInvalid => {
            "The endpoint rejected this key. Check for a typo, or the key is expired or deleted."
        }
        RemediationCode::KeyForbidden => {
            "The key is valid, but Anthropic won't let it count tokens. Save it unverified to keep it."
        }
        RemediationCode::WorkspaceRequired => {
            "This key can act in several workspaces. Choose the one it bills."
        }
        RemediationCode::WorkspaceNotFound => {
            "This key can't see that workspace. Check the id, or grant the key's account access."
        }
        RemediationCode::WorkspaceIdMalformed => "Workspace IDs start with wrkspc_.",
        RemediationCode::WorkspaceListDenied => {
            "This key's account lacks the role to list workspaces. Paste the wrkspc_ id from Console."
        }
        RemediationCode::KeyWorkspaceScopedUsageUnavailable => {
            "This key is bound to one workspace and can't read usage reports. Replace it with an All workspaces key to get reported usage."
        }
        RemediationCode::RoleInsufficient => {
            "The key's linked account lacks the org role to read usage reports. Raise its role to get reported usage."
        }
        RemediationCode::KeyIdUnresolved => {
            "Anthropic can't attribute usage to this key, so usage covers the whole workspace."
        }
        RemediationCode::UsageNeverChecked => {
            "This key was not checked yet. Re-check to see what it can do at Anthropic."
        }
        RemediationCode::NonAnthropicBaseUrl => {
            "This upstream points away from api.anthropic.com, so Anthropic's Admin API is not used and usage stays estimated."
        }
        RemediationCode::ProbeUnreachable => {
            "Couldn't reach the endpoint to check the key. Try again, or save without checking."
        }
        RemediationCode::UsagePollFailed => {
            "Anthropic stopped returning usage reports for this key. Showing the last report."
        }
    }
}

/// [`operator_message`] for a probe answered by a known endpoint. A refusal
/// names Anthropic only when `on_anthropic` (the key was checked against
/// `https://api.anthropic.com`); a custom `base_url` is "your endpoint".
pub const fn endpoint_operator_message(code: RemediationCode, on_anthropic: bool) -> &'static str {
    match code {
        RemediationCode::KeyInvalid if on_anthropic => {
            "Anthropic rejected this key. Check for a typo, or the key is expired or deleted."
        }
        RemediationCode::KeyInvalid => {
            "Your endpoint rejected this key. Check the key and the Base URL."
        }
        RemediationCode::KeyForbidden if !on_anthropic => {
            "The key is valid, but your endpoint won't let it count tokens. Save it unverified to keep it."
        }
        _ => operator_message(code),
    }
}

/// Status of a failure response carrying `code`.
///
/// `ProbeUnreachable` maps to 502; callers answer 504 instead when the probe
/// timed out. Codes that only ever appear as data (preflight remediation,
/// `/usage` `fix`) and never as a failure status return `None`.
pub const fn http_status(code: RemediationCode) -> Option<StatusCode> {
    match code {
        RemediationCode::KeyAdmin
        | RemediationCode::KeyEnterpriseScoped
        | RemediationCode::KeyOat
        | RemediationCode::KeyInvalid
        | RemediationCode::KeyForbidden
        | RemediationCode::WorkspaceRequired
        | RemediationCode::WorkspaceNotFound
        | RemediationCode::WorkspaceIdMalformed => Some(StatusCode::BAD_REQUEST),
        RemediationCode::WorkspaceListDenied | RemediationCode::NonAnthropicBaseUrl => {
            Some(StatusCode::FORBIDDEN)
        }
        RemediationCode::ProbeUnreachable => Some(StatusCode::BAD_GATEWAY),
        RemediationCode::KeyWorkspaceScopedUsageUnavailable
        | RemediationCode::RoleInsufficient
        | RemediationCode::KeyIdUnresolved
        | RemediationCode::UsageNeverChecked
        | RemediationCode::UsagePollFailed => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_operator_text() {
        for code in RemediationCode::ALL {
            assert!(!operator_message(code).is_empty(), "{code:?}");
        }
    }

    #[test]
    fn rejection_names_anthropic_only_on_its_host() {
        let anthropic = endpoint_operator_message(RemediationCode::KeyInvalid, true);
        assert!(anthropic.starts_with("Anthropic rejected this key."));
        let custom = endpoint_operator_message(RemediationCode::KeyInvalid, false);
        assert!(custom.starts_with("Your endpoint rejected this key."));
        assert!(!custom.contains("Anthropic"));
        let forbidden = endpoint_operator_message(RemediationCode::KeyForbidden, false);
        assert!(!forbidden.contains("Anthropic"));
        for code in RemediationCode::ALL {
            assert!(
                !endpoint_operator_message(code, true).is_empty(),
                "{code:?}"
            );
            assert!(
                !endpoint_operator_message(code, false).is_empty(),
                "{code:?}"
            );
        }
    }

    #[test]
    fn statuses_follow_d_z3() {
        assert_eq!(
            http_status(RemediationCode::KeyAdmin),
            Some(StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            http_status(RemediationCode::WorkspaceRequired),
            Some(StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            http_status(RemediationCode::WorkspaceListDenied),
            Some(StatusCode::FORBIDDEN)
        );
        assert_eq!(
            http_status(RemediationCode::NonAnthropicBaseUrl),
            Some(StatusCode::FORBIDDEN)
        );
        assert_eq!(
            http_status(RemediationCode::ProbeUnreachable),
            Some(StatusCode::BAD_GATEWAY)
        );
        assert_eq!(http_status(RemediationCode::UsagePollFailed), None);
    }
}
