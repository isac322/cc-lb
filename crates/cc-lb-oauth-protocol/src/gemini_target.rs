use url::Url;

use crate::{ANTIGRAVITY_OAUTH_CODE_ASSIST_URL, GEMINI_OAUTH_CODE_ASSIST_URL, GeminiOAuthProfile};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeminiOperationFamily {
    Content,
    Metadata,
    Quota,
    Discovery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeminiProjectPolicy {
    Optional,
    Omitted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeminiOAuthOperation {
    GenerateContent,
    StreamGenerateContent,
    CountTokens,
    LoadCodeAssist,
    RetrieveUserQuotaSummary,
    RetrieveUserQuota,
    FetchAvailableModels,
}

impl GeminiOAuthOperation {
    pub const fn action(self) -> &'static str {
        match self {
            Self::GenerateContent => "generateContent",
            Self::StreamGenerateContent => "streamGenerateContent",
            Self::CountTokens => "countTokens",
            Self::LoadCodeAssist => "loadCodeAssist",
            Self::RetrieveUserQuotaSummary => "retrieveUserQuotaSummary",
            Self::RetrieveUserQuota => "retrieveUserQuota",
            Self::FetchAvailableModels => "fetchAvailableModels",
        }
    }

    pub const fn family(self) -> GeminiOperationFamily {
        match self {
            Self::GenerateContent | Self::StreamGenerateContent | Self::CountTokens => {
                GeminiOperationFamily::Content
            }
            Self::LoadCodeAssist => GeminiOperationFamily::Metadata,
            Self::RetrieveUserQuotaSummary | Self::RetrieveUserQuota => {
                GeminiOperationFamily::Quota
            }
            Self::FetchAvailableModels => GeminiOperationFamily::Discovery,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeminiOAuthTarget {
    pub profile: GeminiOAuthProfile,
    pub base: Url,
    pub operation: GeminiOAuthOperation,
    pub explicit_override: bool,
    pub project: GeminiProjectPolicy,
}

impl GeminiOAuthTarget {
    pub fn endpoint(&self) -> Url {
        self.endpoint_for(self.operation)
    }

    /// Reuse the same resolved origin and prefix for operation fallback or signing.
    pub fn endpoint_for(&self, operation: GeminiOAuthOperation) -> Url {
        with_action(self.base.clone(), operation)
    }

    pub fn into_endpoint(self) -> Url {
        with_action(self.base, self.operation)
    }
}

fn with_action(mut base: Url, operation: GeminiOAuthOperation) -> Url {
    let mut path = String::with_capacity(base.path().len() + 1 + operation.action().len());
    path.push_str(base.path());
    path.push(':');
    path.push_str(operation.action());
    base.set_path(&path);
    base
}

/// Resolve a stored override without inferring provenance from its host.
/// Absent overrides retain the existing operation-specific configuration semantics.
pub fn resolve_gemini_oauth_target(
    profile: GeminiOAuthProfile,
    stored_base: Option<&Url>,
    content_default: Option<&Url>,
    metadata_default: Option<&Url>,
    operation: GeminiOAuthOperation,
) -> Result<GeminiOAuthTarget, url::ParseError> {
    let family = operation.family();
    let metadata = matches!(
        family,
        GeminiOperationFamily::Metadata | GeminiOperationFamily::Discovery
    );
    let mut base = if let Some(base) = stored_base {
        base.clone()
    } else if metadata {
        match metadata_default {
            Some(base) => base.clone(),
            None => profile_default(profile)?,
        }
    } else if family == GeminiOperationFamily::Content && profile == GeminiOAuthProfile::Antigravity
    {
        match content_default {
            Some(base) => base.clone(),
            None => profile_default(profile)?,
        }
    } else {
        Url::parse(GEMINI_OAUTH_CODE_ASSIST_URL)?
    };
    if base.cannot_be_a_base() {
        return Err(url::ParseError::RelativeUrlWithCannotBeABaseBase);
    }
    base.set_query(None);
    base.set_fragment(None);
    if stored_base.is_none()
        && !metadata
        && family == GeminiOperationFamily::Content
        && profile == GeminiOAuthProfile::Antigravity
    {
        base.set_path("/v1internal");
    } else {
        normalize_path(&mut base, stored_base.is_none() && metadata);
    }
    let project = if operation == GeminiOAuthOperation::CountTokens
        || (profile == GeminiOAuthProfile::Antigravity
            && operation == GeminiOAuthOperation::LoadCodeAssist)
    {
        GeminiProjectPolicy::Omitted
    } else {
        GeminiProjectPolicy::Optional
    };
    Ok(GeminiOAuthTarget {
        profile,
        base,
        operation,
        explicit_override: stored_base.is_some(),
        project,
    })
}

fn profile_default(profile: GeminiOAuthProfile) -> Result<Url, url::ParseError> {
    Url::parse(match profile {
        GeminiOAuthProfile::GeminiCli => GEMINI_OAUTH_CODE_ASSIST_URL,
        GeminiOAuthProfile::Antigravity => ANTIGRAVITY_OAUTH_CODE_ASSIST_URL,
    })
}

fn normalize_path(base: &mut Url, metadata_config: bool) {
    let path = base.path().trim_end_matches('/');
    let path = match path.rsplit_once(':') {
        Some((
            prefix,
            "generateContent"
            | "streamGenerateContent"
            | "countTokens"
            | "loadCodeAssist"
            | "retrieveUserQuotaSummary"
            | "retrieveUserQuota"
            | "fetchAvailableModels"
            | "onboardUser",
        )) => prefix,
        _ => path,
    };
    if path.is_empty() {
        base.set_path("/v1internal");
    } else if metadata_config && !path.ends_with("/v1internal") {
        let mut canonical = String::with_capacity(path.len() + "/v1internal".len());
        canonical.push_str(path);
        canonical.push_str("/v1internal");
        base.set_path(&canonical);
    } else if path != base.path() {
        let canonical = path.to_owned();
        base.set_path(&canonical);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPERATIONS: [GeminiOAuthOperation; 7] = [
        GeminiOAuthOperation::GenerateContent,
        GeminiOAuthOperation::StreamGenerateContent,
        GeminiOAuthOperation::CountTokens,
        GeminiOAuthOperation::LoadCodeAssist,
        GeminiOAuthOperation::RetrieveUserQuotaSummary,
        GeminiOAuthOperation::RetrieveUserQuota,
        GeminiOAuthOperation::FetchAvailableModels,
    ];
    const PROFILES: [GeminiOAuthProfile; 2] = [
        GeminiOAuthProfile::GeminiCli,
        GeminiOAuthProfile::Antigravity,
    ];

    #[test]
    fn profile_operation_override_matrix() {
        let cases = [
            (None, ""),
            (
                Some("https://EXAMPLE.com:443/?old=1#fragment"),
                "/v1internal",
            ),
            (Some("https://example.com/v1internal/"), "/v1internal"),
            (
                Some("https://example.com/v1internal:generateContent?old=1#fragment"),
                "/v1internal",
            ),
            (Some("https://example.com/gateway"), "/gateway"),
            (
                Some("https://example.com/gateway/v1internal:loadCodeAssist"),
                "/gateway/v1internal",
            ),
        ];
        for profile in PROFILES {
            for operation in OPERATIONS {
                for (input, expected_path) in cases {
                    let stored = input.map(|value| Url::parse(value).unwrap());
                    let target = resolve_gemini_oauth_target(
                        profile,
                        stored.as_ref(),
                        None,
                        None,
                        operation,
                    )
                    .unwrap();
                    let expected_host = if stored.is_some() {
                        "example.com"
                    } else if profile == GeminiOAuthProfile::Antigravity
                        && operation.family() != GeminiOperationFamily::Quota
                    {
                        "daily-cloudcode-pa.googleapis.com"
                    } else {
                        "cloudcode-pa.googleapis.com"
                    };
                    let path = if stored.is_some() {
                        expected_path
                    } else {
                        "/v1internal"
                    };
                    assert_eq!(target.base.host_str(), Some(expected_host));
                    assert_eq!(target.base.port(), None);
                    assert_eq!(target.base.path(), path);
                    assert_eq!(target.base.query(), None);
                    assert_eq!(target.base.fragment(), None);
                    assert_eq!(target.profile, profile);
                    assert_eq!(target.operation, operation);
                    assert_eq!(target.explicit_override, stored.is_some());
                    let endpoint = target.endpoint();
                    assert_eq!(endpoint.path(), format!("{path}:{}", operation.action()));
                    assert_eq!(endpoint.query(), None);
                    assert_eq!(target.clone().into_endpoint(), endpoint);
                    let omitted = operation == GeminiOAuthOperation::CountTokens
                        || (profile == GeminiOAuthProfile::Antigravity
                            && operation == GeminiOAuthOperation::LoadCodeAssist);
                    assert_eq!(
                        target.project,
                        if omitted {
                            GeminiProjectPolicy::Omitted
                        } else {
                            GeminiProjectPolicy::Optional
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn explicit_cloud_code_and_all_known_actions_remain_explicit() {
        for profile in PROFILES {
            for action in OPERATIONS
                .iter()
                .map(|operation| operation.action())
                .chain(["onboardUser"])
            {
                let stored = Url::parse(&format!(
                    "https://cloudcode-pa.googleapis.com/gateway/v1internal:{action}"
                ))
                .unwrap();
                for operation in OPERATIONS {
                    let target =
                        resolve_gemini_oauth_target(profile, Some(&stored), None, None, operation)
                            .unwrap();
                    assert!(target.explicit_override);
                    assert_eq!(
                        target.base.as_str(),
                        "https://cloudcode-pa.googleapis.com/gateway/v1internal"
                    );
                    assert_eq!(
                        target.endpoint().path(),
                        format!("/gateway/v1internal:{}", operation.action())
                    );
                }
            }
        }
    }

    #[test]
    fn absent_override_preserves_content_and_metadata_config_semantics() {
        let content =
            Url::parse("https://content.example:8443/ignored/prefix?old=1#fragment").unwrap();
        for metadata_path in [
            "/metadata",
            "/metadata/v1internal",
            "/metadata/v1internal:loadCodeAssist",
        ] {
            let metadata = Url::parse(&format!(
                "https://metadata.example:9443{metadata_path}?old=1#fragment"
            ))
            .unwrap();
            for profile in PROFILES {
                for operation in OPERATIONS {
                    let target = resolve_gemini_oauth_target(
                        profile,
                        None,
                        Some(&content),
                        Some(&metadata),
                        operation,
                    )
                    .unwrap();
                    let (host, port, path) = match operation.family() {
                        GeminiOperationFamily::Metadata | GeminiOperationFamily::Discovery => {
                            ("metadata.example", Some(9443), "/metadata/v1internal")
                        }
                        GeminiOperationFamily::Content
                            if profile == GeminiOAuthProfile::Antigravity =>
                        {
                            ("content.example", Some(8443), "/v1internal")
                        }
                        _ => ("cloudcode-pa.googleapis.com", None, "/v1internal"),
                    };
                    assert_eq!(target.base.host_str(), Some(host));
                    assert_eq!(target.base.port(), port);
                    assert_eq!(target.base.path(), path);
                    assert_eq!(target.base.query(), None);
                    assert_eq!(target.base.fragment(), None);
                    assert!(!target.explicit_override);
                }
            }
        }
    }

    #[test]
    fn fallback_reuses_base_and_unknown_action_is_preserved() {
        let stored = Url::parse("http://localhost:8123/gateway:custom?old=1#fragment").unwrap();
        let target = resolve_gemini_oauth_target(
            GeminiOAuthProfile::Antigravity,
            Some(&stored),
            None,
            None,
            GeminiOAuthOperation::RetrieveUserQuotaSummary,
        )
        .unwrap();
        assert_eq!(target.base.as_str(), "http://localhost:8123/gateway:custom");
        assert_eq!(
            target
                .endpoint_for(GeminiOAuthOperation::RetrieveUserQuota)
                .as_str(),
            "http://localhost:8123/gateway:custom:retrieveUserQuota"
        );
        assert_eq!(
            target.operation,
            GeminiOAuthOperation::RetrieveUserQuotaSummary
        );
    }

    #[test]
    fn operation_families_are_explicit() {
        assert_eq!(
            OPERATIONS.map(GeminiOAuthOperation::family),
            [
                GeminiOperationFamily::Content,
                GeminiOperationFamily::Content,
                GeminiOperationFamily::Content,
                GeminiOperationFamily::Metadata,
                GeminiOperationFamily::Quota,
                GeminiOperationFamily::Quota,
                GeminiOperationFamily::Discovery,
            ]
        );
    }

    #[test]
    fn opaque_override_returns_parse_error() {
        let stored = Url::parse("mailto:example@example.com").unwrap();
        assert!(
            resolve_gemini_oauth_target(
                GeminiOAuthProfile::GeminiCli,
                Some(&stored),
                None,
                None,
                GeminiOAuthOperation::GenerateContent
            )
            .is_err()
        );
    }
}
