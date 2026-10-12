//! Local prefix gate (P0): rejects keys that can never call models, before
//! any outbound request.

use cc_lb_storage_api::{ApiKeyClass, RemediationCode};

use super::normalize::NormalizedApiKey;

/// Classifies keys whose prefix alone proves they cannot serve inference.
///
/// Matching is ASCII case-insensitive on the normalized key: `sk-ant-admin*`
/// is an Admin key, `sk-ant-api01` an Enterprise management key, and
/// `sk-ant-oat01` an OAuth token. Every other prefix returns `None` and is
/// left to the probes; unknown prefixes are never rejected locally.
pub fn prefix_gate(key: &NormalizedApiKey) -> Option<(ApiKeyClass, RemediationCode)> {
    let bytes = key.as_str().as_bytes();
    let starts_with = |prefix: &str| {
        bytes
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
    };
    if starts_with("sk-ant-admin") {
        Some((ApiKeyClass::Admin, RemediationCode::KeyAdmin))
    } else if starts_with("sk-ant-api01") {
        Some((
            ApiKeyClass::Enterprise,
            RemediationCode::KeyEnterpriseScoped,
        ))
    } else if starts_with("sk-ant-oat01") {
        Some((ApiKeyClass::OauthToken, RemediationCode::KeyOat))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic_api_key::normalize::normalize_api_key;

    /// GATE-04: classifier table.
    #[test]
    fn gate_04_classifier_table() {
        let admin = Some((ApiKeyClass::Admin, RemediationCode::KeyAdmin));
        let enterprise = Some((
            ApiKeyClass::Enterprise,
            RemediationCode::KeyEnterpriseScoped,
        ));
        let oat = Some((ApiKeyClass::OauthToken, RemediationCode::KeyOat));
        let cases = [
            ("sk-ant-admin01-x", admin),
            ("sk-ant-admin02-x", admin),
            ("SK-ANT-ADMIN01-x", admin),
            ("  sk-ant-admin01-x\t\n", admin),
            ("sk-ant-api01-x", enterprise),
            ("Sk-Ant-Api01-x", enterprise),
            ("sk-ant-oat01-x", oat),
            ("SK-ANT-OAT01-x", oat),
            ("sk-ant-api03-x", None),
            ("sk-ant-api02-x", None),
            ("bogus", None),
            ("sk-ant", None),
        ];
        for (raw, expected) in cases {
            let key = normalize_api_key(raw);
            assert!(key.is_ok(), "{raw:?} normalizes");
            let verdict = key.ok().and_then(|key| prefix_gate(&key));
            assert_eq!(verdict, expected, "{raw:?}");
        }
    }
}
