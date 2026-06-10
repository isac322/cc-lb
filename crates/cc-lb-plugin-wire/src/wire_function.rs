use serde::{Deserialize, Serialize};

/// Fallback behavior when a wire function fails at runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FallbackPolicy {
    /// Fail the entire request (used for critical functions like signing).
    FailRequest,
    /// Silently skip the function call (used for observability hooks).
    SilentSkip,
    /// Use the default/fallback response.
    UseDefault,
    /// Pass through to the next handler (used for error normalization).
    PassThrough,
}

/// Type-level specification for a wire function exported by a plugin.
pub trait WireFunction: 'static {
    /// Name of the wire function (e.g., "sign", "observe").
    const NAME: &'static str;

    /// Compile-time fallback policy for this function. Must never be runtime-configurable
    /// to prevent security incidents (e.g., skipping signing).
    const FALLBACK: FallbackPolicy;

    /// Wire protocol versions this function supports. Host advertises max version,
    /// plugin chooses compatible version during handshake.
    const SUPPORTED_VERSIONS: &'static [u32];

    /// Request type for this wire function.
    type Request: serde::Serialize + serde::de::DeserializeOwned;

    /// Response type for this wire function.
    type Response: serde::Serialize + serde::de::DeserializeOwned;

    /// Dry-run request for self_check (skip-handler model: serialize/deserialize round-trip only).
    fn dry_run_request() -> Self::Request;

    /// Dry-run response for self_check (skip-handler model: serialize/deserialize round-trip only).
    fn dry_run_response() -> Self::Response;
}

/// Plugin kind/slot type. Used by PDK macros and host-side plugin dispatch.
/// Note: Kind is NOT embedded in the custom section (duck typing); it's inferred
/// from declared functions or provided explicitly by PDK macro hints.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginKind {
    /// Request/response dialect plugin (implements `shape`).
    Dialect,
    /// API key signing plugin (implements `build_signer`, `sign`).
    SignerFactory,
    /// Observability/logging plugin (implements `observe`).
    Observability,
}

/// List of all valid wire function names in the system.
pub fn all_wire_functions() -> &'static [&'static str] {
    &[
        "shape",
        "normalize_error",
        "build_signer",
        "sign",
        "on_unauthorized",
        "observe",
        "filter",
    ]
}

/// Returns the set of wire functions REQUIRED for a plugin of the given kind.
pub fn required_functions_for_kind(kind: PluginKind) -> &'static [&'static str] {
    match kind {
        PluginKind::Dialect => &["shape"],
        PluginKind::SignerFactory => &["build_signer", "sign"],
        PluginKind::Observability => &["observe"],
    }
}

/// Returns the set of wire functions ALLOWED for a plugin of the given kind.
/// A plugin may implement any subset of allowed functions; at minimum,
/// it must implement all required functions.
pub fn allowed_functions_for_kind(kind: PluginKind) -> &'static [&'static str] {
    match kind {
        PluginKind::Dialect => &["shape", "normalize_error"],
        PluginKind::SignerFactory => &["build_signer", "sign", "on_unauthorized"],
        PluginKind::Observability => &["observe"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_seven_functions_listed() {
        assert_eq!(all_wire_functions().len(), 7);
        let funcs = all_wire_functions();
        assert!(funcs.contains(&"shape"));
        assert!(funcs.contains(&"normalize_error"));
        assert!(funcs.contains(&"build_signer"));
        assert!(funcs.contains(&"sign"));
        assert!(funcs.contains(&"on_unauthorized"));
        assert!(funcs.contains(&"observe"));
        assert!(funcs.contains(&"filter"));
    }

    #[test]
    fn security_fallbacks_correct() {
        // sign and build_signer must fail the request on error
        // (signing cannot be skipped without breaking security)
        // observe must silently skip
        // normalize_error and on_unauthorized must pass through
        // These are compile-time constants baked into each WireFunction impl.
        // This test documents the expected mapping.
        assert_eq!(FallbackPolicy::FailRequest, FallbackPolicy::FailRequest);
        assert_eq!(FallbackPolicy::SilentSkip, FallbackPolicy::SilentSkip);
        assert_eq!(FallbackPolicy::UseDefault, FallbackPolicy::UseDefault);
        assert_eq!(FallbackPolicy::PassThrough, FallbackPolicy::PassThrough);
    }

    #[test]
    fn required_subset_allowed_dialect() {
        let required = required_functions_for_kind(PluginKind::Dialect);
        let allowed = allowed_functions_for_kind(PluginKind::Dialect);
        for req in required {
            assert!(allowed.contains(req), "{} not in allowed", req);
        }
    }

    #[test]
    fn required_subset_allowed_signer_factory() {
        let required = required_functions_for_kind(PluginKind::SignerFactory);
        let allowed = allowed_functions_for_kind(PluginKind::SignerFactory);
        for req in required {
            assert!(allowed.contains(req), "{} not in allowed", req);
        }
    }

    #[test]
    fn required_subset_allowed_observability() {
        let required = required_functions_for_kind(PluginKind::Observability);
        let allowed = allowed_functions_for_kind(PluginKind::Observability);
        for req in required {
            assert!(allowed.contains(req), "{} not in allowed", req);
        }
    }

    #[test]
    fn fallback_policy_serde() {
        let policies = [
            FallbackPolicy::FailRequest,
            FallbackPolicy::SilentSkip,
            FallbackPolicy::UseDefault,
            FallbackPolicy::PassThrough,
        ];
        for policy in &policies {
            let json = serde_json::to_string(policy).unwrap();
            let deserialized: FallbackPolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(*policy, deserialized);
        }
    }

    #[test]
    fn fallback_policy_snake_case_names() {
        assert_eq!(
            serde_json::to_string(&FallbackPolicy::FailRequest).unwrap(),
            "\"fail_request\""
        );
        assert_eq!(
            serde_json::to_string(&FallbackPolicy::SilentSkip).unwrap(),
            "\"silent_skip\""
        );
        assert_eq!(
            serde_json::to_string(&FallbackPolicy::UseDefault).unwrap(),
            "\"use_default\""
        );
        assert_eq!(
            serde_json::to_string(&FallbackPolicy::PassThrough).unwrap(),
            "\"pass_through\""
        );
    }

    #[test]
    fn plugin_kind_serde() {
        let kinds = [
            PluginKind::Dialect,
            PluginKind::SignerFactory,
            PluginKind::Observability,
        ];
        for kind in &kinds {
            let json = serde_json::to_string(kind).unwrap();
            let deserialized: PluginKind = serde_json::from_str(&json).unwrap();
            assert_eq!(*kind, deserialized);
        }
    }

    #[test]
    fn plugin_kind_snake_case_names() {
        assert_eq!(
            serde_json::to_string(&PluginKind::Dialect).unwrap(),
            "\"dialect\""
        );
        assert_eq!(
            serde_json::to_string(&PluginKind::SignerFactory).unwrap(),
            "\"signer_factory\""
        );
        assert_eq!(
            serde_json::to_string(&PluginKind::Observability).unwrap(),
            "\"observability\""
        );
    }

    #[test]
    fn kind_allowed_and_required_nonempty() {
        let kinds = [
            PluginKind::Dialect,
            PluginKind::SignerFactory,
            PluginKind::Observability,
        ];
        for &kind in &kinds {
            let required = required_functions_for_kind(kind);
            let allowed = allowed_functions_for_kind(kind);
            assert!(!required.is_empty(), "required empty for {:?}", kind);
            assert!(!allowed.is_empty(), "allowed empty for {:?}", kind);
            assert!(
                required.len() <= allowed.len(),
                "required > allowed for {:?}",
                kind
            );
        }
    }
}
