use std::sync::OnceLock;

use serde::Serialize;

static RUNTIME_META: OnceLock<(&'static str, &'static str)> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BuildMeta {
    pub version: String,
    pub git_sha: &'static str,
    pub build_time: String,
    pub rustc: &'static str,
    pub features: String,
    pub target: &'static str,
    pub profile: &'static str,
}

/// Registers process-wide git SHA and build timestamp supplied by the binary crate.
///
/// Keeping `option_env!("CC_LB_GIT_SHA")` and `option_env!("CC_LB_BUILD_TIME")`
/// out of shared libraries lets SHA-only or timestamp-only builds recompile just
/// the thin binary crate instead of the full workspace graph.
pub fn init(git_sha: Option<&'static str>, build_time: Option<&'static str>) {
    let _ = RUNTIME_META.set((normalize_sha(git_sha), normalize_nonempty(build_time)));
}

fn normalize_sha(raw: Option<&'static str>) -> &'static str {
    let Some(trimmed) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return "unknown";
    };
    match trimmed.char_indices().nth(7) {
        Some((end, _)) => &trimmed[..end],
        None => trimmed,
    }
}

fn normalize_nonempty(raw: Option<&'static str>) -> &'static str {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown")
}

impl BuildMeta {
    pub fn current() -> Self {
        let (git_sha, build_time) = RUNTIME_META
            .get()
            .copied()
            .unwrap_or(("unknown", "unknown"));
        Self {
            version: option_env!("CC_LB_VERSION")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_owned(),
            git_sha,
            build_time: build_time.to_owned(),
            rustc: option_env!("CC_LB_RUSTC").unwrap_or("unknown"),
            features: option_env!("CC_LB_FEATURES")
                .unwrap_or("unknown")
                .to_owned(),
            target: option_env!("CC_LB_TARGET").unwrap_or("unknown"),
            profile: option_env!("CC_LB_PROFILE").unwrap_or("unknown"),
        }
    }
}

pub fn format_version() -> String {
    let meta = BuildMeta::current();
    format!(
        "{}\nCommit: {}\nBuilt: {} (rustc {})\nFeatures: {}\nTarget: {}",
        meta.version, meta.git_sha, meta.build_time, meta.rustc, meta.features, meta.target
    )
}

pub fn compact_version(binary_name: &str) -> String {
    let meta = BuildMeta::current();
    format!("{binary_name} {} ({})", meta.version, meta.git_sha)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_sha_handles_empty_short_long_and_utf8() {
        assert_eq!(normalize_sha(None), "unknown");
        assert_eq!(normalize_sha(Some("")), "unknown");
        assert_eq!(normalize_sha(Some("   \t ")), "unknown");
        assert_eq!(normalize_sha(Some("abc")), "abc");
        assert_eq!(normalize_sha(Some("  1234567  ")), "1234567");
        assert_eq!(normalize_sha(Some("  0123456789abcdef  ")), "0123456");
        assert_eq!(normalize_sha(Some("éàüçñßøabcd")), "éàüçñßø");
    }

    #[test]
    fn normalize_nonempty_handles_empty_and_trimmed_values() {
        assert_eq!(normalize_nonempty(None), "unknown");
        assert_eq!(normalize_nonempty(Some("")), "unknown");
        assert_eq!(normalize_nonempty(Some("  ")), "unknown");
        assert_eq!(
            normalize_nonempty(Some(" 2026-11-21T12:00:00Z ")),
            "2026-11-21T12:00:00Z"
        );
    }

    #[test]
    fn init_sets_git_sha_and_build_time_on_current_meta() {
        init(Some("abcdef0123456789"), Some("2026-11-21T12:00:00Z"));
        let meta = BuildMeta::current();
        assert_eq!(meta.git_sha, "abcdef0");
        assert_eq!(meta.build_time, "2026-11-21T12:00:00Z");
        assert_eq!(
            compact_version("cc-lb-proxy"),
            format!("cc-lb-proxy {} (abcdef0)", meta.version)
        );
    }
}
