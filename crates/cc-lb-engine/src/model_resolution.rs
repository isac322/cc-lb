//! Canonical model ID resolution and per-model cache threshold tables.
//!
//! Source: https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching#cache-limitations
//! Source: https://docs.anthropic.com/en/about-claude/models/overview
//! Retrieved: 2026-06-07

/// Resolve a model alias to its canonical (dated) form.
///
/// Supports rolling aliases (e.g., `claude-sonnet-4-5`) that map to specific dated versions.
/// Already-dated forms and unknown models pass through unchanged (with a warn log).
///
/// # Examples
///
/// ```ignore
/// assert_eq!(canonical_model_id("claude-sonnet-4-5"), "claude-sonnet-4-5-20250929");
/// assert_eq!(canonical_model_id("claude-sonnet-4-5-20250929"), "claude-sonnet-4-5-20250929");
/// assert_eq!(canonical_model_id("unknown-model"), "unknown-model"); // passes through
/// ```
pub fn canonical_model_id(requested: &str) -> &str {
    match requested {
        "claude-fable-5" => "claude-fable-5",

        // Sonnet aliases
        "claude-sonnet-4-5" => "claude-sonnet-4-5-20250929",

        // Opus aliases
        "claude-opus-4-5" => "claude-opus-4-5-20251101",
        "claude-opus-4-8" => "claude-opus-4-8-20250514",

        // Haiku aliases
        "claude-haiku-4-5" => "claude-haiku-4-5-20251001",

        // Already-dated or unknown: pass through unchanged
        other => {
            // Only warn if it looks like an alias (doesn't end with -YYYYMMDD)
            if !other.ends_with('-') && !is_dated_model(other) {
                tracing::warn!(model = %other, "unknown model alias, returning unchanged");
            }
            other
        }
    }
}

/// Check if a model name appears to be in dated form (ends with -YYYYMMDD).
fn is_dated_model(model: &str) -> bool {
    let parts: Vec<&str> = model.rsplitn(2, '-').collect();
    if parts.len() == 2 {
        let suffix = parts[0];
        suffix.len() == 8 && suffix.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

/// Get the minimum cacheable prompt length (in tokens) for a canonical model.
///
/// # Cache Threshold Reference
///
/// Based on Anthropic docs (retrieved 2026-06-07):
/// - **512 tokens**: Claude Fable 5
/// - **4,096 tokens**: Claude Opus 4.7, 4.6, 4.5; Claude Mythos Preview; Claude Haiku 4.5
/// - **1,024 tokens**: Claude Opus 4.8; Claude Sonnet 4.6, 4.5
/// - **2,048 tokens**: Claude Haiku 3.5 (retired; not included)
///
/// # Examples
///
/// ```ignore
/// assert_eq!(cache_threshold_tokens("claude-sonnet-4-5-20250929"), 1024);
/// assert_eq!(cache_threshold_tokens("claude-opus-4-5-20251101"), 4096);
/// assert_eq!(cache_threshold_tokens("claude-haiku-4-5-20251001"), 4096);
/// assert_eq!(cache_threshold_tokens("unknown-model"), 1024); // default
/// ```
pub fn cache_threshold_tokens(canonical: &str) -> usize {
    match canonical {
        "claude-fable-5" => 512,

        // Sonnet 4.5 / 4.6 family: 1024
        "claude-sonnet-4-5-20250929" => 1024,
        "claude-sonnet-4-5" => 1024,
        "claude-sonnet-4-6" => 1024,

        // Opus 4.8: 1024
        "claude-opus-4-8-20250514" => 1024,
        "claude-opus-4-8" => 1024,

        // Opus 4.5 / 4.6 / 4.7: 4096
        "claude-opus-4-5-20251101" => 4096,
        "claude-opus-4-5" => 4096,
        "claude-opus-4-6" => 4096,
        "claude-opus-4-7" => 4096,

        // Haiku 4.5: 4096
        "claude-haiku-4-5-20251001" => 4096,
        "claude-haiku-4-5" => 4096,

        // Unknown: default to 1024 + warn
        other => {
            tracing::warn!(model = %other, "unknown model, defaulting cache threshold to 1024");
            1024
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_resolves_to_dated_sonnet45() {
        assert_eq!(
            canonical_model_id("claude-sonnet-4-5"),
            "claude-sonnet-4-5-20250929"
        );
    }

    #[test]
    fn dated_sonnet45_passes_through() {
        assert_eq!(
            canonical_model_id("claude-sonnet-4-5-20250929"),
            "claude-sonnet-4-5-20250929"
        );
    }

    #[test]
    fn opus45_alias_resolves() {
        assert_eq!(
            canonical_model_id("claude-opus-4-5"),
            "claude-opus-4-5-20251101"
        );
    }

    #[test]
    fn unknown_passes_through() {
        assert_eq!(canonical_model_id("future-model-xyz"), "future-model-xyz");
    }

    #[test]
    fn threshold_sonnet45_is_1024() {
        assert_eq!(cache_threshold_tokens("claude-sonnet-4-5-20250929"), 1024);
    }

    #[test]
    fn threshold_opus45_is_4096() {
        assert_eq!(cache_threshold_tokens("claude-opus-4-5-20251101"), 4096);
    }

    #[test]
    fn threshold_haiku45_is_4096() {
        assert_eq!(cache_threshold_tokens("claude-haiku-4-5-20251001"), 4096);
    }

    #[test]
    fn threshold_opus48_is_1024() {
        assert_eq!(cache_threshold_tokens("claude-opus-4-8-20250514"), 1024);
    }

    #[test]
    fn alias_haiku45_resolves() {
        assert_eq!(
            canonical_model_id("claude-haiku-4-5"),
            "claude-haiku-4-5-20251001"
        );
    }

    #[test]
    fn fable5_is_a_dateless_canonical_model() {
        assert_eq!(canonical_model_id("claude-fable-5"), "claude-fable-5");
    }

    #[test]
    fn threshold_fable5_is_512() {
        assert_eq!(cache_threshold_tokens("claude-fable-5"), 512);
    }

    #[test]
    fn threshold_unknown_defaults_to_1024() {
        assert_eq!(cache_threshold_tokens("unknown-future-model"), 1024);
    }
}
