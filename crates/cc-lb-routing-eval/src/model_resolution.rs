//! Canonical model ID resolution and per-model cache threshold tables.
//!
//! Source: https://platform.claude.com/docs/en/build-with-claude/prompt-caching#cache-limitations
//! Retrieved: 2026-07-27

use cc_lb_domain::Provider;

/// Resolve an Anthropic model alias to its canonical (dated) form.
///
/// Supports rolling Anthropic aliases (e.g., `claude-sonnet-4-5`) that map to specific dated
/// versions. Already-dated forms and unknown Anthropic models pass through unchanged; unknown
/// aliases are warned. OpenAI and Gemini model IDs pass through unchanged without alias warnings.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(
///     canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5"),
///     "claude-sonnet-4-5-20250929"
/// );
/// assert_eq!(
///     canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5-20250929"),
///     "claude-sonnet-4-5-20250929"
/// );
/// assert_eq!(
///     canonical_model_id(Provider::Openai, "gpt-5"),
///     "gpt-5"
/// );
/// ```
pub fn canonical_model_id(provider: Provider, requested: &str) -> &str {
    cc_lb_providers::builtin_provider_registry()
        .canonical_model_id(&cc_lb_provider_spi::ProviderId::from(provider), requested)
}

/// Get the minimum cacheable prompt length (in tokens) for a canonical model.
///
/// Known models delegate to the isolated provider adapter registry (`AnthropicProviderAdapter`
/// in `cc-lb-provider-anthropic` owns Anthropic's threshold table). Unknown models default to
/// 1,024 tokens with a warning log.
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
    if let Some(threshold) =
        cc_lb_providers::builtin_provider_registry().cache_threshold_tokens(canonical)
    {
        return threshold;
    }
    tracing::warn!(model = %canonical, "unknown model, defaulting cache threshold to 1024");
    1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_resolves_to_dated_sonnet45() {
        assert_eq!(
            canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5"),
            "claude-sonnet-4-5-20250929"
        );
    }

    #[test]
    fn dated_sonnet45_passes_through() {
        assert_eq!(
            canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5-20250929"),
            "claude-sonnet-4-5-20250929"
        );
    }

    #[test]
    fn opus45_alias_resolves() {
        assert_eq!(
            canonical_model_id(Provider::Anthropic, "claude-opus-4-5"),
            "claude-opus-4-5-20251101"
        );
    }

    #[test]
    fn unknown_passes_through() {
        assert_eq!(
            canonical_model_id(Provider::Anthropic, "future-model-xyz"),
            "future-model-xyz"
        );
    }

    #[test]
    fn non_anthropic_models_pass_through_without_alias_resolution() {
        for provider in [Provider::Openai, Provider::Gemini] {
            assert_eq!(
                canonical_model_id(provider, "claude-sonnet-4-5"),
                "claude-sonnet-4-5"
            );
            assert_eq!(canonical_model_id(provider, "gpt-5"), "gpt-5");
        }
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
            canonical_model_id(Provider::Anthropic, "claude-haiku-4-5"),
            "claude-haiku-4-5-20251001"
        );
    }

    #[test]
    fn threshold_haiku45_alias_is_4096() {
        assert_eq!(cache_threshold_tokens("claude-haiku-4-5"), 4096);
    }

    #[test]
    fn fable5_is_a_dateless_canonical_model() {
        assert_eq!(
            canonical_model_id(Provider::Anthropic, "claude-fable-5"),
            "claude-fable-5"
        );
    }

    #[test]
    fn threshold_fable5_is_512() {
        assert_eq!(cache_threshold_tokens("claude-fable-5"), 512);
    }

    #[test]
    fn threshold_opus5_is_512() {
        assert_eq!(cache_threshold_tokens("claude-opus-5"), 512);
    }

    #[test]
    fn threshold_mythos5_is_512() {
        assert_eq!(cache_threshold_tokens("claude-mythos-5"), 512);
    }

    #[test]
    fn threshold_mythos_preview_is_2048() {
        assert_eq!(cache_threshold_tokens("claude-mythos-preview"), 2048);
    }

    #[test]
    fn threshold_haiku35_is_2048_on_its_real_ids() {
        // Haiku 3.5 uses the pre-4.0 `claude-3-5-haiku-*` ordering, not `claude-haiku-3-5`.
        // Falling through to the 1024 default would predict a 1024-2047 token prefix as
        // cacheable when the provider refuses it.
        assert_eq!(cache_threshold_tokens("claude-3-5-haiku-20241022"), 2048);
        assert_eq!(cache_threshold_tokens("claude-3-5-haiku-latest"), 2048);
    }

    #[test]
    fn threshold_opus47_is_2048() {
        assert_eq!(cache_threshold_tokens("claude-opus-4-7"), 2048);
    }

    #[test]
    fn threshold_sonnet5_is_1024() {
        assert_eq!(cache_threshold_tokens("claude-sonnet-5"), 1024);
    }

    #[test]
    fn threshold_unknown_defaults_to_1024() {
        assert_eq!(cache_threshold_tokens("unknown-future-model"), 1024);
    }
}
