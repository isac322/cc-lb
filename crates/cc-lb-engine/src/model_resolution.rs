//! Canonical model ID resolution and per-model cache threshold tables.
//!
//! Source: https://platform.claude.com/docs/en/build-with-claude/prompt-caching#cache-limitations
//! Retrieved: 2026-07-27

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
        "claude-opus-5" => "claude-opus-5",
        "claude-mythos-5" => "claude-mythos-5",
        "claude-mythos-preview" => "claude-mythos-preview",
        "claude-sonnet-5" => "claude-sonnet-5",
        "claude-sonnet-4-6" => "claude-sonnet-4-6",
        "claude-opus-4-6" => "claude-opus-4-6",
        "claude-opus-4-7" => "claude-opus-4-7",

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
/// The match arms below are the single source of truth; they are grouped by threshold and are
/// deliberately not restated here, because a prose copy silently drifts from them.
/// Source: <https://platform.claude.com/docs/en/build-with-claude/prompt-caching#cache-limitations>
/// (retrieved 2026-07-27). The minimum is model-specific and NOT monotonic across a family:
/// Opus 4.5 and 4.6 are 4,096 while 4.7 is 2,048, 4.8 is 1,024 and Opus 5 is 512.
///
/// The explicit 1,024-token arms equal the catch-all default; they exist only to suppress the
/// unknown-model warning and are not threshold corrections.
///
/// Haiku 3.5 predates the `claude-<family>-<major>-<minor>` ordering and is addressed as
/// `claude-3-5-haiku-*`, which is how the rest of the workspace names it. It is retired on the
/// Claude API but still reachable through Bedrock and Google Cloud upstreams, so the arm is live:
/// without it a 1,024-2,047 token prefix would be predicted cacheable here and silently refused by
/// the provider, sending affinity to an upstream that never wrote an entry.
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
        // Opus 5 / Fable 5 / Mythos 5: 512
        "claude-opus-5" => 512,
        "claude-fable-5" => 512,
        "claude-mythos-5" => 512,

        // Opus 4.8 / Sonnet 5 / Sonnet 4.6 / Sonnet 4.5 / Opus 4.1 / Opus 4 / Sonnet 4: 1024
        "claude-opus-4-8-20250514" => 1024,
        "claude-opus-4-8" => 1024,
        "claude-sonnet-5" => 1024,
        "claude-sonnet-4-6" => 1024,
        "claude-sonnet-4-5-20250929" => 1024,
        "claude-sonnet-4-5" => 1024,
        "claude-opus-4-1" => 1024,
        "claude-opus-4" => 1024,
        "claude-sonnet-4" => 1024,

        // Mythos Preview / Opus 4.7 / Haiku 3.5: 2048
        "claude-mythos-preview" => 2048,
        "claude-opus-4-7" => 2048,
        "claude-3-5-haiku-20241022" => 2048,
        "claude-3-5-haiku-latest" => 2048,

        // Opus 4.5 / 4.6 / Haiku 4.5: 4096
        "claude-opus-4-5-20251101" => 4096,
        "claude-opus-4-5" => 4096,
        "claude-opus-4-6" => 4096,
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
    fn canonical_model_id_table() {
        let cases = [
            (
                "alias_resolves_to_dated_sonnet45",
                "claude-sonnet-4-5",
                "claude-sonnet-4-5-20250929",
            ),
            (
                "dated_sonnet45_passes_through",
                "claude-sonnet-4-5-20250929",
                "claude-sonnet-4-5-20250929",
            ),
            (
                "opus45_alias_resolves",
                "claude-opus-4-5",
                "claude-opus-4-5-20251101",
            ),
            (
                "unknown_passes_through",
                "future-model-xyz",
                "future-model-xyz",
            ),
            (
                "alias_haiku45_resolves",
                "claude-haiku-4-5",
                "claude-haiku-4-5-20251001",
            ),
            (
                "fable5_is_a_dateless_canonical_model",
                "claude-fable-5",
                "claude-fable-5",
            ),
        ];

        for (case, input, expected) in cases {
            assert_eq!(canonical_model_id(input), expected, "case={case}");
        }
    }

    #[test]
    fn model_cache_threshold_table() {
        struct Case {
            case: &'static str,
            inputs: &'static [&'static str],
            expected: usize,
        }

        let cases = [
            Case {
                case: "threshold_sonnet45_is_1024",
                inputs: &["claude-sonnet-4-5-20250929"],
                expected: 1024,
            },
            Case {
                case: "threshold_opus45_is_4096",
                inputs: &["claude-opus-4-5-20251101"],
                expected: 4096,
            },
            Case {
                case: "threshold_haiku45_is_4096",
                inputs: &["claude-haiku-4-5-20251001"],
                expected: 4096,
            },
            Case {
                case: "threshold_opus48_is_1024",
                inputs: &["claude-opus-4-8-20250514"],
                expected: 1024,
            },
            Case {
                case: "threshold_haiku45_alias_is_4096",
                inputs: &["claude-haiku-4-5"],
                expected: 4096,
            },
            Case {
                case: "threshold_fable5_is_512",
                inputs: &["claude-fable-5"],
                expected: 512,
            },
            Case {
                case: "threshold_opus5_is_512",
                inputs: &["claude-opus-5"],
                expected: 512,
            },
            Case {
                case: "threshold_mythos5_is_512",
                inputs: &["claude-mythos-5"],
                expected: 512,
            },
            Case {
                case: "threshold_mythos_preview_is_2048",
                inputs: &["claude-mythos-preview"],
                expected: 2048,
            },
            Case {
                // Haiku 3.5 uses the pre-4.0 `claude-3-5-haiku-*` ordering.
                case: "threshold_haiku35_is_2048_on_its_real_ids",
                inputs: &["claude-3-5-haiku-20241022", "claude-3-5-haiku-latest"],
                expected: 2048,
            },
            Case {
                case: "threshold_opus47_is_2048",
                inputs: &["claude-opus-4-7"],
                expected: 2048,
            },
            Case {
                case: "threshold_sonnet5_is_1024",
                inputs: &["claude-sonnet-5"],
                expected: 1024,
            },
            Case {
                case: "threshold_unknown_defaults_to_1024",
                inputs: &["unknown-future-model"],
                expected: 1024,
            },
        ];

        for case in cases {
            for input in case.inputs {
                assert_eq!(
                    cache_threshold_tokens(input),
                    case.expected,
                    "case={} input={input}",
                    case.case
                );
            }
        }
    }
}
