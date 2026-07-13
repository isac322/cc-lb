use serde_json::json;

use crate::{
    model_resolution::canonical_model_id,
    tokenizer::{
        PREFIX_TOKEN_COUNT_MEMO_VERSION, PrefixTokenizer, reset_cached_prefix_token_counts,
        reset_tokenizer_call_count, tokenizer_call_count,
    },
};

use super::{analyze_v3_prompt_cache, analyze_v3_prompt_cache_breakpoints};

const CANONICAL_MODEL: &str = "claude-sonnet-4-5-20250929";

#[test]
fn memoized_prefix_counts_match_fresh_tokenization_for_varied_inputs() {
    // Given: several valid serialized prefix payloads.
    let prefixes = [
        r#"{"content_blocks":[],"model":"claude-sonnet-4-5-20250929"}"#,
        r#"{"content_blocks":[{"source":"system","value":{"text":"hello","type":"text"}}],"model":"claude-sonnet-4-5-20250929"}"#,
        r#"{"content_blocks":[{"source":"message","value":{"cache_control":{"type":"ephemeral"},"text":"안녕하세요 👋","type":"text"}}],"model":"claude-sonnet-4-5-20250929"}"#,
    ];
    let tokenizer = PrefixTokenizer::global();
    reset_cached_prefix_token_counts();

    for prefix in prefixes {
        // When: the same bytes are counted directly and through memoization.
        let fresh = tokenizer.count_tokens(prefix) as u64;
        let memoized = tokenizer.count_tokens_for_cached_prefix(
            PREFIX_TOKEN_COUNT_MEMO_VERSION,
            CANONICAL_MODEL,
            prefix.as_bytes(),
        );

        // Then: memoization preserves the exact token count.
        assert_eq!(memoized, fresh, "memoized count diverged for {prefix}");
    }
}

#[test]
fn memoized_prefix_count_skips_retokens_on_cache_hit() {
    // Given: an empty memo cache and one serialized prefix.
    let prefix = r#"{"content_blocks":[{"source":"system","value":{"text":"stable prefix","type":"text"}}],"model":"claude-sonnet-4-5-20250929"}"#;
    let tokenizer = PrefixTokenizer::global();
    reset_cached_prefix_token_counts();
    reset_tokenizer_call_count();

    // When: the exact same prefix is counted twice.
    let first = tokenizer.count_tokens_for_cached_prefix(
        PREFIX_TOKEN_COUNT_MEMO_VERSION,
        CANONICAL_MODEL,
        prefix.as_bytes(),
    );
    let second = tokenizer.count_tokens_for_cached_prefix(
        PREFIX_TOKEN_COUNT_MEMO_VERSION,
        CANONICAL_MODEL,
        prefix.as_bytes(),
    );

    // Then: the second count is returned from the memo without tokenization.
    assert_eq!(second, first);
    assert_eq!(tokenizer_call_count(), 1);
}

#[test]
fn memoized_prefix_count_misses_after_schema_version_changes() {
    // Given: a cached prefix under the current serialization/tokenizer version.
    let prefix = r#"{"content_blocks":[],"model":"claude-sonnet-4-5-20250929"}"#;
    let tokenizer = PrefixTokenizer::global();
    reset_cached_prefix_token_counts();
    reset_tokenizer_call_count();
    let current = tokenizer.count_tokens_for_cached_prefix(
        PREFIX_TOKEN_COUNT_MEMO_VERSION,
        CANONICAL_MODEL,
        prefix.as_bytes(),
    );

    // When: the cache version changes while the input bytes remain identical.
    let bumped = tokenizer.count_tokens_for_cached_prefix(
        PREFIX_TOKEN_COUNT_MEMO_VERSION + 1,
        CANONICAL_MODEL,
        prefix.as_bytes(),
    );

    // Then: versioning forces a fresh tokenizer call without changing its result.
    assert_eq!(bumped, current);
    assert_eq!(tokenizer_call_count(), 2);
}

#[test]
fn breakpoint_free_runtime_analysis_matches_the_full_analysis() {
    // Given: a request with cacheable content but no cache_control breakpoint.
    let request = json!({
        "model": CANONICAL_MODEL,
        "system": [{"type":"text","text":"stable system"}],
        "messages": [{"role":"user","content":[{"type":"text","text":"tail"}]}]
    });
    let canonical_model = canonical_model_id(CANONICAL_MODEL);
    let full_analysis = analyze_v3_prompt_cache(&request, canonical_model);
    reset_tokenizer_call_count();

    // When: the runtime-only analysis executes.
    let runtime_breakpoints = analyze_v3_prompt_cache_breakpoints(&request, canonical_model);

    // Then: the observable runtime metadata is identical without tokenization.
    assert_eq!(runtime_breakpoints, full_analysis.breakpoints);
    assert_eq!(tokenizer_call_count(), 0);
}
