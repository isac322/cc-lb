use std::collections::BTreeSet;

use cc_lb_engine::prompt_cache_simulator::{
    V3PromptCacheBlock, V3PromptCacheBlockSource, analyze_v3_prompt_cache,
};
use serde_json::{Value, json};

#[path = "prompt_cache_byte_oracle/corpus.rs"]
mod corpus;
#[path = "prompt_cache_byte_oracle/oracles.rs"]
mod oracles;

use oracles::{
    block_digest_serializer_bytes, digest_bytes_preserving_cache_control,
    legacy_block_digest_oracle_bytes, legacy_prefix_oracle_bytes,
    prefix_bytes_stripping_cache_control, prefix_serializer_bytes, token_count,
};

const CANONICAL_MODEL: &str = "claude-sonnet-4-5-20250929";

#[test]
fn prompt_cache_byte_oracle_matches_current_behavior_over_wide_corpus() {
    let cases = corpus::wide_corpus();
    let mut coverage = BTreeSet::new();
    let mut saw_prefix_only_blocks = false;
    let mut saw_four_breakpoints = false;

    for case in &cases {
        let analysis = analyze_v3_prompt_cache(&case.request, CANONICAL_MODEL);
        if case.name == "empty-text-and-prefix-only-blocks" {
            let block_types = analysis
                .blocks
                .iter()
                .filter_map(|block| block.value.get("type").and_then(Value::as_str))
                .collect::<Vec<_>>();
            assert_eq!(block_types, vec!["thinking", "unknown"]);
            assert!(
                analysis.blocks.iter().all(|block| {
                    block.value.get("type").and_then(Value::as_str) != Some("text")
                        || block.value.get("text").and_then(Value::as_str) != Some("")
                }),
                "empty text blocks must not participate in the prefix"
            );
            assert!(
                analysis.breakpoints.is_empty(),
                "prefix-only block types must not become breakpoints"
            );
            saw_prefix_only_blocks = true;
        }
        saw_four_breakpoints |=
            case.name == "four-breakpoint-maximum" && analysis.breakpoints.len() == 4;

        for block in &analysis.blocks {
            record_coverage(&mut coverage, block);
            assert_eq!(
                block_digest_serializer_bytes(block),
                legacy_block_digest_oracle_bytes(block),
                "block-digest bytes diverged for {} at {}",
                case.name,
                block.path
            );
        }

        for breakpoint in &analysis.breakpoints {
            let index = breakpoint.block_index as usize;
            let blocks = &analysis.blocks[..=index];
            let legacy_prefix = legacy_prefix_oracle_bytes(CANONICAL_MODEL, blocks);
            let serializer_prefix = prefix_serializer_bytes(CANONICAL_MODEL, blocks);
            assert_eq!(
                serializer_prefix, legacy_prefix,
                "prefix bytes diverged for {} at {}",
                case.name, breakpoint.path
            );
            let legacy_tokens = token_count(&legacy_prefix);
            let serializer_tokens = token_count(&serializer_prefix);
            assert_eq!(serializer_tokens, legacy_tokens, "tokens: {}", case.name);
            assert_eq!(
                breakpoint.prefix_token_count, legacy_tokens,
                "live token count: {}",
                case.name
            );
        }
    }

    assert!(
        saw_prefix_only_blocks,
        "empty-text/prefix-only corpus case missing"
    );
    assert!(
        saw_four_breakpoints,
        "four-breakpoint maximum corpus case missing"
    );
    assert_eq!(
        coverage,
        BTreeSet::from([
            "document",
            "image",
            "system-string",
            "thinking",
            "text",
            "tool_result",
            "tool_use",
            "tools",
            "unknown",
        ]),
        "prompt-cache byte-oracle corpus must cover eligible and prefix-only block types"
    );
}

#[test]
fn prompt_cache_byte_oracle_prefix_preserves_cache_control() {
    let request = json!({
        "model": CANONICAL_MODEL,
        "messages": [{"role": "user", "content": [{
            "type": "text",
            "text": "cache-control divergence sentinel",
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }]}]
    });
    let analysis = analyze_v3_prompt_cache(&request, CANONICAL_MODEL);
    let block = analysis.blocks.first().expect("cacheable sentinel block");
    let breakpoint = analysis.breakpoints.first().expect("sentinel breakpoint");

    let stripped_digest_bytes = legacy_block_digest_oracle_bytes(block);
    let preserved_digest_bytes = digest_bytes_preserving_cache_control(block);
    assert_ne!(stripped_digest_bytes, preserved_digest_bytes);
    assert_eq!(
        block_digest_serializer_bytes(block),
        stripped_digest_bytes,
        "block-digest oracle must strip cache_control"
    );

    let preserved_prefix = legacy_prefix_oracle_bytes(CANONICAL_MODEL, &analysis.blocks);
    let stripped_prefix = prefix_bytes_stripping_cache_control(CANONICAL_MODEL, &analysis.blocks);
    assert!(
        String::from_utf8_lossy(&preserved_prefix).contains("cache_control"),
        "prefix oracle must retain cache_control bytes"
    );
    assert_ne!(preserved_prefix, stripped_prefix);
    assert_eq!(
        breakpoint.prefix_token_count,
        token_count(&preserved_prefix),
        "live prefix tokenization must use cache_control-preserving bytes"
    );
    assert_ne!(
        breakpoint.prefix_token_count,
        token_count(&stripped_prefix),
        "sentinel must distinguish preserved from stripped prefix tokenization"
    );
}

#[test]
fn prompt_cache_byte_oracle_key_reordering_is_byte_stable() {
    let cases = corpus::wide_corpus();
    let first = cases
        .iter()
        .find(|case| case.name == "key-reordered-a")
        .expect("first key-reordered case");
    let second = cases
        .iter()
        .find(|case| case.name == "key-reordered-b")
        .expect("second key-reordered case");
    let first_analysis = analyze_v3_prompt_cache(&first.request, CANONICAL_MODEL);
    let second_analysis = analyze_v3_prompt_cache(&second.request, CANONICAL_MODEL);

    assert_eq!(first_analysis.blocks, second_analysis.blocks);
    assert_eq!(first_analysis.breakpoints, second_analysis.breakpoints);
    assert_eq!(
        legacy_prefix_oracle_bytes(CANONICAL_MODEL, &first_analysis.blocks),
        legacy_prefix_oracle_bytes(CANONICAL_MODEL, &second_analysis.blocks)
    );
}

fn record_coverage(coverage: &mut BTreeSet<&'static str>, block: &V3PromptCacheBlock) {
    match block.source {
        V3PromptCacheBlockSource::Tools => {
            coverage.insert("tools");
        }
        V3PromptCacheBlockSource::System if block.path == "system" => {
            coverage.insert("system-string");
        }
        V3PromptCacheBlockSource::System | V3PromptCacheBlockSource::Message => {
            if let Some(block_type) = block.value.get("type").and_then(Value::as_str) {
                coverage.insert(match block_type {
                    "text" => "text",
                    "image" => "image",
                    "document" => "document",
                    "tool_use" => "tool_use",
                    "tool_result" => "tool_result",
                    "thinking" => "thinking",
                    _ => "unknown",
                });
            }
        }
    }
}
