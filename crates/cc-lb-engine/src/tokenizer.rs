//! Single tokenizer wrapper. o200k_base used for ALL Anthropic models.
//! Drift expected (Anthropic uses its own tokenizer); monitored via cc_lb_cache_token_drift metric.

use std::collections::HashSet;
use std::sync::OnceLock;

use serde_json::Value as JsonValue;
use tiktoken_rs::{Rank, o200k_base};

#[cfg(test)]
thread_local! {
    static TOKENIZER_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TokenizerBatchStats {
    pub(crate) input_bytes: u64,
    pub(crate) produced_tokens: u64,
    pub(crate) fallback_prefixes: u64,
}

/// Wraps the o200k_base tokenizer for consistent token counting across the proxy.
pub struct PrefixTokenizer {
    encoder: tiktoken_rs::CoreBPE,
}

impl PrefixTokenizer {
    /// Initialize tokenizer with o200k_base encoder.
    fn new() -> Self {
        let encoder = o200k_base().expect("o200k_base encoder initialization failed");
        PrefixTokenizer { encoder }
    }

    /// Return the global singleton tokenizer instance.
    pub fn global() -> &'static PrefixTokenizer {
        static TOKENIZER: OnceLock<PrefixTokenizer> = OnceLock::new();
        TOKENIZER.get_or_init(PrefixTokenizer::new)
    }

    /// Count tokens in plain text.
    pub fn count_tokens(&self, text: &str) -> usize {
        #[cfg(test)]
        TOKENIZER_CALLS.with(|calls| calls.set(calls.get() + 1));
        self.encoder.encode_ordinary(text).len()
    }

    /// Counts nested JSON prefixes without re-tokenizing stable leading pieces.
    ///
    /// Each prefix is `open_prefix_bytes[..offset] + suffix_bytes`. The encoder's
    /// final regex piece is deliberately retained between offsets because adding
    /// another block can change tokenization at that boundary. Inputs that cannot
    /// use the incremental path fall back to exact full-prefix counting.
    pub(crate) fn count_nested_prefixes(
        &self,
        open_prefix_bytes: &[u8],
        breakpoint_offsets: &[usize],
        suffix_bytes: &[u8],
    ) -> (Vec<u64>, TokenizerBatchStats) {
        let Some(open_prefix) = std::str::from_utf8(open_prefix_bytes).ok() else {
            return self.count_nested_prefixes_fallback(
                open_prefix_bytes,
                breakpoint_offsets,
                suffix_bytes,
                TokenizerBatchStats::default(),
            );
        };
        let Some(suffix) = std::str::from_utf8(suffix_bytes).ok() else {
            return self.count_nested_prefixes_fallback(
                open_prefix_bytes,
                breakpoint_offsets,
                suffix_bytes,
                TokenizerBatchStats::default(),
            );
        };

        let mut counts = Vec::with_capacity(breakpoint_offsets.len());
        let mut stats = TokenizerBatchStats::default();
        let mut stable_token_count = 0usize;
        let mut pending = String::new();
        let mut previous_offset = 0usize;
        let allowed_special = HashSet::new();

        for &offset in breakpoint_offsets {
            if offset < previous_offset
                || offset > open_prefix.len()
                || !open_prefix.is_char_boundary(offset)
            {
                return self.count_nested_prefixes_fallback(
                    open_prefix_bytes,
                    breakpoint_offsets,
                    suffix_bytes,
                    stats,
                );
            }

            pending.push_str(&open_prefix[previous_offset..offset]);
            stats.input_bytes = stats.input_bytes.saturating_add(pending.len() as u64);
            let Ok((tokens, last_piece_token_len)) =
                self.encoder.encode(&pending, &allowed_special)
            else {
                return self.count_nested_prefixes_fallback(
                    open_prefix_bytes,
                    breakpoint_offsets,
                    suffix_bytes,
                    stats,
                );
            };
            stats.produced_tokens = stats.produced_tokens.saturating_add(tokens.len() as u64);

            let unstable_token_len =
                self.extended_unstable_token_len(&tokens, last_piece_token_len);
            let stable_end = tokens.len().saturating_sub(unstable_token_len);
            let Ok(unstable_bytes) = self.encoder.decode_bytes(&tokens[stable_end..]) else {
                return self.count_nested_prefixes_fallback(
                    open_prefix_bytes,
                    breakpoint_offsets,
                    suffix_bytes,
                    stats,
                );
            };
            let stable_byte_len = pending.len().saturating_sub(unstable_bytes.len());
            if !pending.as_bytes().ends_with(&unstable_bytes)
                || !pending.is_char_boundary(stable_byte_len)
            {
                return self.count_nested_prefixes_fallback(
                    open_prefix_bytes,
                    breakpoint_offsets,
                    suffix_bytes,
                    stats,
                );
            }

            stable_token_count = stable_token_count.saturating_add(stable_end);
            pending.drain(..stable_byte_len);

            let mut tail = String::with_capacity(pending.len().saturating_add(suffix.len()));
            tail.push_str(&pending);
            tail.push_str(suffix);
            stats.input_bytes = stats.input_bytes.saturating_add(tail.len() as u64);
            let tail_tokens = self.encoder.encode_ordinary(&tail);
            stats.produced_tokens = stats
                .produced_tokens
                .saturating_add(tail_tokens.len() as u64);
            counts.push(stable_token_count.saturating_add(tail_tokens.len()) as u64);
            previous_offset = offset;
        }

        (counts, stats)
    }

    fn extended_unstable_token_len(&self, tokens: &[Rank], last_piece_token_len: usize) -> usize {
        let mut unstable_token_len = last_piece_token_len.min(tokens.len());
        if unstable_token_len == 0
            || !self.token_is_all_space(tokens[tokens.len() - unstable_token_len])
        {
            return unstable_token_len;
        }
        while unstable_token_len < tokens.len()
            && self.token_is_all_space(tokens[tokens.len() - unstable_token_len - 1])
        {
            unstable_token_len += 1;
        }
        unstable_token_len
    }

    fn token_is_all_space(&self, token: Rank) -> bool {
        self.encoder
            .decode_bytes(&[token])
            .is_ok_and(|bytes| bytes.iter().rev().all(|byte| b" \n\t".contains(byte)))
    }

    fn count_nested_prefixes_fallback(
        &self,
        open_prefix_bytes: &[u8],
        breakpoint_offsets: &[usize],
        suffix_bytes: &[u8],
        mut stats: TokenizerBatchStats,
    ) -> (Vec<u64>, TokenizerBatchStats) {
        let mut counts = Vec::with_capacity(breakpoint_offsets.len());
        stats.fallback_prefixes = breakpoint_offsets.len() as u64;
        for &offset in breakpoint_offsets {
            let Some(open_prefix) = open_prefix_bytes.get(..offset) else {
                counts.push(0);
                continue;
            };
            let mut bytes =
                Vec::with_capacity(open_prefix.len().saturating_add(suffix_bytes.len()));
            bytes.extend_from_slice(open_prefix);
            bytes.extend_from_slice(suffix_bytes);
            stats.input_bytes = stats.input_bytes.saturating_add(bytes.len() as u64);
            let count = std::str::from_utf8(&bytes)
                .map(|text| self.count_tokens(text) as u64)
                .unwrap_or(0);
            stats.produced_tokens = stats.produced_tokens.saturating_add(count);
            counts.push(count);
        }
        (counts, stats)
    }

    /// Count tokens for a JSON prefix (serialized to string first).
    pub fn count_tokens_for_prefix(&self, body_prefix_json: &JsonValue) -> usize {
        let serialized = serde_json::to_string(body_prefix_json).unwrap_or_else(|_| String::new());
        self.count_tokens(&serialized)
    }
}

#[cfg(test)]
pub fn tokenizer_call_count() -> u64 {
    TOKENIZER_CALLS.with(std::cell::Cell::get)
}

#[cfg(test)]
pub fn reset_tokenizer_call_count() {
    TOKENIZER_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_tokens_basic() {
        let tokenizer = PrefixTokenizer::global();
        let count = tokenizer.count_tokens("Hello, world!");
        assert!(
            count > 0,
            "Expected non-zero token count for 'Hello, world!'"
        );
    }

    #[test]
    fn count_tokens_for_prefix_basic() {
        let tokenizer = PrefixTokenizer::global();
        let json = serde_json::json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [{"role": "user", "content": "test"}]
        });
        let count = tokenizer.count_tokens_for_prefix(&json);
        assert!(count > 0, "Expected non-zero token count for JSON prefix");
    }

    #[test]
    fn tokenizer_singleton_consistency() {
        let tokenizer1 = PrefixTokenizer::global();
        let tokenizer2 = PrefixTokenizer::global();
        assert!(
            std::ptr::eq(tokenizer1, tokenizer2),
            "global() should return the same instance"
        );
    }

    #[test]
    fn count_tokens_non_empty_text() {
        let tokenizer = PrefixTokenizer::global();
        let text = "The quick brown fox jumps over the lazy dog";
        let count = tokenizer.count_tokens(text);
        assert!(count > 0, "Expected positive token count");
        // o200k_base should tokenize this into roughly 9-10 tokens
        assert!(
            (8..=12).contains(&count),
            "Expected ~10 tokens for test text, got {}",
            count
        );
    }
    #[test]
    fn invalid_incremental_offsets_fall_back_to_exact_counts() {
        let tokenizer = PrefixTokenizer::global();
        let (counts, stats) = tokenizer.count_nested_prefixes(b"abcdef", &[6, 3], b"");
        assert_eq!(
            counts,
            vec![
                tokenizer.count_tokens("abcdef") as u64,
                tokenizer.count_tokens("abc") as u64,
            ]
        );
        assert_eq!(stats.fallback_prefixes, 2);
    }
}
