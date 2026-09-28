//! Single tokenizer wrapper. o200k_base used for ALL Anthropic models.

use std::sync::OnceLock;

use serde_json::Value as JsonValue;
use tiktoken_rs::o200k_base;

pub(crate) const CACHE_THRESHOLD_FAST_ACCEPT_BYTES_PER_TOKEN: usize = 8;
// Anthropic does not expose its tokenizer. Prefixes at least 8 bytes per required
// token are treated as clearly large; only the middle band pays for local o200k.

pub(crate) fn cache_threshold_from_byte_len(
    byte_len: usize,
    token_threshold: usize,
) -> Option<bool> {
    if token_threshold == 0 {
        return Some(true);
    }
    if byte_len < token_threshold {
        return Some(false);
    }
    if byte_len >= token_threshold.saturating_mul(CACHE_THRESHOLD_FAST_ACCEPT_BYTES_PER_TOKEN) {
        return Some(true);
    }
    None
}

#[cfg(test)]
thread_local! {
    static TOKENIZER_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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

    pub(crate) fn meets_cache_threshold(
        &self,
        serialized_prefix: &[u8],
        token_threshold: usize,
    ) -> bool {
        if let Some(decision) =
            cache_threshold_from_byte_len(serialized_prefix.len(), token_threshold)
        {
            return decision;
        }
        let Ok(text) = std::str::from_utf8(serialized_prefix) else {
            return false;
        };
        let inflight = metrics::gauge!("cc_lb_prompt_cache_tokenizer_inflight");
        inflight.increment(1.0);
        struct InflightGuard(metrics::Gauge);
        impl Drop for InflightGuard {
            fn drop(&mut self) {
                self.0.decrement(1.0);
            }
        }
        let _inflight = InflightGuard(inflight);
        let started = std::time::Instant::now();
        let token_count = self.count_tokens(text);
        metrics::histogram!(
            "cc_lb_prompt_cache_analysis_duration_seconds",
            "stage" => "tokenize"
        )
        .record(started.elapsed().as_secs_f64());
        metrics::counter!("cc_lb_prompt_cache_tokenized_bytes_total")
            .increment(serialized_prefix.len() as u64);
        metrics::counter!("cc_lb_prompt_cache_tokenized_tokens_total")
            .increment(token_count as u64);
        metrics::counter!("cc_lb_prompt_cache_tokenizer_fallback_prefixes_total").increment(1);
        token_count >= token_threshold
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
    fn cache_threshold_byte_fast_paths_skip_tokenizer() {
        reset_tokenizer_call_count();
        assert_eq!(cache_threshold_from_byte_len(511, 512), Some(false));
        assert!(!PrefixTokenizer::global().meets_cache_threshold(&[b'a'; 511], 512));
        assert_eq!(tokenizer_call_count(), 0);

        reset_tokenizer_call_count();
        let clearly_large = vec![b'a'; 512 * CACHE_THRESHOLD_FAST_ACCEPT_BYTES_PER_TOKEN];
        assert!(PrefixTokenizer::global().meets_cache_threshold(&clearly_large, 512));
        assert_eq!(tokenizer_call_count(), 0);
    }

    #[test]
    fn cache_threshold_ambiguous_prefix_uses_tokenizer() {
        let prefix = br#"{"content_blocks":[{"source":"message","value":"hello world"}]}"#;
        let threshold = 8;
        assert_eq!(cache_threshold_from_byte_len(prefix.len(), threshold), None);
        let expected = PrefixTokenizer::global().count_tokens(std::str::from_utf8(prefix).unwrap())
            >= threshold;

        reset_tokenizer_call_count();
        assert_eq!(
            PrefixTokenizer::global().meets_cache_threshold(prefix, threshold),
            expected
        );
        assert_eq!(tokenizer_call_count(), 1);
    }
}
