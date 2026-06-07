//! Single tokenizer wrapper. o200k_base used for ALL Anthropic models.
//! Drift expected (Anthropic uses its own tokenizer); monitored via cc_lb_cache_token_drift metric.

use serde_json::Value as JsonValue;
use std::sync::OnceLock;
use tiktoken_rs::o200k_base;

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
        self.encoder.encode_ordinary(text).len()
    }

    /// Count tokens for a JSON prefix (serialized to string first).
    pub fn count_tokens_for_prefix(&self, body_prefix_json: &JsonValue) -> usize {
        let serialized = serde_json::to_string(body_prefix_json).unwrap_or_else(|_| String::new());
        self.count_tokens(&serialized)
    }

    /// Check if prefix token count is above the model's cache threshold.
    /// If model_resolution is not yet available, uses TEMP constant (1024 for Sonnet 4.5).
    pub fn is_above_threshold(&self, prefix_tokens: usize, canonical_model: &str) -> bool {
        let threshold = cache_threshold_tokens(canonical_model);
        prefix_tokens >= threshold
    }
}

/// Temporary threshold mapping. TODO(T3): Replace with import from model_resolution.
/// For now, return a conservative default of 1024 tokens for all models.
fn cache_threshold_tokens(canonical_model: &str) -> usize {
    // TODO(T3): Once model_resolution is merged, replace with:
    // cc_lb_core::model_resolution::cache_threshold_tokens(canonical_model)
    match canonical_model {
        "claude-3-5-sonnet-20241022" => 1024,
        "claude-3-opus-20250219" => 1024,
        "claude-3-sonnet-20240229" => 1024,
        _ => 1024,
    }
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
    fn is_above_threshold_for_sonnet_at_1024() {
        let tokenizer = PrefixTokenizer::global();
        let model = "claude-3-5-sonnet-20241022";

        // Below threshold (512 < 1024)
        assert!(
            !tokenizer.is_above_threshold(512, model),
            "512 tokens should be below threshold for {}",
            model
        );

        // At threshold (1024 >= 1024)
        assert!(
            tokenizer.is_above_threshold(1024, model),
            "1024 tokens should be at/above threshold for {}",
            model
        );

        // Above threshold (2048 >= 1024)
        assert!(
            tokenizer.is_above_threshold(2048, model),
            "2048 tokens should be above threshold for {}",
            model
        );
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
}
