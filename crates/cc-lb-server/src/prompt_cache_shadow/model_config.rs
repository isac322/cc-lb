#[derive(Debug, Clone)]
pub struct ModelCacheConfig {
    pub min_cacheable_prefix_tokens: u32,
    pub supports_1h_ttl: bool,
    pub lookback_blocks: u32,
}

pub fn model_config(model_id: &str) -> Option<ModelCacheConfig> {
    match model_id {
        "claude-sonnet-4-5" => Some(ModelCacheConfig {
            min_cacheable_prefix_tokens: 1024,
            supports_1h_ttl: true,
            lookback_blocks: 18,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sonnet_4_5_supported() {
        let config = model_config("claude-sonnet-4-5");
        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.min_cacheable_prefix_tokens, 1024);
        assert_eq!(config.supports_1h_ttl, true);
        assert_eq!(config.lookback_blocks, 18);
    }

    #[test]
    fn opus_4_7_unsupported() {
        let config = model_config("claude-opus-4-7");
        assert!(config.is_none());
    }
}
