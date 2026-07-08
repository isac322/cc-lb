use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PromptCacheSimulatorKey([u8; 32]);

impl PromptCacheSimulatorKey {
    pub fn seed(canonical_model: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"cc-lb-cache-v3:seed");
        hasher.update(canonical_model.as_bytes());
        Self(hasher.finalize().into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptCachePrefixChain {
    keys: Vec<PromptCacheSimulatorKey>,
}

impl PromptCachePrefixChain {
    pub fn from_block_digests(
        seed: PromptCacheSimulatorKey,
        block_digests: impl IntoIterator<Item = [u8; 32]>,
    ) -> Self {
        let mut previous = seed;
        let mut keys = Vec::new();
        for digest in block_digests {
            let mut hasher = Sha256::new();
            hasher.update(b"cc-lb-cache-v3:prefix");
            hasher.update(previous.0);
            hasher.update(digest);
            previous = PromptCacheSimulatorKey(hasher.finalize().into());
            keys.push(previous);
        }
        Self { keys }
    }

    pub fn prefix_key(&self, block_index: usize) -> Option<PromptCacheSimulatorKey> {
        self.keys.get(block_index).copied()
    }

    pub fn lookback_keys(&self, block_index: usize) -> Vec<PromptCacheSimulatorKey> {
        let start = block_index.saturating_sub(19);
        self.keys
            .iter()
            .skip(start)
            .take(block_index.saturating_sub(start).saturating_add(1))
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{PromptCachePrefixChain, PromptCacheSimulatorKey};

    #[test]
    fn lookback_includes_nineteen_prior_blocks() {
        let chain = PromptCachePrefixChain::from_block_digests(
            PromptCacheSimulatorKey::seed("claude-opus-4-8"),
            (0_u8..25).map(|value| [value; 32]),
        );

        let keys = chain.lookback_keys(24);

        assert_eq!(keys.len(), 20);
        assert_eq!(keys[0], chain.prefix_key(5).expect("n-19 exists"));
        assert_eq!(keys[19], chain.prefix_key(24).expect("n exists"));
    }

    #[test]
    fn lookback_excludes_twenty_prior_blocks() {
        let chain = PromptCachePrefixChain::from_block_digests(
            PromptCacheSimulatorKey::seed("claude-opus-4-8"),
            (0_u8..25).map(|value| [value; 32]),
        );

        let keys = chain.lookback_keys(24);

        assert!(!keys.contains(&chain.prefix_key(4).expect("n-20 exists")));
    }
}
