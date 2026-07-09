use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::tokenizer::PrefixTokenizer;

pub const V3_TOKEN_ESTIMATE_SOURCE: &str = "local_tiktoken_v1";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PromptCacheSimulatorKey([u8; 32]);

impl PromptCacheSimulatorKey {
    pub fn seed(canonical_model: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"cc-lb-cache-v3:seed");
        hasher.update(canonical_model.as_bytes());
        Self(hasher.finalize().into())
    }

    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            let high = usize::from(byte >> 4);
            let low = usize::from(byte & 0x0f);
            output.push(char::from(b"0123456789abcdef"[high]));
            output.push(char::from(b"0123456789abcdef"[low]));
        }
        output
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3PromptCacheBlock {
    pub source: V3PromptCacheBlockSource,
    pub path: String,
    pub message_index: Option<u64>,
    pub value: Value,
    pub explicit_ttl: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V3PromptCacheBlockSource {
    Tools,
    System,
    Message,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3PromptCacheBreakpoint {
    pub block_index: u64,
    pub source: V3PromptCacheBlockSource,
    pub path: String,
    pub message_index: Option<u64>,
    pub ttl: Option<String>,
    pub prefix_key: String,
    pub prefix_token_count: u64,
    pub lookback_prefixes: Vec<V3PromptCacheLookbackPrefix>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3PromptCacheLookbackPrefix {
    pub prefix_key: String,
    pub content_block_index: u64,
    pub prefix_token_count: u64,
    pub lookback_distance: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3PromptCacheAnalysis {
    pub blocks: Vec<V3PromptCacheBlock>,
    pub breakpoints: Vec<V3PromptCacheBreakpoint>,
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
            .enumerate()
            .skip(start)
            .take(block_index.saturating_sub(start).saturating_add(1))
            .rev()
            .map(|(_, key)| *key)
            .collect()
    }

    fn prefix_key_hex(&self, block_index: usize) -> Option<String> {
        self.prefix_key(block_index)
            .map(PromptCacheSimulatorKey::to_hex)
    }
}

pub fn analyze_v3_prompt_cache(value: &Value, canonical_model: &str) -> V3PromptCacheAnalysis {
    let blocks = flatten_cacheable_blocks(value);
    let block_digests = blocks.iter().map(block_digest);
    let chain = PromptCachePrefixChain::from_block_digests(
        PromptCacheSimulatorKey::seed(canonical_model),
        block_digests,
    );
    let prefix_token_counts = prefix_token_counts(canonical_model, &blocks);
    let breakpoints = blocks
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            block.explicit_ttl.as_ref()?;
            let prefix_key = chain.prefix_key_hex(index)?;
            let prefix_token_count = prefix_token_counts.get(index).copied().unwrap_or(0);
            let lookback_prefixes = lookback_prefixes(&chain, &prefix_token_counts, index);
            Some(V3PromptCacheBreakpoint {
                block_index: index as u64,
                source: block.source,
                path: block.path.clone(),
                message_index: block.message_index,
                ttl: block.explicit_ttl.clone(),
                prefix_key,
                prefix_token_count,
                lookback_prefixes,
            })
        })
        .collect();
    V3PromptCacheAnalysis {
        blocks,
        breakpoints,
    }
}

fn flatten_cacheable_blocks(value: &Value) -> Vec<V3PromptCacheBlock> {
    let mut blocks = Vec::new();
    flatten_tools(value.get("tools"), &mut blocks);
    flatten_system(value.get("system"), &mut blocks);
    flatten_messages(value.get("messages"), &mut blocks);
    blocks
}

fn flatten_tools(value: Option<&Value>, blocks: &mut Vec<V3PromptCacheBlock>) {
    let Some(Value::Array(tools)) = value else {
        return;
    };
    for (index, tool) in tools.iter().enumerate() {
        if !tool.is_object() {
            continue;
        }
        blocks.push(V3PromptCacheBlock {
            source: V3PromptCacheBlockSource::Tools,
            path: format!("tools[{index}]"),
            message_index: None,
            value: tool.clone(),
            explicit_ttl: explicit_cache_ttl(tool),
        });
    }
}

fn flatten_system(value: Option<&Value>, blocks: &mut Vec<V3PromptCacheBlock>) {
    match value {
        Some(Value::String(text)) if !text.is_empty() => blocks.push(V3PromptCacheBlock {
            source: V3PromptCacheBlockSource::System,
            path: "system".to_owned(),
            message_index: None,
            value: json!({ "type": "text", "text": text }),
            explicit_ttl: None,
        }),
        Some(Value::Array(items)) => {
            for (index, item) in items.iter().enumerate() {
                push_content_block(
                    blocks,
                    V3PromptCacheBlockSource::System,
                    format!("system[{index}]"),
                    None,
                    item,
                );
            }
        }
        _ => {}
    }
}

fn flatten_messages(value: Option<&Value>, blocks: &mut Vec<V3PromptCacheBlock>) {
    let Some(Value::Array(messages)) = value else {
        return;
    };
    for (message_index, message) in messages.iter().enumerate() {
        let Some(content) = message.get("content") else {
            continue;
        };
        match content {
            Value::String(text) if !text.is_empty() => blocks.push(V3PromptCacheBlock {
                source: V3PromptCacheBlockSource::Message,
                path: format!("messages[{message_index}].content"),
                message_index: Some(message_index as u64),
                value: json!({ "type": "text", "text": text }),
                explicit_ttl: None,
            }),
            Value::Array(items) => {
                for (content_index, item) in items.iter().enumerate() {
                    push_content_block(
                        blocks,
                        V3PromptCacheBlockSource::Message,
                        format!("messages[{message_index}].content[{content_index}]"),
                        Some(message_index as u64),
                        item,
                    );
                }
            }
            _ => {}
        }
    }
}

fn push_content_block(
    blocks: &mut Vec<V3PromptCacheBlock>,
    source: V3PromptCacheBlockSource,
    path: String,
    message_index: Option<u64>,
    value: &Value,
) {
    if !is_cacheable_content_block(value) {
        return;
    }
    blocks.push(V3PromptCacheBlock {
        source,
        path,
        message_index,
        value: value.clone(),
        explicit_ttl: explicit_cache_ttl(value),
    });
}

fn is_cacheable_content_block(value: &Value) -> bool {
    match value {
        Value::Object(map) => match map.get("type").and_then(Value::as_str) {
            Some("text") => map
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty()),
            Some("image" | "document" | "tool_use" | "tool_result") => true,
            _ => false,
        },
        _ => false,
    }
}

fn explicit_cache_ttl(value: &Value) -> Option<String> {
    value
        .get("cache_control")
        .and_then(|cache_control| cache_control.get("ttl"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .get("cache_control")
                .is_some()
                .then(|| "5m".to_owned())
        })
}

fn block_digest(block: &V3PromptCacheBlock) -> [u8; 32] {
    let value = match &block.value {
        Value::Object(map) => {
            let mut value = map.clone();
            value.remove("cache_control");
            Value::Object(value)
        }
        value => value.clone(),
    };
    let hash_input = json!({
        "source": source_name(block.source),
        "path": &block.path,
        "value": value,
    });
    let bytes = serde_json::to_vec(&hash_input).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(b"cc-lb-cache-v3:block");
    hasher.update(bytes);
    hasher.finalize().into()
}

fn prefix_token_counts(canonical_model: &str, blocks: &[V3PromptCacheBlock]) -> Vec<u64> {
    let mut counts = Vec::with_capacity(blocks.len());
    let mut prefix = Vec::with_capacity(blocks.len());
    for block in blocks {
        prefix.push(json!({
            "source": source_name(block.source),
            "value": block.value,
        }));
        let serialized = json!({
            "model": canonical_model,
            "content_blocks": &prefix,
        });
        let bytes = serde_json::to_vec(&serialized).unwrap_or_default();
        let token_count = std::str::from_utf8(&bytes)
            .map(|text| PrefixTokenizer::global().count_tokens(text) as u64)
            .unwrap_or(0);
        counts.push(token_count);
    }
    counts
}

fn lookback_prefixes(
    chain: &PromptCachePrefixChain,
    prefix_token_counts: &[u64],
    block_index: usize,
) -> Vec<V3PromptCacheLookbackPrefix> {
    let start = block_index.saturating_sub(19);
    (start..=block_index)
        .rev()
        .filter_map(|index| {
            Some(V3PromptCacheLookbackPrefix {
                prefix_key: chain.prefix_key_hex(index)?,
                content_block_index: index as u64,
                prefix_token_count: prefix_token_counts.get(index).copied().unwrap_or(0),
                lookback_distance: block_index.saturating_sub(index) as u64,
            })
        })
        .collect()
}

fn source_name(source: V3PromptCacheBlockSource) -> &'static str {
    match source {
        V3PromptCacheBlockSource::Tools => "tools",
        V3PromptCacheBlockSource::System => "system",
        V3PromptCacheBlockSource::Message => "message",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::model_resolution::canonical_model_id;

    use super::{
        PromptCachePrefixChain, PromptCacheSimulatorKey, V3PromptCacheBlockSource,
        analyze_v3_prompt_cache,
    };

    #[test]
    fn lookback_includes_nineteen_prior_blocks() {
        let chain = PromptCachePrefixChain::from_block_digests(
            PromptCacheSimulatorKey::seed("claude-opus-4-8"),
            (0_u8..25).map(|value| [value; 32]),
        );

        let keys = chain.lookback_keys(24);

        assert_eq!(keys.len(), 20);
        assert_eq!(keys[0], chain.prefix_key(24).expect("n exists"));
        assert_eq!(keys[19], chain.prefix_key(5).expect("n-19 exists"));
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

    #[test]
    fn flattens_tools_system_messages_in_provider_order() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "tools": [{"name":"lookup","description":"lookup","input_schema":{"type":"object"},"cache_control":{"type":"ephemeral","ttl":"1h"}}],
            "system": [{"type":"text","text":"system","cache_control":{"type":"ephemeral"}}],
            "messages": [{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.blocks.len(), 3);
        assert_eq!(analysis.blocks[0].source, V3PromptCacheBlockSource::Tools);
        assert_eq!(analysis.blocks[1].source, V3PromptCacheBlockSource::System);
        assert_eq!(analysis.blocks[2].source, V3PromptCacheBlockSource::Message);
        assert_eq!(analysis.breakpoints.len(), 3);
        assert_eq!(analysis.breakpoints[0].ttl.as_deref(), Some("1h"));
        assert_eq!(analysis.breakpoints[1].ttl.as_deref(), Some("5m"));
    }

    #[test]
    fn ignores_message_object_cache_control_and_non_cacheable_blocks() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [
                {"role":"user","cache_control":{"type":"ephemeral"},"content":[
                    {"type":"text","text":""},
                    {"type":"thinking","thinking":"hidden","cache_control":{"type":"ephemeral"}},
                    {"type":"text","text":"cache me","cache_control":{"type":"ephemeral"}}
                ]}
            ]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.blocks.len(), 1);
        assert_eq!(analysis.breakpoints.len(), 1);
        assert_eq!(analysis.breakpoints[0].path, "messages[0].content[2]");
    }

    #[test]
    fn breakpoint_lookback_includes_n_through_nineteen_only() {
        let content = (0..21)
            .map(|index| {
                if index == 20 {
                    json!({"type":"text","text":format!("block-{index}"),"cache_control":{"type":"ephemeral"}})
                } else {
                    json!({"type":"text","text":format!("block-{index}")})
                }
            })
            .collect::<Vec<_>>();
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content": content}]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));
        let lookback = &analysis.breakpoints[0].lookback_prefixes;

        assert_eq!(lookback.len(), 20);
        assert_eq!(lookback[0].content_block_index, 20);
        assert_eq!(lookback[19].content_block_index, 1);
        assert!(
            !lookback
                .iter()
                .any(|prefix| prefix.content_block_index == 0)
        );
    }

    #[test]
    fn moved_cache_control_keeps_prior_content_prefix_matchable() {
        let previous = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content": [
                {"type":"text","text":"stable first block"},
                {"type":"text","text":"stable second block","cache_control":{"type":"ephemeral"}}
            ]}]
        });
        let current = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content": [
                {"type":"text","text":"stable first block"},
                {"type":"text","text":"stable second block"},
                {"type":"text","text":"new suffix block","cache_control":{"type":"ephemeral"}}
            ]}]
        });

        let previous = analyze_v3_prompt_cache(&previous, canonical_model_id("claude-sonnet-4-5"));
        let current = analyze_v3_prompt_cache(&current, canonical_model_id("claude-sonnet-4-5"));
        let previous_breakpoint = previous
            .breakpoints
            .first()
            .expect("previous request has cache breakpoint");
        let current_prior_prefix = current.breakpoints[0]
            .lookback_prefixes
            .iter()
            .find(|prefix| prefix.content_block_index == previous_breakpoint.block_index)
            .expect("current lookback includes previous breakpoint block");

        assert_eq!(previous_breakpoint.block_index, 1);
        assert_eq!(current_prior_prefix.content_block_index, 1);
        assert_eq!(
            previous_breakpoint.prefix_key,
            current_prior_prefix.prefix_key
        );
    }

    #[test]
    fn model_canonicalization_does_not_fabricate_four_six_dates() {
        assert_eq!(canonical_model_id("claude-sonnet-4-6"), "claude-sonnet-4-6");
        assert_eq!(canonical_model_id("claude-opus-4-6"), "claude-opus-4-6");
    }
}
