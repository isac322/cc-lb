use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::Value;

use crate::tokenizer::PrefixTokenizer;

pub const V3_TOKEN_ESTIMATE_SOURCE: &str = "local_tiktoken_v1";

/// Anthropic's hard cap on explicit `cache_control` breakpoints; more is provider-invalid, so
/// analysis is skipped (request forwarded as-is) before any per-breakpoint prefix tokenization.
pub const MAX_EXPLICIT_BREAKPOINTS: usize = 4;

const SERIALIZATION_SCRATCH_INITIAL_CAPACITY: usize = 4 * 1024;
const SERIALIZATION_SCRATCH_MAX_RETAINED_CAPACITY: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PromptCacheSimulatorKey([u8; 32]);

impl PromptCacheSimulatorKey {
    pub fn seed(canonical_model: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"cc-lb-cache-v4:seed");
        hasher.update(canonical_model.as_bytes());
        Self(*hasher.finalize().as_bytes())
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
pub struct V3StructuralLookback {
    pub prefix_key: String,
    pub content_block_index: u64,
    pub lookback_distance: u64,
}

pub type V3PromptCacheLookbackPrefix = V3StructuralLookback;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3StructuralBreakpoint {
    pub block_index: u64,
    pub source: V3PromptCacheBlockSource,
    pub path: String,
    pub message_index: Option<u64>,
    pub ttl: Option<String>,
    pub prefix_key: String,
    pub lookback_prefixes: Vec<V3StructuralLookback>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3PromptCacheAnalysis {
    pub blocks: Vec<V3PromptCacheBlock>,
    pub breakpoints: Vec<V3PromptCacheBreakpoint>,
}

#[derive(Clone, Copy)]
enum CacheBlockValueRef<'a> {
    Json(&'a Value),
    SyntheticText(&'a str),
}

struct CacheBlockRef<'a> {
    source: V3PromptCacheBlockSource,
    path: String,
    message_index: Option<u64>,
    value: CacheBlockValueRef<'a>,
    explicit_ttl: Option<&'a str>,
}

impl CacheBlockRef<'_> {
    fn into_owned(self) -> V3PromptCacheBlock {
        let value = match self.value {
            CacheBlockValueRef::Json(value) => value.clone(),
            CacheBlockValueRef::SyntheticText(text) => {
                let mut value = serde_json::Map::new();
                value.insert("text".to_owned(), Value::String(text.to_owned()));
                value.insert("type".to_owned(), Value::String("text".to_owned()));
                Value::Object(value)
            }
        };
        V3PromptCacheBlock {
            source: self.source,
            path: self.path,
            message_index: self.message_index,
            value,
            explicit_ttl: self.explicit_ttl.map(ToOwned::to_owned),
        }
    }
}

struct SerializationScratch {
    bytes: Vec<u8>,
}

impl Default for SerializationScratch {
    fn default() -> Self {
        Self {
            bytes: Vec::with_capacity(SERIALIZATION_SCRATCH_INITIAL_CAPACITY),
        }
    }
}

impl SerializationScratch {
    fn serialize<T: Serialize>(&mut self, value: &T) -> &[u8] {
        self.clear_for_reuse();
        if serde_json::to_writer(&mut self.bytes, value).is_err() {
            self.bytes.clear();
        }
        &self.bytes
    }

    fn clear_for_reuse(&mut self) {
        if self.bytes.capacity() > SERIALIZATION_SCRATCH_MAX_RETAINED_CAPACITY {
            self.bytes = Vec::with_capacity(SERIALIZATION_SCRATCH_INITIAL_CAPACITY);
        } else {
            self.bytes.clear();
        }
    }
}

struct DigestBlockSerializer<'a, 'value>(&'a CacheBlockRef<'value>);

impl Serialize for DigestBlockSerializer<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("path", &self.0.path)?;
        map.serialize_entry("source", source_name(self.0.source))?;
        map.serialize_entry("value", &DigestValueSerializer(&self.0.value))?;
        map.end()
    }
}

struct DigestValueSerializer<'a, 'value>(&'a CacheBlockValueRef<'value>);

impl Serialize for DigestValueSerializer<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            CacheBlockValueRef::Json(Value::Object(value)) => {
                let entry_count = value
                    .len()
                    .saturating_sub(usize::from(value.contains_key("cache_control")));
                let mut map = serializer.serialize_map(Some(entry_count))?;
                for (key, value) in value {
                    if key != "cache_control" {
                        map.serialize_entry(key, value)?;
                    }
                }
                map.end()
            }
            CacheBlockValueRef::Json(value) => value.serialize(serializer),
            CacheBlockValueRef::SyntheticText(text) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("text", text)?;
                map.serialize_entry("type", "text")?;
                map.end()
            }
        }
    }
}

struct PrefixSerializer<'a, 'value> {
    model: &'a str,
    blocks: &'a [CacheBlockRef<'value>],
}

impl Serialize for PrefixSerializer<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("content_blocks", &PrefixBlocksSerializer(self.blocks))?;
        map.serialize_entry("model", self.model)?;
        map.end()
    }
}

struct PrefixBlocksSerializer<'a, 'value>(&'a [CacheBlockRef<'value>]);

impl Serialize for PrefixBlocksSerializer<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for block in self.0 {
            sequence.serialize_element(&PrefixBlockSerializer(block))?;
        }
        sequence.end()
    }
}

struct PrefixBlockSerializer<'a, 'value>(&'a CacheBlockRef<'value>);

impl Serialize for PrefixBlockSerializer<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("source", source_name(self.0.source))?;
        map.serialize_entry("value", &PrefixValueSerializer(&self.0.value))?;
        map.end()
    }
}

struct PrefixValueSerializer<'a, 'value>(&'a CacheBlockValueRef<'value>);

impl Serialize for PrefixValueSerializer<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            CacheBlockValueRef::Json(value) => value.serialize(serializer),
            CacheBlockValueRef::SyntheticText(text) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("text", text)?;
                map.serialize_entry("type", "text")?;
                map.end()
            }
        }
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
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"cc-lb-cache-v4:prefix");
            hasher.update(&previous.0);
            hasher.update(&digest);
            previous = PromptCacheSimulatorKey(*hasher.finalize().as_bytes());
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
    if exceeds_explicit_breakpoint_cap(value) {
        return V3PromptCacheAnalysis {
            blocks: Vec::new(),
            breakpoints: Vec::new(),
        };
    }
    let blocks = flatten_cacheable_blocks(value);
    let mut serialization_scratch = SerializationScratch::default();
    let block_digests = blocks
        .iter()
        .map(|block| block_digest(block, &mut serialization_scratch));
    let chain = PromptCachePrefixChain::from_block_digests(
        PromptCacheSimulatorKey::seed(canonical_model),
        block_digests,
    );
    let structural_breakpoints = blocks
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            block.explicit_ttl?;
            let prefix_key = chain.prefix_key_hex(index)?;
            Some(V3StructuralBreakpoint {
                block_index: index as u64,
                source: block.source,
                path: block.path.clone(),
                message_index: block.message_index,
                ttl: block.explicit_ttl.map(ToOwned::to_owned),
                prefix_key,
                lookback_prefixes: lookback_prefixes(&chain, index),
            })
        })
        .collect::<Vec<_>>();
    if structural_breakpoints.len() > MAX_EXPLICIT_BREAKPOINTS {
        return V3PromptCacheAnalysis {
            blocks: blocks.into_iter().map(CacheBlockRef::into_owned).collect(),
            breakpoints: Vec::new(),
        };
    }
    let breakpoints = structural_breakpoints
        .into_iter()
        .map(|breakpoint| V3PromptCacheBreakpoint {
            block_index: breakpoint.block_index,
            source: breakpoint.source,
            path: breakpoint.path,
            message_index: breakpoint.message_index,
            ttl: breakpoint.ttl,
            prefix_key: breakpoint.prefix_key,
            prefix_token_count: breakpoint_prefix_token_count(
                canonical_model,
                &blocks,
                breakpoint.block_index as usize,
                &mut serialization_scratch,
            ),
            lookback_prefixes: breakpoint.lookback_prefixes,
        })
        .collect();
    V3PromptCacheAnalysis {
        blocks: blocks.into_iter().map(CacheBlockRef::into_owned).collect(),
        breakpoints,
    }
}

fn exceeds_explicit_breakpoint_cap(value: &Value) -> bool {
    let mut count = 0;
    if let Some(Value::Array(tools)) = value.get("tools") {
        for tool in tools.iter().filter(|tool| tool.is_object()) {
            if record_explicit_breakpoint(&mut count, tool) {
                return true;
            }
        }
    }
    if let Some(Value::Array(system)) = value.get("system") {
        for block in system
            .iter()
            .filter(|block| is_cacheable_content_block(block))
        {
            if record_explicit_breakpoint(&mut count, block) {
                return true;
            }
        }
    }
    let Some(Value::Array(messages)) = value.get("messages") else {
        return false;
    };
    for content in messages
        .iter()
        .filter_map(|message| message.get("content").and_then(Value::as_array))
    {
        for block in content
            .iter()
            .filter(|block| is_cacheable_content_block(block))
        {
            if record_explicit_breakpoint(&mut count, block) {
                return true;
            }
        }
    }
    false
}

fn record_explicit_breakpoint(count: &mut usize, value: &Value) -> bool {
    if value.get("cache_control").is_some() {
        *count = count.saturating_add(1);
    }
    *count > MAX_EXPLICIT_BREAKPOINTS
}

fn flatten_cacheable_blocks(value: &Value) -> Vec<CacheBlockRef<'_>> {
    let mut blocks = Vec::new();
    flatten_tools(value.get("tools"), &mut blocks);
    flatten_system(value.get("system"), &mut blocks);
    flatten_messages(value.get("messages"), &mut blocks);
    blocks
}

fn flatten_tools<'a>(value: Option<&'a Value>, blocks: &mut Vec<CacheBlockRef<'a>>) {
    let Some(Value::Array(tools)) = value else {
        return;
    };
    for (index, tool) in tools.iter().enumerate() {
        if !tool.is_object() {
            continue;
        }
        blocks.push(CacheBlockRef {
            source: V3PromptCacheBlockSource::Tools,
            path: format!("tools[{index}]"),
            message_index: None,
            value: CacheBlockValueRef::Json(tool),
            explicit_ttl: explicit_cache_ttl(tool),
        });
    }
}

fn flatten_system<'a>(value: Option<&'a Value>, blocks: &mut Vec<CacheBlockRef<'a>>) {
    match value {
        Some(Value::String(text)) if !text.is_empty() => blocks.push(CacheBlockRef {
            source: V3PromptCacheBlockSource::System,
            path: "system".to_owned(),
            message_index: None,
            value: CacheBlockValueRef::SyntheticText(text),
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

fn flatten_messages<'a>(value: Option<&'a Value>, blocks: &mut Vec<CacheBlockRef<'a>>) {
    let Some(Value::Array(messages)) = value else {
        return;
    };
    for (message_index, message) in messages.iter().enumerate() {
        let Some(content) = message.get("content") else {
            continue;
        };
        match content {
            Value::String(text) if !text.is_empty() => blocks.push(CacheBlockRef {
                source: V3PromptCacheBlockSource::Message,
                path: format!("messages[{message_index}].content"),
                message_index: Some(message_index as u64),
                value: CacheBlockValueRef::SyntheticText(text),
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

fn push_content_block<'a>(
    blocks: &mut Vec<CacheBlockRef<'a>>,
    source: V3PromptCacheBlockSource,
    path: String,
    message_index: Option<u64>,
    value: &'a Value,
) {
    if !is_cacheable_content_block(value) {
        return;
    }
    blocks.push(CacheBlockRef {
        source,
        path,
        message_index,
        value: CacheBlockValueRef::Json(value),
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

fn explicit_cache_ttl(value: &Value) -> Option<&str> {
    value
        .get("cache_control")
        .and_then(|cache_control| cache_control.get("ttl"))
        .and_then(Value::as_str)
        .or_else(|| value.get("cache_control").is_some().then_some("5m"))
}

fn block_digest(block: &CacheBlockRef<'_>, scratch: &mut SerializationScratch) -> [u8; 32] {
    let bytes = scratch.serialize(&DigestBlockSerializer(block));
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cc-lb-cache-v4:block");
    hasher.update(&bytes);
    let digest = *hasher.finalize().as_bytes();
    scratch.clear_for_reuse();
    digest
}

fn breakpoint_prefix_token_count(
    canonical_model: &str,
    blocks: &[CacheBlockRef<'_>],
    breakpoint_index: usize,
    scratch: &mut SerializationScratch,
) -> u64 {
    let bytes = serialized_prefix(canonical_model, &blocks[..=breakpoint_index], scratch);
    let token_count = std::str::from_utf8(bytes)
        .map(|text| PrefixTokenizer::global().count_tokens(text) as u64)
        .unwrap_or(0);
    scratch.clear_for_reuse();
    token_count
}

fn serialized_prefix<'a>(
    canonical_model: &str,
    blocks: &[CacheBlockRef<'_>],
    scratch: &'a mut SerializationScratch,
) -> &'a [u8] {
    scratch.serialize(&PrefixSerializer {
        model: canonical_model,
        blocks,
    })
}

fn lookback_prefixes(
    chain: &PromptCachePrefixChain,
    block_index: usize,
) -> Vec<V3PromptCacheLookbackPrefix> {
    let start = block_index.saturating_sub(19);
    (start..=block_index)
        .rev()
        .filter_map(|index| {
            Some(V3StructuralLookback {
                prefix_key: chain.prefix_key_hex(index)?,
                content_block_index: index as u64,
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

    use crate::{
        model_resolution::canonical_model_id,
        tokenizer::{reset_tokenizer_call_count, tokenizer_call_count},
    };

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
    fn structural_pass_zero_tokenizer_calls() {
        let content = (0..32)
            .map(|index| json!({"type":"text","text":format!("block-{index}")}))
            .collect::<Vec<_>>();
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content": content}]
        });

        reset_tokenizer_call_count();
        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert!(analysis.breakpoints.is_empty());
        assert_eq!(tokenizer_call_count(), 0);
    }

    #[test]
    fn explicit_breakpoint_tokenize_bounded() {
        let content = (0..8)
            .map(|index| {
                if index == 2 || index == 7 {
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

        reset_tokenizer_call_count();
        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.breakpoints.len(), 2);
        assert_eq!(tokenizer_call_count(), 2);
    }

    #[test]
    fn excessive_breakpoints_skip_analysis_without_tokenizing() {
        let content = (0..5000)
            .map(|index| {
                json!({"type":"text","text":format!("block-{index}"),"cache_control":{"type":"ephemeral"}})
            })
            .collect::<Vec<_>>();
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content": content}]
        });

        reset_tokenizer_call_count();
        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert!(analysis.blocks.is_empty());
        assert!(analysis.breakpoints.is_empty());
        assert_eq!(tokenizer_call_count(), 0);
    }

    #[test]
    fn breakpoints_at_cap_are_analyzed() {
        let content = (0..8)
            .map(|index| {
                if index < 4 {
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

        reset_tokenizer_call_count();
        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.breakpoints.len(), 4);
        assert_eq!(tokenizer_call_count(), 4);
    }

    #[test]
    fn tokenizer_calls_are_bounded_across_block_counts() {
        let one_breakpoint = |block_count: usize| {
            let content = (0..block_count)
                .map(|index| {
                    if index == block_count - 1 {
                        json!({"type":"text","text":format!("block-{index}"),"cache_control":{"type":"ephemeral"}})
                    } else {
                        json!({"type":"text","text":format!("block-{index}")})
                    }
                })
                .collect::<Vec<_>>();
            json!({
                "model": "claude-sonnet-4-5",
                "messages": [{"role":"user","content": content}]
            })
        };

        for block_count in [1, 20, 128, 259] {
            reset_tokenizer_call_count();
            let _ = analyze_v3_prompt_cache(
                &one_breakpoint(block_count),
                canonical_model_id("claude-sonnet-4-5"),
            );
            assert_eq!(tokenizer_call_count(), 1, "block_count={block_count}");
        }
    }

    #[test]
    fn structural_windows_n_n19_hit_n20_miss() {
        let content = (0..25)
            .map(|index| {
                if index == 24 {
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
        let indices = analysis.breakpoints[0]
            .lookback_prefixes
            .iter()
            .map(|prefix| prefix.content_block_index)
            .collect::<Vec<_>>();

        assert_eq!(indices, (5_u64..=24).rev().collect::<Vec<_>>());
        assert!(!indices.contains(&4));
    }

    #[test]
    fn duplicate_prefix_retains_all_memberships() {
        let content = (0..8)
            .map(|index| {
                if index == 4 || index == 7 {
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

        assert_eq!(analysis.breakpoints.len(), 2);
        assert!(analysis.breakpoints.iter().all(|breakpoint| {
            breakpoint
                .lookback_prefixes
                .iter()
                .any(|prefix| prefix.content_block_index == 3)
        }));
    }

    #[test]
    fn cache_control_excluded_from_block_digest() {
        let five_minute = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content": [{
                "type":"text","text":"stable","cache_control":{"type":"ephemeral"}
            }]}]
        });
        let one_hour = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content": [{
                "type":"text","text":"stable","cache_control":{"type":"ephemeral","ttl":"1h"}
            }]}]
        });

        let five_minute =
            analyze_v3_prompt_cache(&five_minute, canonical_model_id("claude-sonnet-4-5"));
        let one_hour = analyze_v3_prompt_cache(&one_hour, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(
            five_minute.breakpoints[0].prefix_key,
            one_hour.breakpoints[0].prefix_key
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
