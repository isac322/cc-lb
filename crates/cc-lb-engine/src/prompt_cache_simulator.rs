//! Simulates Anthropic prompt-cache prefix identity and breakpoint lookback.
//!
//! Requests enable caching through explicit block-level `cache_control` markers or one automatic
//! top-level `cache_control` marker. Both forms share the four-slot breakpoint cap. Prefixes follow
//! provider order (`tools` → `system` → `messages`), and each breakpoint searches at most twenty
//! participating content-block positions, including itself.
//!
//! Request-level invalidators are mixed into the prefix chain at provider boundaries: `speed`
//! enters at `system` (or the first message when no system block exists), while thinking, effort,
//! and tool choice enter at `messages`. Earlier prefix tiers remain stable.

use std::time::{Duration, Instant};

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::Value;

use crate::tokenizer::{PrefixTokenizer, cache_threshold_from_byte_len};

mod optimized;

pub(crate) use optimized::{
    PromptCacheAnalysisExecutor, PromptCacheAnalysisOutput, PromptCacheAnalysisTimings,
};
pub const V3_TOKEN_ESTIMATE_SOURCE: &str = "serialized_prefix_bytes_v1";

/// Anthropic's shared cap for resolved explicit and automatic breakpoints. Provider-invalid
/// requests skip analysis before tokenization.
pub const MAX_EXPLICIT_BREAKPOINTS: usize = 4;

const SERIALIZATION_SCRATCH_INITIAL_CAPACITY: usize = 4 * 1024;
const DEFAULT_CACHE_TTL: &str = "5m";
const DEFAULT_SPEED: &str = "standard";
const INVALIDATOR_SALT_COUNT: usize = 4;
const SALT_NAME_SEPARATOR: &[u8] = b"\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SaltScope {
    System,
    Messages,
}

impl SaltScope {
    fn domain_tag(self) -> &'static [u8] {
        match self {
            Self::System => b"cc-lb-cache-v5:salt-system",
            Self::Messages => b"cc-lb-cache-v5:salt-message",
        }
    }
}

// Thinking and effort stay message-scoped despite Anthropic's model-specific earlier-tier
// invalidation. A config change invalidates every upstream uniformly, so optimistic retention
// cannot change the routing choice; pessimistic invalidation fragments affinity and may force a
// paid tools+system recreation on another upstream.
const THINKING_SALT_SCOPE: SaltScope = SaltScope::Messages;
const EFFORT_SALT_SCOPE: SaltScope = SaltScope::Messages;
const SPEED_SALT_SCOPE: SaltScope = SaltScope::System;
const TOOL_CHOICE_SALT_SCOPE: SaltScope = SaltScope::Messages;

// Keep this as a denylist: unknown future models keep non-default speed salted. A stale denylist
// costs one cache creation; a stale allowlist can falsely predict a standard-speed cache hit.
// Opus 4.6 serves fast requests at standard speed, and Opus 4.7 rejects the field.
const MODELS_NOT_HONORING_FAST_SPEED: [&str; 2] = ["claude-opus-4-6", "claude-opus-4-7"];

#[derive(Clone, Copy)]
struct InvalidatorSalt<'a> {
    name: &'static str,
    scope: SaltScope,
    value: Option<&'a Value>,
}

#[derive(Clone, Copy)]
struct ResolvedBreakpoint<'a> {
    block_index: usize,
    ttl: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PromptCacheSimulatorKey([u8; 32]);

impl PromptCacheSimulatorKey {
    pub fn seed(canonical_model: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"cc-lb-cache-v5:seed");
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
struct V3StructuralAnalysis<'a> {
    blocks: Vec<CacheBlockRef<'a>>,
    breakpoints: Vec<V3StructuralBreakpoint>,
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

    fn is_breakpoint_eligible(&self) -> bool {
        match (self.source, self.value) {
            (V3PromptCacheBlockSource::Tools, _) => true,
            (_, CacheBlockValueRef::SyntheticText(text)) => !text.is_empty(),
            (_, CacheBlockValueRef::Json(value)) => is_breakpoint_eligible(value),
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
        self.bytes.clear();
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

struct OwnedPrefixSerializer<'a> {
    model: &'a str,
    blocks: &'a [V3PromptCacheBlock],
}

impl Serialize for OwnedPrefixSerializer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("content_blocks", &OwnedPrefixBlocksSerializer(self.blocks))?;
        map.serialize_entry("model", self.model)?;
        map.end()
    }
}

struct OwnedPrefixBlocksSerializer<'a>(&'a [V3PromptCacheBlock]);

impl Serialize for OwnedPrefixBlocksSerializer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for block in self.0 {
            sequence.serialize_element(&OwnedPrefixBlockSerializer(block))?;
        }
        sequence.end()
    }
}

struct OwnedPrefixBlockSerializer<'a>(&'a V3PromptCacheBlock);

impl Serialize for OwnedPrefixBlockSerializer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("source", source_name(self.0.source))?;
        map.serialize_entry("value", &self.0.value)?;
        map.end()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptCachePrefixChain {
    keys: Vec<PromptCacheSimulatorKey>,
}

impl PromptCachePrefixChain {
    fn from_request_blocks(
        seed: PromptCacheSimulatorKey,
        blocks: &[CacheBlockRef<'_>],
        salts: &[InvalidatorSalt<'_>; INVALIDATOR_SALT_COUNT],
        scratch: &mut SerializationScratch,
    ) -> Self {
        let mut previous = seed;
        let mut keys = Vec::with_capacity(blocks.len());
        let mut system_salts_applied = false;
        let mut message_salts_applied = false;

        for block in blocks {
            match block.source {
                V3PromptCacheBlockSource::Tools => {}
                V3PromptCacheBlockSource::System => {
                    if !system_salts_applied {
                        previous = mix_scope_salts(previous, SaltScope::System, salts, scratch);
                        system_salts_applied = true;
                    }
                }
                V3PromptCacheBlockSource::Message => {
                    if !system_salts_applied {
                        previous = mix_scope_salts(previous, SaltScope::System, salts, scratch);
                        system_salts_applied = true;
                    }
                    if !message_salts_applied {
                        previous = mix_scope_salts(previous, SaltScope::Messages, salts, scratch);
                        message_salts_applied = true;
                    }
                }
            }

            previous = extend_prefix(previous, block_digest(block, scratch));
            keys.push(previous);
        }

        Self { keys }
    }

    pub fn prefix_key(&self, block_index: usize) -> Option<PromptCacheSimulatorKey> {
        self.keys.get(block_index).copied()
    }

    fn prefix_key_hex(&self, block_index: usize) -> Option<String> {
        self.prefix_key(block_index)
            .map(PromptCacheSimulatorKey::to_hex)
    }
}

fn extend_prefix(
    previous: PromptCacheSimulatorKey,
    block_digest: [u8; 32],
) -> PromptCacheSimulatorKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cc-lb-cache-v5:prefix");
    hasher.update(&previous.0);
    hasher.update(&block_digest);
    PromptCacheSimulatorKey(*hasher.finalize().as_bytes())
}

fn mix_scope_salts(
    mut previous: PromptCacheSimulatorKey,
    scope: SaltScope,
    salts: &[InvalidatorSalt<'_>; INVALIDATOR_SALT_COUNT],
    scratch: &mut SerializationScratch,
) -> PromptCacheSimulatorKey {
    for salt in salts {
        if salt.scope != scope {
            continue;
        }
        let Some(value) = salt.value else {
            continue;
        };
        let bytes = scratch.serialize(value);
        let mut hasher = blake3::Hasher::new();
        hasher.update(scope.domain_tag());
        hasher.update(&previous.0);
        hasher.update(salt.name.as_bytes());
        hasher.update(SALT_NAME_SEPARATOR);
        hasher.update(bytes);
        previous = PromptCacheSimulatorKey(*hasher.finalize().as_bytes());
        scratch.clear_for_reuse();
    }
    previous
}

fn request_invalidator_salts<'a>(
    value: &'a Value,
    canonical_model: &str,
) -> [InvalidatorSalt<'a>; INVALIDATOR_SALT_COUNT] {
    let effort = value
        .get("output_config")
        .and_then(|output_config| output_config.get("effort"));
    [
        InvalidatorSalt {
            name: "thinking",
            scope: THINKING_SALT_SCOPE,
            value: value.get("thinking"),
        },
        InvalidatorSalt {
            // Salted on presence, like `thinking` and for the same reason. The provider's
            // invalidation table says an explicit *model* default is equivalent to omission, but
            // the default is per-model; hardcoding one value here would collapse omitted and
            // explicit onto one key for any model whose default differs, predicting a hit the
            // provider misses. Splitting them instead costs at most one extra cache creation.
            name: "output_config.effort",
            scope: EFFORT_SALT_SCOPE,
            value: effort,
        },
        InvalidatorSalt {
            name: "speed",
            scope: SPEED_SALT_SCOPE,
            value: effective_speed_invalidator(value.get("speed"), canonical_model),
        },
        InvalidatorSalt {
            name: "tool_choice",
            scope: TOOL_CHOICE_SALT_SCOPE,
            value: value.get("tool_choice"),
        },
    ]
}

// `thinking` is salted on presence, with no default-normalization.
//
// `{"type":"disabled"}` renders the same prompt as omitting `thinking` only where thinking is off
// by default; on Opus 5, Sonnet 5, Fable 5 and Mythos 5 it is on unless disabled, so there the two
// are different prompts in different provider cache entries. Collapsing them would predict a hit
// the provider misses and pay a 1.25x creation instead of a 0.1x read.
//
// Rather than carry a third model table (after the threshold map and the fast-speed denylist) to
// tell those cases apart, salt whenever the field is present. The only cost is that a client
// sending an explicit `disabled` on a thinking-off model splits its prefix population from a
// client that omits the field — one extra cache creation. A false hit is structurally impossible.

fn effective_speed_invalidator<'a>(
    value: Option<&'a Value>,
    canonical_model: &str,
) -> Option<&'a Value> {
    if MODELS_NOT_HONORING_FAST_SPEED.contains(&canonical_model) {
        None
    } else {
        non_default_string_invalidator(value, DEFAULT_SPEED)
    }
}

fn non_default_string_invalidator<'a>(
    value: Option<&'a Value>,
    model_default: &str,
) -> Option<&'a Value> {
    value.filter(|value| value.as_str() != Some(model_default))
}

/// Analyzes block-level explicit and top-level automatic Anthropic prompt caching without
/// changing the request.
///
/// Both modes share four breakpoint slots. The flattened chain preserves every participating
/// content-block position for the provider's twenty-position lookback. System- and message-scoped
/// salts enter only at their first matching source boundary, so earlier tiers remain byte-stable.
/// Provider-invalid resolved breakpoint counts return an un-analyzable result with no
/// breakpoints. Long-after-short TTL ordering is deliberately NOT
/// rejected here: the provider documents it as a billing-position constraint, not a request
/// error, so discarding the analysis would blind cache-affinity routing on a request the
/// upstream still serves. `lifecycle::build_cache_score_from_match` applies the ordering rule
/// where it actually matters, when pricing a matched prefix.
pub fn analyze_v3_prompt_cache(value: &Value, canonical_model: &str) -> V3PromptCacheAnalysis {
    analyze_v3_prompt_cache_with_tokenize_duration(value, canonical_model).0
}

pub(super) fn analyze_v3_prompt_cache_with_tokenize_duration(
    value: &Value,
    canonical_model: &str,
) -> (V3PromptCacheAnalysis, Duration) {
    let mut serialization_scratch = SerializationScratch::default();
    let Some(structural) =
        analyze_v3_prompt_cache_structure(value, canonical_model, &mut serialization_scratch)
    else {
        return (unanalyzable_prompt_cache(), Duration::ZERO);
    };
    let mut tokenization_duration = Duration::ZERO;
    let prefix_token_counts = structural
        .breakpoints
        .iter()
        .map(|breakpoint| {
            let (count, duration) = breakpoint_prefix_token_count(
                canonical_model,
                &structural.blocks,
                breakpoint.block_index as usize,
                &mut serialization_scratch,
            );
            tokenization_duration = tokenization_duration.saturating_add(duration);
            count
        })
        .collect();
    (
        finish_structural_analysis(structural, prefix_token_counts)
            .unwrap_or_else(unanalyzable_prompt_cache),
        tokenization_duration,
    )
}

fn analyze_v3_prompt_cache_structure<'a>(
    value: &'a Value,
    canonical_model: &str,
    serialization_scratch: &mut SerializationScratch,
) -> Option<V3StructuralAnalysis<'a>> {
    let blocks = flatten_prefix_blocks(value);
    let resolved_breakpoints = resolve_breakpoints(value, &blocks)?;
    let salts = request_invalidator_salts(value, canonical_model);
    let chain = PromptCachePrefixChain::from_request_blocks(
        PromptCacheSimulatorKey::seed(canonical_model),
        &blocks,
        &salts,
        serialization_scratch,
    );
    let breakpoints = resolved_breakpoints
        .into_iter()
        .filter_map(|resolved| {
            let block = blocks.get(resolved.block_index)?;
            let prefix_key = chain.prefix_key_hex(resolved.block_index)?;
            Some(V3StructuralBreakpoint {
                block_index: resolved.block_index as u64,
                source: block.source,
                path: block.path.clone(),
                message_index: block.message_index,
                ttl: Some(resolved.ttl.to_owned()),
                prefix_key,
                lookback_prefixes: lookback_prefixes(&chain, resolved.block_index),
            })
        })
        .collect();
    Some(V3StructuralAnalysis {
        blocks,
        breakpoints,
    })
}

fn finish_structural_analysis(
    structural: V3StructuralAnalysis<'_>,
    prefix_token_counts: Vec<u64>,
) -> Option<V3PromptCacheAnalysis> {
    if structural.breakpoints.len() != prefix_token_counts.len() {
        return None;
    }
    let breakpoints = structural
        .breakpoints
        .into_iter()
        .zip(prefix_token_counts)
        .map(|(breakpoint, prefix_token_count)| V3PromptCacheBreakpoint {
            block_index: breakpoint.block_index,
            source: breakpoint.source,
            path: breakpoint.path,
            message_index: breakpoint.message_index,
            ttl: breakpoint.ttl,
            prefix_key: breakpoint.prefix_key,
            prefix_token_count,
            lookback_prefixes: breakpoint.lookback_prefixes,
        })
        .collect();
    Some(V3PromptCacheAnalysis {
        blocks: structural
            .blocks
            .into_iter()
            .map(CacheBlockRef::into_owned)
            .collect(),
        breakpoints,
    })
}

fn unanalyzable_prompt_cache() -> V3PromptCacheAnalysis {
    V3PromptCacheAnalysis {
        blocks: Vec::new(),
        breakpoints: Vec::new(),
    }
}

fn resolve_breakpoints<'a>(
    value: &'a Value,
    blocks: &[CacheBlockRef<'a>],
) -> Option<Vec<ResolvedBreakpoint<'a>>> {
    let mut breakpoints = Vec::with_capacity(MAX_EXPLICIT_BREAKPOINTS);
    for (block_index, block) in blocks.iter().enumerate() {
        if let Some(ttl) = block.explicit_ttl {
            breakpoints.push(ResolvedBreakpoint { block_index, ttl });
        }
    }

    if let Some(automatic_ttl) = cache_control_ttl(value) {
        let automatic_block = blocks
            .iter()
            .enumerate()
            .rev()
            .find(|(_, block)| block.is_breakpoint_eligible());
        if let Some((block_index, block)) = automatic_block
            && block.explicit_ttl.is_none()
        {
            breakpoints.push(ResolvedBreakpoint {
                block_index,
                ttl: automatic_ttl,
            });
        }
    }

    (breakpoints.len() <= MAX_EXPLICIT_BREAKPOINTS).then_some(breakpoints)
}

fn flatten_prefix_blocks(value: &Value) -> Vec<CacheBlockRef<'_>> {
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
            explicit_ttl: cache_control_ttl(tool),
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
    if !participates_in_prefix(value) {
        return;
    }
    blocks.push(CacheBlockRef {
        source,
        path,
        message_index,
        value: CacheBlockValueRef::Json(value),
        explicit_ttl: is_breakpoint_eligible(value)
            .then(|| cache_control_ttl(value))
            .flatten(),
    });
}

fn participates_in_prefix(value: &Value) -> bool {
    let Value::Object(map) = value else {
        return false;
    };
    match map.get("type").and_then(Value::as_str) {
        Some("text") => map
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty()),
        Some(_) => true,
        None => false,
    }
}

fn is_breakpoint_eligible(value: &Value) -> bool {
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

fn cache_control_ttl(value: &Value) -> Option<&str> {
    value
        .get("cache_control")
        .and_then(|cache_control| cache_control.get("ttl"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("cache_control")
                .is_some()
                .then_some(DEFAULT_CACHE_TTL)
        })
}

fn block_digest(block: &CacheBlockRef<'_>, scratch: &mut SerializationScratch) -> [u8; 32] {
    let bytes = scratch.serialize(&DigestBlockSerializer(block));
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cc-lb-cache-v5:block");
    hasher.update(bytes);
    let digest = *hasher.finalize().as_bytes();
    scratch.clear_for_reuse();
    digest
}

fn breakpoint_prefix_token_count(
    canonical_model: &str,
    blocks: &[CacheBlockRef<'_>],
    breakpoint_index: usize,
    scratch: &mut SerializationScratch,
) -> (u64, Duration) {
    let bytes = serialized_prefix(canonical_model, &blocks[..=breakpoint_index], scratch);
    let count = bytes.len() as u64;
    scratch.clear_for_reuse();
    (count, Duration::ZERO)
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

#[cfg(test)]
fn cacheable_breakpoint_prefix_keys(
    analysis: &V3PromptCacheAnalysis,
    canonical_model: &str,
    token_threshold: usize,
) -> Vec<String> {
    cacheable_breakpoint_prefix_keys_with_tokenize_duration(
        analysis,
        canonical_model,
        token_threshold,
        true,
    )
    .0
}

pub(super) fn cacheable_breakpoint_prefix_keys_with_tokenize_duration(
    analysis: &V3PromptCacheAnalysis,
    canonical_model: &str,
    token_threshold: usize,
    tokenize_ambiguous: bool,
) -> (Vec<String>, Duration) {
    let mut scratch = SerializationScratch::default();
    let mut tokenization_duration = Duration::ZERO;
    let keys = analysis
        .breakpoints
        .iter()
        .filter_map(|breakpoint| {
            let byte_len = usize::try_from(breakpoint.prefix_token_count).unwrap_or(usize::MAX);
            let accepted = match cache_threshold_from_byte_len(byte_len, token_threshold) {
                Some(decision) => decision,
                None => {
                    if !tokenize_ambiguous {
                        return None;
                    }
                    let block_index = usize::try_from(breakpoint.block_index).ok()?;
                    let blocks = analysis.blocks.get(..=block_index)?;
                    let bytes = scratch.serialize(&OwnedPrefixSerializer {
                        model: canonical_model,
                        blocks,
                    });
                    debug_assert_eq!(bytes.len(), byte_len);
                    let started = Instant::now();
                    let accepted =
                        PrefixTokenizer::global().meets_cache_threshold(bytes, token_threshold);
                    tokenization_duration = tokenization_duration.saturating_add(started.elapsed());
                    scratch.clear_for_reuse();
                    accepted
                }
            };
            accepted.then(|| breakpoint.prefix_key.clone())
        })
        .collect();
    (keys, tokenization_duration)
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
        V3PromptCacheBlockSource, analyze_v3_prompt_cache, cacheable_breakpoint_prefix_keys,
    };

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
    fn keeps_prefix_only_blocks_without_recording_breakpoints() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [
                {"role":"user","cache_control":{"type":"ephemeral"},"content":[
                    {"type":"text","text":""},
                    {"type":"thinking","thinking":"hidden","cache_control":{"type":"ephemeral"}},
                    {"type":"future_block","payload":"opaque","cache_control":{"type":"ephemeral"}},
                    {"type":"text","text":"cache me","cache_control":{"type":"ephemeral"}}
                ]}
            ]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.blocks.len(), 3);
        assert_eq!(analysis.blocks[0].path, "messages[0].content[1]");
        assert_eq!(analysis.blocks[1].path, "messages[0].content[2]");
        assert_eq!(analysis.blocks[2].path, "messages[0].content[3]");
        assert!(
            analysis.blocks[..2]
                .iter()
                .all(|block| block.explicit_ttl.is_none())
        );
        assert_eq!(analysis.breakpoints.len(), 1);
        assert_eq!(analysis.breakpoints[0].block_index, 2);
        assert_eq!(analysis.breakpoints[0].path, "messages[0].content[3]");
    }

    #[test]
    fn malformed_text_blocks_do_not_participate_in_prefix() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content":[
                {"type":"text","cache_control":{"type":"ephemeral"}},
                {"type":"text","text":123,"cache_control":{"type":"ephemeral"}},
                {"type":"future_block","payload":"opaque"},
                {"type":"text","text":"cache me","cache_control":{"type":"ephemeral"}}
            ]}]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.blocks.len(), 2);
        assert_eq!(analysis.blocks[0].path, "messages[0].content[2]");
        assert_eq!(analysis.blocks[1].path, "messages[0].content[3]");
        assert_eq!(analysis.breakpoints.len(), 1);
        assert_eq!(analysis.breakpoints[0].block_index, 1);
    }

    #[test]
    fn automatic_cache_control_uses_last_eligible_block() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "cache_control": {"type":"ephemeral","ttl":"1h"},
            "messages": [{"role":"assistant","content":[
                {"type":"text","text":"eligible"},
                {"type":"thinking","thinking":"trailing prefix-only block"}
            ]}]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.blocks.len(), 2);
        assert_eq!(analysis.breakpoints.len(), 1);
        assert_eq!(analysis.breakpoints[0].block_index, 0);
        assert_eq!(analysis.breakpoints[0].path, "messages[0].content[0]");
        assert_eq!(analysis.breakpoints[0].ttl.as_deref(), Some("1h"));
    }

    #[test]
    fn automatic_cache_control_without_eligible_block_skips_breakpoint() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "cache_control": {"type":"ephemeral"},
            "messages": [{"role":"assistant","content":[
                {"type":"thinking","thinking":"prefix-only"},
                {"type":"future_block","payload":"opaque"}
            ]}]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.blocks.len(), 2);
        assert!(analysis.breakpoints.is_empty());
    }

    #[test]
    fn automatic_cache_control_only_consumes_slot_when_it_synthesizes_breakpoint() {
        let marked_content = (0..4)
            .map(|index| {
                json!({
                    "type":"text",
                    "text":format!("block-{index}"),
                    "cache_control":{"type":"ephemeral"}
                })
            })
            .collect::<Vec<_>>();
        let deduplicated_request = json!({
            "model": "claude-sonnet-4-5",
            "cache_control": {"type":"ephemeral"},
            "messages": [{"role":"user","content":marked_content.clone()}]
        });

        reset_tokenizer_call_count();
        let deduplicated = analyze_v3_prompt_cache(
            &deduplicated_request,
            canonical_model_id("claude-sonnet-4-5"),
        );

        assert_eq!(deduplicated.blocks.len(), 4);
        assert_eq!(deduplicated.breakpoints.len(), 4);
        assert!(
            deduplicated
                .breakpoints
                .iter()
                .all(|breakpoint| breakpoint.ttl.as_deref() == Some("5m"))
        );
        assert_eq!(tokenizer_call_count(), 0);

        let mut synthesized_content = marked_content;
        synthesized_content.push(json!({"type":"text","text":"automatic target"}));
        let over_cap_request = json!({
            "model": "claude-sonnet-4-5",
            "cache_control": {"type":"ephemeral"},
            "messages": [{"role":"user","content":synthesized_content}]
        });

        reset_tokenizer_call_count();
        let over_cap =
            analyze_v3_prompt_cache(&over_cap_request, canonical_model_id("claude-sonnet-4-5"));

        assert!(over_cap.blocks.is_empty());
        assert!(over_cap.breakpoints.is_empty());
        assert_eq!(tokenizer_call_count(), 0);
    }

    #[test]
    fn explicit_ttl_wins_over_conflicting_automatic_ttl() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "cache_control": {"type":"ephemeral","ttl":"1h"},
            "messages": [{"role":"user","content":[{
                "type":"text",
                "text":"conflicting ttl",
                "cache_control":{"type":"ephemeral"}
            }]}]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.blocks.len(), 1);
        assert_eq!(analysis.breakpoints.len(), 1);
        assert_eq!(analysis.breakpoints[0].block_index, 0);
        assert_eq!(analysis.breakpoints[0].ttl.as_deref(), Some("5m"));
    }

    #[test]
    fn one_hour_breakpoint_after_five_minute_still_yields_breakpoints() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content":[
                {
                    "type":"text",
                    "text":"short first",
                    "cache_control":{"type":"ephemeral"}
                },
                {
                    "type":"text",
                    "text":"long second",
                    "cache_control":{"type":"ephemeral","ttl":"1h"}
                }
            ]}]
        });

        let analysis = analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(analysis.blocks.len(), 2);
        assert_eq!(analysis.breakpoints.len(), 2);
        assert_eq!(analysis.breakpoints[0].ttl.as_deref(), Some("5m"));
        assert_eq!(analysis.breakpoints[1].ttl.as_deref(), Some("1h"));
    }

    #[test]
    fn thinking_block_shifts_following_index_and_prefix_key() {
        let without_thinking = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"assistant","content":[
                {"type":"text","text":"stable"},
                {
                    "type":"text",
                    "text":"breakpoint",
                    "cache_control":{"type":"ephemeral"}
                }
            ]}]
        });
        let with_thinking = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"assistant","content":[
                {"type":"text","text":"stable"},
                {"type":"thinking","thinking":"preserved reasoning"},
                {
                    "type":"text",
                    "text":"breakpoint",
                    "cache_control":{"type":"ephemeral"}
                }
            ]}]
        });

        let without_thinking =
            analyze_v3_prompt_cache(&without_thinking, canonical_model_id("claude-sonnet-4-5"));
        let with_thinking =
            analyze_v3_prompt_cache(&with_thinking, canonical_model_id("claude-sonnet-4-5"));

        assert_eq!(without_thinking.breakpoints[0].block_index, 1);
        assert_eq!(with_thinking.breakpoints[0].block_index, 2);
        assert_eq!(
            with_thinking.blocks[1].value["type"].as_str(),
            Some("thinking")
        );
        assert_ne!(
            without_thinking.breakpoints[0].prefix_key,
            with_thinking.breakpoints[0].prefix_key
        );
    }

    #[test]
    fn standard_speed_is_hash_neutral_but_explicit_effort_is_salted() {
        // `standard` speed is structurally the absence of fast mode (opt-in, beta-gated), so it
        // normalizes to absent. `effort` does not: its default is per-model, so an explicit value
        // is always salted rather than compared against a hardcoded default.
        let omitted = json!({
            "model": "claude-sonnet-4-5",
            "tools": [{
                "name":"lookup",
                "description":"lookup",
                "input_schema":{"type":"object"},
                "cache_control":{"type":"ephemeral"}
            }],
            "system": [{
                "type":"text",
                "text":"system",
                "cache_control":{"type":"ephemeral"}
            }],
            "messages": [{"role":"user","content":[{
                "type":"text",
                "text":"message",
                "cache_control":{"type":"ephemeral"}
            }]}]
        });
        let mut explicit_effort = omitted.clone();
        explicit_effort["output_config"] = json!({"effort":"high"});
        let mut explicit_speed = omitted.clone();
        explicit_speed["speed"] = json!("standard");

        let omitted = analyze_v3_prompt_cache(&omitted, canonical_model_id("claude-sonnet-4-5"));
        let explicit_effort =
            analyze_v3_prompt_cache(&explicit_effort, canonical_model_id("claude-sonnet-4-5"));
        let explicit_speed =
            analyze_v3_prompt_cache(&explicit_speed, canonical_model_id("claude-sonnet-4-5"));
        let prefix_keys = |analysis: &super::V3PromptCacheAnalysis| {
            analysis
                .breakpoints
                .iter()
                .map(|breakpoint| breakpoint.prefix_key.clone())
                .collect::<Vec<_>>()
        };

        assert_eq!(prefix_keys(&omitted), prefix_keys(&explicit_speed));
        // Tools tier is never salted, so only the system and message tiers may diverge.
        assert_eq!(prefix_keys(&omitted)[0], prefix_keys(&explicit_effort)[0]);
        assert_ne!(prefix_keys(&omitted)[2], prefix_keys(&explicit_effort)[2]);
    }

    #[test]
    fn explicit_disabled_thinking_is_salted_rather_than_treated_as_absent() {
        // Collapsing the two would be a false-hit prediction on models where thinking is on by
        // default (Opus 5, Sonnet 5, Fable 5, Mythos 5). Splitting them costs at most one extra
        // cache creation on models where thinking is off by default.
        let omitted = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role":"user","content":[{
                "type":"text",
                "text":"message",
                "cache_control":{"type":"ephemeral"}
            }]}]
        });
        let mut disabled = omitted.clone();
        disabled["thinking"] = json!({"type":"disabled"});

        let omitted = analyze_v3_prompt_cache(&omitted, canonical_model_id("claude-sonnet-4-5"));
        let disabled = analyze_v3_prompt_cache(&disabled, canonical_model_id("claude-sonnet-4-5"));

        assert_ne!(
            omitted.breakpoints[0].prefix_key,
            disabled.breakpoints[0].prefix_key
        );
    }

    #[test]
    fn fast_speed_is_hash_neutral_when_model_does_not_honor_it() {
        let omitted = json!({
            "model": "claude-opus-4-6",
            "system": [{
                "type":"text",
                "text":"system",
                "cache_control":{"type":"ephemeral"}
            }]
        });
        let mut fast = omitted.clone();
        fast["speed"] = json!("fast");

        let omitted = analyze_v3_prompt_cache(&omitted, "claude-opus-4-6");
        let fast = analyze_v3_prompt_cache(&fast, "claude-opus-4-6");

        assert_eq!(
            omitted.breakpoints[0].source,
            V3PromptCacheBlockSource::System
        );
        assert_eq!(
            omitted.breakpoints[0].prefix_key,
            fast.breakpoints[0].prefix_key
        );
    }

    #[test]
    fn fast_speed_perturbs_unknown_future_model_system_prefix() {
        let omitted = json!({
            "model": "claude-opus-6",
            "system": [{
                "type":"text",
                "text":"system",
                "cache_control":{"type":"ephemeral"}
            }]
        });
        let mut fast = omitted.clone();
        fast["speed"] = json!("fast");

        let omitted = analyze_v3_prompt_cache(&omitted, "claude-opus-6");
        let fast = analyze_v3_prompt_cache(&fast, "claude-opus-6");

        assert_eq!(
            omitted.breakpoints[0].source,
            V3PromptCacheBlockSource::System
        );
        assert_ne!(
            omitted.breakpoints[0].prefix_key,
            fast.breakpoints[0].prefix_key
        );
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
        assert_eq!(tokenizer_call_count(), 0);
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
        assert_eq!(tokenizer_call_count(), 0);
    }

    #[test]
    fn large_threshold_prefix_skips_tokenizer() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": "a".repeat(20_000),
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });
        let model = canonical_model_id("claude-sonnet-4-5");
        let analysis = analyze_v3_prompt_cache(&request, model);

        reset_tokenizer_call_count();
        let keys = cacheable_breakpoint_prefix_keys(&analysis, model, 1_024);

        assert_eq!(keys, vec![analysis.breakpoints[0].prefix_key.clone()]);
        assert_eq!(tokenizer_call_count(), 0);
    }

    #[test]
    fn ambiguous_threshold_prefix_uses_tokenizer() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": "a".repeat(2_000),
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });
        let model = canonical_model_id("claude-sonnet-4-5");
        let analysis = analyze_v3_prompt_cache(&request, model);
        assert!((1_024..8_192).contains(&analysis.breakpoints[0].prefix_token_count));

        reset_tokenizer_call_count();
        let keys = cacheable_breakpoint_prefix_keys(&analysis, model, 1_024);

        assert!(keys.is_empty());
        assert_eq!(tokenizer_call_count(), 1);
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
            assert_eq!(tokenizer_call_count(), 0, "block_count={block_count}");
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
