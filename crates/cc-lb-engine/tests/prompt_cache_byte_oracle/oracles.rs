use cc_lb_engine::prompt_cache_simulator::{V3PromptCacheBlock, V3PromptCacheBlockSource};
use cc_lb_engine::tokenizer::PrefixTokenizer;
use serde_json::{Value, json};

pub fn legacy_block_digest_oracle_bytes(block: &V3PromptCacheBlock) -> Vec<u8> {
    let value = match &block.value {
        Value::Object(map) => {
            let mut value = map.clone();
            value.remove("cache_control");
            Value::Object(value)
        }
        value => value.clone(),
    };
    serde_json::to_vec(&json!({
        "source": source_name(block.source),
        "path": &block.path,
        "value": value,
    }))
    .expect("legacy block-digest oracle serializes")
}

pub fn legacy_prefix_oracle_bytes(model: &str, blocks: &[V3PromptCacheBlock]) -> Vec<u8> {
    let content_blocks = blocks
        .iter()
        .map(|block| json!({"source": source_name(block.source), "value": block.value}))
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({"model": model, "content_blocks": content_blocks}))
        .expect("legacy prefix oracle serializes")
}

pub fn block_digest_serializer_bytes(block: &V3PromptCacheBlock) -> Vec<u8> {
    let value = match &block.value {
        Value::Object(map) => {
            let mut value = map.clone();
            value.remove("cache_control");
            Value::Object(value)
        }
        value => value.clone(),
    };
    serde_json::to_vec(&json!({
        "source": source_name(block.source),
        "path": &block.path,
        "value": value,
    }))
    .expect("block-digest serializer serializes")
}

pub fn prefix_serializer_bytes(model: &str, blocks: &[V3PromptCacheBlock]) -> Vec<u8> {
    let content_blocks = blocks
        .iter()
        .map(|block| json!({"source": source_name(block.source), "value": block.value}))
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({"model": model, "content_blocks": content_blocks}))
        .expect("prefix serializer serializes")
}

pub fn digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cc-lb-cache-v4:block");
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

pub fn token_count(bytes: &[u8]) -> u64 {
    let text = std::str::from_utf8(bytes).expect("serde_json emits UTF-8");
    PrefixTokenizer::global().count_tokens(text) as u64
}

pub fn digest_bytes_preserving_cache_control(block: &V3PromptCacheBlock) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "source": source_name(block.source),
        "path": &block.path,
        "value": block.value,
    }))
    .expect("negative-control digest serializes")
}

pub fn prefix_bytes_stripping_cache_control(model: &str, blocks: &[V3PromptCacheBlock]) -> Vec<u8> {
    let content_blocks = blocks
        .iter()
        .map(|block| {
            let value = match &block.value {
                Value::Object(map) => {
                    let mut value = map.clone();
                    value.remove("cache_control");
                    Value::Object(value)
                }
                value => value.clone(),
            };
            json!({"source": source_name(block.source), "value": value})
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({"model": model, "content_blocks": content_blocks}))
        .expect("negative-control stripped prefix serializes")
}

fn source_name(source: V3PromptCacheBlockSource) -> &'static str {
    match source {
        V3PromptCacheBlockSource::Tools => "tools",
        V3PromptCacheBlockSource::System => "system",
        V3PromptCacheBlockSource::Message => "message",
    }
}
