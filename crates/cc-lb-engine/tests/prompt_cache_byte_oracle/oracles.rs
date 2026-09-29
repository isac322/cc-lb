use cc_lb_engine::prompt_cache_simulator::{V3PromptCacheBlock, V3PromptCacheBlockSource};
use serde_json::{Value, json};

pub fn prefix_oracle_bytes(model: &str, blocks: &[V3PromptCacheBlock]) -> Vec<u8> {
    let content_blocks = blocks
        .iter()
        .map(|block| json!({"source": source_name(block.source), "value": block.value}))
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({"model": model, "content_blocks": content_blocks}))
        .expect("prefix oracle serializes")
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
