//! Limits-only Anthropic admission quantities, independent of serialized routing prefixes.

use cc_lb_domain::{CacheBreakpoint, TtlClass};
use cc_lb_pricing::LocalCacheInputEstimate;
use serde_json::Value;

use crate::lifecycle::{
    DEFAULT_MAX_INPUT_ESTIMATE, INPUT_ESTIMATE_MAX_BYTES, INPUT_ESTIMATE_MAX_NODES,
};

const IMAGE_TOKENS: u64 = 4_784;

#[derive(Clone, Copy, Default)]
struct Region {
    bytes: u64,
    images: u64,
}

impl Region {
    fn tokens(self, remainder: &mut u64) -> u64 {
        let trailing = (self.bytes % 4).saturating_add(*remainder);
        let text = (self.bytes / 4).saturating_add(trailing / 4);
        *remainder = trailing % 4;
        text.saturating_add(self.images.saturating_mul(IMAGE_TOKENS))
    }
}

#[derive(Clone, Copy)]
enum RegionKind {
    Tail,
    FiveMinute,
    OneHour,
}

#[derive(Clone, Copy)]
enum WalkKind {
    Generic,
    Tools,
    System,
    Messages,
    Message,
    Content,
    Block,
    ToolResultContent,
    ImageSource(&'static str),
    ImagePayload,
}

struct Estimator {
    tail: Region,
    five_minute: Region,
    one_hour: Region,
    one_hour_end: Option<u32>,
    five_minute_end: Option<u32>,
    block_index: u32,
    nodes: usize,
    bytes: usize,
}

impl Estimator {
    fn new(breakpoints: &[CacheBreakpoint]) -> Self {
        Self {
            tail: Region::default(),
            five_minute: Region::default(),
            one_hour: Region::default(),
            one_hour_end: breakpoints
                .iter()
                .filter(|bp| bp.requested_ttl == TtlClass::Ephemeral1h)
                .map(|bp| bp.block_index)
                .max(),
            five_minute_end: breakpoints
                .iter()
                .filter(|bp| bp.requested_ttl == TtlClass::Ephemeral5m)
                .map(|bp| bp.block_index)
                .max(),
            block_index: 0,
            nodes: 0,
            bytes: 0,
        }
    }

    fn exhausted(&self) -> bool {
        self.nodes >= INPUT_ESTIMATE_MAX_NODES || self.bytes >= INPUT_ESTIMATE_MAX_BYTES
    }

    fn region_mut(&mut self, region: RegionKind) -> &mut Region {
        match region {
            RegionKind::Tail => &mut self.tail,
            RegionKind::FiveMinute => &mut self.five_minute,
            RegionKind::OneHour => &mut self.one_hour,
        }
    }

    fn next_block(&mut self) -> RegionKind {
        let index = self.block_index;
        self.block_index = self.block_index.saturating_add(1);
        if self.one_hour_end.is_some_and(|end| index <= end) {
            RegionKind::OneHour
        } else if self.five_minute_end.is_some_and(|end| index <= end) {
            RegionKind::FiveMinute
        } else {
            RegionKind::Tail
        }
    }

    fn content_region(&mut self, value: &Value) -> RegionKind {
        // Match the simulator's block indices without allocating synthetic values.
        let participates = match value.get("type").and_then(Value::as_str) {
            Some("text") => value
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty()),
            Some(_) => true,
            None => false,
        };
        if participates {
            self.next_block()
        } else {
            RegionKind::Tail
        }
    }

    fn add_string(&mut self, text: &str, region: RegionKind) {
        let added = text
            .len()
            .min(INPUT_ESTIMATE_MAX_BYTES.saturating_sub(self.bytes));
        self.bytes = self.bytes.saturating_add(added);
        let target = self.region_mut(region);
        target.bytes = target.bytes.saturating_add(added as u64);
    }

    fn add_image(&mut self, region: RegionKind) {
        let target = self.region_mut(region);
        target.images = target.images.saturating_add(1);
    }

    fn walk_generic(&mut self, value: &Value, region: RegionKind) {
        self.walk(value, WalkKind::Generic, region);
    }

    fn walk(&mut self, value: &Value, kind: WalkKind, region: RegionKind) {
        if self.exhausted() {
            return;
        }
        self.nodes = self.nodes.saturating_add(1);
        if matches!(kind, WalkKind::ImagePayload) {
            return;
        }
        match value {
            Value::String(text) => {
                let region =
                    if matches!(kind, WalkKind::System | WalkKind::Content) && !text.is_empty() {
                        self.next_block()
                    } else {
                        region
                    };
                self.add_string(text, region);
            }
            Value::Array(items) => {
                for item in items {
                    if self.exhausted() {
                        break;
                    }
                    let (child_kind, child_region) = match kind {
                        WalkKind::Tools if item.is_object() => {
                            (WalkKind::Generic, self.next_block())
                        }
                        WalkKind::System => (WalkKind::Generic, self.content_region(item)),
                        WalkKind::Messages => (WalkKind::Message, RegionKind::Tail),
                        WalkKind::Content => (WalkKind::Block, self.content_region(item)),
                        WalkKind::ToolResultContent => (WalkKind::Block, region),
                        _ => (WalkKind::Generic, region),
                    };
                    self.walk(item, child_kind, child_region);
                    if self.exhausted() {
                        break;
                    }
                }
            }
            Value::Object(fields) => {
                let image_payload = if matches!(kind, WalkKind::Block) {
                    native_image_payload(value)
                } else {
                    None
                };
                if image_payload.is_some() {
                    self.add_image(region);
                }
                let is_tool_result = matches!(kind, WalkKind::Block)
                    && value.get("type").and_then(Value::as_str) == Some("tool_result");
                for (key, child) in fields {
                    if self.exhausted() {
                        break;
                    }
                    let (child_kind, child_region) = match kind {
                        WalkKind::Message if key == "content" => {
                            (WalkKind::Content, RegionKind::Tail)
                        }
                        WalkKind::Message => (WalkKind::Generic, RegionKind::Tail),
                        WalkKind::ImageSource(payload) if key == payload => {
                            (WalkKind::ImagePayload, region)
                        }
                        _ if key == "source" => (
                            image_payload.map_or(WalkKind::Generic, WalkKind::ImageSource),
                            region,
                        ),
                        _ if key == "content" && is_tool_result && child.is_array() => {
                            (WalkKind::ToolResultContent, region)
                        }
                        _ => (WalkKind::Generic, region),
                    };
                    self.walk(child, child_kind, child_region);
                    if self.exhausted() {
                        break;
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    fn finish(self) -> LocalCacheInputEstimate {
        // Convert contiguous prefix regions, then tail metadata, with one carry.
        let mut remainder = 0;
        let one_hour = self.one_hour.tokens(&mut remainder);
        let five_minute = self.five_minute.tokens(&mut remainder);
        let tail = self.tail.tokens(&mut remainder);
        let whole = tail.saturating_add(five_minute).saturating_add(one_hour);
        let floor = u64::try_from(DEFAULT_MAX_INPUT_ESTIMATE).unwrap_or(0);
        LocalCacheInputEstimate {
            uncached_input_tokens: tail.saturating_add(floor.saturating_sub(whole)),
            cache_prefix_5m_tokens: five_minute,
            cache_prefix_1h_tokens: one_hour,
        }
    }
}

fn native_image_payload(value: &Value) -> Option<&'static str> {
    if value.get("type").and_then(Value::as_str) != Some("image") {
        return None;
    }
    let source = value.get("source")?;
    let payload = match source.get("type").and_then(Value::as_str)? {
        "base64" => "data",
        "url" => "url",
        "file" => "file_id",
        _ => return None,
    };
    source.get(payload)?.as_str().map(|_| payload)
}

pub(crate) fn estimate_anthropic_input(
    value: &Value,
    breakpoints: &[CacheBreakpoint],
) -> LocalCacheInputEstimate {
    let mut estimator = Estimator::new(breakpoints);
    for (key, kind) in [
        ("tools", WalkKind::Tools),
        ("system", WalkKind::System),
        ("messages", WalkKind::Messages),
        ("instructions", WalkKind::Generic),
        ("input", WalkKind::Generic),
        ("systemInstruction", WalkKind::Generic),
        ("contents", WalkKind::Generic),
    ] {
        if let Some(selected) = value.get(key) {
            estimator.walk(selected, kind, RegionKind::Tail);
        }
    }
    if let Some(request) = value.get("generateContentRequest") {
        for key in ["tools", "contents", "systemInstruction"] {
            if let Some(selected) = request.get(key) {
                estimator.walk_generic(selected, RegionKind::Tail);
            }
        }
    }
    estimator.finish()
}

#[cfg(test)]
mod tests {
    use super::estimate_anthropic_input;
    use crate::lifecycle::{INPUT_ESTIMATE_MAX_BYTES, INPUT_ESTIMATE_MAX_NODES};
    use cc_lb_domain::{CacheBreakpoint, CacheBreakpointSource, PrefixBytes, TtlClass};
    use serde_json::json;

    fn breakpoint(block_index: u32, requested_ttl: TtlClass) -> CacheBreakpoint {
        CacheBreakpoint {
            block_index,
            source: CacheBreakpointSource::System,
            path: String::new(),
            message_index: None,
            prefix_hash: String::new(),
            prefix_bytes: PrefixBytes::new(u64::MAX),
            requested_ttl,
            lookback_prefixes: Vec::new(),
            token_estimate_source: None,
        }
    }

    #[test]
    fn file_id_images_receive_allowance_independent_of_reference_length_before_affinity() {
        for reference in ["file_1".to_owned(), "f".repeat(40_000)] {
            let value = json!({"messages":[{"role":"user","content":[
                {"type":"image","source":{"type":"file","file_id":reference}}
            ]}]});
            let input = estimate_anthropic_input(&value, &[]);
            // role user(4), image(5), file(4); the reference is not text.
            assert_eq!(input.uncached_input_tokens, 4_787);
            assert_eq!(input.cache_prefix_5m_tokens, 0);
            assert_eq!(input.cache_prefix_1h_tokens, 0);
        }
    }

    #[test]
    fn missing_or_non_string_payloads_and_non_content_images_remain_generic() {
        for source in [
            json!({"type":"url", "url":7, "note":"x".repeat(20_000)}),
            json!({"type":"base64", "note":"x".repeat(20_000)}),
            json!({"type":"file", "file_id":null, "note":"x".repeat(20_000)}),
        ] {
            let source_type_len = source
                .get("type")
                .and_then(serde_json::Value::as_str)
                .map_or(0, str::len);
            let value = json!({"messages":[{"role":"user","content":[
                {"type":"image","source":source}
            ]}]});
            let input = estimate_anthropic_input(&value, &[]);
            assert_eq!(
                input.uncached_input_tokens,
                (20_009 + source_type_len) as u64 / 4,
            );
        }
        let image = json!({"type":"image","source":{"type":"url","url":"x".repeat(20_000)}});
        for value in [
            json!({"system":[image]}),
            json!({"messages":[{"content":{"example":image}}]}),
            json!({"messages":[{"content":[{"type":"tool_result","content":{"example":image}}]}]}),
        ] {
            let input = estimate_anthropic_input(&value, &[]);
            assert!(input.uncached_input_tokens >= 5_002);
        }
    }

    #[test]
    fn invalid_ttl_order_keeps_resolved_local_partition_without_opaque_padding() {
        let value = json!({
            "system":[
                {"type":"text","text":"a".repeat(16_000),"cache_control":{"type":"ephemeral","ttl":"5m"}},
                {"type":"text","text":"b".repeat(16_000),"cache_control":{"type":"ephemeral","ttl":"1h"}}
            ],
            "messages":[{"role":"user","content":"tail"}]
        });
        let input = estimate_anthropic_input(
            &value,
            &[
                breakpoint(0, TtlClass::Ephemeral5m),
                breakpoint(1, TtlClass::Ephemeral1h),
            ],
        );
        assert_eq!(input.cache_prefix_1h_tokens, 8_007);
        assert_eq!(input.cache_prefix_5m_tokens, 0);
        assert_eq!(input.uncached_input_tokens, 2);
    }

    #[test]
    fn nonparticipating_shapes_keep_simulator_block_indices_and_generic_strings() {
        let value = json!({
            "tools":["skip", {"name":"a"}],
            "system":[
                "ignore",
                {"type":"text","text":""},
                {"type":"text","text":"p".repeat(16_000),"cache_control":{"type":"ephemeral","ttl":"5m"}}
            ],
            "messages":[{"role":"user","content":"t"}]
        });
        // Only the tool object and nonempty system text participate. Their
        // 16,016 bytes form P5; all 19 other selected-root bytes stay in tail.
        let input = estimate_anthropic_input(&value, &[breakpoint(1, TtlClass::Ephemeral5m)]);
        assert_eq!(input.cache_prefix_5m_tokens, 4_004);
        assert_eq!(input.cache_prefix_1h_tokens, 0);
        assert_eq!(input.uncached_input_tokens, 4);
    }

    #[test]
    fn node_budget_excludes_final_image_after_preceding_text() {
        let mut messages = vec![
            json!({"role":"user","content":"x".repeat(20_000)}),
            json!({
                "role":"assistant",
                "content":[{
                    "type":"tool_use",
                    "id":"tool_1",
                    "name":"process_values",
                    "input":{"values":vec![serde_json::Value::Null; INPUT_ESTIMATE_MAX_NODES]}
                }]
            }),
        ];
        let prefix_input = estimate_anthropic_input(&json!({"messages":messages}), &[]);
        messages.push(json!({
            "role":"user",
            "content":[{"type":"image","source":{"type":"url","url":"ignored"}}]
        }));
        let input = estimate_anthropic_input(&json!({"messages":messages.clone()}), &[]);

        // Keep the initial text and tool metadata before the argument array
        // exhausts the node budget. The final image adds no allowance.
        assert!(prefix_input.uncached_input_tokens > 4_000);
        assert_eq!(input, prefix_input);
        assert_eq!(input.cache_prefix_5m_tokens, 0);
        assert_eq!(input.cache_prefix_1h_tokens, 0);
    }

    #[test]
    fn byte_budget_clamps_oversized_text_before_final_image() {
        let oversized_text = "x".repeat(INPUT_ESTIMATE_MAX_BYTES + 4);
        let value = json!({
            "messages":[
                {"role":"user","content":oversized_text},
                {"role":"user","content":[
                    {"type":"image","source":{"type":"url","url":"ignored"}}
                ]}
            ]
        });
        let input = estimate_anthropic_input(&value, &[]);

        // The text is clamped at 64 MiB; the following image is not walked.
        assert_eq!(
            input.uncached_input_tokens,
            (INPUT_ESTIMATE_MAX_BYTES / 4) as u64
        );
        assert_eq!(input.cache_prefix_5m_tokens, 0);
        assert_eq!(input.cache_prefix_1h_tokens, 0);
    }
}
