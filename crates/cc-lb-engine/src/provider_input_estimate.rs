//! Native vision admission quantities. Routing prefixes and observed usage are separate.

use serde_json::Value;

use crate::lifecycle::{
    DEFAULT_MAX_INPUT_ESTIMATE, INPUT_ESTIMATE_MAX_BYTES, INPUT_ESTIMATE_MAX_NODES,
};
use crate::provider_capability::ProviderCapability;
use crate::provider_image_tokens::{dimensions_base64, gemini_image_tokens, openai_image_tokens};

#[derive(Clone, Copy)]
enum Contract {
    Openai,
    Gemini,
}

#[derive(Clone, Copy)]
enum Position {
    Generic,
    Messages,
    Message,
    Responses,
    ResponseItem,
    ResponseParts,
    ResponsePart,
    ChatParts,
    ChatPart,
    GeminiContents,
    GeminiContent,
    GeminiParts,
    GeminiPart,
    ImageUrl,
    GeminiImageSource,
    Payload,
}

struct Estimator<'a> {
    contract: Contract,
    model: &'a str,
    resolution: Option<&'a Value>,
    nodes: usize,
    bytes: usize,
    images: u64,
}

/// Returns `None` for operations whose existing generic or Anthropic estimate applies.
pub(crate) fn estimate_provider_input(
    value: &Value,
    model: &str,
    capability: Option<ProviderCapability>,
) -> Option<i64> {
    let (contract, responses, chat, gemini) = match capability? {
        ProviderCapability::OpenaiResponses
        | ProviderCapability::OpenaiResponsesCompact
        | ProviderCapability::OpenaiResponsesInputTokens => (Contract::Openai, true, false, false),
        ProviderCapability::OpenaiChatCompletions => (Contract::Openai, false, true, false),
        ProviderCapability::GeminiOpenaiChatCompletions => (Contract::Gemini, false, true, false),
        ProviderCapability::GeminiGenerateContent
        | ProviderCapability::GeminiCountTokens
        | ProviderCapability::GeminiCachedContentsCreate => (Contract::Gemini, false, false, true),
        _ => return None,
    };
    let mut estimator = Estimator {
        contract,
        model,
        resolution: request_resolution(value),
        nodes: 0,
        bytes: 0,
        images: 0,
    };
    for key in [
        "tools",
        "system",
        "messages",
        "instructions",
        "input",
        "systemInstruction",
        "contents",
    ] {
        let position = match key {
            "messages" if chat => Position::Messages,
            "input" if responses => Position::Responses,
            "contents" if gemini => Position::GeminiContents,
            "systemInstruction" if gemini => Position::GeminiContent,
            _ => Position::Generic,
        };
        if let Some(selected) = value.get(key) {
            estimator.walk(selected, position);
        }
    }
    if let Some(request) = value.get("generateContentRequest") {
        estimator.resolution = request_resolution(request).or(estimator.resolution);
        for key in ["tools", "contents", "systemInstruction"] {
            let position = match key {
                "contents" if gemini => Position::GeminiContents,
                "systemInstruction" if gemini => Position::GeminiContent,
                _ => Position::Generic,
            };
            if let Some(selected) = request.get(key) {
                estimator.walk(selected, position);
            }
        }
    }
    let tokens = (estimator.bytes as u64 / 4).saturating_add(estimator.images);
    Some(
        i64::try_from(tokens)
            .unwrap_or(i64::MAX)
            .max(DEFAULT_MAX_INPUT_ESTIMATE),
    )
}

fn request_resolution(value: &Value) -> Option<&Value> {
    value
        .get("generationConfig")
        .or_else(|| value.get("generation_config"))
        .and_then(part_resolution)
}

fn part_resolution(value: &Value) -> Option<&Value> {
    value
        .get("mediaResolution")
        .or_else(|| value.get("media_resolution"))
}

fn image_source(value: &Value) -> bool {
    value
        .get("mimeType")
        .or_else(|| value.get("mime_type"))
        .and_then(Value::as_str)
        .is_some_and(|mime| mime.starts_with("image/") && mime.len() > "image/".len())
}

/// A data URL's metadata is bounded too; an arbitrary URL is never scanned or fetched.
fn data_url_dimensions(value: Option<&Value>) -> Option<(u32, u32)> {
    let text = value?.as_str()?;
    let prefix = text.as_bytes().get(..text.len().min(128))?;
    let comma = prefix.iter().position(|byte| *byte == b',')?;
    let metadata = std::str::from_utf8(&prefix[..comma]).ok()?;
    if !metadata.starts_with("data:") || !metadata.ends_with(";base64") {
        return None;
    }
    dimensions_base64(&text[comma + 1..])
}

impl Estimator<'_> {
    fn exhausted(&self) -> bool {
        self.nodes >= INPUT_ESTIMATE_MAX_NODES || self.bytes >= INPUT_ESTIMATE_MAX_BYTES
    }

    fn add_image(&mut self, detail: Option<&Value>, dimensions: Option<(u32, u32)>) {
        let tokens = match self.contract {
            Contract::Openai => openai_image_tokens(self.model, detail, dimensions),
            Contract::Gemini => gemini_image_tokens(self.model, detail, dimensions),
        };
        self.images = self.images.saturating_add(tokens);
    }

    fn walk(&mut self, value: &Value, position: Position) {
        if self.exhausted() {
            return;
        }
        self.nodes += 1;
        if matches!(position, Position::Payload) {
            return;
        }
        match value {
            Value::String(text) => {
                self.bytes = self
                    .bytes
                    .saturating_add(text.len())
                    .min(INPUT_ESTIMATE_MAX_BYTES);
            }
            Value::Array(items) => {
                let child_position = match position {
                    Position::Messages => Position::Message,
                    Position::Responses => Position::ResponseItem,
                    Position::ResponseParts => Position::ResponsePart,
                    Position::ChatParts => Position::ChatPart,
                    Position::GeminiContents => Position::GeminiContent,
                    Position::GeminiParts => Position::GeminiPart,
                    _ => Position::Generic,
                };
                for item in items {
                    if self.exhausted() {
                        break;
                    }
                    self.walk(item, child_position);
                }
            }
            Value::Object(fields) => {
                let native_type = value.get("type").and_then(Value::as_str);
                let response_image =
                    matches!(position, Position::ResponseItem | Position::ResponsePart)
                        && native_type == Some("input_image");
                let chat_image =
                    matches!(position, Position::ChatPart) && native_type == Some("image_url");
                if response_image || chat_image {
                    let url = value.get("image_url");
                    let (detail, dimensions) = if response_image {
                        (value.get("detail"), data_url_dimensions(url))
                    } else if let Some(url) = url.filter(|url| url.is_object()) {
                        (url.get("detail"), data_url_dimensions(url.get("url")))
                    } else {
                        (None, data_url_dimensions(url))
                    };
                    let detail = match self.contract {
                        Contract::Openai => detail,
                        // OpenAI detail is not Gemini's media-resolution control.
                        Contract::Gemini => part_resolution(value).or(self.resolution),
                    };
                    self.add_image(detail, dimensions);
                }
                for (key, child) in fields {
                    if self.exhausted() {
                        break;
                    }
                    let child_position = match position {
                        _ if (response_image || chat_image) && key == "image_url" => {
                            if child.is_object() {
                                Position::ImageUrl
                            } else {
                                Position::Payload
                            }
                        }
                        _ if response_image && key == "file_id" => Position::Payload,
                        Position::ImageUrl if key == "url" => Position::Payload,
                        Position::Message if key == "content" && child.is_array() => {
                            Position::ChatParts
                        }
                        Position::ResponseItem if key == "content" && child.is_array() => {
                            Position::ResponseParts
                        }
                        Position::ResponseItem
                            if key == "output"
                                && child.is_array()
                                && matches!(
                                    native_type,
                                    Some("function_call_output" | "custom_tool_call_output")
                                ) =>
                        {
                            Position::ResponseParts
                        }
                        Position::GeminiContent if key == "parts" && child.is_array() => {
                            Position::GeminiParts
                        }
                        Position::GeminiPart
                            if matches!(
                                key.as_str(),
                                "inlineData" | "inline_data" | "fileData" | "file_data"
                            ) && image_source(child) =>
                        {
                            let dimensions = if matches!(key.as_str(), "inlineData" | "inline_data")
                            {
                                child
                                    .get("data")
                                    .and_then(Value::as_str)
                                    .and_then(dimensions_base64)
                            } else {
                                None
                            };
                            self.add_image(part_resolution(value).or(self.resolution), dimensions);
                            Position::GeminiImageSource
                        }
                        Position::GeminiImageSource
                            if matches!(key.as_str(), "data" | "fileUri" | "file_uri") =>
                        {
                            Position::Payload
                        }
                        _ => Position::Generic,
                    };
                    self.walk(child, child_position);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn estimate(value: &Value, model: &str, capability: ProviderCapability) -> i64 {
        estimate_provider_input(value, model, Some(capability)).unwrap()
    }

    #[test]
    fn responses_image_references_replace_payload_bytes_but_keep_metadata() {
        for reference in ["file_1".to_owned(), "f".repeat(40_000)] {
            let body = json!({
                "instructions": "x".repeat(20_000),
                "input": [{"role":"user","content":[
                    {"type":"input_image","file_id":reference,"detail":"low"}
                ]}]
            });
            // 20,000 instruction bytes + user(4), input_image(11), low(3).
            assert_eq!(
                estimate(&body, "gpt-4.1", ProviderCapability::OpenaiResponses),
                5_089
            );
        }
    }

    #[test]
    fn chat_images_and_responses_tool_output_use_native_content_positions() {
        let chat = json!({
            "instructions":"x".repeat(20_000),
            "messages":[{"role":"user","content":[
                {"type":"image_url","image_url":{"url":"x".repeat(40_000),"detail":"low"}}
            ]}]
        });
        assert_eq!(
            estimate(&chat, "gpt-4.1", ProviderCapability::OpenaiChatCompletions),
            5_089
        );
        let output = json!({
            "instructions":"x".repeat(20_000),
            "input":[{"type":"custom_tool_call_output","output":[
                {"type":"input_image","file_id":"f".repeat(40_000),"detail":"low"}
            ]}]
        });
        assert_eq!(
            estimate(&output, "gpt-4.1", ProviderCapability::OpenaiResponses),
            5_094
        );
    }

    #[test]
    fn tools_and_non_native_image_shapes_remain_ordinary_text() {
        let image = json!({"type":"input_image","image_url":"x".repeat(20_000),"detail":"low"});
        let tools = json!({"tools":[image.clone()]});
        assert_eq!(
            estimate(&tools, "gpt-4.1", ProviderCapability::OpenaiResponses),
            5_003
        );
        let tool_arguments = json!({"input":[{"type":"function_call","arguments":image}]});
        assert_eq!(
            estimate(
                &tool_arguments,
                "gpt-4.1",
                ProviderCapability::OpenaiResponses
            ),
            5_006
        );
        let wrong_wire = json!({"messages":[{"content":[
            {"type":"input_image","image_url":"x".repeat(20_000),"detail":"low"}
        ]}]});
        assert_eq!(
            estimate(
                &wrong_wire,
                "gpt-4.1",
                ProviderCapability::OpenaiChatCompletions
            ),
            5_003
        );
    }

    #[test]
    fn gemini_image_mime_is_required_and_reference_length_does_not_count() {
        for reference in ["gs://example/image".to_owned(), "f".repeat(40_000)] {
            let body = json!({"contents":[{"parts":[
                {"fileData":{"mimeType":"image/heic","fileUri":reference}}
            ]}]});
            assert_eq!(
                estimate(
                    &body,
                    "gemini-2.5-flash",
                    ProviderCapability::GeminiGenerateContent
                ),
                65_538
            );
        }
        for mime in [
            "audio/wav",
            "video/mp4",
            "application/pdf",
            "application/octet-stream",
        ] {
            let body = json!({"contents":[{"parts":[
                {"inlineData":{"mimeType":mime,"data":"x".repeat(40_000)}}
            ]}]});
            let expected = match mime {
                "application/octet-stream" => 10_006,
                "application/pdf" => 10_003,
                _ => 10_002,
            };
            assert_eq!(
                estimate(
                    &body,
                    "gemini-3-flash",
                    ProviderCapability::GeminiGenerateContent
                ),
                expected
            );
        }
    }

    #[test]
    fn resolution_precedence_wrappers_and_compat_contract_are_preserved() {
        let body = json!({
            "instructions":"x".repeat(20_000),
            "generationConfig":{"mediaResolution":"MEDIA_RESOLUTION_HIGH"},
            "contents":[{"parts":[
                {"inlineData":{"mimeType":"image/heic","data":"x".repeat(40_000)},
                 "mediaResolution":{"level":"MEDIA_RESOLUTION_LOW"}}
            ]}]
        });
        // image/heic(10) + MEDIA_RESOLUTION_LOW(20) remain metadata.
        assert_eq!(
            estimate(
                &body,
                "gemini-3-flash",
                ProviderCapability::GeminiGenerateContent
            ),
            5_287
        );
        let wrapped = json!({
            "instructions":"x".repeat(20_000),
            "generateContentRequest":{
                "generationConfig":{"mediaResolution":"MEDIA_RESOLUTION_MEDIUM"},
                "contents":[{"parts":[{"inlineData":{"mimeType":"image/heic","data":"x".repeat(40_000)}}]}]
            }
        });
        assert_eq!(
            estimate(
                &wrapped,
                "gemini-4-flash",
                ProviderCapability::GeminiCountTokens
            ),
            5_562
        );
        let compat = json!({
            "instructions":"x".repeat(20_000),
            "messages":[{"content":[{"type":"image_url","image_url":{"url":"https://example.com/a","detail":"low"}}]}]
        });
        // Chat detail remains metadata; it is not Gemini mediaResolution.
        assert_eq!(
            estimate(
                &compat,
                "gemini-3-flash",
                ProviderCapability::GeminiOpenaiChatCompletions
            ),
            6_123
        );
    }

    #[test]
    fn recognized_malformed_images_are_nonzero_without_payload_text() {
        for image in [
            json!({"type":"input_image"}),
            json!({"type":"input_image","image_url":null}),
            json!({"type":"input_image","image_url":{"url":7}}),
        ] {
            let body = json!({"input":[{"content":[image]}]});
            assert_eq!(
                estimate(&body, "unknown", ProviderCapability::OpenaiResponses),
                73_802
            );
        }
        let body = json!({"contents":[{"parts":[{"inlineData":{"mimeType":"image/heic"}}]}]});
        assert_eq!(
            estimate(
                &body,
                "gemini-2.5-flash",
                ProviderCapability::GeminiGenerateContent
            ),
            65_538
        );
    }

    #[test]
    fn request_floor_and_traversal_budgets_are_shared_with_text() {
        let body = json!({"input":[{"content":[{"type":"input_image","detail":"low","file_id":"file_1"}]}]});
        assert_eq!(
            estimate(&body, "gpt-4.1", ProviderCapability::OpenaiResponses),
            4_000
        );
        let node_limited = json!({
            "tools":vec![Value::Null; INPUT_ESTIMATE_MAX_NODES],
            "input":[{"content":[{"type":"input_image","file_id":"file_1"}]}]
        });
        assert_eq!(
            estimate(
                &node_limited,
                "unknown",
                ProviderCapability::OpenaiResponses
            ),
            4_000
        );
        let byte_limited = json!({
            "instructions":"x".repeat(INPUT_ESTIMATE_MAX_BYTES + 4),
            "input":[{"content":[{"type":"input_image","file_id":"file_1"}]}]
        });
        assert_eq!(
            estimate(
                &byte_limited,
                "unknown",
                ProviderCapability::OpenaiResponses
            ),
            (INPUT_ESTIMATE_MAX_BYTES / 4) as i64
        );
    }
}
