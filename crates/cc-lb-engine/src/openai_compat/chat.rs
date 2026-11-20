//! Chat Completions on top of the Codex Responses backend.
//!
//! The subscription backend has no `/chat/completions`. A public Chat
//! Completions request is translated into a Responses request, and the
//! backend's Responses SSE stream is translated back into
//! `chat.completion.chunk` events or one `chat.completion` object.

use cc_lb_domain::{
    CoerceOrReject, DropOrReject, EmulateDropOrReject, EmulateOrReject, MetadataHandling,
    OpenaiOauthCompatPolicy,
};
use serde_json::{Map, Value, json};

use super::CompatReject;

/// Largest `n` cc-lb fans out to the subscription backend.
pub const MAX_CHAT_CHOICES: u32 = 8;

/// Client-visible behavior the response side must reproduce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatPlan {
    /// Number of choices the client asked for (1 unless `n` was emulated).
    pub n: u32,
    /// Stop sequences cc-lb enforces on the output text.
    pub stop: Vec<String>,
    /// The client asked for a streamed response.
    pub stream: bool,
    /// The client asked for a final usage chunk (`stream_options.include_usage`).
    pub include_usage: bool,
    /// Model echoed in chat objects.
    pub model: String,
}

/// A translated request and the plan for its response.
#[derive(Clone, Debug, PartialEq)]
pub struct ChatRequestTranslation {
    /// Body for the backend `/responses` endpoint.
    pub responses_body: Value,
    /// Response-side plan.
    pub plan: ChatPlan,
}

/// Translate a public Chat Completions request into a backend Responses body.
pub fn translate_chat_request(
    body: &Value,
    policy: &OpenaiOauthCompatPolicy,
) -> Result<ChatRequestTranslation, CompatReject> {
    let Some(request) = body.as_object() else {
        return Err(CompatReject::unsupported_value("body", "non-object"));
    };
    check_rejects(request, policy)?;
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let messages = request
        .get("messages")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut out = Map::new();
    out.insert("model".into(), Value::String(model.clone()));
    out.insert("input".into(), Value::Array(translate_messages(messages)?));
    out.insert("store".into(), Value::Bool(false));
    out.insert("stream".into(), Value::Bool(true));
    copy_tools(request, &mut out);
    copy_text_options(request, &mut out);
    copy_scalars(request, policy, &mut out);
    let plan = ChatPlan {
        n: choice_count(request, policy)?,
        stop: stop_sequences(request, policy),
        stream: request.get("stream").and_then(Value::as_bool) == Some(true),
        include_usage: request
            .get("stream_options")
            .and_then(|options| options.get("include_usage"))
            .and_then(Value::as_bool)
            == Some(true),
        model,
    };
    Ok(ChatRequestTranslation {
        responses_body: Value::Object(out),
        plan,
    })
}

/// Rejects, checked in a fixed order before anything is built.
fn check_rejects(
    request: &Map<String, Value>,
    policy: &OpenaiOauthCompatPolicy,
) -> Result<(), CompatReject> {
    for key in [
        "function_call",
        "functions",
        "prediction",
        "logit_bias",
        "audio",
    ] {
        if request.contains_key(key) {
            return Err(CompatReject::unsupported_parameter(key));
        }
    }
    let wants_audio = request
        .get("modalities")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|item| item.as_str() == Some("audio")));
    if wants_audio {
        return Err(CompatReject::unsupported_value("modalities", "audio"));
    }
    if request.get("store").and_then(Value::as_bool) == Some(true)
        && policy.store_true == CoerceOrReject::Reject
    {
        return Err(CompatReject::unsupported_parameter("store"));
    }
    reject_if_present(request, &["temperature", "top_p"], policy.sampling)?;
    reject_if_present(request, &["logprobs", "top_logprobs"], policy.logprobs)?;
    reject_if_present(
        request,
        &["seed", "frequency_penalty", "presence_penalty"],
        policy.chat_seed_penalties,
    )?;
    if request.contains_key("stop") && policy.chat_stop == EmulateDropOrReject::Reject {
        return Err(CompatReject::unsupported_parameter("stop"));
    }
    if let Some(tier) = request.get("service_tier").and_then(Value::as_str) {
        let unsupported =
            matches!(tier, "auto" | "flex") && policy.service_tier == CoerceOrReject::Reject;
        if unsupported || tier == "scale" {
            return Err(CompatReject::unsupported_value("service_tier", tier));
        }
    }
    if policy.metadata == MetadataHandling::Reject {
        reject_if_present(
            request,
            &["metadata", "user", "safety_identifier"],
            DropOrReject::Reject,
        )?;
    }
    reject_if_present(request, &["max_tool_calls"], policy.max_tool_calls)
}

fn reject_if_present(
    request: &Map<String, Value>,
    keys: &[&str],
    handling: DropOrReject,
) -> Result<(), CompatReject> {
    if handling == DropOrReject::Reject
        && let Some(key) = keys.iter().find(|key| request.contains_key(**key))
    {
        return Err(CompatReject::unsupported_parameter(*key));
    }
    Ok(())
}

fn choice_count(
    request: &Map<String, Value>,
    policy: &OpenaiOauthCompatPolicy,
) -> Result<u32, CompatReject> {
    let n = request.get("n").and_then(Value::as_u64).unwrap_or(1);
    if n <= 1 {
        return Ok(1);
    }
    if policy.chat_n == EmulateOrReject::Reject || n > u64::from(MAX_CHAT_CHOICES) {
        return Err(CompatReject::unsupported_value("n", &n.to_string()));
    }
    Ok(n as u32)
}

fn stop_sequences(request: &Map<String, Value>, policy: &OpenaiOauthCompatPolicy) -> Vec<String> {
    if policy.chat_stop != EmulateDropOrReject::Emulate {
        return Vec::new();
    }
    let sequences = match request.get("stop") {
        Some(Value::String(stop)) => vec![stop.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    };
    sequences
        .into_iter()
        .filter(|stop| !stop.is_empty())
        .collect()
}

fn translate_messages(messages: &[Value]) -> Result<Vec<Value>, CompatReject> {
    let mut items = Vec::with_capacity(messages.len());
    for (index, message) in messages.iter().enumerate() {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user");
        match role {
            "system" | "developer" => {
                items.push(text_message("developer", "input_text", message));
            }
            "user" => items.push(user_message(index, message)?),
            "assistant" => assistant_items(message, &mut items),
            "tool" => items.push(tool_output(message)),
            other => {
                let param = format!("messages[{index}].role");
                return Err(CompatReject::unsupported_value(param, other));
            }
        }
    }
    Ok(items)
}

fn text_message(role: &str, part_type: &str, message: &Value) -> Value {
    let text = content_text(message.get("content"));
    json!({"type": "message", "role": role, "content": [{"type": part_type, "text": text}]})
}

/// Joined text of a string or text-part array.
fn content_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

fn user_message(index: usize, message: &Value) -> Result<Value, CompatReject> {
    let parts = match message.get("content") {
        Some(Value::Array(parts)) => parts
            .iter()
            .enumerate()
            .map(|(part_index, part)| user_part(index, part_index, part))
            .collect::<Result<Vec<_>, _>>()?,
        other => vec![json!({"type": "input_text", "text": content_text(other)})],
    };
    Ok(json!({"type": "message", "role": "user", "content": parts}))
}

fn user_part(index: usize, part_index: usize, part: &Value) -> Result<Value, CompatReject> {
    let kind = part.get("type").and_then(Value::as_str).unwrap_or("text");
    match kind {
        "text" => {
            Ok(json!({"type": "input_text", "text": part.get("text").cloned().unwrap_or_default()}))
        }
        "image_url" => {
            let image = part.get("image_url");
            let url = image
                .and_then(|image| image.get("url"))
                .cloned()
                .unwrap_or_default();
            let mut out = json!({"type": "input_image", "image_url": url});
            if let Some(detail) = image.and_then(|image| image.get("detail")) {
                out["detail"] = detail.clone();
            }
            Ok(out)
        }
        "file" => {
            let mut out = Map::new();
            out.insert("type".into(), Value::String("input_file".into()));
            if let Some(file) = part.get("file").and_then(Value::as_object) {
                for key in ["file_data", "filename", "file_id"] {
                    if let Some(value) = file.get(key) {
                        out.insert(key.into(), value.clone());
                    }
                }
            }
            Ok(Value::Object(out))
        }
        other => Err(CompatReject::unsupported_value(
            format!("messages[{index}].content[{part_index}].type"),
            other,
        )),
    }
}

fn assistant_items(message: &Value, items: &mut Vec<Value>) {
    let text = content_text(message.get("content"));
    if !text.is_empty() {
        items.push(json!({
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text}],
        }));
    }
    let calls = message.get("tool_calls").and_then(Value::as_array);
    for call in calls.into_iter().flatten() {
        let function = call.get("function");
        items.push(json!({
            "type": "function_call",
            "call_id": call.get("id").cloned().unwrap_or_default(),
            "name": function.and_then(|f| f.get("name")).cloned().unwrap_or_default(),
            "arguments": function
                .and_then(|f| f.get("arguments"))
                .cloned()
                .unwrap_or_else(|| Value::String("{}".into())),
        }));
    }
}

fn tool_output(message: &Value) -> Value {
    json!({
        "type": "function_call_output",
        "call_id": message.get("tool_call_id").cloned().unwrap_or_default(),
        "output": content_text(message.get("content")),
    })
}

fn copy_tools(request: &Map<String, Value>, out: &mut Map<String, Value>) {
    let mut tools: Vec<Value> = request
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(translate_tool)
        .collect();
    if let Some(options) = request.get("web_search_options").and_then(Value::as_object) {
        let mut tool = Map::new();
        tool.insert("type".into(), Value::String("web_search".into()));
        for key in ["search_context_size", "user_location"] {
            if let Some(value) = options.get(key) {
                tool.insert(key.into(), value.clone());
            }
        }
        tools.push(Value::Object(tool));
    }
    if !tools.is_empty() {
        out.insert("tools".into(), Value::Array(tools));
    }
    if let Some(choice) = request.get("tool_choice") {
        out.insert("tool_choice".into(), translate_tool_choice(choice));
    }
    if let Some(parallel) = request.get("parallel_tool_calls") {
        out.insert("parallel_tool_calls".into(), parallel.clone());
    }
}

fn translate_tool(tool: &Value) -> Option<Value> {
    match tool.get("type").and_then(Value::as_str)? {
        "function" => {
            let function = tool.get("function")?;
            let mut out = Map::new();
            out.insert("type".into(), Value::String("function".into()));
            for key in ["name", "description", "parameters", "strict"] {
                if let Some(value) = function.get(key) {
                    out.insert(key.into(), value.clone());
                }
            }
            Some(Value::Object(out))
        }
        "custom" => {
            let custom = tool.get("custom")?;
            let mut out = Map::new();
            out.insert("type".into(), Value::String("custom".into()));
            for key in ["name", "description", "format"] {
                if let Some(value) = custom.get(key) {
                    out.insert(key.into(), value.clone());
                }
            }
            Some(Value::Object(out))
        }
        _ => None,
    }
}

fn translate_tool_choice(choice: &Value) -> Value {
    let name = choice
        .get("function")
        .and_then(|function| function.get("name"))
        .cloned();
    match name {
        Some(name) => json!({"type": "function", "name": name}),
        None => choice.clone(),
    }
}

fn copy_text_options(request: &Map<String, Value>, out: &mut Map<String, Value>) {
    let mut text = Map::new();
    if let Some(format) = request.get("response_format") {
        match format.get("type").and_then(Value::as_str) {
            Some("json_object") => {
                text.insert("format".into(), json!({"type": "json_object"}));
            }
            Some("json_schema") => {
                let mut out_format = Map::new();
                out_format.insert("type".into(), Value::String("json_schema".into()));
                if let Some(schema) = format.get("json_schema").and_then(Value::as_object) {
                    for key in ["name", "schema", "strict", "description"] {
                        if let Some(value) = schema.get(key) {
                            out_format.insert(key.into(), value.clone());
                        }
                    }
                }
                text.insert("format".into(), Value::Object(out_format));
            }
            _ => {}
        }
    }
    if let Some(verbosity) = request.get("verbosity") {
        text.insert("verbosity".into(), verbosity.clone());
    }
    if !text.is_empty() {
        out.insert("text".into(), Value::Object(text));
    }
}

fn copy_scalars(
    request: &Map<String, Value>,
    policy: &OpenaiOauthCompatPolicy,
    out: &mut Map<String, Value>,
) {
    let max_tokens = request
        .get("max_completion_tokens")
        .or_else(|| request.get("max_tokens"));
    if let Some(max_tokens) = max_tokens.filter(|value| !value.is_null()) {
        out.insert("max_output_tokens".into(), max_tokens.clone());
    }
    if let Some(effort) = request.get("reasoning_effort") {
        out.insert("reasoning".into(), json!({"effort": effort}));
    }
    if let Some(key) = request.get("prompt_cache_key") {
        out.insert("prompt_cache_key".into(), key.clone());
    }
    if let Some(tier) = request.get("service_tier").and_then(Value::as_str) {
        let sent = match tier {
            "auto" | "flex" if policy.service_tier == CoerceOrReject::Coerce => "default",
            other => other,
        };
        out.insert("service_tier".into(), Value::String(sent.to_owned()));
    }
}

/// One tool call being assembled for a choice.
#[derive(Clone, Debug)]
struct ToolCall {
    item_id: String,
    call_id: String,
    name: String,
    arguments: String,
}

/// Translates one backend Responses SSE stream into chat chunks for one choice.
#[derive(Clone, Debug)]
pub struct ChatStreamTranslator {
    id: String,
    created: i64,
    model: String,
    index: u32,
    stop: Vec<String>,
    role_sent: bool,
    held: String,
    content: String,
    refusal: String,
    tool_calls: Vec<ToolCall>,
    stopped: bool,
    finish_reason: Option<&'static str>,
    usage: Option<Value>,
    failure: Option<Value>,
}

impl ChatStreamTranslator {
    /// Translator for choice `index`.
    pub fn new(plan: &ChatPlan, completion_id: String, created: i64, index: u32) -> Self {
        Self {
            id: completion_id,
            created,
            model: plan.model.clone(),
            index,
            stop: plan.stop.clone(),
            role_sent: false,
            held: String::new(),
            content: String::new(),
            refusal: String::new(),
            tool_calls: Vec::new(),
            stopped: false,
            finish_reason: None,
            usage: None,
            failure: None,
        }
    }

    /// Feed one backend SSE event; returns the chunks to emit.
    pub fn push(&mut self, event: &Value) -> Vec<Value> {
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match kind {
            "response.output_text.delta" => {
                let delta = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.push_text(delta)
            }
            "response.refusal.delta" if !self.stopped => {
                let delta = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.refusal.push_str(delta);
                self.delta_chunk(json!({"refusal": delta}))
            }
            "response.output_item.added" if !self.stopped => self.push_tool_start(event),
            "response.function_call_arguments.delta" if !self.stopped => self.push_tool_args(event),
            "response.completed" | "response.incomplete" => {
                self.record_terminal(event);
                Vec::new()
            }
            "response.failed" | "error" => {
                self.failure = event
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .or_else(|| event.get("error"))
                    .cloned();
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// The output reached a stop sequence; the caller may end the upstream request.
    pub fn stopped(&self) -> bool {
        self.stopped
    }

    /// Error object from an `error` or `response.failed` event, if any.
    pub fn failure(&self) -> Option<&Value> {
        self.failure.as_ref()
    }

    /// Flush held text and emit the final chunk with `finish_reason`.
    pub fn finish(&mut self) -> Vec<Value> {
        let mut chunks = Vec::new();
        if !self.held.is_empty() {
            let text = std::mem::take(&mut self.held);
            chunks.extend(self.emit_text(&text));
        }
        let reason = self.finish_reason_value();
        chunks.push(self.chunk(json!({}), Some(reason)));
        chunks
    }

    /// Usage in chat shape, once the backend reported it.
    pub fn usage(&self) -> Option<Value> {
        self.usage.as_ref().map(chat_usage)
    }

    fn push_text(&mut self, delta: &str) -> Vec<Value> {
        if self.stopped || delta.is_empty() {
            return Vec::new();
        }
        if self.stop.is_empty() {
            return self.emit_text(delta);
        }
        self.held.push_str(delta);
        if let Some(cut) = earliest_stop(&self.held, &self.stop) {
            let text = self.held[..cut].to_owned();
            self.held.clear();
            self.stopped = true;
            return self.emit_text(&text);
        }
        let keep = longest_stop_prefix(&self.held, &self.stop);
        let ready = self.held[..self.held.len() - keep].to_owned();
        self.held.drain(..ready.len());
        self.emit_text(&ready)
    }

    fn emit_text(&mut self, text: &str) -> Vec<Value> {
        if text.is_empty() {
            return Vec::new();
        }
        self.content.push_str(text);
        self.delta_chunk(json!({"content": text}))
    }

    fn push_tool_start(&mut self, event: &Value) -> Vec<Value> {
        let Some(item) = event.get("item") else {
            return Vec::new();
        };
        if item.get("type").and_then(Value::as_str) != Some("function_call") {
            return Vec::new();
        }
        let call = ToolCall {
            item_id: string_field(item, "id"),
            call_id: string_field(item, "call_id"),
            name: string_field(item, "name"),
            arguments: string_field(item, "arguments"),
        };
        let index = self.tool_calls.len();
        let delta = json!({"tool_calls": [{
            "index": index,
            "id": call.call_id,
            "type": "function",
            "function": {"name": call.name, "arguments": call.arguments},
        }]});
        self.tool_calls.push(call);
        self.delta_chunk(delta)
    }

    fn push_tool_args(&mut self, event: &Value) -> Vec<Value> {
        let item_id = event
            .get("item_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let delta = event
            .get("delta")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(index) = self
            .tool_calls
            .iter()
            .position(|call| call.item_id == item_id)
        else {
            return Vec::new();
        };
        self.tool_calls[index].arguments.push_str(delta);
        self.delta_chunk(
            json!({"tool_calls": [{"index": index, "function": {"arguments": delta}}]}),
        )
    }

    fn record_terminal(&mut self, event: &Value) {
        let response = event.get("response");
        self.usage = response.and_then(|response| response.get("usage")).cloned();
        let reason = response
            .and_then(|response| response.get("incomplete_details"))
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str);
        self.finish_reason = match reason {
            Some("max_output_tokens") => Some("length"),
            Some("content_filter") => Some("content_filter"),
            _ => None,
        };
    }

    fn finish_reason_value(&self) -> &'static str {
        if self.stopped {
            "stop"
        } else if let Some(reason) = self.finish_reason {
            reason
        } else if !self.tool_calls.is_empty() {
            "tool_calls"
        } else {
            "stop"
        }
    }

    fn delta_chunk(&mut self, mut delta: Value) -> Vec<Value> {
        if !self.role_sent {
            self.role_sent = true;
            delta["role"] = Value::String("assistant".into());
        }
        vec![self.chunk(delta, None)]
    }

    fn chunk(&self, delta: Value, finish_reason: Option<&str>) -> Value {
        json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{
                "index": self.index,
                "delta": delta,
                "logprobs": null,
                "finish_reason": finish_reason,
            }],
        })
    }

    fn message(&self) -> Value {
        let mut message = Map::new();
        message.insert("role".into(), Value::String("assistant".into()));
        let content = if self.content.is_empty() && !self.tool_calls.is_empty() {
            Value::Null
        } else {
            Value::String(self.content.clone())
        };
        message.insert("content".into(), content);
        if !self.refusal.is_empty() {
            message.insert("refusal".into(), Value::String(self.refusal.clone()));
        }
        if !self.tool_calls.is_empty() {
            let calls = self
                .tool_calls
                .iter()
                .map(|call| {
                    json!({
                        "id": call.call_id,
                        "type": "function",
                        "function": {"name": call.name, "arguments": call.arguments},
                    })
                })
                .collect();
            message.insert("tool_calls".into(), Value::Array(calls));
        }
        Value::Object(message)
    }
}

fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// Byte offset of the earliest stop-sequence match in `text`.
fn earliest_stop(text: &str, stops: &[String]) -> Option<usize> {
    stops
        .iter()
        .filter_map(|stop| text.find(stop.as_str()))
        .min()
}

/// Length of the longest suffix of `text` that is a proper prefix of a stop
/// sequence; that suffix must be held back until more text arrives.
fn longest_stop_prefix(text: &str, stops: &[String]) -> usize {
    let mut keep = 0;
    for stop in stops {
        for (end, _) in stop.char_indices().skip(1) {
            if end > keep && text.ends_with(&stop[..end]) {
                keep = end;
            }
        }
    }
    keep
}

/// Responses usage to chat usage.
fn chat_usage(usage: &Value) -> Value {
    let number = |path: &[&str]| -> u64 {
        path.iter()
            .try_fold(usage, |value, key| value.get(*key))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    };
    json!({
        "prompt_tokens": number(&["input_tokens"]),
        "completion_tokens": number(&["output_tokens"]),
        "total_tokens": number(&["total_tokens"]),
        "prompt_tokens_details": {"cached_tokens": number(&["input_tokens_details", "cached_tokens"])},
        "completion_tokens_details": {
            "reasoning_tokens": number(&["output_tokens_details", "reasoning_tokens"]),
        },
    })
}

fn add_usage(total: &mut Value, usage: &Value) {
    for path in [
        &["prompt_tokens"][..],
        &["completion_tokens"],
        &["total_tokens"],
        &["prompt_tokens_details", "cached_tokens"],
        &["completion_tokens_details", "reasoning_tokens"],
    ] {
        let add = path
            .iter()
            .try_fold(usage, |value, key| value.get(*key))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let mut slot = &mut *total;
        for key in path {
            slot = &mut slot[*key];
        }
        *slot = Value::from(slot.as_u64().unwrap_or(0) + add);
    }
}

/// Assemble a non-stream `chat.completion` from finished per-choice translators.
pub fn chat_completion_from_streams(
    plan: &ChatPlan,
    completion_id: String,
    created: i64,
    choices: Vec<ChatStreamTranslator>,
) -> Value {
    let mut usage: Option<Value> = None;
    let mut out_choices = Vec::with_capacity(choices.len());
    for choice in &choices {
        if let Some(choice_usage) = choice.usage() {
            match usage.as_mut() {
                Some(total) => add_usage(total, &choice_usage),
                None => usage = Some(choice_usage),
            }
        }
        out_choices.push(json!({
            "index": choice.index,
            "message": choice.message(),
            "logprobs": null,
            "finish_reason": choice.finish_reason_value(),
        }));
    }
    let mut completion = json!({
        "id": completion_id,
        "object": "chat.completion",
        "created": created,
        "model": plan.model,
        "choices": out_choices,
    });
    if let Some(usage) = usage {
        completion["usage"] = usage;
    }
    completion
}

/// Final usage-only chunk for `stream_options.include_usage`.
pub fn usage_chunk(completion_id: &str, created: i64, model: &str, usage: &Value) -> Value {
    json!({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [],
        "usage": usage,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> OpenaiOauthCompatPolicy {
        OpenaiOauthCompatPolicy::default()
    }

    fn translate(body: Value) -> ChatRequestTranslation {
        translate_chat_request(&body, &policy()).expect("translates")
    }

    fn plan(stop: &[&str]) -> ChatPlan {
        ChatPlan {
            n: 1,
            stop: stop.iter().map(|stop| (*stop).to_owned()).collect(),
            stream: true,
            include_usage: false,
            model: "m".into(),
        }
    }

    fn delta(text: &str) -> Value {
        json!({"type": "response.output_text.delta", "delta": text})
    }

    fn completed(input: u64, output: u64) -> Value {
        json!({"type": "response.completed", "response": {"usage": {
            "input_tokens": input, "output_tokens": output, "total_tokens": input + output,
            "input_tokens_details": {"cached_tokens": 1},
            "output_tokens_details": {"reasoning_tokens": 2},
        }}})
    }

    fn content_of(chunks: &[Value]) -> String {
        chunks
            .iter()
            .filter_map(|chunk| chunk["choices"][0]["delta"]["content"].as_str())
            .collect()
    }

    #[test]
    fn messages_map_to_responses_input() {
        let out = translate(json!({
            "model": "m",
            "messages": [
                {"role": "system", "content": "be brief"},
                {"role": "user", "content": [
                    {"type": "text", "text": "hi"},
                    {"type": "image_url", "image_url": {"url": "data:x", "detail": "low"}},
                ]},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{}"}},
                ]},
                {"role": "tool", "tool_call_id": "c1", "content": "42"},
            ],
        }));
        let body = out.responses_body;
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        let input = body["input"].as_array().expect("input list");
        assert_eq!(input[0]["role"], "developer");
        assert_eq!(input[1]["content"][1]["type"], "input_image");
        assert_eq!(input[1]["content"][1]["detail"], "low");
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[2]["call_id"], "c1");
        assert_eq!(
            input[3],
            json!({"type": "function_call_output", "call_id": "c1", "output": "42"})
        );
    }

    #[test]
    fn tools_formats_and_limits_map_to_responses_fields() {
        let out = translate(json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"type": "function", "function": {"name": "f", "parameters": {}}}],
            "tool_choice": {"type": "function", "function": {"name": "f"}},
            "response_format": {"type": "json_schema", "json_schema": {"name": "s", "schema": {}}},
            "max_tokens": 10,
            "max_completion_tokens": 20,
            "reasoning_effort": "low",
            "service_tier": "flex",
        }));
        let body = out.responses_body;
        assert_eq!(
            body["tools"][0],
            json!({"type": "function", "name": "f", "parameters": {}})
        );
        assert_eq!(
            body["tool_choice"],
            json!({"type": "function", "name": "f"})
        );
        assert_eq!(body["text"]["format"]["type"], "json_schema");
        assert_eq!(body["max_output_tokens"], 20);
        assert_eq!(body["reasoning"]["effort"], "low");
        assert_eq!(body["service_tier"], "default");
    }

    #[test]
    fn default_policy_drops_unsupported_controls_and_plans_n_and_stop() {
        let out = translate(json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "temperature": 0.2, "seed": 1, "presence_penalty": 1, "metadata": {"a": "b"},
            "n": 2, "stop": ["END"], "stream": true,
            "stream_options": {"include_usage": true},
        }));
        for key in [
            "temperature",
            "seed",
            "presence_penalty",
            "metadata",
            "n",
            "stop",
        ] {
            assert!(
                out.responses_body.get(key).is_none(),
                "{key} must not be forwarded"
            );
        }
        assert_eq!(out.plan.n, 2);
        assert_eq!(out.plan.stop, vec!["END".to_owned()]);
        assert!(out.plan.stream && out.plan.include_usage);
    }

    #[test]
    fn reject_policies_name_the_parameter() {
        let strict = OpenaiOauthCompatPolicy {
            sampling: DropOrReject::Reject,
            chat_n: EmulateOrReject::Reject,
            chat_stop: EmulateDropOrReject::Reject,
            chat_seed_penalties: DropOrReject::Reject,
            ..OpenaiOauthCompatPolicy::default()
        };
        let base = json!({"model": "m", "messages": []});
        for (key, value, param) in [
            ("temperature", json!(0.1), "temperature"),
            ("seed", json!(1), "seed"),
            ("stop", json!("x"), "stop"),
            ("n", json!(2), "n"),
        ] {
            let mut body = base.clone();
            body[key] = value;
            let error = translate_chat_request(&body, &strict).expect_err("rejects");
            assert_eq!(error.param, param);
        }
        let mut audio = base.clone();
        audio["modalities"] = json!(["text", "audio"]);
        assert_eq!(
            translate_chat_request(&audio, &policy()).unwrap_err().param,
            "modalities"
        );
        let mut too_many = base;
        too_many["n"] = json!(9);
        assert_eq!(
            translate_chat_request(&too_many, &policy())
                .unwrap_err()
                .param,
            "n"
        );
    }

    #[test]
    fn stream_emits_role_then_text_then_finish() {
        let mut translator = ChatStreamTranslator::new(&plan(&[]), "id".into(), 7, 0);
        let mut chunks = translator.push(&delta("Hel"));
        chunks.extend(translator.push(&delta("lo")));
        chunks.extend(translator.push(&completed(3, 2)));
        chunks.extend(translator.finish());
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(content_of(&chunks), "Hello");
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "stop"
        );
        assert_eq!(translator.usage().unwrap()["prompt_tokens"], 3);
    }

    #[test]
    fn stop_sequence_spanning_deltas_cuts_exactly() {
        let mut translator = ChatStreamTranslator::new(&plan(&["STOP"]), "id".into(), 0, 0);
        let mut chunks = translator.push(&delta("abc ST"));
        assert!(!translator.stopped());
        chunks.extend(translator.push(&delta("OP tail")));
        assert!(translator.stopped());
        chunks.extend(translator.push(&delta("more")));
        chunks.extend(translator.finish());
        assert_eq!(content_of(&chunks), "abc ");
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "stop"
        );
    }

    #[test]
    fn held_prefix_is_flushed_when_no_stop_follows() {
        let mut translator = ChatStreamTranslator::new(&plan(&["STOP"]), "id".into(), 0, 0);
        let mut chunks = translator.push(&delta("abc ST"));
        chunks.extend(translator.finish());
        assert_eq!(content_of(&chunks), "abc ST");
    }

    #[test]
    fn tool_calls_stream_with_stable_index() {
        let mut translator = ChatStreamTranslator::new(&plan(&[]), "id".into(), 0, 0);
        let mut chunks = translator.push(&json!({"type": "response.output_item.added", "item": {
            "type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "f", "arguments": "",
        }}));
        chunks.extend(translator.push(&json!({
            "type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": "{\"a\":1}",
        })));
        chunks.extend(translator.finish());
        let first = &chunks[0]["choices"][0]["delta"]["tool_calls"][0];
        assert_eq!(first["id"], "call_1");
        assert_eq!(first["function"]["name"], "f");
        assert_eq!(
            chunks[1]["choices"][0]["delta"]["tool_calls"][0]["index"],
            0
        );
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "tool_calls"
        );
    }

    #[test]
    fn incomplete_max_output_tokens_finishes_with_length() {
        let mut translator = ChatStreamTranslator::new(&plan(&[]), "id".into(), 0, 0);
        translator.push(&json!({"type": "response.incomplete", "response": {
            "incomplete_details": {"reason": "max_output_tokens"},
        }}));
        let chunks = translator.finish();
        assert_eq!(
            chunks.last().unwrap()["choices"][0]["finish_reason"],
            "length"
        );
    }

    #[test]
    fn non_stream_merge_sums_usage_across_choices() {
        let plan = ChatPlan {
            n: 2,
            stream: false,
            ..plan(&[])
        };
        let mut choices = Vec::new();
        for (index, text) in ["a", "b"].into_iter().enumerate() {
            let mut translator = ChatStreamTranslator::new(&plan, "id".into(), 0, index as u32);
            translator.push(&delta(text));
            translator.push(&completed(3, 2));
            translator.finish();
            choices.push(translator);
        }
        let completion = chat_completion_from_streams(&plan, "id".into(), 0, choices);
        assert_eq!(completion["object"], "chat.completion");
        assert_eq!(completion["choices"][1]["message"]["content"], "b");
        assert_eq!(completion["usage"]["prompt_tokens"], 6);
        assert_eq!(
            completion["usage"]["completion_tokens_details"]["reasoning_tokens"],
            4
        );
    }

    #[test]
    fn usage_chunk_has_no_choices() {
        let chunk = usage_chunk("id", 1, "m", &json!({"total_tokens": 3}));
        assert_eq!(chunk["choices"], json!([]));
        assert_eq!(chunk["usage"]["total_tokens"], 3);
    }
}
