use std::convert::Infallible;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::modes::FakeMode;
use crate::routes::with_fixture_headers;
use crate::weather::RequestWeather;

pub(crate) fn streaming_response(
    model: String,
    mode: FakeMode,
    slow_mode_bps: u64,
    weather: RequestWeather,
) -> Response {
    let stream = async_stream::stream! {
        let mut event_index = 0_u64;
        let mut delta_index = 0_u64;
        for (event_name, data) in stream_items(&model, mode, weather.delta_count()) {
            let weather_delay = weather.delay_for_event(event_index, event_name, delta_index);
            if weather_delay > Duration::ZERO {
                sleep(weather_delay).await;
            }
            delay_if_slow(mode, slow_mode_bps, event_name, &data).await;
            yield Ok::<_, Infallible>(Event::default().event(event_name).data(data));
            event_index = event_index.saturating_add(1);
            if event_name == "content_block_delta" {
                delta_index = delta_index.saturating_add(1);
            }
        }
    };

    let response = Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    with_fixture_headers(response)
}
pub(crate) fn opencode_tools_response(model: String, request: &Value) -> Response {
    const TASK_TOOL_ID: &str = "toolu_opencode_task";

    let wants_subagent = request.get("system").is_some_and(|value| {
        content_starts_with(
            value,
            "You are OpenCode, the best coding agent on the planet.",
        )
    }) && request_user_contains(request, "Use the task tool")
        && has_tool(request, "task");
    let items = if wants_subagent && !has_tool_result(request, TASK_TOOL_ID) {
        tool_stream_items(
            &model,
            TASK_TOOL_ID,
            "task",
            json!({
                "description": "Inspect child request",
                "prompt": "Reply with CHILD only.",
                "subagent_type": "general"
            }),
        )
    } else {
        text_stream_items(&model, "fake anthropic fixture response HELLO")
    };

    immediate_streaming_response(items)
}

pub(crate) fn senpi_tools_response(
    model: String,
    request: &Value,
    image_path: Option<&str>,
) -> Response {
    const SUBAGENT_TOOL_ID: &str = "toolu_senpi_subagent";
    const LOOK_AT_TOOL_ID: &str = "toolu_senpi_look_at";

    let system = request.get("system");
    let first_user = request
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|messages| {
            messages
                .iter()
                .find(|message| message.get("role") == Some(&json!("user")))
        })
        .and_then(|message| message.get("content"));
    let is_subagent = system
        .is_some_and(|value| content_starts_with(value, "You are senpi, a coding agent."))
        && first_user.is_some_and(|value| content_starts_with(value, "Task: "));
    let is_look_at = request.get("model").and_then(Value::as_str) == Some("senpi-vision")
        || system.is_some_and(|value| {
            content_starts_with(
                value,
                "You analyze attached media for a downstream agent that cannot inspect the attachments directly.",
            )
        });

    let items = if is_subagent || is_look_at || has_tool_result(request, LOOK_AT_TOOL_ID) {
        text_stream_items(&model, "fake anthropic fixture response HELLO")
    } else if has_tool_result(request, SUBAGENT_TOOL_ID) {
        let input = image_path.map_or_else(
            || {
                json!({
                    "image_data": "iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEklEQVR4nGP4z8DAAMIM/4EAAB/uBfsL2WiLAAAAAElFTkSuQmCC",
                    "goal": "Read the image and answer HELLO"
                })
            },
            |path| {
                json!({
                    "file_path": path,
                    "goal": "Read the image and answer HELLO"
                })
            },
        );
        tool_stream_items(&model, LOOK_AT_TOOL_ID, "look_at", input)
    } else {
        tool_stream_items(
            &model,
            SUBAGENT_TOOL_ID,
            "subagent",
            json!({
                "agent": "reviewer",
                "task": "Print the word HELLO",
                "agentScope": "user"
            }),
        )
    };

    immediate_streaming_response(items)
}

fn content_starts_with(content: &Value, prefix: &str) -> bool {
    match content {
        Value::String(text) => text.starts_with(prefix),
        Value::Array(blocks) => blocks.iter().any(|block| {
            block
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.starts_with(prefix))
        }),
        _ => false,
    }
}

fn request_user_contains(request: &Value, marker: &str) -> bool {
    request
        .get("messages")
        .and_then(Value::as_array)
        .is_some_and(|messages| {
            messages.iter().any(|message| {
                message.get("role").and_then(Value::as_str) == Some("user")
                    && message
                        .get("content")
                        .is_some_and(|content| content_contains(content, marker))
            })
        })
}

fn content_contains(content: &Value, marker: &str) -> bool {
    match content {
        Value::String(text) => text.contains(marker),
        Value::Array(blocks) => blocks.iter().any(|block| {
            block
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.contains(marker))
        }),
        _ => false,
    }
}

fn has_tool(request: &Value, tool_name: &str) -> bool {
    request
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| {
            tools
                .iter()
                .any(|tool| tool.get("name").and_then(Value::as_str) == Some(tool_name))
        })
}

fn has_tool_result(request: &Value, tool_use_id: &str) -> bool {
    request
        .get("messages")
        .and_then(Value::as_array)
        .is_some_and(|messages| {
            messages.iter().any(|message| {
                message
                    .get("content")
                    .and_then(Value::as_array)
                    .is_some_and(|blocks| {
                        blocks.iter().any(|block| {
                            block.get("type").and_then(Value::as_str) == Some("tool_result")
                                && block.get("tool_use_id").and_then(Value::as_str)
                                    == Some(tool_use_id)
                        })
                    })
            })
        })
}

fn immediate_streaming_response(items: Vec<(&'static str, String)>) -> Response {
    let stream = async_stream::stream! {
        for (event_name, data) in items {
            yield Ok::<_, Infallible>(Event::default().event(event_name).data(data));
        }
    };
    with_fixture_headers(
        Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response(),
    )
}

fn text_stream_items(model: &str, text: &str) -> Vec<(&'static str, String)> {
    vec![
        message_start(model),
        (
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""}
            })
            .to_string(),
        ),
        (
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text}
            })
            .to_string(),
        ),
        content_block_stop(),
        message_delta("end_turn"),
        message_stop(),
    ]
}

fn tool_stream_items(
    model: &str,
    tool_use_id: &str,
    name: &str,
    input: Value,
) -> Vec<(&'static str, String)> {
    vec![
        message_start(model),
        (
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "tool_use",
                    "id": tool_use_id,
                    "name": name,
                    "input": {}
                }
            })
            .to_string(),
        ),
        (
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": input.to_string()
                }
            })
            .to_string(),
        ),
        content_block_stop(),
        message_delta("tool_use"),
        message_stop(),
    ]
}

fn message_start(model: &str) -> (&'static str, String) {
    (
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_fake_000000000000000000000000",
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "stop_reason": null,
                "stop_sequence": null,
                "usage": {"input_tokens": 100, "output_tokens": 0}
            }
        })
        .to_string(),
    )
}

fn content_block_stop() -> (&'static str, String) {
    (
        "content_block_stop",
        json!({"type": "content_block_stop", "index": 0}).to_string(),
    )
}

fn message_delta(stop_reason: &str) -> (&'static str, String) {
    (
        "message_delta",
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason, "stop_sequence": null},
            "usage": {"input_tokens": 100, "output_tokens": 50}
        })
        .to_string(),
    )
}

fn message_stop() -> (&'static str, String) {
    ("message_stop", json!({"type": "message_stop"}).to_string())
}

fn stream_items(model: &str, mode: FakeMode, delta_count: u64) -> Vec<(&'static str, String)> {
    let mut items = Vec::new();
    items.push((
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_fake_000000000000000000000000",
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "stop_reason": null,
                "stop_sequence": null,
                "usage": {"input_tokens": 100, "output_tokens": 0}
            }
        })
        .to_string(),
    ));
    items.push((
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        })
        .to_string(),
    ));
    for index in 0..delta_count {
        if mode == FakeMode::TruncateMidStream && index == 3 {
            return items;
        }
        if mode == FakeMode::TamperUnknownEvent && index == 10 {
            items.push(("foo", json!({"x": 1}).to_string()));
        }
        let text = if index == 0 {
            "fake anthropic fixture response HELLO ".to_owned()
        } else {
            format!("fixture-delta-{index:02} ")
        };
        items.push((
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text}
            })
            .to_string(),
        ));
    }
    items.push((
        "content_block_stop",
        json!({"type": "content_block_stop", "index": 0}).to_string(),
    ));
    items.push((
        "message_delta",
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"input_tokens": 100, "output_tokens": 50}
        })
        .to_string(),
    ));
    items.push(("message_stop", json!({"type": "message_stop"}).to_string()));
    items
}

async fn delay_if_slow(mode: FakeMode, slow_mode_bps: u64, event_name: &str, data: &str) {
    if mode != FakeMode::Slow {
        return;
    }
    let bps = slow_mode_bps.max(1);
    let encoded_len = event_name.len() + data.len() + "event: \ndata: \n\n".len();
    let millis = ((encoded_len as u64 * 1_000) / bps).saturating_add(50);
    sleep(Duration::from_millis(millis.max(1))).await;
}
