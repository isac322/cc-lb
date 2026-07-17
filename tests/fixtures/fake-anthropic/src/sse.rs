use std::convert::Infallible;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde_json::json;
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
