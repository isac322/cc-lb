use std::time::Duration;

use serde_json::Value;

#[derive(Clone, Debug, Default)]
pub struct SseSummary {
    pub ttft_ms: Option<u64>,
    pub first_delta_ms: Option<u64>,
    pub inter_delta_gaps_ms: Vec<u64>,
    pub final_response: bool,
    pub malformed_sse: bool,
    pub malformed_json: bool,
    pub unexpected_truncation: bool,
}

pub struct SseTiming {
    pending: Vec<u8>,
    max_pending_bytes: usize,
    last_delta_ms: Option<u64>,
    summary: SseSummary,
}

impl SseTiming {
    pub fn new(max_pending_bytes: usize) -> Self {
        Self {
            pending: Vec::new(),
            max_pending_bytes,
            last_delta_ms: None,
            summary: SseSummary::default(),
        }
    }

    pub fn push(&mut self, bytes: &[u8], elapsed: Duration) {
        if self.pending.len().saturating_add(bytes.len()) > self.max_pending_bytes {
            self.pending.clear();
            self.summary.malformed_sse = true;
            return;
        }
        self.pending.extend_from_slice(bytes);
        while let Some(end) = frame_end(&self.pending) {
            let frame = self.pending.drain(..end).collect::<Vec<_>>();
            self.observe_frame(&frame, duration_ms(elapsed));
        }
    }

    pub fn finish(mut self) -> SseSummary {
        if !self.pending.is_empty() {
            self.summary.malformed_sse = true;
        }
        self.summary.unexpected_truncation = !self.summary.final_response;
        self.summary
    }

    fn observe_frame(&mut self, frame: &[u8], elapsed_ms: u64) {
        let Ok(text) = std::str::from_utf8(frame) else {
            self.summary.malformed_sse = true;
            return;
        };
        let event_name = text
            .lines()
            .find_map(|line| line.strip_prefix("event:"))
            .map(str::trim);
        let data = text
            .lines()
            .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() {
            self.summary.malformed_sse = true;
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(&data) else {
            self.summary.malformed_json = true;
            return;
        };
        let kind = event_name.or_else(|| value.get("type").and_then(Value::as_str));
        match kind {
            Some("content_block_delta") => self.record_delta(elapsed_ms),
            Some("message_stop") => self.summary.final_response = true,
            Some(_) => {}
            None => self.summary.malformed_sse = true,
        }
    }

    fn record_delta(&mut self, elapsed_ms: u64) {
        if self.summary.ttft_ms.is_none() {
            self.summary.ttft_ms = Some(elapsed_ms);
            self.summary.first_delta_ms = Some(elapsed_ms);
        }
        if let Some(previous) = self.last_delta_ms {
            self.summary
                .inter_delta_gaps_ms
                .push(elapsed_ms.saturating_sub(previous));
        }
        self.last_delta_ms = Some(elapsed_ms);
    }
}

fn frame_end(buffer: &[u8]) -> Option<usize> {
    let mut index = 0;
    while index < buffer.len() {
        if buffer[index] == b'\n' && buffer.get(index + 1) == Some(&b'\n') {
            return Some(index + 2);
        }
        if buffer[index] == b'\r' {
            if buffer.get(index + 1) == Some(&b'\r') {
                return Some(index + 2);
            }
            if buffer.get(index + 1) == Some(&b'\n')
                && buffer.get(index + 2) == Some(&b'\r')
                && buffer.get(index + 3) == Some(&b'\n')
            {
                return Some(index + 4);
            }
        }
        index += 1;
    }
    None
}

pub fn duration_ms(value: Duration) -> u64 {
    value.as_millis().min(u128::from(u64::MAX)) as u64
}
