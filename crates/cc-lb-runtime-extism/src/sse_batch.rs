use std::sync::{Arc, Mutex};
use std::time::Instant;

use cc_lb_plugin_api::{ObservabilityError, ObservabilityHook, ObserveEvent};
use serde::Serialize;
use serde_json::json;

use crate::plugin_wrap::parse_versioned;
use crate::{PluginSlot, ResourceLimits};

pub(crate) struct ExtismObservabilityHook {
    slot: Arc<PluginSlot>,
    limits: ResourceLimits,
    batch: Mutex<BatchState>,
}

impl ExtismObservabilityHook {
    pub(crate) fn new(slot: Arc<PluginSlot>, limits: ResourceLimits) -> Self {
        Self {
            slot,
            limits,
            batch: Mutex::new(BatchState::new()),
        }
    }

    fn flush_events(&self, events: Vec<ObserveEventWire>) -> Result<(), ObservabilityError> {
        if events.is_empty() {
            return Ok(());
        }
        let response = self
            .slot
            .call_value_sync(
                "observe",
                json!({
                    "_version": 1,
                    "events": events,
                }),
            )
            .map_err(|source| ObservabilityError::Dropped {
                reason: source.to_string(),
            })?;
        parse_versioned::<ObserveResponse>(response)
            .map_err(|reason| ObservabilityError::Dropped { reason })?;
        Ok(())
    }
}

impl ObservabilityHook for ExtismObservabilityHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        let mut ready = None;
        {
            let mut batch = self.batch.lock().map_err(|_| ObservabilityError::Dropped {
                reason: "observability batch lock poisoned".to_owned(),
            })?;
            batch.events.push(ObserveEventWire::from(event));
            if batch.events.len() >= self.limits.observe_batch_count
                || batch.started.elapsed() >= self.limits.observe_flush_interval
            {
                ready = Some(batch.take());
            }
        }
        if let Some(events) = ready {
            self.flush_events(events)?;
        }
        Ok(())
    }
}

impl Drop for ExtismObservabilityHook {
    fn drop(&mut self) {
        let events = self.batch.lock().ok().map(|mut batch| batch.take());
        if let Some(events) = events {
            let _ = self.flush_events(events);
        }
    }
}

struct BatchState {
    events: Vec<ObserveEventWire>,
    started: Instant,
}

impl BatchState {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            started: Instant::now(),
        }
    }

    fn take(&mut self) -> Vec<ObserveEventWire> {
        self.started = Instant::now();
        std::mem::take(&mut self.events)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum ObserveEventWire {
    RequestStarted {
        request_id: String,
        downstream_user_agent: Option<String>,
    },
    AuthnComplete {
        principal_id: String,
        principal_kind: String,
    },
    UpstreamChosen {
        upstream: cc_lb_plugin_api::Upstream,
    },
    Chunk {
        batch_index: u64,
        event_count: usize,
        total_bytes: usize,
    },
    RequestFinished {
        status: u16,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        duration_ms: u64,
    },
    Error {
        code: String,
        message: String,
        source: String,
    },
}

impl From<ObserveEvent> for ObserveEventWire {
    fn from(value: ObserveEvent) -> Self {
        match value {
            ObserveEvent::RequestStarted {
                request_id,
                downstream_user_agent,
            } => Self::RequestStarted {
                request_id,
                downstream_user_agent,
            },
            ObserveEvent::AuthnComplete { principal_id, kind } => Self::AuthnComplete {
                principal_id,
                principal_kind: serde_json::to_value(kind)
                    .ok()
                    .and_then(|value| value.as_str().map(ToOwned::to_owned))
                    .unwrap_or_else(|| "unknown".to_owned()),
            },
            ObserveEvent::UpstreamChosen { upstream } => Self::UpstreamChosen { upstream },
            ObserveEvent::Chunk {
                batch_index,
                event_count,
                total_bytes,
            } => Self::Chunk {
                batch_index,
                event_count,
                total_bytes,
            },
            ObserveEvent::RequestFinished {
                status,
                input_tokens,
                output_tokens,
                duration_ms,
            } => Self::RequestFinished {
                status: status.as_u16(),
                input_tokens,
                output_tokens,
                duration_ms,
            },
            ObserveEvent::Error {
                code,
                message,
                source,
            } => Self::Error {
                code,
                message,
                source,
            },
        }
    }
}

#[derive(serde::Deserialize)]
struct ObserveResponse {}
