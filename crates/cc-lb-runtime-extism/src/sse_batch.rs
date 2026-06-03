use std::sync::{Arc, Mutex};
use std::time::Instant;

use cc_lb_plugin_api::{ObservabilityError, ObservabilityHook, ObserveEvent};
use cc_lb_plugin_wire::v1::ObserveEventWire;
use cc_lb_plugin_wire::v1::observe::{ObserveFn, ObserveRequest};

use crate::dispatch::DispatchOutcome;
use crate::plugin_wrap::upstream_to_wire;
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
        let event_count = events.len();
        let request = ObserveRequest { events };
        match self.slot.dispatch_wire_call_sync::<ObserveFn>(request) {
            DispatchOutcome::Ok(_) => Ok(()),
            DispatchOutcome::Fallback(_) => {
                record_observe_batch_drop(event_count);
                Ok(())
            }
        }
    }
}

impl ObservabilityHook for ExtismObservabilityHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        let mut ready = None;
        {
            let mut batch = self.batch.lock().map_err(|_| ObservabilityError::Dropped {
                reason: "observability batch lock poisoned".to_owned(),
            })?;
            batch.events.push(observe_event_to_wire(event));
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

fn observe_event_to_wire(value: ObserveEvent) -> ObserveEventWire {
    match value {
        ObserveEvent::RequestStarted {
            request_id,
            downstream_user_agent,
        } => ObserveEventWire::RequestStarted {
            request_id,
            downstream_user_agent,
        },
        ObserveEvent::AuthnComplete { principal_id, kind } => ObserveEventWire::AuthnComplete {
            principal_id,
            principal_kind: serde_json::to_value(kind)
                .ok()
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| "unknown".to_owned()),
        },
        ObserveEvent::UpstreamChosen { upstream } => ObserveEventWire::UpstreamChosen {
            upstream: upstream_to_wire(&upstream),
        },
        ObserveEvent::Chunk {
            batch_index,
            event_count,
            total_bytes,
        } => ObserveEventWire::Chunk {
            batch_index,
            event_count,
            total_bytes,
        },
        ObserveEvent::RequestFinished {
            status,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            duration_ms,
        } => ObserveEventWire::RequestFinished {
            status: status.as_u16(),
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            duration_ms,
        },
        ObserveEvent::Error {
            code,
            message,
            source,
        } => ObserveEventWire::Error {
            code,
            message,
            source,
        },
    }
}

fn record_observe_batch_drop(event_count: usize) {
    metrics::counter!(
        "cc_lb_plugin_observe_batches_dropped_total",
        "reason" => "dispatch_fallback",
    )
    .increment(1);
    metrics::counter!(
        "cc_lb_plugin_observe_events_dropped_total",
        "reason" => "dispatch_fallback",
    )
    .increment(event_count as u64);
}
