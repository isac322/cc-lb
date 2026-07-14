use std::{collections::HashMap, time::Duration, time::Instant};

use cc_lb_lifecycle::{
    EventId, LifecycleEvent, LimitDecisionKind, TerminationReason, UsageSnapshot,
};

use crate::{
    schema::{CapturedResponse, Disposition},
    sink::CaptureSink,
};

const CLIENT_CLOSED_STATUS: u16 = 499;

pub(super) struct ResponseJoiner {
    sink: CaptureSink,
    partials: HashMap<EventId, PendingResponseState>,
    map_cap: usize,
}

impl ResponseJoiner {
    pub(super) fn new(sink: CaptureSink, map_cap: usize) -> Self {
        Self {
            sink,
            partials: HashMap::new(),
            map_cap: map_cap.max(1),
        }
    }

    pub(super) fn handle_event(&mut self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::RequestStarted {
                event_id,
                request_id,
                ts_ms,
                ..
            } => {
                let _ = self.sink.try_seed(event_id.clone(), request_id, ts_ms);
                self.partials
                    .insert(event_id, PendingResponseState::new(Instant::now()));
                self.enforce_cap();
            }
            LifecycleEvent::RouteCompleted {
                event_id,
                result: Ok(route),
                ..
            } => self.update(&event_id, |partial| {
                partial.chosen_upstream_id = Some(route.upstream_id);
            }),
            LifecycleEvent::LimitDecision { event_id, decision } => match decision {
                LimitDecisionKind::Rejected { .. } => self.update(&event_id, |partial| {
                    partial.limit_denied = true;
                }),
                LimitDecisionKind::Reserved { .. } => {}
                // LimitDecisionKind is non-exhaustive; unknown outcomes carry no known deny signal.
                _ => {}
            },
            LifecycleEvent::UpstreamAttempt {
                event_id,
                attempt_num,
                ..
            } => self.update(&event_id, |partial| {
                partial.attempt_num = partial.attempt_num.max(attempt_num);
            }),
            LifecycleEvent::UpstreamResponseStarted {
                event_id, status, ..
            } => self.update(&event_id, |partial| {
                partial.latest_upstream_status = Some(status);
            }),
            LifecycleEvent::UsageObserved {
                event_id, usage, ..
            } => self.update(&event_id, |partial| {
                partial.usage = Some(usage);
            }),
            LifecycleEvent::StreamCompleted {
                event_id,
                result: Ok(success),
            } => self.update(&event_id, |partial| {
                partial.usage = Some(success.usage);
            }),
            LifecycleEvent::RequestTerminated {
                event_id,
                reason,
                client_status,
                duration_ms,
                ..
            } => self.finalize(event_id, &reason, client_status, duration_ms),
            LifecycleEvent::ParseCompleted { .. }
            | LifecycleEvent::AuthCompleted { .. }
            | LifecycleEvent::AuthenticationCompleted { .. }
            | LifecycleEvent::RouteCompleted { .. }
            | LifecycleEvent::ProviderErrorObserved { .. }
            | LifecycleEvent::RequestLogUpstreamErrorObserved { .. }
            | LifecycleEvent::StreamCompleted { .. }
            | LifecycleEvent::Priced { .. }
            | LifecycleEvent::CacheObserved { .. }
            | LifecycleEvent::PromptCacheObservationsProduced { .. } => {}
            // LifecycleEvent is non-exhaustive; future variants are ignored until mapped explicitly.
            _ => {}
        }
    }

    pub(super) fn sweep_orphans(&mut self, ttl: Duration) {
        let now = Instant::now();
        self.partials
            .retain(|_, partial| now.saturating_duration_since(partial.updated_at) < ttl);
    }

    fn update(&mut self, event_id: &str, update: impl FnOnce(&mut PendingResponseState)) {
        if let Some(partial) = self.partials.get_mut(event_id) {
            partial.updated_at = Instant::now();
            update(partial);
        }
    }

    fn finalize(
        &mut self,
        event_id: EventId,
        reason: &TerminationReason,
        client_status: u16,
        duration_ms: u64,
    ) {
        let Some(partial) = self.partials.remove(&event_id) else {
            return;
        };
        let disposition = partial.disposition(reason, client_status);
        let response = partial.into_response(client_status, duration_ms);
        let _ = self
            .sink
            .try_response(event_id, response, disposition, true);
    }

    fn enforce_cap(&mut self) {
        if self.partials.len() <= self.map_cap {
            return;
        }
        let oldest = self
            .partials
            .iter()
            .min_by_key(|(_, partial)| partial.updated_at)
            .map(|(event_id, _)| event_id.clone());
        if let Some(event_id) = oldest {
            self.partials.remove(&event_id);
        }
    }
}

struct PendingResponseState {
    updated_at: Instant,
    chosen_upstream_id: Option<uuid::Uuid>,
    latest_upstream_status: Option<u16>,
    attempt_num: u32,
    usage: Option<UsageSnapshot>,
    limit_denied: bool,
}

impl PendingResponseState {
    fn new(now: Instant) -> Self {
        Self {
            updated_at: now,
            chosen_upstream_id: None,
            latest_upstream_status: None,
            attempt_num: 0,
            usage: None,
            limit_denied: false,
        }
    }

    fn disposition(&self, reason: &TerminationReason, client_status: u16) -> Disposition {
        if self.limit_denied {
            Disposition::RoutedLimitRejected
        } else if client_status == CLIENT_CLOSED_STATUS {
            Disposition::RoutedClientDisconnected
        } else if self.attempt_num == 0 {
            Disposition::RoutedPreDispatchError
        } else if matches!(reason, TerminationReason::Success) {
            Disposition::RoutedDispatchedSuccess
        } else {
            Disposition::RoutedDispatchedError
        }
    }

    fn into_response(self, client_status: u16, duration_ms: u64) -> CapturedResponse {
        let (input_tokens, output_tokens, cache_read, cache_creation_5m, cache_creation_1h) =
            self.usage.map_or((None, None, None, None, None), |usage| {
                (
                    Some(usage.input_tokens),
                    Some(usage.output_tokens),
                    Some(usage.cache_read_input_tokens),
                    Some(usage.cache_creation_input_tokens_5m),
                    Some(usage.cache_creation_input_tokens_1h),
                )
            });
        CapturedResponse {
            input_tokens,
            output_tokens,
            cache_read_input_tokens: cache_read,
            cache_creation_input_tokens_5m: cache_creation_5m,
            cache_creation_input_tokens_1h: cache_creation_1h,
            chosen_upstream_id: self.chosen_upstream_id,
            upstream_status: self.latest_upstream_status,
            client_status: Some(client_status),
            duration_ms: Some(duration_ms),
            attempt_num: Some(self.attempt_num),
        }
    }
}
