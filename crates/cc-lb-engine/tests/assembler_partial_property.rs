use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cc_lb_contract::{BusReceiver, RequestEventBus};
use cc_lb_engine::InMemoryBus;
use cc_lb_lifecycle::{
    EventId, LifecycleEvent, ParseInfo, RouteInfo, StreamSuccess, TerminationReason, UsageSnapshot,
    UsageSource,
};
use cc_lb_observability::NoopMetricsHook;
use cc_lb_request_log::{HeaderSnapshot, RequestEventUpdate};
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, RequestEventStreamFilters, StorageResult,
};
use proptest::prelude::*;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Default)]
struct PropertyStore {
    state: Mutex<PropertyStoreState>,
}

#[derive(Default)]
struct PropertyStoreState {
    rows: Vec<(u64, RequestEvent)>,
    cursors_by_event_id: HashMap<String, u64>,
    next_cursor: u64,
}

impl PropertyStore {
    fn rows(&self) -> Vec<RequestEvent> {
        self.state
            .lock()
            .expect("property store lock")
            .rows
            .iter()
            .map(|(_, event)| event.clone())
            .collect()
    }
}

#[async_trait]
impl RequestEventStore for PropertyStore {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<u64> {
        let mut state = self.state.lock().expect("property store lock");
        if let Some(event_id) = event.event_id.as_deref()
            && let Some(cursor) = state.cursors_by_event_id.get(event_id)
        {
            return Ok(*cursor);
        }
        state.next_cursor += 1;
        let cursor = state.next_cursor;
        if let Some(event_id) = event.event_id.as_deref() {
            state
                .cursors_by_event_id
                .insert(event_id.to_owned(), cursor);
        }
        state.rows.push((cursor, event.clone()));
        Ok(cursor)
    }

    async fn query_request_events(
        &self,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        Ok(Vec::new())
    }

    async fn current_request_event_cursor(&self) -> StorageResult<u64> {
        Ok(self.state.lock().expect("property store lock").next_cursor)
    }

    async fn query_request_events_between_cursors(
        &self,
        after: u64,
        until: u64,
        limit: usize,
        _filters: &RequestEventStreamFilters,
    ) -> StorageResult<Vec<(u64, RequestEvent)>> {
        Ok(self
            .state
            .lock()
            .expect("property store lock")
            .rows
            .iter()
            .filter(|(cursor, _)| *cursor > after && *cursor <= until)
            .take(limit)
            .cloned()
            .collect())
    }
}

#[derive(Clone, Copy, Debug)]
enum Profile {
    Single,
    A,
    B,
}

impl Profile {
    fn event_id(self) -> EventId {
        match self {
            Self::Single => "prop-single-event".to_owned(),
            Self::A => "prop-event-a".to_owned(),
            Self::B => "prop-event-b".to_owned(),
        }
    }

    fn request_id(self) -> &'static str {
        match self {
            Self::Single => "req-single",
            Self::A => "req-a",
            Self::B => "req-b",
        }
    }

    fn upstream_id(self) -> Uuid {
        match self {
            Self::Single => Uuid::from_u128(1),
            Self::A => Uuid::from_u128(2),
            Self::B => Uuid::from_u128(3),
        }
    }

    fn upstream_name(self) -> &'static str {
        match self {
            Self::Single => "upstream-single",
            Self::A => "upstream-a",
            Self::B => "upstream-b",
        }
    }

    fn cache_prefix_hash(self) -> &'static str {
        match self {
            Self::Single => "cache-single",
            Self::A => "cache-a",
            Self::B => "cache-b",
        }
    }

    fn token_base(self) -> u64 {
        match self {
            Self::Single => 100,
            Self::A => 1_000,
            Self::B => 2_000,
        }
    }

    fn ts_ms(self) -> u64 {
        match self {
            Self::Single => 1_730_000_000_000,
            Self::A => 1_730_000_001_000,
            Self::B => 1_730_000_002_000,
        }
    }

    fn from_event_id(event_id: &str) -> Option<Self> {
        match event_id {
            "prop-single-event" => Some(Self::Single),
            "prop-event-a" => Some(Self::A),
            "prop-event-b" => Some(Self::B),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum EventOp {
    Started,
    Parse,
    Route,
    Upstream,
    Usage,
    Stream,
    Terminated,
}

fn event_op_strategy(include_terminated: bool) -> BoxedStrategy<EventOp> {
    let base = prop_oneof![
        Just(EventOp::Started),
        Just(EventOp::Parse),
        Just(EventOp::Route),
        Just(EventOp::Upstream),
        Just(EventOp::Usage),
        Just(EventOp::Stream),
    ];
    if include_terminated {
        prop_oneof![base, Just(EventOp::Terminated)].boxed()
    } else {
        base.boxed()
    }
}

fn usage(profile: Profile) -> UsageSnapshot {
    let base = profile.token_base();
    UsageSnapshot {
        input_tokens: base,
        output_tokens: base + 10,
        cache_creation_input_tokens: base + 20,
        cache_creation_input_tokens_5m: base + 30,
        cache_creation_input_tokens_1h: base + 40,
        cache_read_input_tokens: base + 50,
        thinking_tokens: base + 60,
        web_search_requests: base + 70,
        web_fetch_requests: base + 80,
        service_tier: Some(format!("tier-{base}")),
        inference_geo: Some(format!("geo-{base}")),
        iterations: None,
    }
}

fn lifecycle_event(profile: Profile, op: EventOp) -> LifecycleEvent {
    let event_id = profile.event_id();
    match op {
        EventOp::Started => LifecycleEvent::RequestStarted {
            event_id,
            request_id: profile.request_id().to_owned(),
            ts_ms: profile.ts_ms(),
            stream: true,
        },
        EventOp::Parse => LifecycleEvent::ParseCompleted {
            event_id,
            result: Ok(ParseInfo {
                path: "/v1/messages".to_owned(),
                method: "POST".to_owned(),
                model: Some("claude-3-5-sonnet-20241022".to_owned()),
                stream: true,
                body_bytes: profile.token_base(),
                cache_control_block_count: Some(profile.token_base()),
                cache_prefix_hash: Some(profile.cache_prefix_hash().to_owned()),
                ..ParseInfo::default()
            }),
        },
        EventOp::Route => LifecycleEvent::RouteCompleted {
            event_id,
            result: Ok(RouteInfo {
                upstream_id: profile.upstream_id(),
                upstream_name: profile.upstream_name().to_owned(),
                model: Some("claude-3-5-sonnet-20241022".to_owned()),
                upstream_kind: Some("anthropic_key".to_owned()),
                route_ms: Some(profile.token_base()),
                routing_trace: None,
                predicted_cache_read_tokens: None,
                matched_v3_cache_key: None,
                breakpoint_content_block_index: None,
                matched_content_block_index: None,
                lookback_distance: None,
                predicted_cache_creation_tokens_5m: None,
                predicted_cache_creation_tokens_1h: None,
                token_estimate_source: None,
                cache_value_micros: None,
                formula_winner_upstream_id: None,
                kept_upstream_id: None,
                quota_urgency_5h: None,
                quota_urgency_7d: None,
                quota_urgency_combined: None,
                quota_weight_factor: None,
                quota_cache_multiplier: None,
                quota_warning_multiplier: None,
                quota_effective_weight: None,
                quota_uniform_fallback: None,
                wrh_key_source: None,
                lineage_would_have_predicted_read_tokens: None,
                lineage_would_have_picked_upstream_id: None,
            }),
            routing_trace: None,
        },
        EventOp::Upstream => LifecycleEvent::UpstreamResponseStarted {
            event_id,
            status: 200,
            headers: HeaderSnapshot::default(),
            bulkhead_wait_ms: Some(profile.token_base()),
            dns_ms: Some(profile.token_base() + 1),
            connect_ms: Some(profile.token_base() + 2),
            connection_reused: Some(matches!(profile, Profile::B)),
            shape_ms: Some(profile.token_base() + 3),
            sign_ms: Some(profile.token_base() + 4),
            upstream_ttfb_ms: Some(profile.token_base() + 5),
        },
        EventOp::Usage => LifecycleEvent::UsageObserved {
            event_id,
            usage: usage(profile),
            source: UsageSource::MessageDelta,
        },
        EventOp::Stream => LifecycleEvent::StreamCompleted {
            event_id,
            result: Ok(StreamSuccess {
                usage: usage(profile),
                sse_event_count: profile.token_base(),
                first_body_chunk_ms: Some(profile.token_base() + 6),
                ..StreamSuccess::default()
            }),
        },
        EventOp::Terminated => LifecycleEvent::RequestTerminated {
            event_id,
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: profile.token_base(),
            first_body_chunk_ms: Some(profile.token_base() + 6),
            internal_errors: Vec::new(),
            limit_reconcile_ms: Some(profile.token_base() + 7),
            observability_post_ms: Some(profile.token_base() + 8),
            proxy_setup_ms: Some(profile.token_base() + 9),
            upstream_body_ms: Some(profile.token_base() + 10),
        },
    }
}

fn run_events(events: Vec<LifecycleEvent>) -> (Vec<RequestEvent>, Vec<RequestEventUpdate>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let (tx, rx) = mpsc::channel(events.len().max(1) + 8);
        let store = Arc::new(PropertyStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let BusReceiver::InMemory(mut bus_rx) = bus.subscribe() else {
            panic!("expected in-memory receiver");
        };
        let handle = cc_lb_engine::spawn_request_event_assembler(
            rx,
            store.clone(),
            Some(bus.clone() as Arc<dyn RequestEventBus>),
            Arc::new(NoopMetricsHook),
        );
        for event in events {
            tx.send(event).await.expect("send lifecycle event");
        }
        drop(tx);
        handle.shutdown().await;
        let mut updates = Vec::new();
        while let Ok(update) = bus_rx.try_recv() {
            updates.push(update);
        }
        (store.rows(), updates)
    })
}

fn assert_row_fields_match_profile(row: &RequestEvent, profile: Profile) {
    if row.request_id != "req_unknown_shadow" {
        assert_eq!(row.request_id, profile.request_id());
    }
    if let Some(upstream_id) = row.upstream_id {
        assert_eq!(upstream_id, profile.upstream_id());
    }
    if let Some(upstream_name) = row.upstream_name.as_deref() {
        assert_eq!(upstream_name, profile.upstream_name());
    }
    if let Some(route_ms) = row.route_ms {
        assert_eq!(route_ms, profile.token_base());
    }
    if let Some(input_tokens) = row.input_tokens {
        assert_eq!(input_tokens, profile.token_base());
    }
    if let Some(output_tokens) = row.output_tokens {
        assert_eq!(output_tokens, profile.token_base() + 10);
    }
    if let Some(cache_prefix_hash) = row.cache_prefix_hash.as_deref() {
        assert_eq!(cache_prefix_hash, profile.cache_prefix_hash());
    }
    if let Some(cache_blocks) = row.cache_control_block_count {
        assert_eq!(cache_blocks, profile.token_base());
    }
    if row.cost_usd_micros.is_some() {
        assert_eq!(row.input_tokens, Some(profile.token_base()));
        assert_eq!(row.output_tokens, Some(profile.token_base() + 10));
    }
}

#[derive(Default)]
struct PartialPresence {
    model: bool,
    upstream_id: bool,
    upstream_response_status: bool,
    input_tokens: bool,
    cache_prefix_hash: bool,
    route_ms: bool,
    first_body_chunk_ms: bool,
    cost_usd_micros: bool,
}

impl PartialPresence {
    fn from_update(update: &RequestEventUpdate) -> Option<Self> {
        let RequestEventUpdate::Partial(partial) = update else {
            return None;
        };
        Some(Self {
            model: partial.model.is_some(),
            upstream_id: partial.upstream_id.is_some(),
            upstream_response_status: partial.upstream_response_status.is_some(),
            input_tokens: partial.input_tokens.is_some(),
            cache_prefix_hash: partial.cache_prefix_hash.is_some(),
            route_ms: partial.route_ms.is_some(),
            first_body_chunk_ms: partial.first_body_chunk_ms.is_some(),
            cost_usd_micros: partial.cost_usd_micros.is_some(),
        })
    }

    fn assert_no_regression_from(&self, previous: &Self) {
        assert!(!previous.model || self.model);
        assert!(!previous.upstream_id || self.upstream_id);
        assert!(!previous.upstream_response_status || self.upstream_response_status);
        assert!(!previous.input_tokens || self.input_tokens);
        assert!(!previous.cache_prefix_hash || self.cache_prefix_hash);
        assert!(!previous.route_ms || self.route_ms);
        assert!(!previous.first_body_chunk_ms || self.first_body_chunk_ms);
        assert!(!previous.cost_usd_micros || self.cost_usd_micros);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn arbitrary_single_event_sequence_persists_at_most_one_final_row(
        ops in prop::collection::vec(event_op_strategy(true), 0..40),
    ) {
        let events = ops
            .into_iter()
            .map(|op| lifecycle_event(Profile::Single, op))
            .collect::<Vec<_>>();
        let (rows, _) = run_events(events);

        prop_assert!(rows.len() <= 1, "expected at most one row, got {rows:?}");
        for row in rows {
            prop_assert_eq!(row.event_id.as_deref(), Some("prop-single-event"));
            if row.request_id != "req_unknown_shadow" {
                prop_assert_eq!(row.request_id, "req-single");
            }
        }
    }

    #[test]
    fn interleaved_distinct_event_ids_do_not_bleed_usage_cache_or_route_fields(
        steps in prop::collection::vec((any::<bool>(), event_op_strategy(true)), 0..60),
    ) {
        let events = steps
            .into_iter()
            .map(|(second, op)| lifecycle_event(if second { Profile::B } else { Profile::A }, op))
            .collect::<Vec<_>>();
        let (rows, _) = run_events(events);

        for row in rows {
            let event_id = row.event_id.as_deref().unwrap_or_default();
            let profile = Profile::from_event_id(event_id)
                .expect("row event_id should be one of the generated profiles");
            assert_row_fields_match_profile(&row, profile);
        }
    }

    #[test]
    fn partial_snapshots_for_one_event_only_gain_optional_fields(
        ops in prop::collection::vec(event_op_strategy(false), 0..60),
    ) {
        let events = ops
            .into_iter()
            .map(|op| lifecycle_event(Profile::Single, op))
            .collect::<Vec<_>>();
        let (_, updates) = run_events(events);
        let mut previous = PartialPresence::default();

        for update in updates {
            if let Some(current) = PartialPresence::from_update(&update) {
                current.assert_no_regression_from(&previous);
                previous = current;
            }
        }
    }
}
