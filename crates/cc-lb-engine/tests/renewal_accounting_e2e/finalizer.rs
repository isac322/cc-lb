use std::collections::HashMap;

use cc_lb_control::{InMemoryBus, RequestEventBus};
use cc_lb_engine::{api_keys::limit_engine::LimitEngine, cache_keepalive::RenewalFinalization};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_pricing::{
    CatalogSnapshot, CatalogStatus, Pricing, UpstreamKind as PricingUpstreamKind, UsdPerMillion,
    global_catalog, virtual_cost_micros_full,
};
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveSessionRecord, CacheKeepaliveTurnRow, RequestEvent,
    RequestEventProjections, RequestEventStore,
};

pub(crate) struct Completion {
    pub(crate) reservation_id: Option<String>,
    pub(crate) event_key_id: Option<String>,
    pub(crate) projection_key_id: Option<String>,
}

pub(crate) struct FinalizeInput<'a> {
    pub(crate) storage: &'a dyn RequestEventStore,
    pub(crate) limit_engine: &'a LimitEngine,
    pub(crate) session: &'a CacheKeepaliveSessionRecord,
    pub(crate) finalization: RenewalFinalization,
    pub(crate) bus: Option<&'a InMemoryBus>,
}

pub(crate) async fn persist_finalization(input: FinalizeInput<'_>) -> Completion {
    let source_ref_id = format!(
        "{}:{}",
        input.session.session_key_hash, input.session.generation
    );
    let event_id = format!("renewal:{source_ref_id}");
    let cost = virtual_cost_micros_full(
        "claude-test",
        input.finalization.usage.input_tokens,
        input.finalization.usage.output_tokens,
        input.finalization.usage.cache_creation_input_tokens_5m,
        input.finalization.usage.cache_creation_input_tokens_1h,
        input.finalization.usage.cache_read_input_tokens,
        Some(PricingUpstreamKind::AnthropicOAuth),
        None,
    );
    let event = renewal_event(&event_id, &source_ref_id, &input, cost.total_micros);
    let projections = renewal_projections(&source_ref_id, &input, cost.total_micros);
    input
        .storage
        .append_request_event_with_projections(&event, &projections)
        .await
        .expect("persist renewal event and projections");

    let reservation_id = input
        .finalization
        .accounting_guard
        .reservation_id()
        .map(ToOwned::to_owned);
    if let Some(reservation_id) = reservation_id.as_deref() {
        assert!(
            input.limit_engine.reconcile_by_id(
                reservation_id,
                input.finalization.usage.input_tokens,
                input.finalization.usage.output_tokens,
                cost.total_micros,
            ),
            "durable renewal write must precede one direct reconciliation"
        );
        input.finalization.accounting_guard.forget();
    }
    if let Some(bus) = input.bus {
        bus.publish_lifecycle(LifecycleEvent::RequestStarted {
            event_id,
            request_id: "renewal-observability".to_owned(),
            ts_ms: 1_003_000,
            stream: false,
            source_kind: Some("renewal".to_owned()),
            source_ref_id: Some(source_ref_id.to_owned()),
        });
    }

    Completion {
        reservation_id,
        event_key_id: event.key_id,
        projection_key_id: projections
            .turn
            .as_ref()
            .and_then(|turn| turn.accounting_key_id.clone()),
    }
}

pub(crate) fn install_test_pricing() {
    let mut models = HashMap::new();
    models.insert(
        "claude-test".to_owned(),
        Pricing {
            model: "claude-test".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: UsdPerMillion::from_whole_usd(3),
            by_tier: Default::default(),
        },
    );
    let mut cache_read_per_million_usd = HashMap::new();
    cache_read_per_million_usd.insert("claude-test".to_owned(), UsdPerMillion::from_whole_usd(1));
    global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: String::new(),
        fetched_at_ms: 0,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn renewal_event(
    event_id: &str,
    source_ref_id: &str,
    input: &FinalizeInput<'_>,
    cost_micros: i64,
) -> RequestEvent {
    RequestEvent {
        ts: 1_003,
        ts_ms: Some(1_003_000),
        request_id: event_id.to_owned(),
        event_id: Some(event_id.to_owned()),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(source_ref_id.to_owned()),
        principal_id: Some(input.session.principal_id.clone()),
        key_id: input.session.accounting_key_id.clone(),
        upstream_id: Some(input.session.upstream_id),
        model: Some("claude-test".to_owned()),
        status: input.finalization.status,
        input_tokens: Some(input.finalization.usage.input_tokens),
        output_tokens: Some(input.finalization.usage.output_tokens),
        cache_read_input_tokens: Some(input.finalization.usage.cache_read_input_tokens),
        cost_usd_micros: Some(cost_micros),
        duration_ms: u64::try_from(input.finalization.duration.as_millis()).unwrap_or(u64::MAX),
        ..RequestEvent::default()
    }
}

fn renewal_projections(
    source_ref_id: &str,
    input: &FinalizeInput<'_>,
    cost_micros: i64,
) -> RequestEventProjections {
    RequestEventProjections {
        turn: Some(CacheKeepaliveTurnRow {
            source_ref_id: source_ref_id.to_owned(),
            session_key_hash: input.session.session_key_hash.clone(),
            principal_id: input.session.principal_id.clone(),
            accounting_key_id: input.session.accounting_key_id.clone(),
            upstream_id: input.session.upstream_id,
            model: "claude-test".to_owned(),
            input_tokens: input.finalization.usage.input_tokens,
            output_tokens: input.finalization.usage.output_tokens,
            cache_creation_input_tokens: input.finalization.usage.cache_creation_input_tokens,
            cache_creation_input_tokens_5m: input.finalization.usage.cache_creation_input_tokens_5m,
            cache_creation_input_tokens_1h: input.finalization.usage.cache_creation_input_tokens_1h,
            cache_read_input_tokens: input.finalization.usage.cache_read_input_tokens,
            cost_micros,
            hit_miss: "hit".to_owned(),
            ts: 1_003,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: source_ref_id.to_owned(),
            principal_id: input.session.principal_id.clone(),
            session_key_hash: Some(input.session.session_key_hash.clone()),
            upstream_id: input.session.upstream_id,
            decision: "reschedule".to_owned(),
            reason: "cache_hit".to_owned(),
            error: None,
            generation: input.session.generation,
            ttl: input.session.ttl,
            config_snapshot: input.session.config_snapshot.clone(),
            last_message_at_ms: 1_003_000,
            ts: 1_003,
        },
    }
}
