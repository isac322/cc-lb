#[derive(Clone, Copy, Debug, PartialEq)]
struct QuotaFields {
    urgency_5h: Option<f64>,
    urgency_7d: Option<f64>,
    urgency_combined: Option<f64>,
    weight_factor: Option<f64>,
    cache_multiplier: Option<f64>,
    warning_multiplier: Option<f64>,
    effective_weight: Option<f64>,
    uniform_fallback: Option<bool>,
}

const NULL_QUOTA_FIELDS: QuotaFields = QuotaFields {
    urgency_5h: None,
    urgency_7d: None,
    urgency_combined: None,
    weight_factor: None,
    cache_multiplier: None,
    warning_multiplier: None,
    effective_weight: None,
    uniform_fallback: None,
};

fn quota_candidate(upstream_id: Uuid, fields: QuotaFields) -> CandidateUrgency {
    CandidateUrgency {
        upstream_id,
        tier: SubscriptionTier::KnownBase,
        urgency: fields.effective_weight.unwrap_or_default(),
        quota_urgency: fields.urgency_combined.unwrap_or_default(),
        quota_urgency_5h: fields.urgency_5h,
        quota_urgency_7d: fields.urgency_7d,
        quota_urgency_combined: fields.urgency_combined,
        quota_weight_factor: fields.weight_factor.unwrap_or(1.0),
        quota_uniform_fallback: fields.uniform_fallback.unwrap_or(false),
        predicted_cache_read_tokens: 0,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        cache_ratio: 0.0,
        cache_weight_multiplier: fields.cache_multiplier.unwrap_or(1.0),
        warning_multiplier: fields.warning_multiplier.unwrap_or(1.0),
        cache_savings_ratio: 0.0,
        estimated_input_cost_micros: 0,
        effective_weight: fields.effective_weight.unwrap_or_default(),
        cache_value_micros: None,
        matched_v3_cache_key: None,
        matched_content_block_index: None,
        breakpoint_content_block_index: None,
        lookback_distance: None,
        token_estimate_source: None,
    }
}

fn quota_trace(
    candidates: Vec<CandidateUrgency>,
    resolved_upstream_id: Uuid,
    formula_winner_upstream_id: Uuid,
) -> RoutingTrace {
    RoutingTrace {
        stages: vec![StageDecision {
            stage_name: "subscription-preference".to_owned(),
            upstream_id: Some(formula_winner_upstream_id),
            reason: None,
            duration_us: 0,
            subscription_preference: Some(SubscriptionPreferenceTrace {
                chosen_tier: SubscriptionTier::KnownBase,
                candidates,
                wrh_key_source: WrhKeySource::RequestId,
                previous_tier: None,
                rendezvous_salt_version: Some("v11".to_owned()),
                cache_cost_basis_version: None,
                formula_winner_upstream_id: Some(formula_winner_upstream_id),
                kept_upstream_id: Some(resolved_upstream_id),
                incumbent_upstream_id: Some(resolved_upstream_id),
                estimated_switch_cache_loss_micros: None,
                cache_loss_status: None,
                switch_gate_reason: Some("kept_incumbent".to_owned()),
                bucket_v3_cache_affinity_key: None,
                lineage_would_have_predicted_read_tokens: None,
                lineage_would_have_picked_upstream_id: None,
            }),
            cache_affinity: None,
        }],
        terminal_decision: Some(TerminalDecision {
            upstream_id: Some(resolved_upstream_id),
            strategy: TerminalStrategy::FirstPick,
        }),
    }
}

fn route_info_from_trace(resolved_upstream_id: Uuid, routing_trace: RoutingTrace) -> RouteInfo {
    let selected =
        crate::lifecycle::resolved_candidate_urgency(&routing_trace, resolved_upstream_id);
    let selected_fields = selected.map_or(NULL_QUOTA_FIELDS, |candidate| QuotaFields {
        urgency_5h: candidate.quota_urgency_5h,
        urgency_7d: candidate.quota_urgency_7d,
        urgency_combined: candidate.quota_urgency_combined,
        weight_factor: Some(candidate.quota_weight_factor),
        cache_multiplier: Some(candidate.cache_weight_multiplier),
        warning_multiplier: Some(candidate.warning_multiplier),
        effective_weight: Some(candidate.effective_weight),
        uniform_fallback: Some(candidate.quota_uniform_fallback),
    });
    RouteInfo {
        upstream_id: resolved_upstream_id,
        upstream_name: "terminal-upstream".to_owned(),
        model: Some("claude-sonnet-4".to_owned()),
        upstream_kind: Some("anthropic_oauth".to_owned()),
        route_ms: Some(7),
        routing_trace: Some(routing_trace),
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
        kept_upstream_id: Some(resolved_upstream_id),
        quota_urgency_5h: selected_fields.urgency_5h,
        quota_urgency_7d: selected_fields.urgency_7d,
        quota_urgency_combined: selected_fields.urgency_combined,
        quota_weight_factor: selected_fields.weight_factor,
        quota_cache_multiplier: selected_fields.cache_multiplier,
        quota_warning_multiplier: selected_fields.warning_multiplier,
        quota_effective_weight: selected_fields.effective_weight,
        quota_uniform_fallback: selected_fields.uniform_fallback,
        wrh_key_source: Some("request_id".to_owned()),
        lineage_would_have_predicted_read_tokens: None,
        lineage_would_have_picked_upstream_id: None,
    }
}

fn partial_quota_fields(partial: &RequestEventPartial) -> QuotaFields {
    QuotaFields {
        urgency_5h: partial.quota_urgency_5h,
        urgency_7d: partial.quota_urgency_7d,
        urgency_combined: partial.quota_urgency_combined,
        weight_factor: partial.quota_weight_factor,
        cache_multiplier: partial.quota_cache_multiplier,
        warning_multiplier: partial.quota_warning_multiplier,
        effective_weight: partial.quota_effective_weight,
        uniform_fallback: partial.quota_uniform_fallback,
    }
}

fn event_quota_fields(event: &RequestEvent) -> QuotaFields {
    QuotaFields {
        urgency_5h: event.quota_urgency_5h,
        urgency_7d: event.quota_urgency_7d,
        urgency_combined: event.quota_urgency_combined,
        weight_factor: event.quota_weight_factor,
        cache_multiplier: event.quota_cache_multiplier,
        warning_multiplier: event.quota_warning_multiplier,
        effective_weight: event.quota_effective_weight,
        uniform_fallback: event.quota_uniform_fallback,
    }
}

async fn assemble_route(
    event_id: &str,
    route: RouteInfo,
) -> (RequestEventPartial, RequestEvent, RequestEvent) {
    use crate::event_bus::InMemoryBus;

    let (tx, rx) = mpsc::channel(8);
    let store = Arc::new(CapturingStore::default());
    let bus = Arc::new(InMemoryBus::new());
    let BusReceiver::InMemory(mut updates) = bus.subscribe() else {
        panic!("expected in-memory request event receiver");
    };
    let handle = spawn_request_event_assembler(
        rx,
        store.clone(),
        Some(bus as Arc<dyn RequestEventBus>),
        noop_metrics(),
    );

    tx.send(LifecycleEvent::RequestStarted {
        event_id: eid(event_id),
        request_id: format!("req-{event_id}"),
        ts_ms: 1_730_000_000_000,
        stream: false,
    })
    .await
    .unwrap();
    tx.send(LifecycleEvent::RouteCompleted {
        event_id: eid(event_id),
        routing_trace: route.routing_trace.clone(),
        result: Ok(route),
    })
    .await
    .unwrap();
    tx.send(LifecycleEvent::RequestTerminated {
        event_id: eid(event_id),
        reason: TerminationReason::Success,
        client_status: 200,
        duration_ms: 42,
        first_body_chunk_ms: None,
        internal_errors: Vec::new(),
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: None,
        upstream_body_ms: None,
    })
    .await
    .unwrap();
    drop(tx);
    handle.shutdown().await;

    let mut route_partial = None;
    let mut final_event = None;
    while let Ok(update) = updates.try_recv() {
        match update {
            RequestEventUpdate::Partial(partial) if partial.upstream_id.is_some() => {
                route_partial = Some(partial);
            }
            RequestEventUpdate::Final(update) => final_event = Some(update.event),
            RequestEventUpdate::Partial(_) => {}
        }
    }
    let stored_event = store
        .rows
        .lock()
        .unwrap()
        .first()
        .cloned()
        .expect("assembler persisted a request event");
    (
        route_partial.expect("route partial was published"),
        final_event.expect("final event was published"),
        stored_event,
    )
}
