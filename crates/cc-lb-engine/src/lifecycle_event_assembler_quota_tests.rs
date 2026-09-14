#[tokio::test(flavor = "current_thread")]
async fn t2__route_quota_fields_follow_resolved_upstream() {
    const FIRST_LOSER: QuotaFields = QuotaFields {
        urgency_5h: Some(0.11),
        urgency_7d: Some(0.22),
        urgency_combined: Some(0.23),
        warning_multiplier: Some(1.0),
    };
    const RESOLVED: QuotaFields = QuotaFields {
        urgency_5h: Some(0.125),
        urgency_7d: Some(0.75),
        urgency_combined: Some(0.8),
        warning_multiplier: Some(0.2),
    };
    const FORMULA_WINNER: QuotaFields = QuotaFields {
        urgency_5h: Some(0.9),
        urgency_7d: Some(1.1),
        urgency_combined: Some(1.2),
        warning_multiplier: Some(1.0),
    };
    let first_loser_id = Uuid::from_u128(1);
    let resolved_upstream_id = Uuid::from_u128(2);
    let formula_winner_id = Uuid::from_u128(3);
    let trace = quota_trace(
        vec![
            quota_candidate(first_loser_id, FIRST_LOSER),
            quota_candidate(resolved_upstream_id, RESOLVED),
            quota_candidate(formula_winner_id, FORMULA_WINNER),
        ],
        resolved_upstream_id,
        formula_winner_id,
    );
    let route = route_info_from_trace(resolved_upstream_id, trace);

    let (partial, final_event, stored_event) = assemble_route("quota-exact", route).await;

    assert_ne!(resolved_upstream_id, first_loser_id);
    assert_ne!(resolved_upstream_id, formula_winner_id);
    assert_eq!(partial_quota_fields(&partial), RESOLVED);
    assert_eq!(event_quota_fields(&final_event), RESOLVED);
    assert_eq!(event_quota_fields(&stored_event), RESOLVED);
    println!(
        "task6_exact candidate_order=[{first_loser_id},{resolved_upstream_id},{formula_winner_id}] resolved={resolved_upstream_id} matched={RESOLVED:?} partial={:?} final={:?} stored={:?}",
        partial_quota_fields(&partial),
        event_quota_fields(&final_event),
        event_quota_fields(&stored_event),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t2__exact_fable_effective_weekly_pressure_survives_assembly() {
    const FABLE: QuotaFields = QuotaFields {
        urgency_5h: Some(0.405_465_108_108_164_4),
        urgency_7d: Some(0.821_399_906_936_681_4),
        urgency_combined: Some(0.823_368_670_558_544_7),
        warning_multiplier: Some(1.0),
    };
    let upstream_id = Uuid::from_u128(5);
    let trace = quota_trace(
        vec![quota_candidate(upstream_id, FABLE)],
        upstream_id,
        upstream_id,
    );
    let mut route = route_info_from_trace(upstream_id, trace);
    route.model = Some("claude-fable-5".to_owned());

    let (partial, final_event, stored_event) = assemble_route("quota-fable", route).await;

    assert_eq!(partial_quota_fields(&partial), FABLE);
    assert_eq!(event_quota_fields(&final_event), FABLE);
    assert_eq!(event_quota_fields(&stored_event), FABLE);
    assert_eq!(final_event.model.as_deref(), Some("claude-fable-5"));
    assert_eq!(stored_event.model.as_deref(), Some("claude-fable-5"));
}

#[tokio::test(flavor = "current_thread")]
async fn t2__route_quota_fields_are_null_when_resolved_upstream_missing_from_trace() {
    let first_loser_id = Uuid::from_u128(1);
    let formula_winner_id = Uuid::from_u128(3);
    let missing_resolved_id = Uuid::from_u128(4);
    let trace = quota_trace(
        vec![
            quota_candidate(
                first_loser_id,
                QuotaFields {
                    urgency_5h: Some(0.11),
                    urgency_7d: Some(0.22),
                    urgency_combined: Some(0.23),
                    warning_multiplier: Some(1.0),
                },
            ),
            quota_candidate(
                formula_winner_id,
                QuotaFields {
                    urgency_5h: Some(0.9),
                    urgency_7d: Some(1.1),
                    urgency_combined: Some(1.2),
                    warning_multiplier: Some(1.0),
                },
            ),
        ],
        missing_resolved_id,
        formula_winner_id,
    );
    let route = route_info_from_trace(missing_resolved_id, trace);

    let (partial, final_event, stored_event) = assemble_route("quota-missing", route).await;

    assert_ne!(missing_resolved_id, first_loser_id);
    assert_ne!(missing_resolved_id, formula_winner_id);
    assert_eq!(partial_quota_fields(&partial), NULL_QUOTA_FIELDS);
    assert_eq!(event_quota_fields(&final_event), NULL_QUOTA_FIELDS);
    assert_eq!(event_quota_fields(&stored_event), NULL_QUOTA_FIELDS);
    println!(
        "task6_mismatch candidate_order=[{first_loser_id},{formula_winner_id}] resolved={missing_resolved_id} partial={:?} final={:?} stored={:?}",
        partial_quota_fields(&partial),
        event_quota_fields(&final_event),
        event_quota_fields(&stored_event),
    );
}
