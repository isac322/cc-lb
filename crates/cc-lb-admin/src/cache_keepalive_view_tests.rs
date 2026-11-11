use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, UsdPerMillion};
use cc_lb_storage_api::{
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem, CacheKeepaliveSessionStatus,
    CacheKeepaliveTurnRecord, CacheTtl, StorageError,
};
use uuid::Uuid;

use super::{CacheKeepaliveActivitySource, derive_activity_view};
use crate::v1::principal_cache_keepalive::view::{CacheKeepaliveActivityBatch, summary_for_items};

use super::economics::{
    CacheKeepalivePnlRates, CacheKeepaliveTurnPnlInput, derive_turn_pnl, format_net_pnl,
    format_turn_pnl, rates_from_catalog,
};

const CACHE_READ_MICROS_PER_MILLION: u64 = 300_000;
const CACHE_CREATE_5M_MICROS_PER_MILLION: u64 = 3_750_000;
const CACHE_CREATE_1H_MICROS_PER_MILLION: u64 = 6_000_000;

fn rates(cache_create_micros_per_million: u64) -> CacheKeepalivePnlRates {
    CacheKeepalivePnlRates {
        avoided_micros_per_million: cache_create_micros_per_million - CACHE_READ_MICROS_PER_MILLION,
    }
}

fn pnl_sum(inputs: &[CacheKeepaliveTurnPnlInput], rates: CacheKeepalivePnlRates) -> i64 {
    inputs
        .iter()
        .map(|input| derive_turn_pnl(*input, rates).net_micros)
        .sum()
}

fn install_priced_snapshot(catalog: &PriceCatalog, payload_hash: &str) {
    let mut models = HashMap::new();
    models.insert(
        "claude-sonnet-4-5".to_owned(),
        Pricing {
            native: None,
            model: "claude-sonnet-4-5".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: BTreeMap::new(),
        },
    );
    let mut cache_creation_per_million_usd = HashMap::new();
    cache_creation_per_million_usd.insert(
        "claude-sonnet-4-5".to_owned(),
        UsdPerMillion::from_micros_usd(CACHE_CREATE_5M_MICROS_PER_MILLION),
    );
    let mut cache_read_per_million_usd = HashMap::new();
    cache_read_per_million_usd.insert(
        "claude-sonnet-4-5".to_owned(),
        UsdPerMillion::from_micros_usd(CACHE_READ_MICROS_PER_MILLION),
    );
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: payload_hash.to_owned(),
        fetched_at_ms: 1,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates: HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn priced_catalog() -> Arc<PriceCatalog> {
    let catalog = PriceCatalog::new_empty();
    install_priced_snapshot(catalog.as_ref(), "fixture");
    catalog
}

fn summary_item(
    source: CacheKeepaliveSessionEntrySource,
    last_message_at_ms: u64,
) -> CacheKeepaliveSessionListItem {
    CacheKeepaliveSessionListItem {
        id: match source {
            CacheKeepaliveSessionEntrySource::Session => "session",
            CacheKeepaliveSessionEntrySource::Decision => "decision",
        }
        .to_owned(),
        source,
        session_key_hash: matches!(source, CacheKeepaliveSessionEntrySource::Session)
            .then(|| "session".to_owned()),
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::nil(),
        cycle_id: None,
        last_message_at_ms,
        ttl: CacheTtl::Ttl5m,
        generation: 1,
        refresh_count: matches!(source, CacheKeepaliveSessionEntrySource::Session).then_some(2),
        has_renewal_in_horizon: matches!(source, CacheKeepaliveSessionEntrySource::Session),
        status: matches!(source, CacheKeepaliveSessionEntrySource::Session)
            .then_some(CacheKeepaliveSessionStatus::Active),
        enqueue_state: None,
        terminal_reason: None,
        cancel_reason: None,
        request_incomplete: false,
        decision: matches!(source, CacheKeepaliveSessionEntrySource::Decision)
            .then(|| "not_tracked".to_owned()),
        reason: "fixture".to_owned(),
        display_reason: None,
        error: None,
        config_snapshot: None,
    }
}

fn summary_turn(source_ref_id: &str, ts: u64) -> CacheKeepaliveTurnRecord {
    summary_turn_with(source_ref_id, ts, "claude-sonnet-4-5", 20_000)
}

fn summary_turn_with(
    source_ref_id: &str,
    ts: u64,
    model: &str,
    cache_read_input_tokens: u64,
) -> CacheKeepaliveTurnRecord {
    CacheKeepaliveTurnRecord {
        source_ref_id: source_ref_id.to_owned(),
        session_key_hash: "session".to_owned(),
        principal_id: "principal".to_owned(),
        accounting_key_id: None,
        upstream_id: Uuid::nil(),
        cycle_id: None,
        model: model.to_owned(),
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_creation_input_tokens_5m: 0,
        cache_creation_input_tokens_1h: 0,
        cache_read_input_tokens,
        cost_micros: Some(0),
        hit_miss: "hit".to_owned(),
        ts,
    }
}

#[test]
fn cache_keepalive_avoided_savings_subtract_follow_up_cache_read_cost() {
    let catalog = priced_catalog();
    for (ttl, creation_rate) in [
        (CacheTtl::Ttl5m, CACHE_CREATE_5M_MICROS_PER_MILLION),
        (CacheTtl::Ttl1h, CACHE_CREATE_1H_MICROS_PER_MILLION),
    ] {
        let pnl = derive_turn_pnl(
            CacheKeepaliveTurnPnlInput {
                renewal_tokens: 20_000,
                spent_micros: 6_000,
                followed_up: true,
                pending: false,
            },
            rates_from_catalog(catalog.as_ref(), "claude-sonnet-4-5", ttl)
                .expect("complete cache pricing"),
        );
        let expected =
            (20_000 * (creation_rate - CACHE_READ_MICROS_PER_MILLION) / 1_000_000) as i64;
        assert_eq!(pnl.avoided_micros, expected, "{ttl:?}");
        assert_eq!(pnl.net_micros, expected - 6_000, "{ttl:?}");
    }
}

#[test]
fn cache_keepalive_pnl_matches_the_frozen_multi_turn_examples() {
    // Given: the frozen mockup rates and three independently terminal outcomes.
    let renewed = [
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20000,
            spent_micros: 12000,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20000,
            spent_micros: 18000,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20000,
            spent_micros: 18000,
            followed_up: false,
            pending: true,
        },
    ];
    let capped = [
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 24000,
            spent_micros: 14400,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 24000,
            spent_micros: 28800,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 24000,
            spent_micros: 43200,
            followed_up: false,
            pending: false,
        },
    ];
    let expired = [
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 32000,
            spent_micros: 28800,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 32000,
            spent_micros: 48000,
            followed_up: false,
            pending: false,
        },
    ];

    // When: the server derives each session's multi-turn P&L.
    let renewed_net = pnl_sum(&renewed, rates(CACHE_CREATE_5M_MICROS_PER_MILLION));
    let capped_net = pnl_sum(&capped, rates(CACHE_CREATE_5M_MICROS_PER_MILLION));
    let expired_net = pnl_sum(&expired, rates(CACHE_CREATE_1H_MICROS_PER_MILLION));
    let over_renewed = derive_turn_pnl(
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20000,
            spent_micros: 84000,
            followed_up: true,
            pending: false,
        },
        rates(CACHE_CREATE_5M_MICROS_PER_MILLION),
    );

    // Then: all frozen worked examples retain their exact microdollar values.
    assert_eq!(renewed_net, 90_000);
    assert_eq!(capped_net, 79_200);
    assert_eq!(expired_net, 105_600);
    assert_eq!(over_renewed.net_micros, -15_000);
}

#[test]
fn cache_keepalive_pnl_uses_catalog_rates_and_exact_surface_formats() {
    // Given: a real catalog-shaped rate snapshot and a pending loss.
    let catalog = priced_catalog();
    let pending = derive_turn_pnl(
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20000,
            spent_micros: 18000,
            followed_up: false,
            pending: true,
        },
        rates(CACHE_CREATE_5M_MICROS_PER_MILLION),
    );

    // When: the server looks up both TTL variants and formats P&L values.
    let rates_5m = rates_from_catalog(catalog.as_ref(), "claude-sonnet-4-5", CacheTtl::Ttl5m);
    let rates_1h = rates_from_catalog(catalog.as_ref(), "claude-sonnet-4-5", CacheTtl::Ttl1h);

    // Then: production derives price rates instead of embedding mock constants in the view model.
    assert_eq!(rates_5m, Some(rates(CACHE_CREATE_5M_MICROS_PER_MILLION)));
    assert_eq!(rates_1h, Some(rates(CACHE_CREATE_1H_MICROS_PER_MILLION)));
    assert_eq!(format_net_pnl(93_600), "+$0.0936");
    assert_eq!(format_net_pnl(102_000), "+$0.102");
    assert_eq!(format_turn_pnl(pending), "−$0.018 pending");
}

#[test]
fn batched_cache_keepalive_turns_preserve_serialized_summary_behavior() {
    const NOW_MS: u64 = 1_730_000_100_000;

    // Given: one renewing session, one old decision row, and the legacy per-session turn slice.
    let catalog = priced_catalog();
    let session = summary_item(CacheKeepaliveSessionEntrySource::Session, NOW_MS);
    let decision = summary_item(
        CacheKeepaliveSessionEntrySource::Decision,
        NOW_MS.saturating_sub(10 * 60 * 1_000),
    );
    let turns = vec![summary_turn("older", 100), summary_turn("newer", 200)];
    let batch = CacheKeepaliveActivityBatch::from_turns(turns, Vec::new());
    let summary = summary_for_items(catalog.as_ref(), NOW_MS, &[session, decision], &batch)
        .expect("batched summary");
    assert_eq!(summary.renewing_now, 1);
    assert_eq!(summary.sessions_last_5m, 1);
    assert_eq!(summary.retained_successful_hits, 2);
    assert_eq!(summary.total_spent, 0.0);
    // Unknown follow-up evidence is $0 avoided so far, not an unknown price.
    assert_eq!(summary.total_avoided, Some(0.0));
    assert_eq!(summary.cost_saved, Some(0.0));
    assert_eq!(summary.evidence_status, "unknown");
}
#[test]
fn unknown_pricing_stays_null_and_summary_skips_it() {
    const NOW_MS: u64 = 1_730_000_100_000;
    let catalog = PriceCatalog::new_empty();
    let session = summary_item(CacheKeepaliveSessionEntrySource::Session, NOW_MS);
    let turns = vec![summary_turn_with("unknown", 100, "missing-model", 20_000)];
    let activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &session,
            session: None,
            turns: &turns,
            evidence: &[],
            now_ms: NOW_MS,
            horizon_start_ms: None,
        },
        catalog.as_ref(),
    );
    assert!(activity.pnl.is_none());
    assert_eq!(activity.net_pnl_display, "-");
    assert!(activity.turns[0].pnl.is_none());

    let partial_catalog = PriceCatalog::new_empty();
    let mut partial_models = HashMap::new();
    partial_models.insert(
        "partial-model".to_owned(),
        Pricing {
            native: None,
            model: "partial-model".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(1),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            output_per_million_usd: UsdPerMillion::from_whole_usd(1),
            by_tier: BTreeMap::new(),
        },
    );
    let mut partial_read = HashMap::new();
    partial_read.insert(
        "partial-model".to_owned(),
        UsdPerMillion::from_micros_usd(CACHE_READ_MICROS_PER_MILLION),
    );
    let mut partial_snapshot = CatalogSnapshot {
        payload_hash: "partial".to_owned(),
        fetched_at_ms: 1,
        models: partial_models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: partial_read,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates: HashMap::new(),
        status: CatalogStatus::Ok,
    };
    partial_catalog.install_snapshot(partial_snapshot.clone());
    let partial_turns = vec![summary_turn_with("partial", 100, "partial-model", 20_000)];
    let partial_activity = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &session,
            session: None,
            turns: &partial_turns,
            evidence: &[],
            now_ms: NOW_MS,
            horizon_start_ms: None,
        },
        partial_catalog.as_ref(),
    );
    assert!(partial_activity.pnl.is_none());
    assert!(partial_activity.turns[0].pnl.is_none());

    // A known write price alone cannot value the saving: the follow-up read is paid.
    partial_snapshot.cache_creation_per_million_usd.insert(
        "partial-model".to_owned(),
        UsdPerMillion::from_micros_usd(CACHE_CREATE_5M_MICROS_PER_MILLION),
    );
    partial_snapshot.cache_read_per_million_usd.clear();
    partial_catalog.install_snapshot(partial_snapshot);
    assert!(
        rates_from_catalog(partial_catalog.as_ref(), "partial-model", CacheTtl::Ttl5m).is_none()
    );
    let partial_batch = CacheKeepaliveActivityBatch::from_turns(partial_turns, Vec::new());
    let partial_summary = summary_for_items(
        partial_catalog.as_ref(),
        NOW_MS,
        std::slice::from_ref(&session),
        &partial_batch,
    )
    .expect("incomplete read pricing is tolerated");
    assert_eq!(partial_summary.total_avoided, None);
    assert_eq!(partial_summary.cost_saved, None);

    let batch = CacheKeepaliveActivityBatch::from_turns(turns, Vec::new());
    let summary = summary_for_items(catalog.as_ref(), NOW_MS, &[session], &batch)
        .expect("unknown pricing is tolerated");
    assert_eq!(summary.renewing_now, 1);
    assert_eq!(summary.sessions_last_5m, 1);
    assert_eq!(summary.retained_successful_hits, 1);
    assert_eq!(summary.cost_saved, None);
}

#[test]
fn pricing_preserves_persisted_spend() {
    let rates = rates(CACHE_CREATE_5M_MICROS_PER_MILLION);
    let named = derive_turn_pnl(
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20000,
            spent_micros: 18_001,
            followed_up: false,
            pending: true,
        },
        rates,
    );
    assert_eq!(named.spent_micros, 18_001);

    let tiny_turns = [
        derive_turn_pnl(
            CacheKeepaliveTurnPnlInput {
                renewal_tokens: 2,
                spent_micros: 7,
                followed_up: false,
                pending: false,
            },
            rates,
        ),
        derive_turn_pnl(
            CacheKeepaliveTurnPnlInput {
                renewal_tokens: 2,
                spent_micros: 0,
                followed_up: false,
                pending: false,
            },
            rates,
        ),
    ];
    assert_eq!(tiny_turns[0].spent_micros, 7);
    assert_eq!(tiny_turns[1].spent_micros, 0);
    assert_eq!(super::economics::sum_turn_pnl(&tiny_turns).spent_micros, 7);
    assert_eq!(
        derive_turn_pnl(
            CacheKeepaliveTurnPnlInput {
                renewal_tokens: 4,
                spent_micros: 99,
                followed_up: false,
                pending: false,
            },
            rates,
        )
        .spent_micros,
        99,
        "persisted spend remains authoritative rather than being recomputed from tokens"
    );
}

#[test]
fn catalog_absence_and_replacement_reprice_without_storage_change() {
    const NOW_MS: u64 = 1_730_000_100_000;
    let catalog = PriceCatalog::new_empty();
    let session = summary_item(CacheKeepaliveSessionEntrySource::Session, NOW_MS);
    let turns = vec![summary_turn("same-stored-turn", 100)];
    let before = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &session,
            session: None,
            turns: &turns,
            evidence: &[],
            now_ms: NOW_MS,
            horizon_start_ms: None,
        },
        catalog.as_ref(),
    );
    assert!(before.pnl.is_none());

    install_priced_snapshot(catalog.as_ref(), "replacement");
    let after = derive_activity_view(
        CacheKeepaliveActivitySource {
            item: &session,
            session: None,
            turns: &turns,
            evidence: &[],
            now_ms: NOW_MS,
            horizon_start_ms: None,
        },
        catalog.as_ref(),
    );
    assert_eq!(after.pnl.map(|pnl| pnl.net_micros), Some(0));
    assert_eq!(turns[0].source_ref_id, "same-stored-turn");
    assert_eq!(turns[0].cache_read_input_tokens, 20_000);
}

#[test]
fn pnl_format_rounding_does_not_change_micro_accounting() {
    let exact = [
        derive_turn_pnl(
            CacheKeepaliveTurnPnlInput {
                renewal_tokens: 1,
                spent_micros: 49,
                followed_up: false,
                pending: false,
            },
            CacheKeepalivePnlRates {
                avoided_micros_per_million: 0,
            },
        ),
        derive_turn_pnl(
            CacheKeepaliveTurnPnlInput {
                renewal_tokens: 1,
                spent_micros: 50,
                followed_up: false,
                pending: false,
            },
            CacheKeepalivePnlRates {
                avoided_micros_per_million: 0,
            },
        ),
    ];
    assert_eq!(exact[0].net_micros, -49);
    assert_eq!(exact[1].net_micros, -50);
    assert_eq!(format_net_pnl(exact[0].net_micros), "−$0.000");
    assert_eq!(format_net_pnl(exact[1].net_micros), "−$0.0001");
    assert_eq!(super::economics::sum_turn_pnl(&exact).net_micros, -99);
}

#[test]
fn pnl_saturates_then_dollar_converter_returns_invalid_input() {
    const NOW_MS: u64 = 1_730_000_100_000;
    let rates = CacheKeepalivePnlRates {
        avoided_micros_per_million: u64::MAX,
    };
    let saturated_turn = derive_turn_pnl(
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: u64::MAX,
            spent_micros: i64::MAX,
            followed_up: false,
            pending: true,
        },
        rates,
    );
    assert_eq!(saturated_turn.spent_micros, i64::MAX);
    assert_eq!(saturated_turn.net_micros, -i64::MAX);
    assert_eq!(
        super::economics::sum_turn_pnl(&[saturated_turn, saturated_turn]).net_micros,
        i64::MIN
    );

    let catalog = PriceCatalog::new_empty();
    let mut models = HashMap::new();
    models.insert(
        "overflow-model".to_owned(),
        Pricing {
            native: None,
            model: "overflow-model".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(0),
            output_per_million_usd: UsdPerMillion::from_whole_usd(0),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            by_tier: BTreeMap::new(),
        },
    );
    let mut cache_creation_per_million_usd = HashMap::new();
    cache_creation_per_million_usd.insert(
        "overflow-model".to_owned(),
        UsdPerMillion::from_micros_usd(u64::MAX),
    );
    let mut cache_read_per_million_usd = HashMap::new();
    cache_read_per_million_usd.insert(
        "overflow-model".to_owned(),
        UsdPerMillion::from_micros_usd(u64::MAX),
    );
    catalog.install_snapshot(CatalogSnapshot {
        payload_hash: "overflow".to_owned(),
        fetched_at_ms: 1,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates: HashMap::new(),
        status: CatalogStatus::Ok,
    });
    let session = summary_item(CacheKeepaliveSessionEntrySource::Session, NOW_MS);
    let mut overflow_turn = summary_turn_with("overflow", 100, "overflow-model", u64::MAX);
    overflow_turn.cost_micros = Some(i64::MAX);
    let batch = CacheKeepaliveActivityBatch::from_turns(vec![overflow_turn], Vec::new());
    let error = summary_for_items(catalog.as_ref(), NOW_MS, &[session], &batch)
        .err()
        .expect("API dollar converter rejects the saturated amount");
    assert!(matches!(
        error,
        StorageError::InvalidInput { ref field, ref reason }
            if field == "cache_keepalive_pnl"
                && reason == "amount exceeds the API dollar range"
    ));
}
