use std::collections::{BTreeMap, HashMap};

use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, UsdPerMillion};
use cc_lb_storage_api::CacheTtl;

use super::economics::{
    CacheKeepalivePnlRates, CacheKeepaliveTurnPnlInput, derive_turn_pnl, format_net_pnl,
    format_turn_pnl, rates_from_catalog,
};

const CACHE_READ_MICROS_PER_MILLION: u64 = 300_000;
const CACHE_CREATE_5M_MICROS_PER_MILLION: u64 = 3_750_000;
const CACHE_CREATE_1H_MICROS_PER_MILLION: u64 = 6_000_000;

fn rates(cache_create_micros_per_million: u64) -> CacheKeepalivePnlRates {
    CacheKeepalivePnlRates {
        cache_read_micros_per_million: CACHE_READ_MICROS_PER_MILLION,
        avoided_create_micros_per_million: cache_create_micros_per_million,
    }
}

fn pnl_sum(inputs: &[CacheKeepaliveTurnPnlInput], rates: CacheKeepalivePnlRates) -> i64 {
    inputs
        .iter()
        .map(|input| derive_turn_pnl(*input, rates).net_micros)
        .sum()
}

#[test]
fn cache_keepalive_pnl_matches_the_frozen_multi_turn_examples() {
    // Given: the frozen mockup rates and three independently terminal outcomes.
    let renewed = [
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20_000,
            renewals: 2,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20_000,
            renewals: 3,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20_000,
            renewals: 3,
            followed_up: false,
            pending: true,
        },
    ];
    let capped = [
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 24_000,
            renewals: 2,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 24_000,
            renewals: 4,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 24_000,
            renewals: 6,
            followed_up: false,
            pending: false,
        },
    ];
    let expired = [
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 32_000,
            renewals: 3,
            followed_up: true,
            pending: false,
        },
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 32_000,
            renewals: 5,
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
            renewal_tokens: 20_000,
            renewals: 14,
            followed_up: true,
            pending: false,
        },
        rates(CACHE_CREATE_5M_MICROS_PER_MILLION),
    );

    // Then: all frozen worked examples retain their exact microdollar values.
    assert_eq!(renewed_net, 102_000);
    assert_eq!(capped_net, 93_600);
    assert_eq!(expired_net, 115_200);
    assert_eq!(over_renewed.net_micros, -9_000);
}

#[test]
fn cache_keepalive_pnl_uses_catalog_rates_and_exact_surface_formats() {
    // Given: a real catalog-shaped rate snapshot and a pending loss.
    let catalog = PriceCatalog::new_empty();
    let mut models = HashMap::new();
    models.insert(
        "claude-sonnet-4-5".to_owned(),
        Pricing {
            model: "claude-sonnet-4-5".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(3),
            output_per_million_usd: UsdPerMillion::from_whole_usd(15),
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
        payload_hash: "fixture".to_owned(),
        fetched_at_ms: 1,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    });
    let pending = derive_turn_pnl(
        CacheKeepaliveTurnPnlInput {
            renewal_tokens: 20_000,
            renewals: 3,
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
