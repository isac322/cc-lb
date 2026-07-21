use cc_lb_pricing::PriceCatalog;
use cc_lb_storage_api::CacheTtl;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CacheKeepalivePnlRates {
    pub cache_read_micros_per_million: u64,
    pub avoided_create_micros_per_million: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CacheKeepaliveTurnPnlInput {
    pub renewal_tokens: u64,
    pub renewals: u32,
    pub followed_up: bool,
    pub pending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CacheKeepaliveTurnPnl {
    pub avoided_micros: i64,
    pub spent_micros: i64,
    pub net_micros: i64,
    pub pending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CacheKeepaliveSessionPnl {
    pub avoided_micros: i64,
    pub spent_micros: i64,
    pub net_micros: i64,
}

pub fn rates_from_catalog(
    catalog: &PriceCatalog,
    model: &str,
    ttl: CacheTtl,
) -> Option<CacheKeepalivePnlRates> {
    let pricing = catalog.routing_cache_pricing(model, None)?;
    let avoided_create_micros_per_million = match ttl {
        CacheTtl::Ttl5m => pricing.cache_creation_5m_per_million_usd?,
        CacheTtl::Ttl1h => pricing.cache_creation_1h_per_million_usd?,
    }
    .as_micros_usd();
    Some(CacheKeepalivePnlRates {
        cache_read_micros_per_million: pricing.cache_read_per_million_usd?.as_micros_usd(),
        avoided_create_micros_per_million,
    })
}

pub fn derive_turn_pnl(
    input: CacheKeepaliveTurnPnlInput,
    rates: CacheKeepalivePnlRates,
) -> CacheKeepaliveTurnPnl {
    let read_per_renewal =
        token_cost_micros(input.renewal_tokens, rates.cache_read_micros_per_million);
    let spent_micros = read_per_renewal.saturating_mul(i64::from(input.renewals));
    let avoided_micros = if input.followed_up {
        token_cost_micros(
            input.renewal_tokens,
            rates.avoided_create_micros_per_million,
        )
    } else {
        0
    };
    CacheKeepaliveTurnPnl {
        avoided_micros,
        spent_micros,
        net_micros: avoided_micros.saturating_sub(spent_micros),
        pending: input.pending,
    }
}

pub fn sum_turn_pnl(turns: &[CacheKeepaliveTurnPnl]) -> CacheKeepaliveSessionPnl {
    turns.iter().fold(
        CacheKeepaliveSessionPnl {
            avoided_micros: 0,
            spent_micros: 0,
            net_micros: 0,
        },
        |total, turn| CacheKeepaliveSessionPnl {
            avoided_micros: total.avoided_micros.saturating_add(turn.avoided_micros),
            spent_micros: total.spent_micros.saturating_add(turn.spent_micros),
            net_micros: total.net_micros.saturating_add(turn.net_micros),
        },
    )
}

pub fn format_net_pnl(micros: i64) -> String {
    if micros == 0 {
        return "$0.00".to_owned();
    }
    format_micros(micros, 4, true, true)
}

pub fn format_turn_pnl(pnl: CacheKeepaliveTurnPnl) -> String {
    let amount = format_micros(pnl.net_micros, 3, true, false);
    if pnl.pending {
        format!("{amount} pending")
    } else {
        amount
    }
}

fn token_cost_micros(tokens: u64, rate_micros_per_million: u64) -> i64 {
    let micros = u128::from(tokens) * u128::from(rate_micros_per_million) / 1_000_000;
    micros.try_into().unwrap_or(i64::MAX)
}

fn format_micros(
    micros: i64,
    decimal_places: usize,
    include_sign: bool,
    trim_one_zero: bool,
) -> String {
    let quantum = 10_u128.pow(u32::try_from(6_usize.saturating_sub(decimal_places)).unwrap_or(0));
    let magnitude = i128::from(micros).unsigned_abs();
    let rounded = magnitude.saturating_add(quantum / 2) / quantum;
    let scale = 10_u128.pow(u32::try_from(decimal_places).unwrap_or(0));
    let whole = rounded / scale;
    let fraction = rounded % scale;
    let mut amount = format!("${whole}.{fraction:0decimal_places$}");
    if trim_one_zero && amount.ends_with('0') {
        amount.pop();
    }
    if !include_sign || micros == 0 {
        return amount;
    }
    let sign = if micros < 0 { '−' } else { '+' };
    format!("{sign}{amount}")
}
