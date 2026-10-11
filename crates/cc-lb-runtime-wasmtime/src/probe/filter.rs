use std::sync::Arc;

use cc_lb_plugin_wire::v1::{ArchivedFilterResponse, CachePricingSummary, FilterRequest};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

use crate::cache::call_filter_hook;
use crate::cell::PluginCell;
use crate::error::WasmtimeRuntimeError;

use super::{hdr, probe_failed, synth_principal};
use cc_lb_plugin_wire::schema::HookKind;
use cc_lb_plugin_wire::v2;

pub(super) fn probe_filter(cell: &Arc<PluginCell>) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_filter_request()).map_err(|error| {
        probe_failed(HookKind::Filter, format!("encode FilterRequest: {error}"))
    })?;
    validate_filter_response(cell, input.as_slice())
}

fn validate_filter_response(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<(), WasmtimeRuntimeError> {
    let output = call_filter_hook(cell, input)
        .map_err(|error| probe_failed(HookKind::Filter, error.to_string()))?;
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    rkyv::access::<ArchivedFilterResponse, RkyvError>(&aligned).map_err(|error| {
        probe_failed(HookKind::Filter, format!("decode FilterResponse: {error}"))
    })?;
    Ok(())
}

fn sample_filter_request() -> FilterRequest {
    FilterRequest {
        request_id: Box::from("probe-req-1"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from("claude-3-haiku-20240307"),
        cache_pricing: cache_pricing(),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(&br#"{"model":"claude-3-haiku-20240307","messages":[]}"#[..]),
        principal: synth_principal(),
        candidates: Box::new([]),
    }
}

fn cache_pricing() -> CachePricingSummary {
    CachePricingSummary {
        status: Box::from("unknown"),
        input_micros_per_million: None,
        cache_creation_5m_micros_per_million: None,
        cache_creation_1h_micros_per_million: None,
        cache_read_micros_per_million: None,
    }
}

pub(super) fn probe_filter_v2(cell: &Arc<PluginCell>) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_filter_request_v2()).map_err(|error| {
        probe_failed(
            HookKind::Filter,
            format!("encode FilterRequest V2: {error}"),
        )
    })?;
    validate_filter_response(cell, input.as_slice())
}

fn sample_filter_request_v2() -> v2::FilterRequest {
    let base = sample_filter_request();
    v2::FilterRequest {
        routing_now_unix_secs: 1_800_000_000,
        request_id: base.request_id,
        thread_id: base.thread_id,
        service_tier: base.service_tier,
        canonical_model_id: base.canonical_model_id,
        cache_pricing: base.cache_pricing,
        method: base.method,
        path: base.path,
        query: base.query,
        headers: base.headers,
        body: base.body,
        principal: base.principal,
        candidates: Box::new([v2::UpstreamCandidate {
            upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
            name: Box::from("probe-subscription"),
            kind: Box::from("anthropic_oauth"),
            observed_at_unix_secs: 0,
            predicted_cache_read_tokens: 128,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
            subscription_quotas: Box::new([v2::QuotaWindowSnapshot {
                window: Box::from("7d"),
                state: Box::from("fresh"),
                utilization: Some(0.25),
                resets_at_unix_secs: Some(1_700_604_800),
                observed_at_unix_millis: Some(1_700_000_000_000),
                status: Some(Box::from("allowed")),
                disabled_reason: None,
                max_staleness_secs: 300,
            }]),
        }]),
    }
}
