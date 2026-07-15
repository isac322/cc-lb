use std::sync::Arc;

use cc_lb_plugin_wire::v1::{ArchivedFilterResponse, CachePricingSummary, FilterRequest};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

use crate::cache::call_filter_hook;
use crate::cell::PluginCell;
use crate::error::WasmtimeRuntimeError;

use super::{hdr, probe_failed, synth_principal};
use cc_lb_plugin_wire::schema::HookKind;

pub(super) fn probe_filter_v1(cell: &Arc<PluginCell>) -> Result<(), WasmtimeRuntimeError> {
    probe_filter(cell, &sample_filter_request_v1())
}

fn probe_filter<T>(cell: &Arc<PluginCell>, request: &T) -> Result<(), WasmtimeRuntimeError>
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                RkyvError,
            >,
        >,
{
    let input = rkyv::to_bytes::<RkyvError>(request).map_err(|error| {
        probe_failed(HookKind::Filter, format!("encode FilterRequest: {error}"))
    })?;
    let output = call_filter_hook(cell, input.as_slice())
        .map_err(|error| probe_failed(HookKind::Filter, error.to_string()))?;
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    rkyv::access::<ArchivedFilterResponse, RkyvError>(&aligned).map_err(|error| {
        probe_failed(HookKind::Filter, format!("decode FilterResponse: {error}"))
    })?;
    Ok(())
}

fn sample_filter_request_v1() -> FilterRequest {
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
