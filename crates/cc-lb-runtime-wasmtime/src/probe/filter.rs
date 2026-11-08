use std::sync::Arc;

use cc_lb_plugin_wire::v1::{ArchivedFilterResponse, CachePricingSummary, FilterRequest};
use cc_lb_plugin_wire::v2::{
    ArchivedFilterResponse as ArchivedFilterResponseV2,
    CachePricingSummary as CachePricingSummaryV2, FilterRequest as FilterRequestV2,
    Header as HeaderV2, Principal as PrincipalV2,
};
use cc_lb_plugin_wire::v3::{
    self, FilterRequest as FilterRequestV3, NativeCachePricing, NativeCachePricingComponent,
    NativeCachePricingLifetime, NativeCachePricingProvider, NativeCachePricingRate,
    NativeCachePricingStatus,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

use crate::cache::call_hook_export_scoped;
use crate::cell::PluginCell;
use crate::error::WasmtimeRuntimeError;

use super::{hdr, probe_failed, sample_body, sample_model, sample_path, synth_principal};
use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily, WireVersion};

pub(super) fn probe_filter(
    cell: &Arc<PluginCell>,
    provider: ProviderFamily,
    export: &str,
    wire_version: WireVersion,
) -> Result<(), WasmtimeRuntimeError> {
    match wire_version {
        WireVersion::V1 => probe_filter_v1(cell, provider, export),
        WireVersion::V2 => probe_filter_v2(cell, provider, export),
        WireVersion::V3 => probe_filter_v3(cell, provider, export),
    }
}

fn probe_filter_v1(
    cell: &Arc<PluginCell>,
    provider: ProviderFamily,
    export: &str,
) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_filter_request(provider)).map_err(|error| {
        probe_failed(
            HookKind::Filter,
            format!("encode V1 FilterRequest: {error}"),
        )
    })?;
    let output = call_hook_export_scoped(
        cell,
        input.as_slice(),
        HookKind::Filter,
        export,
        <[u8]>::to_vec,
    )
    .map_err(|error| probe_failed(HookKind::Filter, error.to_string()))?;
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    rkyv::access::<ArchivedFilterResponse, RkyvError>(&aligned).map_err(|error| {
        probe_failed(
            HookKind::Filter,
            format!("decode V1 FilterResponse: {error}"),
        )
    })?;
    Ok(())
}

fn probe_filter_v2(
    cell: &Arc<PluginCell>,
    provider: ProviderFamily,
    export: &str,
) -> Result<(), WasmtimeRuntimeError> {
    let input =
        rkyv::to_bytes::<RkyvError>(&sample_filter_request_v2(provider)).map_err(|error| {
            probe_failed(
                HookKind::Filter,
                format!("encode V2 FilterRequest: {error}"),
            )
        })?;
    let output = call_hook_export_scoped(
        cell,
        input.as_slice(),
        HookKind::Filter,
        export,
        <[u8]>::to_vec,
    )
    .map_err(|error| probe_failed(HookKind::Filter, error.to_string()))?;
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    rkyv::access::<ArchivedFilterResponseV2, RkyvError>(&aligned).map_err(|error| {
        probe_failed(
            HookKind::Filter,
            format!("decode V2 FilterResponse: {error}"),
        )
    })?;
    Ok(())
}

fn probe_filter_v3(
    cell: &Arc<PluginCell>,
    provider: ProviderFamily,
    export: &str,
) -> Result<(), WasmtimeRuntimeError> {
    let input =
        rkyv::to_bytes::<RkyvError>(&sample_filter_request_v3(provider)).map_err(|error| {
            probe_failed(
                HookKind::Filter,
                format!("encode V3 FilterRequest: {error}"),
            )
        })?;
    let output = call_hook_export_scoped(
        cell,
        input.as_slice(),
        HookKind::Filter,
        export,
        <[u8]>::to_vec,
    )
    .map_err(|error| probe_failed(HookKind::Filter, error.to_string()))?;
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    rkyv::access::<ArchivedFilterResponseV2, RkyvError>(&aligned).map_err(|error| {
        probe_failed(
            HookKind::Filter,
            format!("decode V3 FilterResponse: {error}"),
        )
    })?;
    Ok(())
}

fn sample_filter_request(provider: ProviderFamily) -> FilterRequest {
    FilterRequest {
        request_id: Box::from("probe-req-1"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from(sample_model(provider)),
        cache_pricing: cache_pricing(),
        method: Box::from("POST"),
        path: Box::from(sample_path(provider)),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(sample_body(provider)),
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
fn sample_filter_request_v2(provider: ProviderFamily) -> FilterRequestV2 {
    FilterRequestV2 {
        request_id: Box::from("probe-req-1"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from(sample_model(provider)),
        cache_pricing: CachePricingSummaryV2 {
            status: Box::from("unknown"),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        method: Box::from("POST"),
        path: Box::from(sample_path(provider)),
        query: None,
        headers: Box::new([HeaderV2 {
            name: Box::from("content-type"),
            value: Box::from(br#"application/json"# as &[u8]),
        }]),
        body: Box::from(sample_body(provider)),
        principal: PrincipalV2 {
            id: Box::from("probe-principal"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        candidates: Box::new([]),
    }
}

fn sample_filter_request_v3(provider: ProviderFamily) -> FilterRequestV3 {
    FilterRequestV3 {
        request_id: Box::from("probe-req-1"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from(sample_model(provider)),
        cache_pricing: CachePricingSummaryV2 {
            status: Box::from("unknown"),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        native_pricing: NativeCachePricing {
            version: 1,
            provider: match provider {
                ProviderFamily::Anthropic => NativeCachePricingProvider::Anthropic,
                ProviderFamily::Openai => NativeCachePricingProvider::OpenAi,
                ProviderFamily::Gemini => NativeCachePricingProvider::Gemini,
            },
            rates: Box::new([
                NativeCachePricingRate {
                    component: NativeCachePricingComponent::OrdinaryInput,
                    lifetime: None,
                    status: NativeCachePricingStatus::Known,
                    micros_per_million: Some(2_000_000),
                },
                NativeCachePricingRate {
                    component: NativeCachePricingComponent::TotalWrite,
                    lifetime: Some(NativeCachePricingLifetime::ThirtyMinutes),
                    status: NativeCachePricingStatus::Unknown,
                    micros_per_million: None,
                },
            ]),
        },
        method: Box::from("POST"),
        path: Box::from(sample_path(provider)),
        query: None,
        headers: Box::new([v3::Header {
            name: Box::from("content-type"),
            value: Box::from(br#"application/json"# as &[u8]),
        }]),
        body: Box::from(sample_body(provider)),
        principal: v3::Principal {
            id: Box::from("probe-principal"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        candidates: Box::new([]),
    }
}
