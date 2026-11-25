use std::sync::Arc;

use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily};
use cc_lb_plugin_wire::v2::{
    ArchivedFilterResponse as ArchivedFilterResponseV2,
    CachePricingSummary as CachePricingSummaryV2,
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

use super::{map_probe_call_error, probe_failed, sample_body, sample_model, sample_path};

pub(super) fn probe_filter_v3(
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
    .map_err(|error| map_probe_call_error(HookKind::Filter, error))?;
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
