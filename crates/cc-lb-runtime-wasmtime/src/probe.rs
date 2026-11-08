use std::sync::Arc;

use cc_lb_plugin_wire::metadata::{HookMode, PluginMetadata};
use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily, WireVersion};
use cc_lb_plugin_wire::v1::{
    ArchivedShapeResponse, ArchivedTransformResponseResult, ArchivedTransformSseEventResult,
    Header, Principal, ShapeRequest, SseEvent, TransformResponseRequest, TransformSseEventRequest,
    Upstream,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;
use wasmtime::InstancePre;

use crate::budget::StoreBudget;
use crate::cache::call_hook_export_scoped;
use crate::cell::PluginCell;
use crate::engine::HostState;
use crate::error::WasmtimeRuntimeError;

mod filter;
mod v2;

use filter::probe_filter;
use v2::{
    probe_envelope as probe_shape_envelope_gemini_v2,
    probe_frame as probe_transform_realtime_frame_v2,
    probe_response as probe_transform_response_v2, probe_shape as probe_shape_v2,
    probe_sse as probe_transform_sse_event_v2,
};
pub(crate) fn probe_hook_dispatch(
    instance_pre: Arc<InstancePre<HostState>>,
    hook: HookKind,
    wire_version: WireVersion,
    provider: Option<ProviderFamily>,
    metadata: &PluginMetadata,
    memory_max_pages: u32,
) -> Result<(), WasmtimeRuntimeError> {
    let noop = matches!(
        hook,
        HookKind::TransformResponse
            | HookKind::TransformSseEvent
            | HookKind::TransformRealtimeFrame
    ) && metadata
        .hooks
        .get(
            provider
                .map_or(hook.export_name(), |p| hook.qualified_export_name(p))
                .trim_start_matches("cc_lb_"),
        )
        .is_some_and(|hook_metadata| hook_metadata.mode == HookMode::Noop);

    let cell = Arc::new(PluginCell {
        instance_pre,
        metadata: metadata.clone(),
        memory_max_pages,
        store_budget: Arc::new(StoreBudget::new(1)),
        plugin_name: Arc::from(metadata.name.as_str()),
        content_hash: [0; 32],
    });
    let export = provider.map_or(hook.export_name(), |p| hook.qualified_export_name(p));
    if noop {
        if provider.is_some() {
            crate::cache::validate_hook_export(&cell, hook, export)?;
        }
        return Ok(());
    }
    let provider = provider.unwrap_or(ProviderFamily::Anthropic);
    match (hook, wire_version) {
        (HookKind::Filter, version) => probe_filter(&cell, provider, export, version),
        (HookKind::Shape, WireVersion::V1) => probe_shape_v1(&cell, provider, export),
        (HookKind::Shape, WireVersion::V2) => probe_shape_v2(&cell, provider, export),
        (HookKind::Shape, WireVersion::V3) => Err(probe_failed(
            HookKind::Shape,
            "wire v3 is unsupported for shape hooks".into(),
        )),
        (HookKind::TransformResponse, WireVersion::V1) => {
            probe_transform_response_v1(&cell, provider, export)
        }
        (HookKind::TransformResponse, WireVersion::V2) => {
            probe_transform_response_v2(&cell, provider, export)
        }
        (HookKind::TransformResponse, WireVersion::V3) => Err(probe_failed(
            HookKind::TransformResponse,
            "wire v3 is unsupported for response transforms".into(),
        )),
        (HookKind::TransformSseEvent, WireVersion::V1) => {
            probe_transform_sse_event_v1(&cell, provider, export)
        }
        (HookKind::TransformSseEvent, WireVersion::V2) => {
            probe_transform_sse_event_v2(&cell, provider, export)
        }
        (HookKind::TransformRealtimeFrame, WireVersion::V1) => Err(probe_failed(
            HookKind::TransformRealtimeFrame,
            "wire v1 is unsupported for realtime frame".into(),
        )),
        (HookKind::TransformRealtimeFrame, WireVersion::V2) => {
            probe_transform_realtime_frame_v2(&cell, provider, export)
        }
        (HookKind::TransformRealtimeFrame, WireVersion::V3) => Err(probe_failed(
            HookKind::TransformRealtimeFrame,
            "wire v3 is unsupported for realtime frame transforms".into(),
        )),
        (HookKind::TransformSseEvent, WireVersion::V3) => Err(probe_failed(
            HookKind::TransformSseEvent,
            "wire v3 is unsupported for SSE transforms".into(),
        )),
        (HookKind::ShapeEnvelopeGemini, WireVersion::V2) => {
            probe_shape_envelope_gemini_v2(&cell, provider, export)
        }
        (HookKind::ShapeEnvelopeGemini, WireVersion::V1) => Err(probe_failed(
            HookKind::ShapeEnvelopeGemini,
            "wire v1 is unsupported for Gemini shape envelope".into(),
        )),
        (HookKind::ShapeEnvelopeGemini, WireVersion::V3) => Err(probe_failed(
            HookKind::ShapeEnvelopeGemini,
            "wire v3 is unsupported for Gemini shape envelope".into(),
        )),
    }
}

fn probe_shape_v1(
    cell: &Arc<PluginCell>,
    provider: ProviderFamily,
    export: &str,
) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_shape_request(provider))
        .map_err(|error| probe_failed(HookKind::Shape, format!("encode ShapeRequest: {error}")))?;
    let output = call_hook_export_scoped(
        cell,
        input.as_slice(),
        HookKind::Shape,
        export,
        <[u8]>::to_vec,
    )
    .map_err(|error| probe_failed(HookKind::Shape, error.to_string()))?;
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    rkyv::access::<ArchivedShapeResponse, RkyvError>(&aligned)
        .map_err(|error| probe_failed(HookKind::Shape, format!("decode ShapeResponse: {error}")))?;
    Ok(())
}

fn probe_transform_response_v1(
    cell: &Arc<PluginCell>,
    provider: ProviderFamily,
    export: &str,
) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_transform_response_request(provider)).map_err(
        |error| {
            probe_failed(
                HookKind::TransformResponse,
                format!("encode TransformResponseRequest: {error}"),
            )
        },
    )?;
    let output = call_hook_export_scoped(
        cell,
        input.as_slice(),
        HookKind::TransformResponse,
        export,
        copy_aligned,
    )
    .map_err(|error| probe_failed(HookKind::TransformResponse, error.to_string()))?;
    rkyv::access::<ArchivedTransformResponseResult, RkyvError>(&output).map_err(|error| {
        probe_failed(
            HookKind::TransformResponse,
            format!("decode TransformResponseResult: {error}"),
        )
    })?;
    Ok(())
}

fn probe_transform_sse_event_v1(
    cell: &Arc<PluginCell>,
    provider: ProviderFamily,
    export: &str,
) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_transform_sse_event_request(provider))
        .map_err(|error| {
            probe_failed(
                HookKind::TransformSseEvent,
                format!("encode TransformSseEventRequest: {error}"),
            )
        })?;
    let output = call_hook_export_scoped(
        cell,
        input.as_slice(),
        HookKind::TransformSseEvent,
        export,
        copy_aligned,
    )
    .map_err(|error| probe_failed(HookKind::TransformSseEvent, error.to_string()))?;
    rkyv::access::<ArchivedTransformSseEventResult, RkyvError>(&output).map_err(|error| {
        probe_failed(
            HookKind::TransformSseEvent,
            format!("decode TransformSseEventResult: {error}"),
        )
    })?;
    Ok(())
}

fn sample_shape_request(provider: ProviderFamily) -> ShapeRequest {
    ShapeRequest {
        request_id: Box::from("probe-req-1"),
        method: Box::from("POST"),
        path: Box::from(sample_path(provider)),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(sample_body(provider)),
        principal: synth_principal(),
        upstream: sample_upstream(provider),
    }
}

fn sample_transform_response_request(provider: ProviderFamily) -> TransformResponseRequest {
    TransformResponseRequest {
        request_id: Box::from("probe-req-1"),
        principal: synth_principal(),
        upstream: sample_upstream(provider),
        request_method: Box::from("POST"),
        request_path: Box::from(sample_path(provider)),
        canonical_model_id: Box::from(sample_model(provider)),
        response_status: 200,
        response_headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(match provider {
            ProviderFamily::Anthropic => &br#"{"content":[]}"#[..],
            ProviderFamily::Openai => &br#"{"id":"resp_probe","output":[]}"#[..],
            ProviderFamily::Gemini => &br#"{"candidates":[]}"#[..],
        }),
    }
}

fn sample_transform_sse_event_request(provider: ProviderFamily) -> TransformSseEventRequest {
    TransformSseEventRequest {
        request_id: Box::from("probe-req-1"),
        principal: synth_principal(),
        upstream: sample_upstream(provider),
        request_method: Box::from("POST"),
        request_path: Box::from(sample_path(provider)),
        canonical_model_id: Box::from(sample_model(provider)),
        response_status: 200,
        response_headers: Box::new([hdr("content-type", "text/event-stream")]),
        event: SseEvent {
            event: Box::from(match provider {
                ProviderFamily::Anthropic => "content_block_start",
                ProviderFamily::Openai => "response.created",
                ProviderFamily::Gemini => "",
            }),
            data: Box::from(match provider {
                ProviderFamily::Anthropic => &br#"{"type":"content_block_start"}"#[..],
                ProviderFamily::Openai => {
                    &br#"{"type":"response.created","response":{"id":"resp_probe","output":[]}}"#[..]
                }
                ProviderFamily::Gemini => &br#"{"candidates":[]}"#[..],
            }),
        },
    }
}

fn synth_principal() -> Principal {
    Principal {
        id: Box::from("probe-principal"),
        kind: Box::from("api_key"),
        claims: Box::new([]),
    }
}

fn hdr(name: impl Into<String>, value: impl AsRef<[u8]>) -> Header {
    Header {
        name: name.into().into_boxed_str(),
        value: value.as_ref().to_vec().into_boxed_slice(),
    }
}

fn probe_failed(hook: HookKind, reason: String) -> WasmtimeRuntimeError {
    WasmtimeRuntimeError::ProbeFailed {
        hook: hook.as_str(),
        reason,
    }
}

fn copy_aligned(output: &[u8]) -> AlignedVec<16> {
    let mut aligned = AlignedVec::with_capacity(output.len());
    aligned.extend_from_slice(output);
    aligned
}

fn sample_upstream(provider: ProviderFamily) -> Upstream {
    let base_url = Some(Box::from("https://example.test"));
    match provider {
        ProviderFamily::Anthropic => Upstream::AnthropicDirect { base_url },
        ProviderFamily::Openai => Upstream::OpenaiDirect { base_url },
        ProviderFamily::Gemini => Upstream::GeminiDirect { base_url },
    }
}

fn sample_path(provider: ProviderFamily) -> &'static str {
    match provider {
        ProviderFamily::Anthropic => "/v1/messages",
        ProviderFamily::Openai => "/v1/responses",
        ProviderFamily::Gemini => "/v1beta/models/gemini-2.5-flash:generateContent",
    }
}

fn sample_model(provider: ProviderFamily) -> &'static str {
    match provider {
        ProviderFamily::Anthropic => "claude-3-haiku-20240307",
        ProviderFamily::Openai => "gpt-4.1",
        ProviderFamily::Gemini => "gemini-2.5-flash",
    }
}

fn sample_body(provider: ProviderFamily) -> &'static [u8] {
    match provider {
        ProviderFamily::Anthropic => br#"{"model":"claude-3-haiku-20240307","messages":[]}"#,
        ProviderFamily::Openai => br#"{"model":"gpt-4.1","input":"probe"}"#,
        ProviderFamily::Gemini => br#"{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}"#,
    }
}
