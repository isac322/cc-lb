use std::sync::Arc;

use cc_lb_plugin_wire::metadata::{HookMode, PluginMetadata};
use cc_lb_plugin_wire::schema::{HookKind, WireVersion};
use cc_lb_plugin_wire::v1::{
    ArchivedShapeResponse, ArchivedTransformResponseResult, ArchivedTransformSseEventResult,
    Header, ObserveEvent, Principal, ShapeRequest, SseEvent, TransformResponseRequest,
    TransformSseEventRequest, Upstream,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;
use wasmtime::InstancePre;

use crate::budget::StoreBudget;
use crate::cache::{
    call_observe_hook, call_shape_hook, call_transform_response_hook, call_transform_sse_event_hook,
};
use crate::cell::PluginCell;
use crate::engine::HostState;
use crate::error::WasmtimeRuntimeError;

mod filter;

use filter::{probe_filter_v1, probe_filter_v2};

pub(crate) fn probe_hook_dispatch(
    instance_pre: Arc<InstancePre<HostState>>,
    hook: HookKind,
    wire_version: WireVersion,
    metadata: &PluginMetadata,
    memory_max_pages: u32,
) -> Result<(), WasmtimeRuntimeError> {
    if matches!(
        hook,
        HookKind::TransformResponse | HookKind::TransformSseEvent
    ) && metadata
        .hooks
        .get(hook.as_str())
        .is_some_and(|hook_metadata| hook_metadata.mode == HookMode::Noop)
    {
        return Ok(());
    }

    let cell = Arc::new(PluginCell {
        version_id: 0,
        instance_pre,
        metadata: metadata.clone(),
        memory_max_pages,
        store_budget: Arc::new(StoreBudget::new(1)),
        plugin_name: Arc::from(metadata.name.as_str()),
        content_hash: [0; 32],
    });
    match (hook, wire_version) {
        (HookKind::Filter, WireVersion::V1) => probe_filter_v1(&cell),
        (HookKind::Filter, WireVersion::V2) => probe_filter_v2(&cell),
        (HookKind::Shape, WireVersion::V1) => probe_shape_v1(&cell),
        (HookKind::Observe, WireVersion::V1) => probe_observe_v1(&cell),
        (HookKind::TransformResponse, WireVersion::V1) => probe_transform_response_v1(&cell),
        (HookKind::TransformSseEvent, WireVersion::V1) => probe_transform_sse_event_v1(&cell),
        (HookKind::Shape, WireVersion::V2)
        | (HookKind::Observe, WireVersion::V2)
        | (HookKind::TransformResponse, WireVersion::V2)
        | (HookKind::TransformSseEvent, WireVersion::V2) => Err(probe_failed(
            hook,
            format!("unsupported wire version {}", wire_version.as_u8()),
        )),
    }
}

fn probe_shape_v1(cell: &Arc<PluginCell>) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_shape_request())
        .map_err(|error| probe_failed(HookKind::Shape, format!("encode ShapeRequest: {error}")))?;
    let output = call_shape_hook(cell, input.as_slice())
        .map_err(|error| probe_failed(HookKind::Shape, error.to_string()))?;
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    rkyv::access::<ArchivedShapeResponse, RkyvError>(&aligned)
        .map_err(|error| probe_failed(HookKind::Shape, format!("decode ShapeResponse: {error}")))?;
    Ok(())
}

fn probe_observe_v1(cell: &Arc<PluginCell>) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_observe_event()).map_err(|error| {
        probe_failed(HookKind::Observe, format!("encode ObserveEvent: {error}"))
    })?;
    call_observe_hook(cell, input.as_slice())
        .map_err(|error| probe_failed(HookKind::Observe, error.to_string()))?;
    Ok(())
}

fn probe_transform_response_v1(cell: &Arc<PluginCell>) -> Result<(), WasmtimeRuntimeError> {
    let input =
        rkyv::to_bytes::<RkyvError>(&sample_transform_response_request()).map_err(|error| {
            probe_failed(
                HookKind::TransformResponse,
                format!("encode TransformResponseRequest: {error}"),
            )
        })?;
    let output = call_transform_response_hook(cell, input.as_slice())
        .map_err(|error| probe_failed(HookKind::TransformResponse, error.to_string()))?;
    rkyv::access::<ArchivedTransformResponseResult, RkyvError>(&output).map_err(|error| {
        probe_failed(
            HookKind::TransformResponse,
            format!("decode TransformResponseResult: {error}"),
        )
    })?;
    Ok(())
}

fn probe_transform_sse_event_v1(cell: &Arc<PluginCell>) -> Result<(), WasmtimeRuntimeError> {
    let input =
        rkyv::to_bytes::<RkyvError>(&sample_transform_sse_event_request()).map_err(|error| {
            probe_failed(
                HookKind::TransformSseEvent,
                format!("encode TransformSseEventRequest: {error}"),
            )
        })?;
    let output = call_transform_sse_event_hook(cell, input.as_slice())
        .map_err(|error| probe_failed(HookKind::TransformSseEvent, error.to_string()))?;
    rkyv::access::<ArchivedTransformSseEventResult, RkyvError>(&output).map_err(|error| {
        probe_failed(
            HookKind::TransformSseEvent,
            format!("decode TransformSseEventResult: {error}"),
        )
    })?;
    Ok(())
}

fn sample_shape_request() -> ShapeRequest {
    ShapeRequest {
        request_id: Box::from("probe-req-1"),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(&br#"{"model":"claude-3-haiku-20240307","messages":[]}"#[..]),
        principal: synth_principal(),
        upstream: Upstream::AnthropicDirect {
            base_url: Some(Box::from("https://example.test")),
        },
    }
}

fn sample_observe_event() -> ObserveEvent {
    ObserveEvent::RequestStarted {
        request_id: Box::from("probe-req-1"),
        downstream_user_agent: Some(Box::from("cc-lb-probe/1.0")),
    }
}

fn sample_transform_response_request() -> TransformResponseRequest {
    TransformResponseRequest {
        request_id: Box::from("probe-req-1"),
        principal: synth_principal(),
        upstream: Upstream::AnthropicDirect {
            base_url: Some(Box::from("https://example.test")),
        },
        request_method: Box::from("POST"),
        request_path: Box::from("/v1/messages"),
        canonical_model_id: Box::from("claude-3-haiku-20240307"),
        response_status: 200,
        response_headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(&br#"{"content":[]}"#[..]),
    }
}

fn sample_transform_sse_event_request() -> TransformSseEventRequest {
    TransformSseEventRequest {
        request_id: Box::from("probe-req-1"),
        principal: synth_principal(),
        upstream: Upstream::AnthropicDirect {
            base_url: Some(Box::from("https://example.test")),
        },
        request_method: Box::from("POST"),
        request_path: Box::from("/v1/messages"),
        canonical_model_id: Box::from("claude-3-haiku-20240307"),
        response_status: 200,
        response_headers: Box::new([hdr("content-type", "text/event-stream")]),
        event: SseEvent {
            event: Box::from("content_block_start"),
            data: Box::from(&br#"{"type":"content_block_start"}"#[..]),
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
