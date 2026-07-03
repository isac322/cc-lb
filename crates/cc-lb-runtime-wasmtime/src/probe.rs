use std::sync::Arc;

use cc_lb_plugin_wire::metadata::PluginMetadata;
use cc_lb_plugin_wire::schema::{HookKind, WireVersion};
use cc_lb_plugin_wire::v1::{
    ArchivedFilterResponse, ArchivedShapeResponse, FilterRequest, Header, ObserveEvent, Principal,
    ShapeRequest, Upstream,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;
use wasmtime::InstancePre;

use crate::cache::{call_filter_hook, call_observe_hook, call_shape_hook};
use crate::cell::PluginCell;
use crate::engine::HostState;
use crate::error::WasmtimeRuntimeError;

pub(crate) fn probe_hook_dispatch(
    instance_pre: Arc<InstancePre<HostState>>,
    hook: HookKind,
    wire_version: WireVersion,
    metadata: &PluginMetadata,
    memory_max_pages: u32,
) -> Result<(), WasmtimeRuntimeError> {
    let cell = Arc::new(PluginCell {
        version_id: 0,
        instance_pre,
        metadata: metadata.clone(),
        memory_max_pages,
        plugin_name: Arc::from(metadata.name.as_str()),
        content_hash: [0; 32],
    });
    match (hook, wire_version) {
        (HookKind::Filter, WireVersion::V1) => probe_filter_v1(&cell),
        (HookKind::Shape, WireVersion::V1) => probe_shape_v1(&cell),
        (HookKind::Observe, WireVersion::V1) => probe_observe_v1(&cell),
    }
}

fn probe_filter_v1(cell: &Arc<PluginCell>) -> Result<(), WasmtimeRuntimeError> {
    let input = rkyv::to_bytes::<RkyvError>(&sample_filter_request()).map_err(|error| {
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

fn sample_filter_request() -> FilterRequest {
    FilterRequest {
        request_id: Box::from("probe-req-1"),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(&br#"{"model":"claude-3-haiku-20240307","messages":[]}"#[..]),
        principal: synth_principal(),
        candidates: Box::new([]),
    }
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
