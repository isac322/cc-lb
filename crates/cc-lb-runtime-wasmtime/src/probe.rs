use std::sync::Arc;

use cc_lb_plugin_wire::metadata::{HookMode, PluginMetadata};
use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily, WireVersion};
use rkyv::util::AlignedVec;
use wasmtime::InstancePre;

use crate::budget::StoreBudget;
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
    provider: ProviderFamily,
    metadata: &PluginMetadata,
    memory_max_pages: u32,
) -> Result<(), WasmtimeRuntimeError> {
    let export = hook.qualified_export_name(provider);
    let hook_key = export.strip_prefix("cc_lb_").unwrap_or(export);
    let noop = matches!(
        hook,
        HookKind::TransformResponse
            | HookKind::TransformSseEvent
            | HookKind::TransformRealtimeFrame
    ) && metadata
        .hooks
        .get(hook_key)
        .is_some_and(|hook_metadata| hook_metadata.mode == HookMode::Noop);

    let cell = Arc::new(PluginCell {
        instance_pre,
        metadata: metadata.clone(),
        memory_max_pages,
        store_budget: Arc::new(StoreBudget::new(1)),
        plugin_name: Arc::from(metadata.name.as_str()),
        content_hash: [0; 32],
    });
    if noop {
        crate::cache::validate_hook_export(&cell, hook, export)?;
        return Ok(());
    }
    match (hook, wire_version) {
        (HookKind::Filter, version) => probe_filter(&cell, provider, export, version),
        (HookKind::Shape, WireVersion::V2) => probe_shape_v2(&cell, provider, export),
        (HookKind::Shape, WireVersion::V3) => Err(probe_failed(
            HookKind::Shape,
            "wire v3 is unsupported for shape hooks".into(),
        )),
        (HookKind::TransformResponse, WireVersion::V2) => {
            probe_transform_response_v2(&cell, provider, export)
        }
        (HookKind::TransformResponse, WireVersion::V3) => Err(probe_failed(
            HookKind::TransformResponse,
            "wire v3 is unsupported for response transforms".into(),
        )),
        (HookKind::TransformSseEvent, WireVersion::V2) => {
            probe_transform_sse_event_v2(&cell, provider, export)
        }
        (HookKind::TransformSseEvent, WireVersion::V3) => Err(probe_failed(
            HookKind::TransformSseEvent,
            "wire v3 is unsupported for SSE transforms".into(),
        )),
        (HookKind::TransformRealtimeFrame, WireVersion::V2) => {
            probe_transform_realtime_frame_v2(&cell, provider, export)
        }
        (HookKind::TransformRealtimeFrame, WireVersion::V3) => Err(probe_failed(
            HookKind::TransformRealtimeFrame,
            "wire v3 is unsupported for realtime frame transforms".into(),
        )),
        (HookKind::ShapeEnvelopeGemini, WireVersion::V2) => {
            probe_shape_envelope_gemini_v2(&cell, provider, export)
        }
        (HookKind::ShapeEnvelopeGemini, WireVersion::V3) => Err(probe_failed(
            HookKind::ShapeEnvelopeGemini,
            "wire v3 is unsupported for Gemini shape envelope".into(),
        )),
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
