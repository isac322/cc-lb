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

use filter::probe_filter_v3;
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
    epoch_deadline_ticks: u64,
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
        store_budget: Arc::new(StoreBudget::with_epoch_deadline_ticks(
            1,
            epoch_deadline_ticks,
        )),
        plugin_name: Arc::from(metadata.name.as_str()),
        content_hash: [0; 32],
    });
    if noop {
        crate::cache::validate_hook_export(&cell, export)?;
        return Ok(());
    }
    match (hook, wire_version) {
        (HookKind::Filter, WireVersion::V3) => probe_filter_v3(&cell, provider, export),
        (HookKind::Shape, WireVersion::V2) => probe_shape_v2(&cell, provider, export),
        (HookKind::TransformResponse, WireVersion::V2) => {
            probe_transform_response_v2(&cell, provider, export)
        }
        (HookKind::TransformSseEvent, WireVersion::V2) => {
            probe_transform_sse_event_v2(&cell, provider, export)
        }
        (HookKind::TransformRealtimeFrame, WireVersion::V2) => {
            probe_transform_realtime_frame_v2(&cell, provider, export)
        }
        (HookKind::ShapeEnvelopeGemini, WireVersion::V2) => {
            probe_shape_envelope_gemini_v2(&cell, provider, export)
        }
        _ => Err(probe_failed(
            hook,
            format!(
                "wire v{} is unsupported for {} hooks",
                wire_version.as_u8(),
                hook.as_str()
            ),
        )),
    }
}

fn probe_failed(hook: HookKind, reason: String) -> WasmtimeRuntimeError {
    WasmtimeRuntimeError::ProbeFailed {
        hook: hook.as_str(),
        reason,
    }
}

pub(super) fn map_probe_call_error(
    hook: HookKind,
    error: WasmtimeRuntimeError,
) -> WasmtimeRuntimeError {
    if error.is_pool_saturated() || error.is_epoch_interrupt() {
        error
    } else {
        probe_failed(hook, error.to_string())
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
