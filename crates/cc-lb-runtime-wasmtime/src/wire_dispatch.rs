use std::sync::Arc;

use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily, WireVersion};
use rkyv::util::AlignedVec;

use crate::HotEngineConfig;
use crate::cache::call_hook_export_scoped;
use crate::cell::{LoadedPluginSlot, PluginCell};
use crate::error::WasmtimeRuntimeError;
use crate::policy::{PluginWireBounds, ShapeOriginPolicy};

#[derive(Clone, Copy)]
struct ResolvedHook {
    export: &'static str,
    version: WireVersion,
    active: bool,
}

/// Immutable module snapshot with export selectors resolved outside the proxy path.
/// Typed functions are bound only inside each invocation's fresh Store.
#[derive(Clone)]
pub struct WasmPluginWireDispatch {
    pub(crate) cell: Arc<PluginCell>,
    pub(crate) kind: HookKind,
    pub(crate) config: Arc<HotEngineConfig>,
    filter: Option<ResolvedHook>,
    shape: Option<ResolvedHook>,
    response: Option<ResolvedHook>,
    sse: Option<ResolvedHook>,
    realtime_frame: Option<ResolvedHook>,
    envelope: Option<ResolvedHook>,
}

impl WasmPluginWireDispatch {
    /// Snapshot a slot. Existing direct callers retain the Anthropic contract.
    /// Provider pipelines use `for_provider` while constructing the view.
    pub fn from_slot(slot: Arc<LoadedPluginSlot>, config: Arc<HotEngineConfig>) -> Self {
        let mut dispatch = Self {
            cell: slot.current.load_full(),
            kind: slot.kind,
            config,
            filter: None,
            shape: None,
            response: None,
            sse: None,
            realtime_frame: None,
            envelope: None,
        };
        dispatch.resolve(ProviderFamily::Anthropic);
        dispatch
    }

    /// Select a provider's validated exports once; absence means skip the plugin.
    pub fn for_provider(&self, provider: ProviderFamily) -> Option<Self> {
        let mut selected = self.clone();
        selected.resolve(provider);
        let applicable = match selected.kind {
            HookKind::Filter => selected.filter.is_some(),
            HookKind::ShapeEnvelopeGemini => selected.envelope.is_some(),
            HookKind::TransformRealtimeFrame => selected.realtime_frame.is_some_and(|h| h.active),
            _ => {
                selected.shape.is_some()
                    || selected.response.is_some_and(|h| h.active)
                    || selected.sse.is_some_and(|h| h.active)
            }
        };
        applicable.then_some(selected)
    }
    fn resolve(&mut self, provider: ProviderFamily) {
        self.filter = self.resolve_hook(HookKind::Filter, provider);
        self.shape = self.resolve_hook(HookKind::Shape, provider);
        self.response = self.resolve_hook(HookKind::TransformResponse, provider);
        self.sse = self.resolve_hook(HookKind::TransformSseEvent, provider);
        self.realtime_frame = self.resolve_hook(HookKind::TransformRealtimeFrame, provider);
        self.envelope = self.resolve_hook(HookKind::ShapeEnvelopeGemini, provider);
    }
    fn resolve_hook(&self, hook: HookKind, provider: ProviderFamily) -> Option<ResolvedHook> {
        if hook == HookKind::ShapeEnvelopeGemini && provider != ProviderFamily::Gemini {
            return None;
        }
        let export = hook.qualified_export_name(provider);
        if let Some(metadata) = self.cell.metadata.hooks.get(export.strip_prefix("cc_lb_")?) {
            return Some(ResolvedHook {
                export,
                version: WireVersion::from_u8(metadata.wire_version)?,
                active: !metadata.mode.is_noop(),
            });
        }
        if provider != ProviderFamily::Anthropic {
            return None;
        }
        let metadata = self.cell.metadata.hooks.get(hook.as_str())?;
        Some(ResolvedHook {
            export: hook.export_name(),
            version: WireVersion::from_u8(metadata.wire_version)?,
            active: !metadata.mode.is_noop(),
        })
    }

    pub fn kind(&self) -> HookKind {
        self.kind
    }
    pub fn config(&self) -> &Arc<HotEngineConfig> {
        &self.config
    }
    pub fn wire_bounds(&self) -> &PluginWireBounds {
        &self.config.wire_bounds
    }
    pub fn shape_origin_policy(&self) -> ShapeOriginPolicy {
        self.config.shape_origin_policy
    }
    pub fn cookie_redaction(&self) -> bool {
        self.config.cookie_redaction
    }

    fn call<R, F>(
        &self,
        hook: HookKind,
        selected: Option<ResolvedHook>,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        let selected = selected.ok_or_else(|| WasmtimeRuntimeError::ModuleRejected {
            reason: format!(
                "hook `{}` is absent from this provider's prebuilt pipeline",
                hook.as_str()
            ),
        })?;
        call_hook_export_scoped(&self.cell, input, hook, selected.export, with_output)
    }

    /// Consume checked guest output before its fresh Store is dropped.
    ///
    /// ```compile_fail
    /// use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntimeError};
    /// fn escape_guest_output<'a>(
    ///     dispatch: &'a WasmPluginWireDispatch,
    ///     input: &'a [u8],
    /// ) -> Result<&'a [u8], WasmtimeRuntimeError> {
    ///     dispatch.call_filter_scoped(input, |guest_output| guest_output)
    /// }
    /// ```
    pub fn call_filter_scoped<R, F>(
        &self,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        self.call(HookKind::Filter, self.filter, input, with_output)
    }
    pub fn call_shape_scoped<R, F>(
        &self,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        self.call(HookKind::Shape, self.shape, input, with_output)
    }
    pub fn call_transform_response_scoped<R, F>(
        &self,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        self.call(
            HookKind::TransformResponse,
            self.response,
            input,
            with_output,
        )
    }
    pub fn call_transform_sse_event_scoped<R, F>(
        &self,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        self.call(HookKind::TransformSseEvent, self.sse, input, with_output)
    }
    pub fn call_transform_realtime_frame(
        &self,
        input: &[u8],
    ) -> Result<AlignedVec<16>, WasmtimeRuntimeError> {
        self.call(
            HookKind::TransformRealtimeFrame,
            self.realtime_frame,
            input,
            copy_aligned,
        )
    }
    pub fn call_shape_envelope_gemini_scoped<R, F>(
        &self,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        self.call(
            HookKind::ShapeEnvelopeGemini,
            self.envelope,
            input,
            with_output,
        )
    }
    pub fn call_transform_response(
        &self,
        input: &[u8],
    ) -> Result<AlignedVec<16>, WasmtimeRuntimeError> {
        self.call_transform_response_scoped(input, copy_aligned)
    }
    pub fn call_transform_sse_event(
        &self,
        input: &[u8],
    ) -> Result<AlignedVec<16>, WasmtimeRuntimeError> {
        self.call_transform_sse_event_scoped(input, copy_aligned)
    }
    pub fn realtime_frame_wire_version(&self) -> Option<WireVersion> {
        self.realtime_frame.map(|h| h.version)
    }
    pub fn filter_wire_version(&self) -> Option<WireVersion> {
        self.filter.map(|h| h.version)
    }
    pub fn shape_wire_version(&self) -> Option<WireVersion> {
        self.shape.map(|h| h.version)
    }
    pub fn response_wire_version(&self) -> Option<WireVersion> {
        self.response.map(|h| h.version)
    }
    pub fn sse_wire_version(&self) -> Option<WireVersion> {
        self.sse.map(|h| h.version)
    }
    pub fn envelope_wire_version(&self) -> Option<WireVersion> {
        self.envelope.map(|h| h.version)
    }

    pub fn has_shape_envelope_gemini(&self) -> bool {
        self.envelope.is_some_and(|h| h.active)
    }
    pub fn has_response_transform(&self) -> bool {
        self.response.is_some_and(|h| h.active)
    }
    pub fn has_sse_event_transform(&self) -> bool {
        self.sse.is_some_and(|h| h.active)
    }
    pub fn has_realtime_frame_transform(&self) -> bool {
        self.realtime_frame.is_some_and(|h| h.active)
    }
}

fn copy_aligned(output: &[u8]) -> AlignedVec<16> {
    let mut aligned = AlignedVec::with_capacity(output.len());
    aligned.extend_from_slice(output);
    aligned
}
