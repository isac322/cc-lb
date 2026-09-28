use std::sync::Arc;

use rkyv::util::AlignedVec;

use crate::cache::{
    call_filter_hook_scoped, call_shape_hook_scoped, call_transform_response_hook_scoped,
    call_transform_sse_event_hook_scoped,
};
use crate::cell::{LoadedPluginSlot, PluginCell};
use crate::error::WasmtimeRuntimeError;
use crate::policy::{PluginWireBounds, ShapeOriginPolicy};
use crate::{HotEngineConfig, SlotKind};

/// Wire-level dispatch handle for a single loaded plugin slot.
///
/// Snapshots the slot's [`PluginCell`] at construction time so
/// hot-swap replacements on the same [`RuntimeSlotKey`] cannot
/// disturb an in-flight view — each `DynamicView` keeps the cells
/// it was built with until it is itself replaced.
///
/// All methods take and return raw rkyv-encoded bytes; callers own
/// the host↔wire type conversion.
pub struct WasmPluginWireDispatch {
    pub(crate) cell: Arc<PluginCell>,
    pub(crate) kind: SlotKind,
    pub(crate) config: Arc<HotEngineConfig>,
}

impl WasmPluginWireDispatch {
    /// Snapshot the current cell from `slot` for the lifetime of this handle.
    pub fn from_slot(slot: Arc<LoadedPluginSlot>, config: Arc<HotEngineConfig>) -> Self {
        let cell = slot.current.load_full();
        let kind = slot.kind;
        Self { cell, kind, config }
    }

    /// The [`SlotKind`] this dispatch handle was built for.
    pub fn kind(&self) -> SlotKind {
        self.kind
    }

    /// Shareable config handle (wire bounds, origin policy, etc.).
    pub fn config(&self) -> &Arc<HotEngineConfig> {
        &self.config
    }

    /// Wire bounds from the runtime config.
    pub fn wire_bounds(&self) -> &PluginWireBounds {
        &self.config.wire_bounds
    }

    /// Shape origin policy from the runtime config.
    pub fn shape_origin_policy(&self) -> ShapeOriginPolicy {
        self.config.shape_origin_policy
    }

    /// Whether cookie headers should be stripped before crossing the plugin boundary.
    pub fn cookie_redaction(&self) -> bool {
        self.config.cookie_redaction
    }

    /// Dispatch a filter call. `input` must be rkyv-encoded `FilterRequest` bytes.
    /// Returns rkyv-encoded `FilterResponse` bytes.
    pub fn call_filter(&self, input: &[u8]) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.call_filter_scoped(input, <[u8]>::to_vec)
    }

    /// Dispatch a filter call and consume its checked output while guest memory is borrowed.
    ///
    /// The callback receives only the bounded guest-memory slice. Its higher-ranked lifetime
    /// prevents that slice from escaping in the return value:
    ///
    /// ```compile_fail
    /// use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntimeError};
    ///
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
        call_filter_hook_scoped(&self.cell, input, with_output)
    }

    /// Dispatch a shape call. `input` must be rkyv-encoded `ShapeRequest` bytes.
    /// Returns rkyv-encoded `ShapeResponse` bytes.
    pub fn call_shape(&self, input: &[u8]) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.call_shape_scoped(input, <[u8]>::to_vec)
    }

    /// Dispatch a shape call and consume its checked output before the fresh Store is dropped.
    pub fn call_shape_scoped<R, F>(
        &self,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        call_shape_hook_scoped(&self.cell, input, with_output)
    }

    /// Dispatch a transform_response call. Returns aligned bytes for rkyv access.
    pub fn call_transform_response(
        &self,
        input: &[u8],
    ) -> Result<AlignedVec<16>, WasmtimeRuntimeError> {
        self.call_transform_response_scoped(input, |output| {
            let mut aligned = AlignedVec::with_capacity(output.len());
            aligned.extend_from_slice(output);
            aligned
        })
    }

    /// Dispatch a response transform and consume its checked output before the Store is dropped.
    pub fn call_transform_response_scoped<R, F>(
        &self,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        call_transform_response_hook_scoped(&self.cell, input, with_output)
    }

    /// Dispatch a transform_sse_event call. Returns aligned bytes for rkyv access.
    pub fn call_transform_sse_event(
        &self,
        input: &[u8],
    ) -> Result<AlignedVec<16>, WasmtimeRuntimeError> {
        self.call_transform_sse_event_scoped(input, |output| {
            let mut aligned = AlignedVec::with_capacity(output.len());
            aligned.extend_from_slice(output);
            aligned
        })
    }

    /// Dispatch an SSE transform and consume its checked output before the Store is dropped.
    pub fn call_transform_sse_event_scoped<R, F>(
        &self,
        input: &[u8],
        with_output: F,
    ) -> Result<R, WasmtimeRuntimeError>
    where
        F: for<'a> FnOnce(&'a [u8]) -> R,
    {
        call_transform_sse_event_hook_scoped(&self.cell, input, with_output)
    }

    /// Wire version for the filter hook, if declared in plugin metadata.
    pub fn filter_wire_version(&self) -> Option<cc_lb_plugin_wire::schema::WireVersion> {
        self.cell
            .metadata
            .hooks
            .get(cc_lb_plugin_wire::schema::HookKind::Filter.as_str())
            .and_then(|m| cc_lb_plugin_wire::schema::WireVersion::from_u8(m.wire_version))
    }

    /// Wire version for the shape hook, if declared in plugin metadata.
    pub fn shape_wire_version(&self) -> Option<cc_lb_plugin_wire::schema::WireVersion> {
        self.cell
            .metadata
            .hooks
            .get(cc_lb_plugin_wire::schema::HookKind::Shape.as_str())
            .and_then(|m| cc_lb_plugin_wire::schema::WireVersion::from_u8(m.wire_version))
    }

    /// Whether the transform_response hook is non-noop (active).
    pub fn has_response_transform(&self) -> bool {
        self.cell
            .metadata
            .hooks
            .get(cc_lb_plugin_wire::schema::HookKind::TransformResponse.as_str())
            .is_some_and(|m| !m.mode.is_noop())
    }

    /// Whether the transform_sse_event hook is non-noop (active).
    pub fn has_sse_event_transform(&self) -> bool {
        self.cell
            .metadata
            .hooks
            .get(cc_lb_plugin_wire::schema::HookKind::TransformSseEvent.as_str())
            .is_some_and(|m| !m.mode.is_noop())
    }
}
