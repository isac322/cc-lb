use std::sync::Arc;

use cc_lb_plugin_api::SlotKey;

use crate::cache::{
    call_filter_hook, call_observe_hook, call_shape_hook, call_transform_response_hook,
    call_transform_sse_event_hook,
};
use crate::{PluginCell, SlotKind, WasmtimeRuntime, WasmtimeRuntimeError};

impl WasmtimeRuntime {
    fn dispatch<F>(
        &self,
        slot_key: &SlotKey,
        expected_kind: SlotKind,
        run: F,
    ) -> Result<Vec<u8>, WasmtimeRuntimeError>
    where
        F: FnOnce(&Arc<PluginCell>) -> Result<Vec<u8>, WasmtimeRuntimeError>,
    {
        let slot = self
            .get_slot(slot_key)
            .ok_or_else(|| WasmtimeRuntimeError::ModuleRejected {
                reason: format!("no slot registered for {:?}", slot_key),
            })?;
        if slot.kind != expected_kind {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "slot {slot_key:?} is `{:?}`, callable as `{:?}` only",
                    slot.kind, expected_kind,
                ),
            });
        }
        let cell = slot.current.load_full();
        run(&cell)
    }

    pub fn call_filter(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Filter, |cell| {
            call_filter_hook(cell, input)
        })
    }

    pub fn call_shape(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Shape, |cell| {
            call_shape_hook(cell, input)
        })
    }

    pub fn call_observe(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Observe, |cell| {
            call_observe_hook(cell, input)
        })
    }

    pub fn call_transform_response(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::TransformResponse, |cell| {
            call_transform_response_hook(cell, input).map(|bytes| bytes.as_slice().to_vec())
        })
    }

    pub fn call_transform_sse_event(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::TransformSseEvent, |cell| {
            call_transform_sse_event_hook(cell, input).map(|bytes| bytes.as_slice().to_vec())
        })
    }
}
