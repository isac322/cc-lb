use std::sync::Arc;

use crate::cache::{call_filter_hook, call_shape_hook};
use crate::{PluginCell, RuntimeSlotKey, SlotKind, WasmtimeRuntime, WasmtimeRuntimeError};

impl WasmtimeRuntime {
    fn dispatch<F>(
        &self,
        slot_key: &RuntimeSlotKey,
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
        slot_key: &RuntimeSlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Filter, |cell| {
            call_filter_hook(cell, input)
        })
    }

    pub fn call_shape(
        &self,
        slot_key: &RuntimeSlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Shape, |cell| {
            call_shape_hook(cell, input)
        })
    }
}
