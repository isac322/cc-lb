use std::sync::Arc;

use crate::cache::call_hook_export_scoped;
use crate::{HookKind, PluginCell, RuntimeSlotKey, WasmtimeRuntime, WasmtimeRuntimeError};

impl WasmtimeRuntime {
    fn dispatch<F>(
        &self,
        slot_key: &RuntimeSlotKey,
        expected_kind: HookKind,
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
        self.dispatch(slot_key, HookKind::Filter, |cell| {
            let export = HookKind::Filter
                .qualified_export_name(cc_lb_plugin_wire::schema::ProviderFamily::Anthropic);
            let export = if cell.metadata.hooks.contains_key("filter_anthropic") {
                export
            } else {
                HookKind::Filter.export_name()
            };
            call_hook_export_scoped(cell, input, HookKind::Filter, export, <[u8]>::to_vec)
        })
    }

    pub fn call_shape(
        &self,
        slot_key: &RuntimeSlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, HookKind::Shape, |cell| {
            let export = HookKind::Shape
                .qualified_export_name(cc_lb_plugin_wire::schema::ProviderFamily::Anthropic);
            let export = if cell.metadata.hooks.contains_key("shape_anthropic") {
                export
            } else {
                HookKind::Shape.export_name()
            };
            call_hook_export_scoped(cell, input, HookKind::Shape, export, <[u8]>::to_vec)
        })
    }
}
