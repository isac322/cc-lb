use std::sync::Arc;

use rkyv::util::AlignedVec;

use super::{HookFn, call_hook_scoped};
use crate::cell::PluginCell;
use crate::error::WasmtimeRuntimeError;

fn copy_to_aligned(slice: &[u8]) -> AlignedVec<16> {
    let mut aligned = AlignedVec::with_capacity(slice.len());
    aligned.extend_from_slice(slice);
    aligned
}

/// Synchronous filter call. Builds a fresh `Store`, runs the hook,
/// drops the `Store`.
pub fn call_filter_hook(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_filter_hook_scoped(cell, input, <[u8]>::to_vec)
}

pub(crate) fn call_filter_hook_scoped<R, F>(
    cell: &Arc<PluginCell>,
    input: &[u8],
    with_output: F,
) -> Result<R, WasmtimeRuntimeError>
where
    F: for<'a> FnOnce(&'a [u8]) -> R,
{
    call_hook_scoped(cell, input, HookFn::Filter, with_output)
}

/// Synchronous shape call. Input is rkyv-encoded `ShapeRequest`,
/// output rkyv-encoded `ShapeResponse`.
pub fn call_shape_hook(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_shape_hook_scoped(cell, input, <[u8]>::to_vec)
}

pub(crate) fn call_shape_hook_scoped<R, F>(
    cell: &Arc<PluginCell>,
    input: &[u8],
    with_output: F,
) -> Result<R, WasmtimeRuntimeError>
where
    F: for<'a> FnOnce(&'a [u8]) -> R,
{
    call_hook_scoped(cell, input, HookFn::Shape, with_output)
}

pub fn call_transform_response_hook(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<AlignedVec<16>, WasmtimeRuntimeError> {
    call_transform_response_hook_scoped(cell, input, copy_to_aligned)
}

pub(crate) fn call_transform_response_hook_scoped<R, F>(
    cell: &Arc<PluginCell>,
    input: &[u8],
    with_output: F,
) -> Result<R, WasmtimeRuntimeError>
where
    F: for<'a> FnOnce(&'a [u8]) -> R,
{
    call_hook_scoped(cell, input, HookFn::TransformResponse, with_output)
}

pub fn call_transform_sse_event_hook(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<AlignedVec<16>, WasmtimeRuntimeError> {
    call_transform_sse_event_hook_scoped(cell, input, copy_to_aligned)
}

pub(crate) fn call_transform_sse_event_hook_scoped<R, F>(
    cell: &Arc<PluginCell>,
    input: &[u8],
    with_output: F,
) -> Result<R, WasmtimeRuntimeError>
where
    F: for<'a> FnOnce(&'a [u8]) -> R,
{
    call_hook_scoped(cell, input, HookFn::TransformSseEvent, with_output)
}
