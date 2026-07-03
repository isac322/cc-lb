use crate::{Config, WasmtimeAllocationStrategy};

use super::ValidationError;

const WASMTIME_PAGE_BYTES: u64 = 64 * 1024;
const WASMTIME_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const WASMTIME_MIN_GUARD_BYTES: u64 = 32 * 1024 * 1024;
const WASMTIME_MAX_POOL_TOTAL: u32 = 4096;
const DEFAULT_WASMTIME_MEMORY_MAX_PAGES: u32 = 2048;
const DEFAULT_WASMTIME_MEMORY_RESERVATION_BYTES: u64 = 256 * 1024 * 1024;

pub(super) fn validate_wasmtime_runtime(config: &Config) -> Result<(), ValidationError> {
    if let Some(pages) = config.runtime.wasmtime.memory_max_pages
        && (pages == 0 || pages > 65_536)
    {
        return Err(ValidationError::new(
            "runtime.wasmtime.memory_max_pages",
            format!("must be in range 1..=65536 (got {pages})"),
        ));
    }

    let effective_pages = config
        .runtime
        .wasmtime
        .memory_max_pages
        .unwrap_or(DEFAULT_WASMTIME_MEMORY_MAX_PAGES);
    let max_memory_bytes = u64::from(effective_pages) * WASMTIME_PAGE_BYTES;
    let effective_reservation = config
        .runtime
        .wasmtime
        .memory_reservation_bytes
        .unwrap_or(DEFAULT_WASMTIME_MEMORY_RESERVATION_BYTES);
    if config.runtime.wasmtime.allocation_strategy == WasmtimeAllocationStrategy::Pooling
        && effective_reservation < max_memory_bytes
    {
        return Err(ValidationError::new(
            "runtime.wasmtime.memory_reservation_bytes",
            format!("must be at least {max_memory_bytes} for pooling allocation"),
        ));
    }

    if let Some(bytes) = config.runtime.wasmtime.memory_reservation_bytes {
        validate_wasmtime_bytes(
            "runtime.wasmtime.memory_reservation_bytes",
            bytes,
            max_memory_bytes,
        )?;
    }
    if let Some(bytes) = config.runtime.wasmtime.memory_guard_bytes {
        validate_wasmtime_bytes(
            "runtime.wasmtime.memory_guard_bytes",
            bytes,
            WASMTIME_MIN_GUARD_BYTES,
        )?;
    }
    if let Some(total) = config.runtime.wasmtime.pool_total_memories {
        validate_wasmtime_pool_total("runtime.wasmtime.pool_total_memories", total)?;
    }
    if let Some(total) = config.runtime.wasmtime.pool_total_core_instances {
        validate_wasmtime_pool_total("runtime.wasmtime.pool_total_core_instances", total)?;
    }
    validate_wire_bounds(config)
}

fn validate_wasmtime_bytes(
    field: &'static str,
    bytes: u64,
    min: u64,
) -> Result<(), ValidationError> {
    if bytes < min || bytes > WASMTIME_MAX_BYTES || !bytes.is_multiple_of(WASMTIME_PAGE_BYTES) {
        return Err(ValidationError::new(
            field,
            format!(
                "must be 64KiB-aligned and in range {min}..={WASMTIME_MAX_BYTES} (got {bytes})"
            ),
        ));
    }
    Ok(())
}

fn validate_wasmtime_pool_total(field: &'static str, total: u32) -> Result<(), ValidationError> {
    if total == 0 || total > WASMTIME_MAX_POOL_TOTAL {
        return Err(ValidationError::new(
            field,
            format!("must be in range 1..={WASMTIME_MAX_POOL_TOTAL} (got {total})"),
        ));
    }
    Ok(())
}

fn validate_wire_bounds(config: &Config) -> Result<(), ValidationError> {
    let bounds = &config.runtime.wasmtime.wire_bounds;
    for (field, value) in [
        (
            "runtime.wasmtime.wire_bounds.output_body_bytes",
            bounds.output_body_bytes,
        ),
        (
            "runtime.wasmtime.wire_bounds.max_headers",
            u64::from(bounds.max_headers),
        ),
        (
            "runtime.wasmtime.wire_bounds.max_header_value_bytes",
            u64::from(bounds.max_header_value_bytes),
        ),
        (
            "runtime.wasmtime.wire_bounds.reason_bytes",
            u64::from(bounds.reason_bytes),
        ),
    ] {
        if value == 0 {
            return Err(ValidationError::new(field, "must be > 0"));
        }
    }
    Ok(())
}
