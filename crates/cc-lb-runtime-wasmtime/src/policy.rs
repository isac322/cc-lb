//! Runtime-scoped plugin policy knobs. Mirror of the same fields in
//! `cc-lb-config::WasmtimeConfig`; the bootstrap in `cc-lb-server::app`
//! copies each value across so this crate stays independent of the
//! config crate.

/// Behaviour when a filter or shape plugin fails at the runtime
/// boundary (trap, fuel exhaustion, pool saturation, invalid wire
/// output). `PassThrough` is the historical behaviour: filter treats
/// the failure as no-op and shape falls back to raw upstream
/// passthrough. `FailClosed` returns 503 upstream unavailable — pick
/// this when the plugin enforces load-bearing authz / tenant policy
/// and cannot silently degrade.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PluginFailurePolicy {
    #[default]
    PassThrough,
    FailClosed,
}

/// Whether a shape plugin may return a URL whose origin
/// (scheme+host+port) differs from the selected upstream. Historical
/// behaviour is `Unrestricted`; deployments where a compromised or
/// buggy shape plugin misrouting to a wrong host is a concern can
/// opt into `SelectedUpstreamOrigin` to reject any URL that does not
/// match the selected upstream's `base_url` origin.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ShapeOriginPolicy {
    #[default]
    Unrestricted,
    SelectedUpstreamOrigin,
}

/// Upper bounds enforced on plugin wire I/O. Defaults match current
/// unbounded-ish behaviour by tracking the request body cap already
/// enforced at the HTTP layer, so constructing with defaults is a
/// no-op vs. pre-Sprint-3 behaviour. Tightening any field is opt-in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PluginWireBounds {
    pub output_body_bytes: u64,
    pub max_headers: u32,
    pub max_header_value_bytes: u32,
    pub normalize_error_body_bytes: u64,
    pub reason_bytes: u32,
}

impl PluginWireBounds {
    /// `output_body_bytes` = 100 MiB matches `DEFAULT_FILES_CAP_BYTES`
    /// in `cc-lb-core::lifecycle` so the shape output plus the rkyv
    /// envelope can carry the largest legitimate request body without
    /// rejection at the plugin boundary.
    pub const DEFAULT_OUTPUT_BODY_BYTES: u64 = 100 * 1024 * 1024;
    pub const DEFAULT_MAX_HEADERS: u32 = 100;
    pub const DEFAULT_MAX_HEADER_VALUE_BYTES: u32 = 8 * 1024;
    pub const DEFAULT_NORMALIZE_ERROR_BODY_BYTES: u64 = 256 * 1024;
    pub const DEFAULT_REASON_BYTES: u32 = 256;
}

impl Default for PluginWireBounds {
    fn default() -> Self {
        Self {
            output_body_bytes: Self::DEFAULT_OUTPUT_BODY_BYTES,
            max_headers: Self::DEFAULT_MAX_HEADERS,
            max_header_value_bytes: Self::DEFAULT_MAX_HEADER_VALUE_BYTES,
            normalize_error_body_bytes: Self::DEFAULT_NORMALIZE_ERROR_BODY_BYTES,
            reason_bytes: Self::DEFAULT_REASON_BYTES,
        }
    }
}
