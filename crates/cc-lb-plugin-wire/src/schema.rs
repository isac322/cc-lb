//! Wire versioning contract shared between host and plugin authors.
//!
//! The host and each plugin agree on a `WireVersion` per hook. Plugin
//! authors declare their hook's version in `#[hook(...)]` metadata, and
//! the host verifies at admission time that the declared version is in
//! the host's supported list AND that the wasm's embedded layout
//! fingerprint matches the host's computed fingerprint.

use core::fmt;

/// Wire schema version. Distinct enum values indicate incompatible
/// struct layouts. Filter-only additions currently stay on V1 because
/// the filter plugin is internal to cc-lb.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum WireVersion {
    V1 = 1,
}

impl WireVersion {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::V1),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
        }
    }
}

impl fmt::Display for WireVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Plugin hook kind. Each hook is a distinct wire contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HookKind {
    Filter,
    Shape,
    TransformResponse,
    TransformSseEvent,
}

impl HookKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Shape => "shape",
            Self::TransformResponse => "transform_response",
            Self::TransformSseEvent => "transform_sse_event",
        }
    }

    /// Wasm function export name for this hook (guest-side).
    pub const fn export_name(self) -> &'static str {
        match self {
            Self::Filter => "cc_lb_filter",
            Self::Shape => "cc_lb_shape",
            Self::TransformResponse => "cc_lb_transform_response",
            Self::TransformSseEvent => "cc_lb_transform_sse_event",
        }
    }

    /// Custom section name prefix. The full section name for a specific
    /// version is `{prefix}.{version_str}` (e.g., `cc_lb.schema.filter.v1`).
    pub const fn section_prefix(self) -> &'static str {
        match self {
            Self::Filter => "cc_lb.schema.filter",
            Self::Shape => "cc_lb.schema.shape",
            Self::TransformResponse => "cc_lb.schema.transform_response",
            Self::TransformSseEvent => "cc_lb.schema.transform_sse_event",
        }
    }

    /// All hook kinds — for iteration.
    pub const ALL: &'static [HookKind] = &[
        Self::Filter,
        Self::Shape,
        Self::TransformResponse,
        Self::TransformSseEvent,
    ];

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "filter" => Some(Self::Filter),
            "shape" => Some(Self::Shape),
            "transform_response" => Some(Self::TransformResponse),
            "transform_sse_event" => Some(Self::TransformSseEvent),
            _ => None,
        }
    }
}

impl fmt::Display for HookKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Host-supported versions per hook. Update when adding new versions.
pub const HOST_SUPPORTED_FILTER_VERSIONS: &[WireVersion] = &[WireVersion::V1];
pub const HOST_SUPPORTED_SHAPE_VERSIONS: &[WireVersion] = &[WireVersion::V1];
pub const HOST_SUPPORTED_TRANSFORM_RESPONSE_VERSIONS: &[WireVersion] = &[WireVersion::V1];
pub const HOST_SUPPORTED_TRANSFORM_SSE_EVENT_VERSIONS: &[WireVersion] = &[WireVersion::V1];

pub fn host_supported_versions(hook: HookKind) -> &'static [WireVersion] {
    match hook {
        HookKind::Filter => HOST_SUPPORTED_FILTER_VERSIONS,
        HookKind::Shape => HOST_SUPPORTED_SHAPE_VERSIONS,
        HookKind::TransformResponse => HOST_SUPPORTED_TRANSFORM_RESPONSE_VERSIONS,
        HookKind::TransformSseEvent => HOST_SUPPORTED_TRANSFORM_SSE_EVENT_VERSIONS,
    }
}

pub fn host_supports(hook: HookKind, version: WireVersion) -> bool {
    host_supported_versions(hook).contains(&version)
}

/// Layout fingerprint published by each wire struct via the
/// `#[derive(WireSchema)]` proc-macro (defined in
/// `cc-lb-pdk-wasmtime-macros`). The macro walks the struct AST,
/// serialises fields to a canonical descriptor string, then embeds
/// `BLAKE3(descriptor)` as a 32-byte const.
pub trait WireSchema {
    const FINGERPRINT: [u8; 32];
    const DESCRIPTOR: &'static str;
}

/// Packed `(out_ptr, out_len)` return value used by every guest hook export.
pub const fn pack_ret(ptr: u32, len: u32) -> u64 {
    ((ptr as u64) << 32) | (len as u64)
}

/// Inverse of [`pack_ret`].
pub const fn unpack_ret(packed: u64) -> (u32, u32) {
    let ptr = (packed >> 32) as u32;
    let len = (packed & 0xFFFF_FFFF) as u32;
    (ptr, len)
}
