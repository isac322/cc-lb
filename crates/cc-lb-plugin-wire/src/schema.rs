//! Versioned, fingerprinted host/guest wire contracts.
use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum WireVersion {
    V1 = 1,
    V2 = 2,
    V3 = 3,
}

impl WireVersion {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            3 => Some(Self::V3),
            _ => None,
        }
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
            Self::V3 => "v3",
        }
    }
}
impl fmt::Display for WireVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ProviderFamily {
    Anthropic,
    Openai,
    Gemini,
}
impl ProviderFamily {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::Openai => "openai",
            Self::Gemini => "gemini",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "anthropic" => Some(Self::Anthropic),
            "openai" => Some(Self::Openai),
            "gemini" => Some(Self::Gemini),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HookKind {
    Filter,
    Shape,
    TransformResponse,
    TransformSseEvent,
    ShapeEnvelopeGemini,
}
impl HookKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Shape => "shape",
            Self::TransformResponse => "transform_response",
            Self::TransformSseEvent => "transform_sse_event",
            Self::ShapeEnvelopeGemini => "shape_envelope_gemini",
        }
    }
    pub const fn export_name(self) -> &'static str {
        match self {
            Self::Filter => "cc_lb_filter",
            Self::Shape => "cc_lb_shape",
            Self::TransformResponse => "cc_lb_transform_response",
            Self::TransformSseEvent => "cc_lb_transform_sse_event",
            Self::ShapeEnvelopeGemini => "cc_lb_shape_envelope_gemini",
        }
    }
    pub const fn qualified_export_name(self, provider: ProviderFamily) -> &'static str {
        match (self, provider) {
            (Self::Filter, ProviderFamily::Anthropic) => "cc_lb_filter_anthropic",
            (Self::Filter, ProviderFamily::Openai) => "cc_lb_filter_openai",
            (Self::Filter, ProviderFamily::Gemini) => "cc_lb_filter_gemini",
            (Self::Shape, ProviderFamily::Anthropic) => "cc_lb_shape_anthropic",
            (Self::Shape, ProviderFamily::Openai) => "cc_lb_shape_openai",
            (Self::Shape, ProviderFamily::Gemini) => "cc_lb_shape_gemini",
            (Self::TransformResponse, ProviderFamily::Anthropic) => {
                "cc_lb_transform_response_anthropic"
            }
            (Self::TransformResponse, ProviderFamily::Openai) => "cc_lb_transform_response_openai",
            (Self::TransformResponse, ProviderFamily::Gemini) => "cc_lb_transform_response_gemini",
            (Self::TransformSseEvent, ProviderFamily::Anthropic) => {
                "cc_lb_transform_sse_event_anthropic"
            }
            (Self::TransformSseEvent, ProviderFamily::Openai) => "cc_lb_transform_sse_event_openai",
            (Self::TransformSseEvent, ProviderFamily::Gemini) => "cc_lb_transform_sse_event_gemini",
            (Self::ShapeEnvelopeGemini, _) => "cc_lb_shape_envelope_gemini",
        }
    }
    /// Legacy bare hooks are Anthropic-only. The envelope hook is Gemini-only.
    pub fn parse_entrypoint(value: &str) -> Option<(Self, Option<ProviderFamily>)> {
        if value == "shape_envelope_gemini" {
            return Some((Self::ShapeEnvelopeGemini, Some(ProviderFamily::Gemini)));
        }
        if let Some(hook) = Self::parse(value) {
            return Some((hook, None));
        }
        let (hook, provider) = value.rsplit_once('_')?;
        Some((Self::parse(hook)?, Some(ProviderFamily::parse(provider)?)))
    }
    pub const fn section_prefix(self) -> &'static str {
        match self {
            Self::Filter => "cc_lb.schema.filter",
            Self::Shape => "cc_lb.schema.shape",
            Self::TransformResponse => "cc_lb.schema.transform_response",
            Self::TransformSseEvent => "cc_lb.schema.transform_sse_event",
            Self::ShapeEnvelopeGemini => "cc_lb.schema.shape_envelope_gemini",
        }
    }
    pub const ALL: &'static [Self] = &[
        Self::Filter,
        Self::Shape,
        Self::TransformResponse,
        Self::TransformSseEvent,
    ];
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "filter" => Some(Self::Filter),
            "shape" => Some(Self::Shape),
            "transform_response" => Some(Self::TransformResponse),
            "transform_sse_event" => Some(Self::TransformSseEvent),
            "shape_envelope_gemini" => Some(Self::ShapeEnvelopeGemini),
            _ => None,
        }
    }
}
impl fmt::Display for HookKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
pub const HOST_SUPPORTED_FILTER_VERSIONS: &[WireVersion] =
    &[WireVersion::V1, WireVersion::V2, WireVersion::V3];
pub const HOST_SUPPORTED_SHAPE_VERSIONS: &[WireVersion] = &[WireVersion::V1, WireVersion::V2];
pub const HOST_SUPPORTED_TRANSFORM_RESPONSE_VERSIONS: &[WireVersion] =
    &[WireVersion::V1, WireVersion::V2];
pub const HOST_SUPPORTED_TRANSFORM_SSE_EVENT_VERSIONS: &[WireVersion] =
    &[WireVersion::V1, WireVersion::V2];
pub const HOST_SUPPORTED_SHAPE_ENVELOPE_GEMINI_VERSIONS: &[WireVersion] = &[WireVersion::V2];
pub fn host_supported_versions(hook: HookKind) -> &'static [WireVersion] {
    match hook {
        HookKind::Filter => HOST_SUPPORTED_FILTER_VERSIONS,
        HookKind::Shape => HOST_SUPPORTED_SHAPE_VERSIONS,
        HookKind::TransformResponse => HOST_SUPPORTED_TRANSFORM_RESPONSE_VERSIONS,
        HookKind::TransformSseEvent => HOST_SUPPORTED_TRANSFORM_SSE_EVENT_VERSIONS,
        HookKind::ShapeEnvelopeGemini => HOST_SUPPORTED_SHAPE_ENVELOPE_GEMINI_VERSIONS,
    }
}
pub fn host_supports(hook: HookKind, version: WireVersion) -> bool {
    host_supported_versions(hook).contains(&version)
}
pub trait WireSchema {
    const FINGERPRINT: [u8; 32];
    const DESCRIPTOR: &'static str;
}
pub const fn pack_ret(ptr: u32, len: u32) -> u64 {
    ((ptr as u64) << 32) | len as u64
}
pub const fn unpack_ret(packed: u64) -> (u32, u32) {
    ((packed >> 32) as u32, packed as u32)
}
