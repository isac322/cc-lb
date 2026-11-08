use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{
    Error, Expr, Ident, Item, ItemMod, Lit, Meta, MetaNameValue, Token,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

pub(crate) struct PluginArgs {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) description: String,
    pub(crate) usage: String,
}

impl Parse for PluginArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let metas: Punctuated<Meta, Token![,]> = Punctuated::parse_terminated(input)?;
        let mut name = None;
        let mut version = None;
        let mut description = None;
        let mut usage = None;
        for meta in metas {
            match meta {
                Meta::NameValue(nv) if nv.path.is_ident("name") => name = Some(expect_string(&nv)?),
                Meta::NameValue(nv) if nv.path.is_ident("version") => {
                    version = Some(expect_string(&nv)?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("description") => {
                    description = Some(expect_string(&nv)?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("usage") => {
                    usage = Some(expect_string(&nv)?);
                }
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "expected `name = \"...\"`, `version = \"...\"`, `description = \"...\"`, or `usage = \"...\"`",
                    ));
                }
            }
        }
        Ok(Self {
            name: name.ok_or_else(|| {
                Error::new(Span::call_site(), "#[plugin] missing required `name`")
            })?,
            version: version.ok_or_else(|| {
                Error::new(Span::call_site(), "#[plugin] missing required `version`")
            })?,
            description: description.ok_or_else(|| {
                Error::new(
                    Span::call_site(),
                    "#[plugin] missing required `description`",
                )
            })?,
            usage: usage.ok_or_else(|| {
                Error::new(Span::call_site(), "#[plugin] missing required `usage`")
            })?,
        })
    }
}

pub(crate) struct HandlerArgs {
    pub(crate) kind: HandlerKind,
    pub(crate) provider: Option<ProviderFamily>,
    pub(crate) wire_version: u8,
    pub(crate) description: String,
    pub(crate) usage: String,
    pub(crate) view: bool,
    pub(crate) mode: HandlerMode,
}

impl Parse for HandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let metas: Punctuated<Meta, Token![,]> = Punctuated::parse_terminated(input)?;
        let mut kind = None;
        let mut provider = None;
        let mut wire_version = None;
        let mut description = None;
        let mut usage = None;
        let mut view = false;
        let mut mode = HandlerMode::Active;
        for meta in metas {
            match meta {
                Meta::Path(path)
                    if path.is_ident("filter")
                        || path.is_ident("shape")
                        || path.is_ident("transform_response")
                        || path.is_ident("transform_sse_event")
                        || path.is_ident("shape_envelope_gemini") =>
                {
                    kind = Some(HandlerKind::from_path(&path).ok_or_else(|| {
                        Error::new_spanned(
                            &path,
"unknown handler kind (supported: filter, shape, transform_response, transform_sse_event, transform_realtime_frame, shape_envelope_gemini)",
                        )
                    })?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("provider") => {
                    let value = expect_string(&nv)?;
                    provider = Some(ProviderFamily::from_str(&value).ok_or_else(|| {
                        Error::new_spanned(
                            &nv.value,
                            "expected `provider = \"anthropic\"|\"openai\"|\"gemini\"`",
                        )
                    })?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("wire") => {
                    wire_version = Some(expect_u8(&nv)?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("description") => {
                    description = Some(expect_string(&nv)?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("usage") => {
                    usage = Some(expect_string(&nv)?)
                }
                Meta::NameValue(nv) if nv.path.is_ident("mode") => {
                    let value = expect_string(&nv)?;
                    mode = HandlerMode::from_str(&value).ok_or_else(|| {
                        Error::new_spanned(
                            &nv.value,
                            "expected `mode = \"active\"` or `mode = \"noop\"`",
                        )
                    })?;
                }
                Meta::Path(path) if path.is_ident("view") => view = true,
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "expected `<kind>`, `provider = \"anthropic\"|\"openai\"|\"gemini\"`, `wire = 1|2`, `description = \"...\"`, `usage = \"...\"`, `mode = \"active\"|\"noop\"`, or `view`",
                    ));
                }
            }
        }
        let kind = kind.ok_or_else(|| {
            Error::new(Span::call_site(), "#[handler] missing required hook kind")
        })?;
        if mode == HandlerMode::Noop && !kind.supports_noop_mode() {
            return Err(Error::new(
                Span::call_site(),
                "`mode = \"noop\"` is only valid for transform_response and transform_sse_event handlers",
            ));
        }
        let wire_version = wire_version
            .ok_or_else(|| Error::new(Span::call_site(), "#[handler] missing required `wire`"))?;
        if !kind.supports_wire_version(wire_version, provider) {
            return Err(Error::new(
                Span::call_site(),
                "this hook does not support the requested wire version/provider combination; wire 2 requires a provider-qualified existing hook, and shape_envelope_gemini requires provider = \"gemini\" and wire = 2",
            ));
        }
        Ok(Self {
            kind,
            provider,
            wire_version,
            description: description.ok_or_else(|| {
                Error::new(
                    Span::call_site(),
                    "#[handler] missing required `description`",
                )
            })?,
            usage: usage.ok_or_else(|| {
                Error::new(Span::call_site(), "#[handler] missing required `usage`")
            })?,
            view,
            mode,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderFamily {
    Anthropic,
    Openai,
    Gemini,
}

impl ProviderFamily {
    fn from_str(value: &str) -> Option<Self> {
        match value {
            "anthropic" => Some(Self::Anthropic),
            "openai" => Some(Self::Openai),
            "gemini" => Some(Self::Gemini),
            _ => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::Openai => "openai",
            Self::Gemini => "gemini",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerMode {
    Active,
    Noop,
}

impl HandlerMode {
    fn from_str(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "noop" => Some(Self::Noop),
            _ => None,
        }
    }

    pub(crate) const fn wire_name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Noop => "noop",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerKind {
    Filter,
    Shape,
    TransformResponse,
    TransformSseEvent,
    TransformRealtimeFrame,
    ShapeEnvelopeGemini,
}

impl HandlerKind {
    fn from_path(path: &syn::Path) -> Option<Self> {
        Some(match path.get_ident()?.to_string().as_str() {
            "filter" => Self::Filter,
            "shape" => Self::Shape,
            "transform_response" => Self::TransformResponse,
            "transform_sse_event" => Self::TransformSseEvent,
            "transform_realtime_frame" => Self::TransformRealtimeFrame,
            "shape_envelope_gemini" => Self::ShapeEnvelopeGemini,
        })
    }

    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Shape => "shape",
            Self::TransformResponse => "transform_response",
            Self::TransformSseEvent => "transform_sse_event",
            Self::TransformRealtimeFrame => "transform_realtime_frame",
            Self::ShapeEnvelopeGemini => "shape_envelope_gemini",
        }
    }

    pub(crate) const fn supports_noop_mode(self) -> bool {
        matches!(
            self,
            Self::TransformResponse | Self::TransformSseEvent | Self::TransformRealtimeFrame
        )
    }

    pub(crate) const fn supports_wire_version(
        self,
        wire_version: u8,
        provider: Option<ProviderFamily>,
    ) -> bool {
        match self {
            Self::ShapeEnvelopeGemini => {
                wire_version == 2 && matches!(provider, Some(ProviderFamily::Gemini))
            }
            Self::Filter
            | Self::Shape
            | Self::TransformResponse
            | Self::TransformSseEvent => {
                wire_version == 1 || (wire_version == 2 && provider.is_some())
            }
            Self::TransformRealtimeFrame => wire_version == 2 && provider.is_some(),
        }
    }

    pub(crate) fn export_name(self, provider: Option<ProviderFamily>) -> String {
        let base = match self {
            Self::Filter => "cc_lb_filter",
            Self::Shape => "cc_lb_shape",
            Self::TransformResponse => "cc_lb_transform_response",
            Self::TransformSseEvent => "cc_lb_transform_sse_event",
            Self::TransformRealtimeFrame => "cc_lb_transform_realtime_frame",
            Self::ShapeEnvelopeGemini => "cc_lb_shape_envelope_gemini",
        };
        if matches!(self, Self::ShapeEnvelopeGemini) {
            return base.to_owned();
        }
        match provider {
            Some(provider) => format!("{base}_{}", provider.as_str()),
            None => base.to_owned(),
        }
    }

    pub(crate) fn metadata_key(self, provider: Option<ProviderFamily>) -> String {
        if matches!(self, Self::ShapeEnvelopeGemini) {
            return "shape_envelope_gemini".to_owned();
        }
        match provider {
            Some(provider) => format!("{}_{}", self.wire_name(), provider.as_str()),
            None => self.wire_name().to_owned(),
        }
    }

    pub(crate) fn dispatch_helper(self, wire_version: u8, view: bool) -> Option<&'static str> {
        match (self, wire_version, view) {
            (Self::Filter, 1, false) => Some("run_filter"),
            (Self::Filter, 1, true) => Some("run_filter_view"),
            (Self::Filter, 2, false) => Some("run_filter_v2"),
            (Self::Filter, 2, true) => Some("run_filter_v2_view"),
            (Self::Shape, 1, false) => Some("run_shape"),
            (Self::Shape, 1, true) => Some("run_shape_view"),
            (Self::Shape, 2, false) => Some("run_shape_v2"),
            (Self::Shape, 2, true) => Some("run_shape_v2_view"),
            (Self::TransformResponse, 1, false) => Some("run_transform_response"),
            (Self::TransformResponse, 1, true) => Some("run_transform_response_view"),
            (Self::TransformResponse, 2, false) => Some("run_transform_response_v2"),
            (Self::TransformResponse, 2, true) => Some("run_transform_response_v2_view"),
            (Self::TransformSseEvent, 1, false) => Some("run_transform_sse_event"),
            (Self::TransformSseEvent, 1, true) => Some("run_transform_sse_event_view"),
            (Self::TransformSseEvent, 2, false) => Some("run_transform_sse_event_v2"),
            (Self::TransformSseEvent, 2, true) => Some("run_transform_sse_event_v2_view"),
            (Self::TransformRealtimeFrame, 2, false) => Some("run_transform_realtime_frame_v2"),
            (Self::TransformRealtimeFrame, 2, true) => Some("run_transform_realtime_frame_v2_view"),
            (Self::ShapeEnvelopeGemini, 2, false) => Some("run_shape_envelope_gemini"),
            (Self::ShapeEnvelopeGemini, 2, true) => Some("run_shape_envelope_gemini_view"),
            _ => None,
        }
    }

    pub(crate) fn schema_section(self, wire_version: u8) -> String {
        format!("cc_lb.schema.{}.v{}", self.wire_name(), wire_version)
    }

    pub(crate) fn fingerprint_type(self, wire_version: u8) -> Option<TokenStream2> {
        match (self, wire_version) {
            (Self::Filter, 1) => Some(quote! { ::cc_lb_pdk_wasmtime::types::v1::FilterRequest }),
            (Self::Shape, 1) => Some(quote! { ::cc_lb_pdk_wasmtime::types::v1::ShapeRequest }),
            (Self::TransformResponse, 1) => {
                Some(quote! { ::cc_lb_pdk_wasmtime::types::v1::TransformResponseRequest })
            }
            (Self::TransformSseEvent, 1) => {
                Some(quote! { ::cc_lb_pdk_wasmtime::types::v1::TransformSseEventRequest })
            }
            (Self::Filter, 2) => Some(quote! { ::cc_lb_pdk_wasmtime::types::v2::FilterRequest }),
            (Self::Shape, 2) => Some(quote! { ::cc_lb_pdk_wasmtime::types::v2::ShapeRequest }),
            (Self::TransformResponse, 2) => {
                Some(quote! { ::cc_lb_pdk_wasmtime::types::v2::TransformResponseRequest })
            }
            (Self::TransformSseEvent, 2) => {
                Some(quote! { ::cc_lb_pdk_wasmtime::types::v2::TransformSseEventRequest })
            }
            (Self::TransformRealtimeFrame, 2) => {
                Some(quote! { ::cc_lb_pdk_wasmtime::types::v2::TransformRealtimeFrameRequest })
            }
            (Self::ShapeEnvelopeGemini, 2) => {
                Some(quote! { ::cc_lb_pdk_wasmtime::types::v2::ShapeEnvelopeGeminiRequest })
            }
            _ => None,
        }
    }

    pub(crate) fn const_suffix(self) -> &'static str {
        match self {
            Self::Filter => "FILTER",
            Self::Shape => "SHAPE",
            Self::TransformResponse => "TRANSFORM_RESPONSE",
            Self::TransformSseEvent => "TRANSFORM_SSE_EVENT",
            Self::TransformRealtimeFrame => "TRANSFORM_REALTIME_FRAME",
            Self::ShapeEnvelopeGemini => "SHAPE_ENVELOPE_GEMINI",
        }
    }
}

pub(crate) struct DiscoveredHandler {
    pub(crate) fn_ident: Ident,
    pub(crate) kind: HandlerKind,
    pub(crate) provider: Option<ProviderFamily>,
    pub(crate) wire_version: u8,
    pub(crate) description: String,
    pub(crate) usage: String,
    pub(crate) view: bool,
    pub(crate) mode: HandlerMode,
}

pub(crate) fn collect_handlers(module: &ItemMod) -> syn::Result<Vec<DiscoveredHandler>> {
    let Some((_, items)) = &module.content else {
        return Err(Error::new_spanned(
            &module.ident,
            "#[plugin] requires an inline module",
        ));
    };
    let mut handlers = Vec::new();
    for item in items {
        let Item::Fn(function) = item else { continue };
        for attr in &function.attrs {
            if !is_handler_attr(attr.path()) {
                continue;
            }
            let args: HandlerArgs = attr.parse_args()?;
            handlers.push(DiscoveredHandler {
                fn_ident: function.sig.ident.clone(),
                kind: args.kind,
                provider: args.provider,
                wire_version: args.wire_version,
                description: args.description,
                usage: args.usage,
                view: args.view,
                mode: args.mode,
            });
        }
    }
    Ok(handlers)
}

fn is_handler_attr(path: &syn::Path) -> bool {
    path.segments
        .last()
        .is_some_and(|segment| segment.ident == "handler")
}

fn expect_string(nv: &MetaNameValue) -> syn::Result<String> {
    if let syn::Expr::Lit(syn::ExprLit {
        lit: Lit::Str(s), ..
    }) = &nv.value
    {
        Ok(s.value())
    } else {
        Err(Error::new_spanned(&nv.value, "expected string literal"))
    }
}

fn expect_u8(nv: &MetaNameValue) -> syn::Result<u8> {
    if let Expr::Lit(syn::ExprLit {
        lit: Lit::Int(value),
        ..
    }) = &nv.value
    {
        value.base10_parse::<u8>()
    } else {
        Err(Error::new_spanned(&nv.value, "expected integer literal"))
    }
}

#[cfg(test)]
mod tests;
