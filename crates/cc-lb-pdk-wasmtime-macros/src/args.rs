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
                        || path.is_ident("transform_sse_event") =>
                {
                    kind = Some(HandlerKind::from_path(&path).ok_or_else(|| {
                        Error::new_spanned(
                            &path,
                            "unknown handler kind (supported: filter, shape, transform_response, transform_sse_event)",
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
                        "expected `<kind>`, `wire = 1`, `description = \"...\"`, `usage = \"...\"`, `mode = \"active\"|\"noop\"`, or `view`",
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
        if !kind.supports_wire_version(wire_version) {
            return Err(Error::new(
                Span::call_site(),
                "this hook does not support the requested wire version; all hooks require 1",
            ));
        }
        Ok(Self {
            kind,
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerKind {
    Filter,
    Shape,
    TransformResponse,
    TransformSseEvent,
}

impl HandlerKind {
    fn from_path(path: &syn::Path) -> Option<Self> {
        Some(match path.get_ident()?.to_string().as_str() {
            "filter" => Self::Filter,
            "shape" => Self::Shape,
            "transform_response" => Self::TransformResponse,
            "transform_sse_event" => Self::TransformSseEvent,
            _ => return None,
        })
    }

    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Shape => "shape",
            Self::TransformResponse => "transform_response",
            Self::TransformSseEvent => "transform_sse_event",
        }
    }

    pub(crate) const fn supports_noop_mode(self) -> bool {
        matches!(self, Self::TransformResponse | Self::TransformSseEvent)
    }

    pub(crate) const fn supports_wire_version(self, wire_version: u8) -> bool {
        wire_version == 1
    }

    pub(crate) fn export_name(self) -> &'static str {
        match self {
            Self::Filter => "cc_lb_filter",
            Self::Shape => "cc_lb_shape",
            Self::TransformResponse => "cc_lb_transform_response",
            Self::TransformSseEvent => "cc_lb_transform_sse_event",
        }
    }

    pub(crate) fn dispatch_helper(self, wire_version: u8, view: bool) -> Option<&'static str> {
        match (self, wire_version, view) {
            (Self::Filter, 1, false) => Some("run_filter"),
            (Self::Filter, 1, true) => Some("run_filter_view"),
            (Self::Shape, 1, false) => Some("run_shape"),
            (Self::Shape, 1, true) => Some("run_shape_view"),
            (Self::TransformResponse, 1, false) => Some("run_transform_response"),
            (Self::TransformResponse, 1, true) => Some("run_transform_response_view"),
            (Self::TransformSseEvent, 1, false) => Some("run_transform_sse_event"),
            (Self::TransformSseEvent, 1, true) => Some("run_transform_sse_event_view"),
            (Self::Filter, _, _)
            | (Self::Shape, _, _)
            | (Self::TransformResponse, _, _)
            | (Self::TransformSseEvent, _, _) => None,
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
            (Self::Filter, _)
            | (Self::Shape, _)
            | (Self::TransformResponse, _)
            | (Self::TransformSseEvent, _) => None,
        }
    }

    pub(crate) fn const_suffix(self) -> &'static str {
        match self {
            Self::Filter => "FILTER",
            Self::Shape => "SHAPE",
            Self::TransformResponse => "TRANSFORM_RESPONSE",
            Self::TransformSseEvent => "TRANSFORM_SSE_EVENT",
        }
    }
}

pub(crate) struct DiscoveredHandler {
    pub(crate) fn_ident: Ident,
    pub(crate) kind: HandlerKind,
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
