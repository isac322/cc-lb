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
}

impl Parse for HandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let metas: Punctuated<Meta, Token![,]> = Punctuated::parse_terminated(input)?;
        let mut kind = None;
        let mut wire_version = None;
        let mut description = None;
        let mut usage = None;
        let mut view = false;
        for meta in metas {
            match meta {
                Meta::Path(path)
                    if path.is_ident("filter")
                        || path.is_ident("shape")
                        || path.is_ident("observe") =>
                {
                    kind = Some(HandlerKind::from_path(&path).ok_or_else(|| {
                        Error::new_spanned(
                            &path,
                            "unknown handler kind (supported: filter, shape, observe)",
                        )
                    })?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("wire") => {
                    let version = expect_u8(&nv)?;
                    if version != 1 {
                        return Err(Error::new_spanned(
                            &nv.value,
                            "only `wire = 1` is supported by this PDK",
                        ));
                    }
                    wire_version = Some(version);
                }
                Meta::NameValue(nv) if nv.path.is_ident("description") => {
                    description = Some(expect_string(&nv)?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("usage") => {
                    usage = Some(expect_string(&nv)?)
                }
                Meta::Path(path) if path.is_ident("view") => view = true,
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "expected `<kind>`, `wire = 1`, `description = \"...\"`, `usage = \"...\"`, or `view`",
                    ));
                }
            }
        }
        Ok(Self {
            kind: kind.ok_or_else(|| {
                Error::new(Span::call_site(), "#[handler] missing required hook kind")
            })?,
            wire_version: wire_version.ok_or_else(|| {
                Error::new(Span::call_site(), "#[handler] missing required `wire`")
            })?,
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
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerKind {
    Filter,
    Shape,
    Observe,
}

impl HandlerKind {
    fn from_path(path: &syn::Path) -> Option<Self> {
        Some(match path.get_ident()?.to_string().as_str() {
            "filter" => Self::Filter,
            "shape" => Self::Shape,
            "observe" => Self::Observe,
            _ => return None,
        })
    }

    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Shape => "shape",
            Self::Observe => "observe",
        }
    }

    pub(crate) fn export_name(self) -> &'static str {
        match self {
            Self::Filter => "cc_lb_filter",
            Self::Shape => "cc_lb_shape",
            Self::Observe => "cc_lb_observe",
        }
    }

    pub(crate) fn dispatch_helper(self, view: bool) -> &'static str {
        match (self, view) {
            (Self::Filter, false) => "run_filter",
            (Self::Filter, true) => "run_filter_view",
            (Self::Shape, false) => "run_shape",
            (Self::Shape, true) => "run_shape_view",
            (Self::Observe, false) => "run_observe",
            (Self::Observe, true) => "run_observe_view",
        }
    }

    pub(crate) fn schema_section(self, wire_version: u8) -> String {
        format!("cc_lb.schema.{}.v{}", self.wire_name(), wire_version)
    }

    pub(crate) fn fingerprint_type(self) -> TokenStream2 {
        match self {
            Self::Filter => quote! { ::cc_lb_pdk_wasmtime::types::FilterRequest },
            Self::Shape => quote! { ::cc_lb_pdk_wasmtime::types::ShapeRequest },
            Self::Observe => quote! { ::cc_lb_pdk_wasmtime::types::ObserveEvent },
        }
    }

    pub(crate) fn const_suffix(self) -> &'static str {
        match self {
            Self::Filter => "FILTER",
            Self::Shape => "SHAPE",
            Self::Observe => "OBSERVE",
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
