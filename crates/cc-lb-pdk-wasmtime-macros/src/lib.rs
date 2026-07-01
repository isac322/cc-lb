//! Procedural macros for `cc-lb-pdk-wasmtime`.
//!
//! Two attributes:
//!
//! * `#[plugin(name = "...", version = "...")]` on an inline module
//!   containing one or more `#[handler]` functions. Emits the
//!   `cc_lb_alloc` / `cc_lb_free` exports, per-handler `cc_lb_*`
//!   export wrappers, per-handler `cc_lb.schema.<kind>.v1` custom
//!   sections (32-byte BLAKE3 of that hook's wire-schema tag), and
//!   one `cc_lb.plugin.v1` custom section (UTF-8 JSON metadata).
//! * `#[handler(name = "<kind>", view?)]` on a function inside the
//!   plugin module. `kind` is one of `filter`, `shape`,
//!   `normalize_error`, `observe`. Optional `view` flag selects the
//!   zero-copy dispatch (`&Archived<Request>` instead of owned).
//!
//! See `docs/rfc/0001-plugin-runtime-vnext.md`.
#![forbid(unsafe_code)]

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{
    Error, Ident, Item, ItemFn, ItemMod, Lit, Meta, MetaNameValue, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// `#[plugin(name = "...", version = "...")]`.
#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as PluginArgs);
    let mut module = parse_macro_input!(item as ItemMod);

    let handlers = match collect_handlers(&module) {
        Ok(h) => h,
        Err(e) => return e.to_compile_error().into(),
    };

    if handlers.is_empty() {
        return Error::new_spanned(
            &module.ident,
            "#[plugin] module must contain at least one #[handler(name = \"...\")] function",
        )
        .to_compile_error()
        .into();
    }

    // Reject duplicate hook kinds in the same plugin — a single guest
    // module can only ship one implementation per slot.
    for (i, h) in handlers.iter().enumerate() {
        if handlers[i + 1..].iter().any(|other| other.kind == h.kind) {
            return Error::new_spanned(
                &h.fn_ident,
                format!(
                    "duplicate #[handler(name = \"{}\")] — each hook kind may appear once per plugin",
                    h.kind.wire_name()
                ),
            )
            .to_compile_error()
            .into();
        }
    }

    let Some((_, items)) = &mut module.content else {
        return Error::new_spanned(
            &module.ident,
            "#[plugin] must be applied to an inline `mod foo { ... }`",
        )
        .to_compile_error()
        .into();
    };

    items.push(syn::parse_quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn cc_lb_alloc(size: u32, align: u32) -> u32 {
            ::cc_lb_pdk_wasmtime::__private::alloc_bytes(size, align)
        }
    });
    items.push(syn::parse_quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn cc_lb_free(ptr: u32, size: u32, align: u32) {
            ::cc_lb_pdk_wasmtime::__private::free_bytes(ptr, size, align)
        }
    });

    for handler in &handlers {
        let export_ident = syn::Ident::new(handler.kind.export_name(), Span::call_site());
        let dispatch_ident = syn::Ident::new(
            handler.kind.dispatch_helper(handler.view),
            Span::call_site(),
        );
        let fn_ident = &handler.fn_ident;
        items.push(syn::parse_quote! {
            #[unsafe(no_mangle)]
            pub extern "C" fn #export_ident(in_ptr: u32, in_len: u32) -> u64 {
                ::cc_lb_pdk_wasmtime::__private::#dispatch_ident(in_ptr, in_len, #fn_ident)
            }
        });
    }

    let metadata_json = format!(
        r#"{{"name":"{}","version":"{}","abi":"wasmtime-rkyv-v1"}}"#,
        escape_json(&args.name),
        escape_json(&args.version),
    );
    let metadata_bytes = metadata_json.as_bytes();
    let metadata_len = metadata_bytes.len();
    let metadata_array = byte_array_tokens(metadata_bytes);

    let schema_section_items: Vec<TokenStream2> = handlers
        .iter()
        .map(|h| {
            let section = h.kind.schema_section();
            let digest = blake3::hash(h.kind.schema_tag());
            let array = byte_array_tokens(digest.as_bytes());
            let static_ident = syn::Ident::new(
                &format!("__CC_LB_SCHEMA_HASH_{}", h.kind.const_suffix()),
                Span::call_site(),
            );
            quote! {
                #[used]
                #[unsafe(link_section = #section)]
                static #static_ident: [u8; 32] = #array;
            }
        })
        .collect();

    quote! {
        #module

        #(#schema_section_items)*

        #[used]
        #[unsafe(link_section = "cc_lb.plugin.v1")]
        static __CC_LB_PLUGIN_METADATA: [u8; #metadata_len] = #metadata_array;
    }
    .into()
}

/// `#[handler(name = "<kind>", view?)]`.
#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let _args = parse_macro_input!(attr as HandlerArgs);
    let input = parse_macro_input!(item as ItemFn);
    quote! { #input }.into()
}

// ---------- argument parsing ----------

struct PluginArgs {
    name: String,
    version: String,
}

impl Parse for PluginArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let metas: Punctuated<Meta, Token![,]> = Punctuated::parse_terminated(input)?;
        let mut name = None;
        let mut version = None;
        for meta in metas {
            match meta {
                Meta::NameValue(nv) if nv.path.is_ident("name") => {
                    name = Some(expect_string(&nv)?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("version") => {
                    version = Some(expect_string(&nv)?);
                }
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "expected `name = \"...\"` or `version = \"...\"`",
                    ));
                }
            }
        }
        Ok(PluginArgs {
            name: name.ok_or_else(|| {
                Error::new(Span::call_site(), "#[plugin] missing required `name`")
            })?,
            version: version.ok_or_else(|| {
                Error::new(Span::call_site(), "#[plugin] missing required `version`")
            })?,
        })
    }
}

struct HandlerArgs {
    kind: HandlerKind,
    view: bool,
}

impl Parse for HandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let metas: Punctuated<Meta, Token![,]> = Punctuated::parse_terminated(input)?;
        let mut kind = None;
        let mut view = false;
        for meta in metas {
            match meta {
                Meta::NameValue(nv) if nv.path.is_ident("name") => {
                    let raw = expect_string(&nv)?;
                    let parsed = HandlerKind::from_wire_name(&raw).ok_or_else(|| {
                        Error::new_spanned(
                            &nv.value,
                            format!(
                                "unknown handler kind `{raw}` (supported: filter, shape, normalize_error, observe)"
                            ),
                        )
                    })?;
                    kind = Some(parsed);
                }
                Meta::Path(p) if p.is_ident("view") => {
                    view = true;
                }
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "expected `name = \"...\"` or `view`",
                    ));
                }
            }
        }
        Ok(HandlerArgs {
            kind: kind.ok_or_else(|| {
                Error::new(Span::call_site(), "#[handler] missing required `name`")
            })?,
            view,
        })
    }
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

// ---------- handler taxonomy ----------

#[derive(Clone, Copy, PartialEq, Eq)]
enum HandlerKind {
    Filter,
    Shape,
    NormalizeError,
    Observe,
}

impl HandlerKind {
    fn from_wire_name(s: &str) -> Option<Self> {
        Some(match s {
            "filter" => HandlerKind::Filter,
            "shape" => HandlerKind::Shape,
            "normalize_error" => HandlerKind::NormalizeError,
            "observe" => HandlerKind::Observe,
            _ => return None,
        })
    }

    fn wire_name(self) -> &'static str {
        match self {
            HandlerKind::Filter => "filter",
            HandlerKind::Shape => "shape",
            HandlerKind::NormalizeError => "normalize_error",
            HandlerKind::Observe => "observe",
        }
    }

    fn export_name(self) -> &'static str {
        match self {
            HandlerKind::Filter => "cc_lb_filter",
            HandlerKind::Shape => "cc_lb_shape",
            HandlerKind::NormalizeError => "cc_lb_normalize_error",
            HandlerKind::Observe => "cc_lb_observe",
        }
    }

    fn dispatch_helper(self, view: bool) -> &'static str {
        match (self, view) {
            (HandlerKind::Filter, false) => "run_filter",
            (HandlerKind::Filter, true) => "run_filter_view",
            (HandlerKind::Shape, false) => "run_shape",
            (HandlerKind::Shape, true) => "run_shape_view",
            (HandlerKind::NormalizeError, false) => "run_normalize_error",
            (HandlerKind::NormalizeError, true) => "run_normalize_error_view",
            (HandlerKind::Observe, false) => "run_observe",
            (HandlerKind::Observe, true) => "run_observe_view",
        }
    }

    fn schema_section(self) -> &'static str {
        // Constants live in `cc-lb-plugin-types::schema` so the host
        // runtime (`cc-lb-runtime-wasmtime::inspect`) and this macro
        // crate share one source of truth — drift impossible.
        match self {
            HandlerKind::Filter => cc_lb_plugin_types::schema::SECTION_FILTER,
            HandlerKind::Shape => cc_lb_plugin_types::schema::SECTION_SHAPE,
            HandlerKind::NormalizeError => cc_lb_plugin_types::schema::SECTION_NORMALIZE_ERROR,
            HandlerKind::Observe => cc_lb_plugin_types::schema::SECTION_OBSERVE,
        }
    }

    fn schema_tag(self) -> &'static [u8] {
        match self {
            HandlerKind::Filter => cc_lb_plugin_types::schema::WIRE_SCHEMA_TAG_FILTER,
            HandlerKind::Shape => cc_lb_plugin_types::schema::WIRE_SCHEMA_TAG_SHAPE,
            HandlerKind::NormalizeError => {
                cc_lb_plugin_types::schema::WIRE_SCHEMA_TAG_NORMALIZE_ERROR
            }
            HandlerKind::Observe => cc_lb_plugin_types::schema::WIRE_SCHEMA_TAG_OBSERVE,
        }
    }

    fn const_suffix(self) -> &'static str {
        match self {
            HandlerKind::Filter => "FILTER",
            HandlerKind::Shape => "SHAPE",
            HandlerKind::NormalizeError => "NORMALIZE_ERROR",
            HandlerKind::Observe => "OBSERVE",
        }
    }
}

struct DiscoveredHandler {
    fn_ident: Ident,
    kind: HandlerKind,
    view: bool,
}

fn collect_handlers(module: &ItemMod) -> syn::Result<Vec<DiscoveredHandler>> {
    let Some((_, items)) = &module.content else {
        return Err(Error::new_spanned(
            &module.ident,
            "#[plugin] requires an inline module",
        ));
    };
    let mut handlers = Vec::new();
    for item in items {
        let Item::Fn(f) = item else { continue };
        for attr in &f.attrs {
            if !is_handler_attr(attr.path()) {
                continue;
            }
            let args: HandlerArgs = attr.parse_args()?;
            handlers.push(DiscoveredHandler {
                fn_ident: f.sig.ident.clone(),
                kind: args.kind,
                view: args.view,
            });
        }
    }
    Ok(handlers)
}

fn is_handler_attr(p: &syn::Path) -> bool {
    p.segments
        .last()
        .map(|s| s.ident == "handler")
        .unwrap_or(false)
}

// ---------- helpers ----------

fn byte_array_tokens(bytes: &[u8]) -> TokenStream2 {
    let elems = bytes.iter().map(|b| quote! { #b });
    quote! { [ #(#elems),* ] }
}

fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use core::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}
