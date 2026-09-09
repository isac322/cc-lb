use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{Error, ItemMod};

use crate::args::{DiscoveredHandler, PluginArgs, collect_handlers};
use crate::util::{byte_array_tokens, escape_json};

pub(crate) fn expand(args: PluginArgs, mut module: ItemMod) -> TokenStream2 {
    let handlers = match collect_handlers(&module) {
        Ok(handlers) => handlers,
        Err(error) => return error.to_compile_error(),
    };
    if handlers.is_empty() {
        return Error::new_spanned(
            &module.ident,
            "#[plugin] module must contain at least one #[handler(<kind>, wire = N, description = \"...\", usage = \"...\")] function",
        )
        .to_compile_error();
    }
    if let Err(error) = reject_duplicates(&handlers) {
        return error.to_compile_error();
    }
    let Some((_, items)) = &mut module.content else {
        return Error::new_spanned(
            &module.ident,
            "#[plugin] must be applied to an inline `mod foo { ... }`",
        )
        .to_compile_error();
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
        let Some(dispatch_helper) = handler
            .kind
            .dispatch_helper(handler.wire_version, handler.view)
        else {
            return Error::new_spanned(
                &handler.fn_ident,
                "unsupported hook wire version reached plugin expansion",
            )
            .to_compile_error();
        };
        let dispatch_ident = syn::Ident::new(dispatch_helper, Span::call_site());
        let fn_ident = &handler.fn_ident;
        items.push(syn::parse_quote! {
            #[unsafe(no_mangle)]
            pub extern "C" fn #export_ident(in_ptr: u32, in_len: u32) -> u64 {
                ::cc_lb_pdk_wasmtime::__private::#dispatch_ident(in_ptr, in_len, #fn_ident)
            }
        });
    }

    let metadata_json = plugin_metadata_json(&args, &handlers);
    let metadata_bytes = metadata_json.as_bytes();
    let metadata_len = metadata_bytes.len();
    let metadata_array = byte_array_tokens(metadata_bytes);
    let schema_section_items = match schema_sections(&handlers) {
        Ok(items) => items,
        Err(error) => return error.to_compile_error(),
    };

    quote! {
        #module

        #(#schema_section_items)*

        #[used]
        #[cfg_attr(target_arch = "wasm32", unsafe(link_section = "cc_lb.plugin.v1"))]
        #[cfg_attr(all(not(target_arch = "wasm32"), target_vendor = "apple"), unsafe(link_section = "__DATA,__cc_lb_meta"))]
        #[cfg_attr(all(not(target_arch = "wasm32"), not(target_vendor = "apple")), unsafe(link_section = "cc_lb.plugin.v1"))]
        static __CC_LB_PLUGIN_METADATA: [u8; #metadata_len] = #metadata_array;
    }
}

fn reject_duplicates(handlers: &[DiscoveredHandler]) -> syn::Result<()> {
    for (idx, handler) in handlers.iter().enumerate() {
        if handlers[idx + 1..]
            .iter()
            .any(|other| other.kind == handler.kind)
        {
            return Err(Error::new_spanned(
                &handler.fn_ident,
                format!(
                    "duplicate #[handler({})] — each hook kind may appear once per plugin",
                    handler.kind.wire_name()
                ),
            ));
        }
    }
    Ok(())
}

fn schema_sections(handlers: &[DiscoveredHandler]) -> syn::Result<Vec<TokenStream2>> {
    handlers
        .iter()
        .map(|handler| {
            let section = handler.kind.schema_section(handler.wire_version);
            let fingerprint_type = handler
                .kind
                .fingerprint_type(handler.wire_version)
                .ok_or_else(|| {
                    Error::new_spanned(
                        &handler.fn_ident,
                        "unsupported hook wire version reached schema expansion",
                    )
                })?;
            let static_ident = syn::Ident::new(
                &format!("__CC_LB_SCHEMA_HASH_{}", handler.kind.const_suffix()),
                Span::call_site(),
            );
            Ok(quote! {
                #[used]
                #[cfg_attr(target_arch = "wasm32", unsafe(link_section = #section))]
                #[cfg_attr(all(not(target_arch = "wasm32"), target_vendor = "apple"), unsafe(link_section = "__DATA,__cc_lb_schema"))]
                #[cfg_attr(all(not(target_arch = "wasm32"), not(target_vendor = "apple")), unsafe(link_section = #section))]
                static #static_ident: [u8; 32] = <#fingerprint_type as ::cc_lb_pdk_wasmtime::types::schema::WireSchema>::FINGERPRINT;
            })
        })
        .collect()
}

fn plugin_metadata_json(args: &PluginArgs, handlers: &[DiscoveredHandler]) -> String {
    let mut sorted: Vec<&DiscoveredHandler> = handlers.iter().collect();
    sorted.sort_by_key(|handler| handler.kind.wire_name());
    let hooks = sorted
        .iter()
        .map(|handler| {
            format!(
                r#""{}":{{"wire_version":{},"description":"{}","usage":"{}","mode":"{}"}}"#,
                handler.kind.wire_name(),
                handler.wire_version,
                escape_json(&handler.description),
                escape_json(&handler.usage),
                handler.mode.wire_name(),
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        r#"{{"name":"{}","version":"{}","description":"{}","usage":"{}","hooks":{{{}}}}}"#,
        escape_json(&args.name),
        escape_json(&args.version),
        escape_json(&args.description),
        escape_json(&args.usage),
        hooks,
    )
}
