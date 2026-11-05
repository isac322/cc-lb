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
        let export_name = handler.kind.export_name(handler.provider);
        let export_ident = syn::Ident::new(&export_name, Span::call_site());
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

    let metadata_section = if handlers.iter().any(|handler| handler.provider.is_some()) {
        "cc_lb.plugin.v2"
    } else {
        "cc_lb.plugin.v1"
    };

    quote! {
        #module

        #(#schema_section_items)*

        #[used]
        #[cfg_attr(target_arch = "wasm32", unsafe(link_section = #metadata_section))]
        #[cfg_attr(all(not(target_arch = "wasm32"), target_vendor = "apple"), unsafe(link_section = "__DATA,__cc_lb_meta"))]
        #[cfg_attr(all(not(target_arch = "wasm32"), not(target_vendor = "apple")), unsafe(link_section = #metadata_section))]
        static __CC_LB_PLUGIN_METADATA: [u8; #metadata_len] = #metadata_array;
    }
}

fn reject_duplicates(handlers: &[DiscoveredHandler]) -> syn::Result<()> {
    for (idx, handler) in handlers.iter().enumerate() {
        if handlers.iter().skip(idx + 1).any(|other| {
            other.kind == handler.kind
                && other
                    .provider
                    .unwrap_or(crate::args::ProviderFamily::Anthropic)
                    == handler
                        .provider
                        .unwrap_or(crate::args::ProviderFamily::Anthropic)
        }) {
            return Err(Error::new_spanned(
                &handler.fn_ident,
                format!(
                    "duplicate handler entrypoint `{}` — each (hook, provider) may appear once; legacy handlers occupy Anthropic",
                    handler.kind.metadata_key(handler.provider)
                ),
            ));
        }
    }
    Ok(())
}

fn schema_sections(handlers: &[DiscoveredHandler]) -> syn::Result<Vec<TokenStream2>> {
    let mut seen = std::collections::BTreeSet::new();
    let mut sections = Vec::new();
    for handler in handlers {
        if !seen.insert((handler.kind.wire_name(), handler.wire_version)) {
            continue;
        }
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
            &format!(
                "__CC_LB_SCHEMA_HASH_{}_V{}",
                handler.kind.const_suffix(),
                handler.wire_version
            ),
            Span::call_site(),
        );
        sections.push(quote! {
            #[used]
            #[cfg_attr(target_arch = "wasm32", unsafe(link_section = #section))]
            #[cfg_attr(all(not(target_arch = "wasm32"), target_vendor = "apple"), unsafe(link_section = "__DATA,__cc_lb_schema"))]
            #[cfg_attr(all(not(target_arch = "wasm32"), not(target_vendor = "apple")), unsafe(link_section = #section))]
            static #static_ident: [u8; 32] = <#fingerprint_type as ::cc_lb_pdk_wasmtime::types::schema::WireSchema>::FINGERPRINT;
        });
    }
    Ok(sections)
}

fn plugin_metadata_json(args: &PluginArgs, handlers: &[DiscoveredHandler]) -> String {
    let mut sorted: Vec<&DiscoveredHandler> = handlers.iter().collect();
    sorted.sort_by_cached_key(|handler| handler.kind.metadata_key(handler.provider));
    let hooks = sorted
        .iter()
        .map(|handler| {
            format!(
                r#""{}":{{"wire_version":{},"description":"{}","usage":"{}","mode":"{}"}}"#,
                handler.kind.metadata_key(handler.provider),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn plugin_args() -> PluginArgs {
        syn::parse2(quote! {
            name = "test-plugin",
            version = "1.0.0",
            description = "Test plugin",
            usage = "Test only"
        })
        .expect("plugin arguments parse")
    }

    fn module(handlers: TokenStream2) -> ItemMod {
        syn::parse2(quote! {
            mod test_plugin {
                #handlers
            }
        })
        .expect("handler module parses")
    }

    #[test]
    fn qualified_handlers_emit_provider_exports_and_one_shared_schema() {
        let module = module(quote! {
            #[handler(shape, provider = "openai", wire = 1, description = "OpenAI", usage = "Test")]
            fn openai(req: ShapeRequest) -> ShapeResponse { req.into() }
            #[handler(shape, provider = "gemini", wire = 1, description = "Gemini", usage = "Test")]
            fn gemini(req: ShapeRequest) -> ShapeResponse { req.into() }
        });
        let handlers = collect_handlers(&module).expect("handlers collected");
        assert!(reject_duplicates(&handlers).is_ok());
        assert_eq!(
            schema_sections(&handlers).expect("schema sections").len(),
            1
        );
        let metadata = plugin_metadata_json(&plugin_args(), &handlers);
        assert!(metadata.contains(r#""shape_openai":{"wire_version":1"#));
        assert!(metadata.contains(r#""shape_gemini":{"wire_version":1"#));

        let expanded = expand(plugin_args(), module);
        let file: syn::File =
            syn::parse2(expanded.clone()).expect("expansion is valid Rust syntax");
        let generated_module = file
            .items
            .iter()
            .find_map(|item| {
                if let syn::Item::Mod(module) = item {
                    Some(module)
                } else {
                    None
                }
            })
            .expect("generated module");
        let exports: Vec<_> = generated_module
            .content
            .as_ref()
            .expect("inline module")
            .1
            .iter()
            .filter_map(|item| {
                if let syn::Item::Fn(function) = item {
                    Some(function.sig.ident.to_string())
                } else {
                    None
                }
            })
            .collect();
        assert!(exports.iter().any(|name| name == "cc_lb_shape_openai"));
        assert!(exports.iter().any(|name| name == "cc_lb_shape_gemini"));
        assert!(!exports.iter().any(|name| name == "cc_lb_shape"));
        assert!(expanded.to_string().contains("\"cc_lb.plugin.v2\""));
    }
    #[test]
    fn wire_two_provider_hooks_emit_versioned_schema_statics_and_envelope_export() {
        let module = module(quote! {
            #[handler(shape, wire = 1, description = "Legacy", usage = "Test")]
            fn legacy_shape(req: ShapeRequest) -> ShapeResponse { req.into() }
            #[handler(shape, provider = "openai", wire = 2, description = "OpenAI V2", usage = "Test")]
            fn openai_shape(req: ::cc_lb_plugin_wire::v2::ShapeRequest) -> ::cc_lb_plugin_wire::v2::ShapeResponse { req.into() }
            #[handler(shape_envelope_gemini, provider = "gemini", wire = 2, description = "Gemini envelope", usage = "Test")]
            fn envelope(req: ::cc_lb_plugin_wire::v2::ShapeEnvelopeGeminiRequest) -> ::cc_lb_plugin_wire::v2::ShapeEnvelopeGeminiResponse { req.into() }
        });
        let handlers = collect_handlers(&module).expect("handlers collected");
        assert_eq!(
            schema_sections(&handlers).expect("schema sections").len(),
            3
        );
        let metadata = plugin_metadata_json(&plugin_args(), &handlers);
        assert!(metadata.contains(r#""shape":{"wire_version":1"#));
        assert!(metadata.contains(r#""shape_openai":{"wire_version":2"#));
        assert!(metadata.contains(r#""shape_envelope_gemini":{"wire_version":2"#));

        let expanded = expand(plugin_args(), module).to_string();
        assert!(expanded.contains("CC_LB_SCHEMA_HASH_SHAPE_V1"));
        assert!(expanded.contains("CC_LB_SCHEMA_HASH_SHAPE_V2"));
        assert!(expanded.contains("cc_lb_shape_envelope_gemini"));
    }

    #[test]
    fn duplicate_provider_handler_emits_compile_error() {
        let module = module(quote! {
            #[handler(shape, provider = "openai", wire = 1, description = "One", usage = "Test")]
            fn one() {}
            #[handler(shape, provider = "openai", wire = 1, description = "Two", usage = "Test")]
            fn two() {}
        });
        let expanded = expand(plugin_args(), module).to_string();
        assert!(expanded.contains("compile_error"));
        assert!(expanded.contains("duplicate handler entrypoint"));
        assert!(expanded.contains("shape_openai"));
    }

    #[test]
    fn legacy_handler_keeps_export_section_and_metadata() {
        let module = module(quote! {
            #[handler(filter, wire = 1, description = "Filter", usage = "Test")]
            fn filter() {}
        });
        let handlers = collect_handlers(&module).expect("handler collected");
        assert_eq!(
            plugin_metadata_json(&plugin_args(), &handlers),
            r#"{"name":"test-plugin","version":"1.0.0","description":"Test plugin","usage":"Test only","hooks":{"filter":{"wire_version":1,"description":"Filter","usage":"Test","mode":"active"}}}"#
        );
        let expanded = expand(plugin_args(), module).to_string();
        assert!(expanded.contains("fn cc_lb_filter"));
        assert!(!expanded.contains("cc_lb_filter_anthropic"));
        assert!(expanded.contains("\"cc_lb.plugin.v1\""));
        assert!(!expanded.contains("\"cc_lb.plugin.v2\""));
    }

    #[test]
    fn legacy_and_qualified_anthropic_overlap_is_compile_error() {
        for kind in [
            "filter",
            "shape",
            "transform_response",
            "transform_sse_event",
        ] {
            let kind = syn::Ident::new(kind, Span::call_site());
            let module = module(quote! {
                #[handler(#kind, wire = 1, description = "Legacy", usage = "Test")]
                fn legacy() {}
                #[handler(#kind, provider = "anthropic", wire = 1, description = "New", usage = "Test")]
                fn qualified() {}
            });
            assert!(
                expand(plugin_args(), module)
                    .to_string()
                    .contains("compile_error")
            );
        }
    }

    #[test]
    fn legacy_and_other_provider_hooks_can_coexist() {
        let module = module(quote! {
            #[handler(filter, wire = 1, description = "Legacy Anthropic", usage = "Test")]
            fn legacy() {}
            #[handler(filter, provider = "openai", wire = 1, description = "OpenAI", usage = "Test")]
            fn openai() {}
            #[handler(filter, provider = "gemini", wire = 1, description = "Gemini", usage = "Test")]
            fn gemini() {}
        });
        let handlers = collect_handlers(&module).expect("handlers collected");
        assert!(reject_duplicates(&handlers).is_ok());
        assert_eq!(
            schema_sections(&handlers).expect("schema sections").len(),
            1
        );
        let expanded = expand(plugin_args(), module).to_string();
        assert!(!expanded.contains("compile_error"));
        assert!(expanded.contains("fn cc_lb_filter "));
        assert!(expanded.contains("fn cc_lb_filter_openai "));
        assert!(expanded.contains("fn cc_lb_filter_gemini "));
        assert!(expanded.contains("\"cc_lb.plugin.v2\""));
    }
}
