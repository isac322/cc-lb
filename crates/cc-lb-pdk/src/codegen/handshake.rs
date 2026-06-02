use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitStr;

use crate::parse::PluginDescriptor;

pub(crate) fn emit_handshake_export(plugin: &PluginDescriptor) -> TokenStream {
    let supported_entries = plugin.handlers.iter().map(|handler| {
        let name = LitStr::new(&handler.name, Span::call_site());
        let versions = &handler.versions;
        quote! {
            plugin_supported.insert(#name.to_owned(), ::std::vec![#(#versions),*]);
        }
    });
    let implemented_entries = plugin.handlers.iter().map(|handler| {
        let name = LitStr::new(&handler.name, Span::call_site());
        quote! {
            implemented_functions.insert(#name.to_owned());
        }
    });
    let required_capabilities = plugin.required_capabilities.iter().map(|capability| {
        let capability = LitStr::new(capability, Span::call_site());
        quote! {
            required_capabilities.insert(#capability.to_owned());
        }
    });

    quote! {
        #[extism_pdk::plugin_fn]
        pub fn cc_lb_handshake(input: ::std::string::String) -> extism_pdk::FnResult<::std::string::String> {
            use ::std::collections::{BTreeMap, BTreeSet};

            let offer: cc_lb_plugin_wire::handshake::HandshakeOffer = serde_json::from_str(&input)?;
            offer.validate()?;

            let mut plugin_supported: BTreeMap<::std::string::String, ::std::vec::Vec<u32>> = BTreeMap::new();
            #(#supported_entries)*

            let mut implemented_functions: BTreeSet<::std::string::String> = BTreeSet::new();
            #(#implemented_entries)*

            let mut required_capabilities: BTreeSet<::std::string::String> = BTreeSet::new();
            #(#required_capabilities)*

            let mut chosen_versions: BTreeMap<::std::string::String, u32> = BTreeMap::new();
            for (function, supported_versions) in &plugin_supported {
                let Some(offered_versions) = offer.function_versions.get(function) else {
                    continue;
                };
                let Some(chosen) = offered_versions
                    .iter()
                    .filter(|version| supported_versions.contains(version))
                    .max()
                    .copied()
                else {
                    continue;
                };
                chosen_versions.insert(function.clone(), chosen);
            }

            let accept = cc_lb_plugin_wire::handshake::HandshakeAccept {
                handshake_schema_version: offer.handshake_schema_version,
                envelope_version: offer.envelope_version,
                chosen_versions,
                plugin_supported,
                implemented_functions,
                required_capabilities,
            };
            accept.validate_against_offer(&offer)?;

            Ok(serde_json::to_string(&accept)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{HandlerDescriptor, PluginDescriptor};
    use syn::{ItemFn, parse_quote};

    #[test]
    fn emits_extism_plugin_fn_named_cc_lb_handshake() {
        let generated = emit_handshake_export(&plugin_descriptor());
        let item: ItemFn = syn::parse2(generated).expect("generated handshake export parses");

        assert_eq!(item.sig.ident, "cc_lb_handshake");
        assert!(item.attrs.iter().any(|attr| {
            attr.path()
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "plugin_fn")
        }));
    }

    #[test]
    fn generated_export_uses_ordered_handshake_structures() {
        let generated = emit_handshake_export(&plugin_descriptor()).to_string();

        assert!(generated.contains("BTreeMap"));
        assert!(generated.contains("BTreeSet"));
        assert!(!generated.contains("HashMap"));
        assert!(!generated.contains("HashSet"));
    }

    #[test]
    fn generated_export_negotiates_highest_common_version_and_validates_accept() {
        let generated = emit_handshake_export(&plugin_descriptor()).to_string();

        assert!(generated.contains("serde_json :: from_str"));
        assert!(generated.contains("offer . validate"));
        assert!(generated.contains("function_versions . get"));
        assert!(generated.contains("max"));
        assert!(generated.contains("validate_against_offer"));
        assert!(generated.contains("serde_json :: to_string"));
    }

    #[test]
    fn generated_export_contains_declared_functions_versions_and_capabilities() {
        let generated = emit_handshake_export(&plugin_descriptor()).to_string();

        assert!(generated.contains("route"));
        assert!(generated.contains("shape"));
        assert!(generated.contains("streaming"));
        assert!(generated.contains("storage"));
    }

    fn plugin_descriptor() -> PluginDescriptor {
        PluginDescriptor {
            plugin_name: "round-robin".to_owned(),
            plugin_version: "1.0.0".to_owned(),
            required_capabilities: vec!["streaming".to_owned(), "storage".to_owned()],
            handlers: vec![
                HandlerDescriptor {
                    name: "route".to_owned(),
                    versions: vec![1, 3],
                    fn_ident: parse_quote!(route),
                    request_type: parse_quote!(RouteRequest),
                    response_type: parse_quote!(RouteResponse),
                },
                HandlerDescriptor {
                    name: "shape".to_owned(),
                    versions: vec![1],
                    fn_ident: parse_quote!(shape),
                    request_type: parse_quote!(ShapeRequest),
                    response_type: parse_quote!(ShapeResponse),
                },
            ],
        }
    }
}
