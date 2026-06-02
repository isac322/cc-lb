use proc_macro2::{Literal, TokenStream};
use quote::{format_ident, quote};

use crate::parse::HandlerDescriptor;

pub(crate) fn emit_handler_wrapper(handler: &HandlerDescriptor) -> TokenStream {
    let export_ident = format_ident!("{}", handler.name);
    let handler_ident = &handler.fn_ident;
    let request_type = &handler.request_type;
    let response_type = &handler.response_type;
    let version_arms = handler
        .versions
        .iter()
        .map(|version| emit_version_arm(*version, request_type, response_type, handler_ident));

    quote! {
        #[extism_pdk::plugin_fn]
        pub fn #export_ident(input: ::std::string::String) -> extism_pdk::FnResult<::std::string::String> {
            let envelope: serde_json::Value = serde_json::from_str(&input)?;
            let envelope_version = envelope
                .get("_v")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| extism_pdk::Error::msg("plugin envelope missing numeric _v"))?;

            match envelope_version {
                #(#version_arms)*
                unsupported_version => Err(extism_pdk::Error::msg(format!(
                    "unsupported plugin envelope _v: {unsupported_version}"
                )).into()),
            }
        }
    }
}

fn emit_version_arm(
    version: u32,
    request_type: &syn::Type,
    response_type: &syn::Type,
    handler_ident: &syn::Ident,
) -> TokenStream {
    let version = Literal::u64_suffixed(u64::from(version));

    quote! {
        #version => {
            let mut payload_envelope = envelope;
            if let serde_json::Value::Object(object) = &mut payload_envelope {
                object.remove("_v");
            }

            let payload: #request_type = serde_json::from_value(payload_envelope)?;
            let result: #response_type = #handler_ident(payload)?;
            let mut out = serde_json::to_value(&result)?;
            out["_v"] = serde_json::Value::from(#version);

            Ok(serde_json::to_string(&out)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::HandlerDescriptor;
    use syn::{Ident, ItemFn, parse_quote};

    #[test]
    fn emits_extism_plugin_fn_named_after_handler_export() {
        let generated = emit_handler_wrapper(&descriptor());
        let item: ItemFn = syn::parse2(generated).expect("generated handler wrapper parses");

        assert_eq!(item.sig.ident, "route");
        assert!(item.attrs.iter().any(|attr| {
            attr.path()
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "plugin_fn")
        }));
    }

    #[test]
    fn v_dispatch() {
        let generated = emit_handler_wrapper(&descriptor()).to_string();

        assert!(generated.contains(r#"envelope . get ("_v")"#));
        assert!(generated.contains("match envelope_version"));
        assert!(generated.contains("1u64 =>"));
        assert!(generated.contains("2u64 =>"));
        assert!(generated.contains("unsupported plugin envelope _v"));
    }

    #[test]
    fn removes_envelope_version_before_typed_request_decode() {
        let generated = emit_handler_wrapper(&descriptor()).to_string();

        assert!(generated.contains("payload_envelope"));
        assert!(generated.contains(r#"object . remove ("_v")"#));
        assert!(generated.contains("serde_json :: from_value (payload_envelope)"));
        assert!(generated.contains("RouteRequest"));
    }

    #[test]
    fn wraps_response_with_matched_version() {
        let generated = emit_handler_wrapper(&descriptor()).to_string();

        assert!(generated.contains("let mut out = serde_json :: to_value"));
        assert!(generated.contains(r#"out ["_v"] = serde_json :: Value :: from (1u64)"#));
        assert!(generated.contains(r#"out ["_v"] = serde_json :: Value :: from (2u64)"#));
        assert!(generated.contains("serde_json :: to_string (& out)"));
    }

    #[test]
    fn calls_user_handler() {
        let generated = emit_handler_wrapper(&descriptor()).to_string();

        assert!(generated.contains("route_handler (payload)"));
        assert!(
            generated.contains("let result : cc_lb_plugin_wire :: v1 :: route :: RouteResponse")
        );
    }

    fn descriptor() -> HandlerDescriptor {
        HandlerDescriptor {
            name: "route".to_owned(),
            versions: vec![1, 2],
            fn_ident: Ident::new("route_handler", proc_macro2::Span::call_site()),
            request_type: parse_quote!(cc_lb_plugin_wire::v1::route::RouteRequest),
            response_type: parse_quote!(cc_lb_plugin_wire::v1::route::RouteResponse),
        }
    }
}
