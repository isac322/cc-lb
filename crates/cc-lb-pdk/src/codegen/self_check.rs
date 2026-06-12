use proc_macro2::TokenStream;
use quote::quote;

use crate::parse::{HandlerDescriptor, PluginDescriptor};

pub(crate) fn emit_self_check_export(plugin: &PluginDescriptor) -> TokenStream {
    let handler_checks = plugin.handlers.iter().map(emit_handler_check);

    quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn cc_lb_self_check() -> i32 {
            cc_lb_plugin_wire::guest::run_string_export(|input| -> ::std::result::Result<::std::string::String, ::std::string::String> {
                let req: cc_lb_plugin_wire::self_check::SelfCheckRequest = cc_lb_plugin_wire::serde_json::from_str(&input)
                    .map_err(|error| error.to_string())?;
                req.validate().map_err(|error| error.to_string())?;

                let mut failures: ::std::vec::Vec<cc_lb_plugin_wire::self_check::SelfCheckFailure> = ::std::vec::Vec::new();

                #(#handler_checks)*

                let response = cc_lb_plugin_wire::self_check::SelfCheckResponse {
                    status: if failures.is_empty() {
                        cc_lb_plugin_wire::self_check::SelfCheckStatus::Success
                    } else {
                        cc_lb_plugin_wire::self_check::SelfCheckStatus::Failure
                    },
                    failures,
                    completed_at: req.initiated_at,
                };
                response.validate().map_err(|error| error.to_string())?;

                let serialized = cc_lb_plugin_wire::serde_json::to_string(&response)
                    .map_err(|error| error.to_string())?;
                if serialized.as_bytes().len() > cc_lb_plugin_wire::limits::SELF_CHECK_OUTPUT_MAX_BYTES {
                    return Err(::std::format!(
                        "cc_lb_self_check response exceeded {} bytes",
                        cc_lb_plugin_wire::limits::SELF_CHECK_OUTPUT_MAX_BYTES,
                    ));
                }

                Ok(serialized)
            })
        }
    }
}

fn emit_handler_check(handler: &HandlerDescriptor) -> TokenStream {
    let Some(wire_function) = wire_function_type(&handler.name) else {
        let name = &handler.name;
        return quote! {
            compile_error!(concat!("unknown cc-lb wire function for self-check: ", #name));
        };
    };

    let request_type = &handler.request_type;
    let response_type = &handler.response_type;
    let checks = handler.versions.iter().map(|version| {
        let label = format!("{}@{}", handler.name, version);
        quote! {
            if let Err(message) = (|| -> ::std::result::Result<(), ::std::string::String> {
                let sample = <#wire_function as cc_lb_plugin_wire::wire_function::WireFunction>::dry_run_request();
                let bytes = cc_lb_plugin_wire::serde_json::to_vec(&sample)
                    .map_err(|error| ::std::format!("{} request serialize failed: {}", #label, error))?;
                let decoded: #request_type = cc_lb_plugin_wire::serde_json::from_slice(&bytes)
                    .map_err(|error| ::std::format!("{} request deserialize failed: {}", #label, error))?;
                let bytes = cc_lb_plugin_wire::serde_json::to_vec(&decoded)
                    .map_err(|error| ::std::format!("{} request reserialize failed: {}", #label, error))?;
                let _: <#wire_function as cc_lb_plugin_wire::wire_function::WireFunction>::Request = cc_lb_plugin_wire::serde_json::from_slice(&bytes)
                    .map_err(|error| ::std::format!("{} request wire decode failed: {}", #label, error))?;

                let sample = <#wire_function as cc_lb_plugin_wire::wire_function::WireFunction>::dry_run_response();
                let bytes = cc_lb_plugin_wire::serde_json::to_vec(&sample)
                    .map_err(|error| ::std::format!("{} response serialize failed: {}", #label, error))?;
                let decoded: #response_type = cc_lb_plugin_wire::serde_json::from_slice(&bytes)
                    .map_err(|error| ::std::format!("{} response deserialize failed: {}", #label, error))?;
                let bytes = cc_lb_plugin_wire::serde_json::to_vec(&decoded)
                    .map_err(|error| ::std::format!("{} response reserialize failed: {}", #label, error))?;
                let _: <#wire_function as cc_lb_plugin_wire::wire_function::WireFunction>::Response = cc_lb_plugin_wire::serde_json::from_slice(&bytes)
                    .map_err(|error| ::std::format!("{} response wire decode failed: {}", #label, error))?;

                Ok(())
            })() {
                failures.push(cc_lb_plugin_wire::self_check::SelfCheckFailure {
                    stage: cc_lb_plugin_wire::self_check::SelfCheckStage::WireFunctionTest,
                    message,
                });
            }
        }
    });

    quote! {
        #(#checks)*
    }
}

fn wire_function_type(name: &str) -> Option<TokenStream> {
    match name {
        "shape" => Some(quote!(cc_lb_plugin_wire::v1::shape::ShapeFn)),
        "normalize_error" => Some(quote!(
            cc_lb_plugin_wire::v1::normalize_error::NormalizeErrorFn
        )),
        "build_signer" => Some(quote!(cc_lb_plugin_wire::v1::build_signer::BuildSignerFn)),
        "sign" => Some(quote!(cc_lb_plugin_wire::v1::sign::SignFn)),
        "on_unauthorized" => Some(quote!(
            cc_lb_plugin_wire::v1::on_unauthorized::OnUnauthorizedFn
        )),
        "observe" => Some(quote!(cc_lb_plugin_wire::v1::observe::ObserveFn)),
        "filter" => Some(quote!(cc_lb_plugin_wire::v3::filter::FilterFn)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::ToTokens;
    use syn::{Ident, ItemFn, parse_quote};

    fn descriptor() -> PluginDescriptor {
        PluginDescriptor {
            plugin_name: "dry-run-plugin".to_string(),
            plugin_version: "1.0.0".to_string(),
            required_capabilities: Vec::new(),
            handlers: vec![HandlerDescriptor {
                name: "filter".to_string(),
                versions: vec![1],
                fn_ident: Ident::new("filter_handler", proc_macro2::Span::call_site()),
                request_type: parse_quote!(cc_lb_plugin_wire::v3::filter::FilterRequest),
                response_type: parse_quote!(cc_lb_plugin_wire::v3::filter::FilterResponse),
            }],
        }
    }

    #[test]
    fn emits_self_check() {
        let generated = emit_self_check_export(&descriptor());
        let item: ItemFn = syn::parse2(generated).expect("generated self-check export parses");

        assert_eq!(item.sig.ident, "cc_lb_self_check");
        assert!(item.sig.abi.is_some());
        assert!(item.sig.inputs.is_empty());
        assert_eq!(item.sig.output.to_token_stream().to_string(), "-> i32");
        assert!(
            item.to_token_stream()
                .to_string()
                .contains("SelfCheckRequest")
        );
        assert!(
            item.to_token_stream()
                .to_string()
                .contains("SelfCheckResponse")
        );
    }

    #[test]
    fn handler_not_called() {
        let tokens = emit_self_check_export(&descriptor()).to_string();

        assert!(!tokens.contains("filter_handler"));
    }

    #[test]
    fn uses_dry_run() {
        let tokens = emit_self_check_export(&descriptor()).to_string();

        assert!(tokens.contains("dry_run_request"));
        assert!(tokens.contains("dry_run_response"));
        assert!(!tokens.contains("Default :: default"));
    }

    #[test]
    fn uses_registered_handler_types() {
        let tokens = emit_self_check_export(&descriptor()).to_string();

        assert!(tokens.contains("FilterRequest"));
        assert!(tokens.contains("FilterResponse"));
    }

    #[test]
    fn deterministic() {
        let first = emit_self_check_export(&descriptor()).to_string();
        let second = emit_self_check_export(&descriptor()).to_string();

        assert_eq!(first, second);
    }

    #[test]
    fn generated_code_uses_wire_guest_helpers_not_direct_extism_pdk() {
        let tokens = emit_self_check_export(&descriptor()).to_string();

        assert!(tokens.contains("cc_lb_plugin_wire :: guest :: run_string_export"));
        assert!(!tokens.contains("extism_pdk"));
    }

    #[test]
    fn supports_current_wire_function_names() {
        let mut plugin = descriptor();
        plugin.handlers = vec![
            HandlerDescriptor {
                name: "shape".to_string(),
                versions: vec![1],
                fn_ident: Ident::new("shape_handler", proc_macro2::Span::call_site()),
                request_type: parse_quote!(cc_lb_plugin_wire::v1::shape::ShapeRequest),
                response_type: parse_quote!(cc_lb_plugin_wire::v1::shape::ShapeResponse),
            },
            HandlerDescriptor {
                name: "normalize_error".to_string(),
                versions: vec![1],
                fn_ident: Ident::new("normalize_error_handler", proc_macro2::Span::call_site()),
                request_type: parse_quote!(
                    cc_lb_plugin_wire::v1::normalize_error::NormalizeErrorRequest
                ),
                response_type: parse_quote!(
                    cc_lb_plugin_wire::v1::normalize_error::NormalizeErrorResponse
                ),
            },
            HandlerDescriptor {
                name: "build_signer".to_string(),
                versions: vec![1],
                fn_ident: Ident::new("build_signer_handler", proc_macro2::Span::call_site()),
                request_type: parse_quote!(cc_lb_plugin_wire::v1::build_signer::BuildSignerRequest),
                response_type: parse_quote!(
                    cc_lb_plugin_wire::v1::build_signer::BuildSignerResponse
                ),
            },
            HandlerDescriptor {
                name: "sign".to_string(),
                versions: vec![1],
                fn_ident: Ident::new("sign_handler", proc_macro2::Span::call_site()),
                request_type: parse_quote!(cc_lb_plugin_wire::v1::sign::SignRequest),
                response_type: parse_quote!(cc_lb_plugin_wire::v1::sign::SignResponse),
            },
            HandlerDescriptor {
                name: "on_unauthorized".to_string(),
                versions: vec![1],
                fn_ident: Ident::new("on_unauthorized_handler", proc_macro2::Span::call_site()),
                request_type: parse_quote!(
                    cc_lb_plugin_wire::v1::on_unauthorized::OnUnauthorizedRequest
                ),
                response_type: parse_quote!(
                    cc_lb_plugin_wire::v1::on_unauthorized::OnUnauthorizedResponse
                ),
            },
            HandlerDescriptor {
                name: "observe".to_string(),
                versions: vec![1],
                fn_ident: Ident::new("observe_handler", proc_macro2::Span::call_site()),
                request_type: parse_quote!(cc_lb_plugin_wire::v1::observe::ObserveRequest),
                response_type: parse_quote!(cc_lb_plugin_wire::v1::observe::ObserveResponse),
            },
            HandlerDescriptor {
                name: "filter".to_string(),
                versions: vec![1],
                fn_ident: Ident::new("filter_handler", proc_macro2::Span::call_site()),
                request_type: parse_quote!(cc_lb_plugin_wire::v3::filter::FilterRequest),
                response_type: parse_quote!(cc_lb_plugin_wire::v3::filter::FilterResponse),
            },
        ];

        let tokens = emit_self_check_export(&plugin).to_string();

        assert!(tokens.contains("ShapeFn"));
        assert!(tokens.contains("NormalizeErrorFn"));
        assert!(tokens.contains("BuildSignerFn"));
        assert!(tokens.contains("SignFn"));
        assert!(tokens.contains("OnUnauthorizedFn"));
        assert!(tokens.contains("ObserveFn"));
        assert!(tokens.contains("FilterFn"));
    }
}
