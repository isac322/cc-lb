//! cc-lb-pdk: Procedural macros for Extism plugins with custom section and handshake generation.
//!
//! This crate provides `#[plugin]` and `#[handler]` macros that wrap extism-pdk functions
//! to automatically generate custom section metadata and cc-lb handshake protocol support.
//!
//! The `#[plugin]` macro parses plugin metadata and emits the generated cc-lb exports.

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, ItemMod};

mod codegen;
mod parse;
mod runtime;

/// `#[plugin]` macro for marking a cc-lb plugin module.
///
/// Parses plugin metadata and discovers `#[handler(...)]` functions, then emits generated
/// metadata, handshake, self-check, and handler wrapper exports from the parsed descriptor.
#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match syn::parse::<parse::PluginArgs>(attr) {
        Ok(args) => args,
        Err(error) => return parse::compile_error(error).into(),
    };

    let input = match syn::parse::<ItemMod>(item) {
        Ok(input) => input,
        Err(error) => {
            let error = syn::Error::new(
                error.span(),
                "#[plugin] must be applied to an inline module containing #[handler] functions",
            );
            return parse::compile_error(error).into();
        }
    };

    let descriptor = match parse::parse_plugin_descriptor(&args, &input) {
        Ok(descriptor) => descriptor,
        Err(error) => {
            let error = parse::compile_error(error);
            return quote! {
                #input
                #error
            }
            .into();
        }
    };

    let custom_section = codegen::emit_custom_section(&descriptor);
    let handshake_export = codegen::emit_handshake_export(&descriptor);
    let self_check_export = codegen::emit_self_check_export(&descriptor);
    let mut output = input;

    if let Some((_, items)) = &mut output.content {
        items.push(syn::Item::Verbatim(self_check_export));
        items.extend(
            descriptor
                .handlers
                .iter()
                .map(codegen::emit_handler_wrapper)
                .map(syn::Item::Verbatim),
        );
    }

    quote! {
        #output
        #custom_section
        #handshake_export
    }
    .into()
}

/// `#[handler]` macro for marking request/response handlers.
///
/// T10: Validates handler attributes and signatures, then returns the function unchanged.
/// T11-T14 will generate the actual wrapper exports from the enclosing `#[plugin]` module.
#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match syn::parse::<parse::HandlerArgs>(attr) {
        Ok(args) => args,
        Err(error) => return parse::compile_error(error).into(),
    };

    let input = match syn::parse::<ItemFn>(item) {
        Ok(input) => input,
        Err(error) => {
            let error = syn::Error::new(error.span(), "#[handler] must be applied to a function");
            return parse::compile_error(error).into();
        }
    };

    if let Err(error) = parse::parse_handler_descriptor(&args, &input) {
        let error = parse::compile_error(error);
        return quote! {
            #input
            #error
        }
        .into();
    }

    quote! {
        #input
    }
    .into()
}
