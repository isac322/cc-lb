//! cc-lb-pdk: Procedural macros for Extism plugins with custom section and handshake generation.
//!
//! This crate provides `#[plugin]` and `#[handler]` macros that wrap extism-pdk functions
//! to automatically generate custom section metadata and cc-lb handshake protocol support.
//!
//! T10-T14 will implement the macro logic. Current stubs accept the decorated items but produce no code generation.

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// `#[plugin]` macro for marking the plugin entry point.
///
/// Stub: Currently passes through the decorated item unchanged.
/// T10-T14: Will wrap the function to generate custom section and handshake exports.
#[proc_macro_attribute]
pub fn plugin(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    quote! {
        #input
    }
    .into()
}

/// `#[handler]` macro for marking request/response handlers.
///
/// Stub: Currently passes through the decorated item unchanged.
/// T10-T14: Will register the handler in plugin metadata.
#[proc_macro_attribute]
pub fn handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    quote! {
        #input
    }
    .into()
}
