//! Procedural macros for `cc-lb-pdk-wasmtime`.
//!
//! `#[cc_lb_plugin(...)]` emits guest ABI exports, schema-fingerprint custom
//! sections, and V1 legacy or V2 provider-qualified metadata.
//! `#[handler(...)]` accepts an optional `provider = "anthropic"|"openai"|"gemini"`
//! selector and validates per-entrypoint metadata consumed by the plugin macro.
#![forbid(unsafe_code)]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, ItemMod, parse_macro_input};

mod args;
mod plugin;
mod util;

#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as args::PluginArgs);
    let module = parse_macro_input!(item as ItemMod);
    plugin::expand(args, module).into()
}

#[proc_macro_attribute]
pub fn cc_lb_plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    plugin(attr, item)
}

#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let _args = parse_macro_input!(attr as args::HandlerArgs);
    let input = parse_macro_input!(item as ItemFn);
    quote! { #input }.into()
}
