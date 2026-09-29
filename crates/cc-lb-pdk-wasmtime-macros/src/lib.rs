//! Procedural macros for `cc-lb-pdk-wasmtime`.
//!
//! `#[cc_lb_plugin(...)]` emits guest ABI exports, schema-fingerprint custom
//! sections, and the consolidated `cc_lb.plugin.v1` metadata section.
//! `#[handler(...)]` validates per-hook metadata while the enclosing plugin
//! macro consumes the local AST.
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
