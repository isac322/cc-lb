use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, quote};

pub(crate) fn byte_array_tokens(bytes: &[u8]) -> TokenStream2 {
    let elems = bytes.iter().map(|b| quote! { #b });
    quote! { [ #(#elems),* ] }
}

pub(crate) fn compact_tokens<T: ToTokens>(value: T) -> String {
    value.to_token_stream().to_string().replace(' ', "")
}

pub(crate) fn escape_json(s: &str) -> String {
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
