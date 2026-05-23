use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, Visibility, parse_macro_input};

#[proc_macro_attribute]
pub fn plugin_fn(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut function = parse_macro_input!(item as ItemFn);
    let export_name = function.sig.ident.clone();
    let inner_name = format_ident!("__{}_impl", export_name);
    function.sig.ident = inner_name.clone();
    function.vis = Visibility::Inherited;

    quote! {
        #function

        #[unsafe(no_mangle)]
        pub extern "C" fn #export_name() -> i32 {
            ::extism_pdk::__private::run(#inner_name)
        }
    }
    .into()
}
