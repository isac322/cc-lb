use cc_lb_plugin_wire::identity::{PluginIdentity, CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{LitByteStr, LitStr};

use crate::parse::PluginDescriptor;

const ABI_ENVELOPE_VERSION: u32 = 1;

pub(crate) fn emit_custom_section(plugin: &PluginDescriptor) -> TokenStream {
    let payload = serialize_identity(plugin);
    let len = payload.len();
    let payload = LitByteStr::new(&payload, Span::call_site());
    let section_name = LitStr::new(CC_LB_PLUGIN_SECTION_NAME, Span::call_site());

    quote! {
        #[used]
        #[unsafe(link_section = #section_name)]
        static CC_LB_PLUGIN_METADATA: [u8; #len] = *#payload;
    }
}

fn serialize_identity(plugin: &PluginDescriptor) -> Vec<u8> {
    let identity = PluginIdentity {
        magic: CC_LB_PLUGIN_MAGIC,
        abi_envelope: ABI_ENVELOPE_VERSION,
        plugin_name: plugin.plugin_name.clone(),
        plugin_version: plugin.plugin_version.clone(),
    };

    serde_json::to_vec(&identity).expect("plugin identity JSON serialization should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{HandlerDescriptor, PluginDescriptor};
    use quote::ToTokens;
    use syn::{parse_quote, Expr, ExprLit, ExprUnary, ItemStatic, Lit};

    #[test]
    fn generated_static_uses_used_link_section_and_byte_array() {
        let generated = emit_custom_section(&plugin_descriptor());
        let item: ItemStatic =
            syn::parse2(generated).expect("generated custom section static parses");

        assert_eq!(item.ident, "CC_LB_PLUGIN_METADATA");
        assert!(item.attrs.iter().any(|attr| attr.path().is_ident("used")));

        let generated = item.to_token_stream().to_string();
        assert!(generated.contains("unsafe (link_section = \"cc_lb.plugin.v1\")"));
        assert!(generated.contains("[u8 ;"));
    }

    #[test]
    fn generated_payload_is_serialized_plugin_identity_json() {
        let generated = emit_custom_section(&plugin_descriptor());
        let item: ItemStatic =
            syn::parse2(generated).expect("generated custom section static parses");
        let payload = static_payload(&item);
        let identity: PluginIdentity =
            serde_json::from_slice(&payload).expect("payload decodes as PluginIdentity");

        assert_eq!(identity.magic, CC_LB_PLUGIN_MAGIC);
        assert_eq!(identity.abi_envelope, ABI_ENVELOPE_VERSION);
        assert_eq!(identity.plugin_name, "round-robin");
        assert_eq!(identity.plugin_version, "1.0.0");
    }

    #[test]
    fn generated_payload_uses_eight_byte_magic_not_legacy_ascii_magic() {
        let payload = serialize_identity(&plugin_descriptor());
        let identity: PluginIdentity =
            serde_json::from_slice(&payload).expect("payload decodes as PluginIdentity");

        assert_eq!(identity.magic.len(), 8);
        assert_eq!(identity.magic, CC_LB_PLUGIN_MAGIC);
        assert!(!payload
            .windows(b"cc-lb-plugin".len())
            .any(|window| window == b"cc-lb-plugin"));
    }

    fn static_payload(item: &ItemStatic) -> Vec<u8> {
        let Expr::Unary(ExprUnary { expr, .. }) = item.expr.as_ref() else {
            panic!("static initializer is a dereferenced byte string");
        };
        let Expr::Lit(ExprLit {
            lit: Lit::ByteStr(payload),
            ..
        }) = expr.as_ref()
        else {
            panic!("static initializer dereferences a byte string");
        };
        payload.value()
    }

    fn plugin_descriptor() -> PluginDescriptor {
        PluginDescriptor {
            plugin_name: "round-robin".to_owned(),
            plugin_version: "1.0.0".to_owned(),
            required_capabilities: vec!["log".to_owned()],
            handlers: vec![HandlerDescriptor {
                name: "route".to_owned(),
                versions: vec![1],
                fn_ident: parse_quote!(route),
                request_type: parse_quote!(RouteRequest),
                response_type: parse_quote!(RouteResponse),
            }],
        }
    }
}
