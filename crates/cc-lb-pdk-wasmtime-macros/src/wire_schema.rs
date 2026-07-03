use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, GenericArgument, PathArguments, Type};

use crate::util::{byte_array_tokens, compact_tokens};

pub(crate) fn expand(input: &DeriveInput) -> TokenStream2 {
    match descriptor_for(input) {
        Ok(descriptor) => expand_impl(input, &descriptor),
        Err(error) => error.to_compile_error(),
    }
}

fn expand_impl(input: &DeriveInput, descriptor: &str) -> TokenStream2 {
    let ident = &input.ident;
    let digest = blake3::hash(descriptor.as_bytes());
    let fingerprint = byte_array_tokens(digest.as_bytes());
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::cc_lb_plugin_wire::schema::WireSchema for #ident #ty_generics #where_clause {
            const FINGERPRINT: [u8; 32] = #fingerprint;
            const DESCRIPTOR: &'static str = #descriptor;
        }
    }
}

fn descriptor_for(input: &DeriveInput) -> syn::Result<String> {
    match &input.data {
        Data::Struct(data) => Ok(format!(
            "{}{}",
            input.ident,
            fields_descriptor(&data.fields, true)
        )),
        Data::Enum(data) => {
            let variants = data
                .variants
                .iter()
                .map(|variant| {
                    format!(
                        "{}{}",
                        variant.ident,
                        fields_descriptor(&variant.fields, false)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            Ok(format!("{}[{variants}]", input.ident))
        }
        Data::Union(data) => Err(Error::new_spanned(
            data.union_token,
            "WireSchema cannot be derived for unions",
        )),
    }
}

fn fields_descriptor(fields: &Fields, sort_named_fields: bool) -> String {
    match fields {
        Fields::Named(named) => {
            let mut parts = named
                .named
                .iter()
                .filter_map(|field| {
                    field
                        .ident
                        .as_ref()
                        .map(|ident| format!("{ident}:{}", canonical_type(&field.ty)))
                })
                .collect::<Vec<_>>();
            if sort_named_fields {
                parts.sort();
            }
            format!("{{{}}}", parts.join(","))
        }
        Fields::Unnamed(unnamed) => {
            let parts = unnamed
                .unnamed
                .iter()
                .map(|field| canonical_type(&field.ty))
                .collect::<Vec<_>>()
                .join(",");
            format!("({parts})")
        }
        Fields::Unit => String::new(),
    }
}

fn canonical_type(ty: &Type) -> String {
    match ty {
        Type::Array(array) => format!(
            "[{};{}]",
            canonical_type(&array.elem),
            compact_tokens(&array.len)
        ),
        Type::Path(path) => canonical_path(&path.path),
        Type::Reference(reference) => format!("&{}", canonical_type(&reference.elem)),
        Type::Slice(slice) => format!("[{}]", canonical_type(&slice.elem)),
        Type::Tuple(tuple) => {
            let elems = tuple
                .elems
                .iter()
                .map(canonical_type)
                .collect::<Vec<_>>()
                .join(",");
            format!("({elems})")
        }
        _ => compact_tokens(ty),
    }
}

fn canonical_path(path: &syn::Path) -> String {
    let Some(segment) = path.segments.last() else {
        return String::new();
    };
    let ident = segment.ident.to_string();
    match &segment.arguments {
        PathArguments::None => ident,
        PathArguments::AngleBracketed(args) => {
            let type_args = args
                .args
                .iter()
                .filter_map(|arg| match arg {
                    GenericArgument::Type(ty) => Some(canonical_type(ty)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if type_args.is_empty() {
                ident
            } else {
                format!("{ident}<{}>", type_args.join(","))
            }
        }
        PathArguments::Parenthesized(args) => {
            let inputs = args
                .inputs
                .iter()
                .map(canonical_type)
                .collect::<Vec<_>>()
                .join(",");
            match &args.output {
                syn::ReturnType::Default => format!("{ident}({inputs})"),
                syn::ReturnType::Type(_, output) => {
                    format!("{ident}({inputs})->{}", canonical_type(output))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_descriptor_sorts_named_fields() {
        let input: DeriveInput = syn::parse_quote! {
            struct Example<'a> {
                zed: Option<QueryRef<'a>>,
                alpha: Box<[u8]>,
            }
        };
        assert_eq!(
            descriptor_for(&input).expect("descriptor"),
            "Example{alpha:Box<[u8]>,zed:Option<QueryRef>}"
        );
    }

    #[test]
    fn enum_descriptor_keeps_variant_order() {
        let input: DeriveInput = syn::parse_quote! {
            enum Event<'a> {
                Unit,
                Named { value: &'a [u8] },
                Tuple(Box<str>),
            }
        };
        assert_eq!(
            descriptor_for(&input).expect("descriptor"),
            "Event[Unit,Named{value:&[u8]},Tuple(Box<str>)]"
        );
    }
}
