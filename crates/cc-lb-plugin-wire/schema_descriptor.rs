use std::collections::{BTreeMap, BTreeSet};

use quote::ToTokens;
use syn::{Fields, GenericArgument, Item, ItemStruct, PathArguments, Type};

/// Describe a root and every struct reachable from its field types. Declaration
/// order and rkyv layout attributes matter; whitespace and docs do not.
pub fn describe(sources: &[&str], root: &str) -> String {
    let mut structs = BTreeMap::new();
    let mut unsupported = BTreeSet::new();
    for source in sources {
        let file = syn::parse_file(source).expect("parse wire declarations");
        validate_attributes(&file.attrs);
        for item in file.items {
            match item {
                Item::Use(item) => {
                    validate_attributes(&item.attrs);
                    validate_import(&item.tree, "");
                }
                Item::Mod(_) | Item::ExternCrate(_) => panic!("unsupported wire source namespace"),
                Item::Struct(item) => {
                    structs.insert(item.ident.to_string(), item);
                }
                Item::Enum(item) => {
                    unsupported.insert(item.ident.to_string());
                }
                Item::Type(item) => {
                    unsupported.insert(item.ident.to_string());
                }
                Item::Union(item) => {
                    unsupported.insert(item.ident.to_string());
                }
                _ => {}
            }
        }
    }
    assert!(
        !structs.contains_key("InlineAsBox") && !unsupported.contains("InlineAsBox"),
        "wire archive adapter must use the canonical rkyv import"
    );
    let mut pending = vec![root.to_owned()];
    let mut visited = BTreeSet::new();
    let mut descriptors = BTreeMap::new();
    while let Some(name) = pending.pop() {
        if !visited.insert(name.clone()) {
            continue;
        }
        let item = structs.get(&name).expect("wire dependency exists");
        let Fields::Named(fields) = &item.fields else {
            panic!("wire structs must have named fields")
        };
        let derives_archive = validate_attributes(&item.attrs);
        assert!(
            derives_archive,
            "reachable wire structs require the canonical Archive derive"
        );
        assert!(
            item.generics
                .params
                .iter()
                .all(|param| matches!(param, syn::GenericParam::Lifetime(_)))
                && item.generics.where_clause.is_none(),
            "unsupported wire generics"
        );
        for field in &fields.named {
            validate_attributes(&field.attrs);
            visit_type(&field.ty, &structs, &unsupported, &mut pending);
        }
        descriptors.insert(name, struct_descriptor(item));
    }
    let mut output = descriptors.remove(root).expect("root descriptor exists");
    for descriptor in descriptors.values() {
        output.push(';');
        output.push_str(descriptor);
    }
    output
}

fn compact(tokens: impl ToTokens) -> String {
    tokens
        .to_token_stream()
        .to_string()
        .split_whitespace()
        .collect()
}

fn struct_descriptor(item: &ItemStruct) -> String {
    let mut descriptor = item.ident.to_string();
    for attribute in &item.attrs {
        if attribute.path().is_ident("rkyv") {
            descriptor.push_str(&compact(attribute));
        }
    }
    descriptor.push('{');
    for field in &item.fields {
        for attribute in &field.attrs {
            if attribute.path().is_ident("rkyv") {
                descriptor.push_str(&compact(attribute));
            }
        }
        descriptor.push_str(&field.ident.as_ref().expect("named wire field").to_string());
        descriptor.push(':');
        descriptor.push_str(&compact(&field.ty));
        descriptor.push(',');
    }
    descriptor.push('}');
    descriptor
}

// This deliberately supports only the current wire graph. New layout constructs
// must be reviewed and explicitly supported before their fingerprint is trusted.
// Return whether these validated attributes derive the canonical Archive.
fn validate_attributes(attrs: &[syn::Attribute]) -> bool {
    let mut derives_archive = false;
    for attr in attrs {
        assert!(
            attr.path().is_ident("doc")
                || attr.path().is_ident("derive")
                || attr.path().is_ident("rkyv"),
            "unsupported wire layout attribute: {}",
            compact(attr)
        );
        if attr.path().is_ident("derive") {
            let derives = attr
                .parse_args_with(
                    syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
                )
                .expect("parse wire derives");
            assert!(
                derives
                    .iter()
                    .all(|path| path.get_ident().is_some_and(|ident| matches!(
                        ident.to_string().as_str(),
                        "Archive" | "Serialize" | "Deserialize" | "Clone" | "Debug"
                    ))),
                "unsupported wire derive"
            );
            derives_archive |= derives.iter().any(|path| path.is_ident("Archive"));
        }
        if attr.path().is_ident("rkyv") {
            let tokens = compact(attr);
            assert!(
                matches!(
                    tokens.as_str(),
                    "#[rkyv(derive(Debug))]" | "#[rkyv(with=InlineAsBox)]"
                ),
                "unsupported archive layout attribute: {tokens}"
            );
        }
    }
    derives_archive
}

fn visit_type(
    ty: &Type,
    structs: &BTreeMap<String, ItemStruct>,
    unsupported: &BTreeSet<String>,
    pending: &mut Vec<String>,
) {
    match ty {
        Type::Reference(ty) if ty.mutability.is_none() => {
            visit_type(&ty.elem, structs, unsupported, pending)
        }
        Type::Slice(ty) => visit_type(&ty.elem, structs, unsupported, pending),
        Type::Path(ty)
            if ty.qself.is_none()
                && ty.path.leading_colon.is_none()
                && ty.path.segments.len() == 1 =>
        {
            let segment = &ty.path.segments[0];
            let name = segment.ident.to_string();
            assert!(
                !unsupported.contains(&name),
                "unsupported reachable wire declaration: {name}"
            );
            if structs.contains_key(&name) {
                pending.push(name);
                match &segment.arguments {
                    PathArguments::None => {}
                    PathArguments::AngleBracketed(args)
                        if args
                            .args
                            .iter()
                            .all(|arg| matches!(arg, GenericArgument::Lifetime(_))) => {}
                    _ => panic!("unsupported wire struct arguments"),
                }
            } else if matches!(name.as_str(), "Box" | "Option") {
                let PathArguments::AngleBracketed(args) = &segment.arguments else {
                    panic!("wire container needs one type")
                };
                assert_eq!(args.args.len(), 1, "wire container needs one type");
                let GenericArgument::Type(inner) = &args.args[0] else {
                    panic!("unsupported wire container argument")
                };
                visit_type(inner, structs, unsupported, pending);
            } else {
                assert!(
                    matches!(
                        name.as_str(),
                        "str"
                            | "bool"
                            | "u8"
                            | "u16"
                            | "u32"
                            | "u64"
                            | "i8"
                            | "i16"
                            | "i32"
                            | "i64"
                            | "f32"
                            | "f64"
                    ) && matches!(segment.arguments, PathArguments::None),
                    "unsupported wire type: {name}"
                );
            }
        }
        _ => panic!("unsupported wire type: {}", compact(ty)),
    }
}

// Imports are intentionally restricted to the bindings used by the current
// graph. A redirected Box or InlineAsBox would otherwise retain the same text
// descriptor while changing its archived layout.
fn validate_import(tree: &syn::UseTree, prefix: &str) {
    match tree {
        syn::UseTree::Path(path) => {
            validate_import(&path.tree, &format!("{prefix}{}::", path.ident))
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                validate_import(item, prefix);
            }
        }
        syn::UseTree::Name(name) => {
            let path = format!("{prefix}{}", name.ident);
            assert!(
                matches!(
                    path.as_str(),
                    "alloc::boxed::Box"
                        | "rkyv::with::InlineAsBox"
                        | "rkyv::Archive"
                        | "rkyv::Deserialize"
                        | "rkyv::Serialize"
                        | "crate::v1::ArchivedFilterResponse"
                        | "crate::v1::FilterResponse"
                        | "crate::v1::PerCandidateReason"
                        | "crate::v1::CachePricingSummary"
                        | "crate::v1::CachePricingSummaryRef"
                        | "crate::v1::Header"
                        | "crate::v1::HeaderRef"
                        | "crate::v1::Principal"
                        | "crate::v1::PrincipalRef"
                        | "crate::v1::QueryRef"
                ),
                "unsupported wire import: {path}"
            );
        }
        _ => panic!("unsupported wire import binding"),
    }
}
