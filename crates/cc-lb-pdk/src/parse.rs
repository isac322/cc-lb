use std::collections::BTreeSet;

use proc_macro2::TokenStream;
#[cfg(test)]
use quote::ToTokens;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Error, FnArg, GenericArgument, Ident, Item, ItemFn, ItemMod, LitInt, LitStr,
    PathArguments, Result, ReturnType, Token, Type, bracketed,
};

use cc_lb_plugin_wire::limits::{
    CAPABILITY_NAME_MAX_BYTES, CAPABILITY_PATTERN, PLUGIN_NAME_MAX_BYTES, PLUGIN_NAME_PATTERN,
    PLUGIN_VERSION_MAX_BYTES, VERSION_MAX, VERSION_MIN,
};

pub(crate) struct PluginArgs {
    pub(crate) name: LitStr,
    pub(crate) version: LitStr,
    pub(crate) requires: Vec<LitStr>,
}

pub(crate) struct HandlerArgs {
    pub(crate) name: LitStr,
    pub(crate) versions: Vec<LitInt>,
}

#[allow(dead_code)]
pub(crate) struct PluginDescriptor {
    pub(crate) plugin_name: String,
    pub(crate) plugin_version: String,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) handlers: Vec<HandlerDescriptor>,
}

#[allow(dead_code)]
pub(crate) struct HandlerDescriptor {
    pub(crate) name: String,
    pub(crate) versions: Vec<u32>,
    pub(crate) fn_ident: Ident,
    pub(crate) request_type: Type,
    pub(crate) response_type: Type,
}

impl Parse for PluginArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut version = None;
        let mut requires = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            if key == "name" {
                assign_once(&mut name, key, input.parse::<LitStr>()?)?;
            } else if key == "version" {
                assign_once(&mut version, key, input.parse::<LitStr>()?)?;
            } else if key == "requires" {
                assign_once(&mut requires, key, parse_lit_str_array(input)?)?;
            } else {
                return Err(Error::new(
                    key.span(),
                    "unknown #[plugin] argument; expected name, version, or requires",
                ));
            }

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(Self {
            name: required_field(name, input, "#[plugin] requires name = \"...\"")?,
            version: required_field(version, input, "#[plugin] requires version = \"...\"")?,
            requires: requires.unwrap_or_default(),
        })
    }
}

impl Parse for HandlerArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut versions = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            if key == "name" {
                assign_once(&mut name, key, input.parse::<LitStr>()?)?;
            } else if key == "versions" {
                assign_once(&mut versions, key, parse_lit_int_array(input)?)?;
            } else {
                return Err(Error::new(
                    key.span(),
                    "unknown #[handler] argument; expected name or versions",
                ));
            }

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(Self {
            name: required_field(name, input, "#[handler] requires name = \"...\"")?,
            versions: required_field(versions, input, "#[handler] requires versions = [...]")?,
        })
    }
}

pub(crate) fn parse_plugin_descriptor(
    args: &PluginArgs,
    module: &ItemMod,
) -> Result<PluginDescriptor> {
    let plugin_name = args.name.value();
    let plugin_version = args.version.value();

    validate_plugin_name(&args.name, &plugin_name)?;
    validate_plugin_version(&args.version, &plugin_version)?;
    let required_capabilities = validate_required_capabilities(&args.requires)?;

    let Some((_, items)) = &module.content else {
        return Err(Error::new_spanned(
            module,
            "#[plugin] must be placed on an inline module so #[handler] functions can be discovered",
        ));
    };

    let mut handlers = Vec::new();
    let mut error = None;
    collect_handlers(items, &mut handlers, &mut error);

    if let Some(error) = error {
        return Err(error);
    }

    if handlers.is_empty() {
        return Err(Error::new_spanned(
            &module.ident,
            "#[plugin] module must contain at least one function annotated with #[handler(...)]",
        ));
    }

    reject_duplicate_handler_names(&handlers)?;

    Ok(PluginDescriptor {
        plugin_name,
        plugin_version,
        required_capabilities,
        handlers,
    })
}

pub(crate) fn parse_handler_descriptor(
    args: &HandlerArgs,
    item_fn: &ItemFn,
) -> Result<HandlerDescriptor> {
    descriptor_from_handler_args(args, item_fn)
}

pub(crate) fn compile_error(error: Error) -> TokenStream {
    error.to_compile_error()
}

fn collect_handlers(
    items: &[Item],
    handlers: &mut Vec<HandlerDescriptor>,
    error: &mut Option<Error>,
) {
    for item in items {
        match item {
            Item::Fn(item_fn) => collect_function_handler(item_fn, handlers, error),
            Item::Mod(item_mod) => {
                if let Some((_, nested_items)) = &item_mod.content {
                    collect_handlers(nested_items, handlers, error);
                }
            }
            _ => {}
        }
    }
}

fn collect_function_handler(
    item_fn: &ItemFn,
    handlers: &mut Vec<HandlerDescriptor>,
    error: &mut Option<Error>,
) {
    let handler_attrs: Vec<_> = item_fn
        .attrs
        .iter()
        .filter(|attr| is_handler_attr(attr))
        .collect();

    if handler_attrs.is_empty() {
        return;
    }

    if handler_attrs.len() > 1 {
        combine_error(
            error,
            Error::new_spanned(
                &item_fn.sig.ident,
                "handler functions may only have one #[handler(...)] attribute",
            ),
        );
        return;
    }

    let attr = handler_attrs[0];
    match attr.parse_args::<HandlerArgs>() {
        Ok(args) => match descriptor_from_handler_args(&args, item_fn) {
            Ok(descriptor) => handlers.push(descriptor),
            Err(next_error) => combine_error(error, next_error),
        },
        Err(next_error) => combine_error(error, next_error),
    }
}

fn descriptor_from_handler_args(args: &HandlerArgs, item_fn: &ItemFn) -> Result<HandlerDescriptor> {
    let name = args.name.value();
    validate_handler_name(&args.name, &name)?;
    let versions = validate_versions(&args.versions)?;
    let (request_type, response_type) = extract_handler_types(item_fn)?;

    Ok(HandlerDescriptor {
        name,
        versions,
        fn_ident: item_fn.sig.ident.clone(),
        request_type,
        response_type,
    })
}

fn extract_handler_types(item_fn: &ItemFn) -> Result<(Type, Type)> {
    if item_fn.sig.inputs.len() != 1 {
        return Err(Error::new_spanned(
            &item_fn.sig.inputs,
            "#[handler] functions must accept exactly one request argument",
        ));
    }

    let request_type = match item_fn.sig.inputs.first().expect("length checked") {
        FnArg::Typed(arg) => (*arg.ty).clone(),
        FnArg::Receiver(receiver) => {
            return Err(Error::new_spanned(
                receiver,
                "#[handler] functions must be free functions without self receivers",
            ));
        }
    };

    let response_type = match &item_fn.sig.output {
        ReturnType::Type(_, ty) => unwrap_response_type(ty)?,
        ReturnType::Default => {
            return Err(Error::new_spanned(
                &item_fn.sig.ident,
                "#[handler] functions must return a response type",
            ));
        }
    };

    Ok((request_type, response_type))
}

fn unwrap_response_type(ty: &Type) -> Result<Type> {
    let Type::Path(type_path) = ty else {
        return Ok(ty.clone());
    };

    let Some(segment) = type_path.path.segments.last() else {
        return Ok(ty.clone());
    };

    if segment.ident != "Result" && segment.ident != "FnResult" {
        return Ok(ty.clone());
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(Error::new_spanned(
            ty,
            "Result/FnResult handler returns must specify a response type parameter",
        ));
    };

    for arg in &args.args {
        if let GenericArgument::Type(response_type) = arg {
            return Ok(response_type.clone());
        }
    }

    Err(Error::new_spanned(
        ty,
        "Result/FnResult handler returns must include a response type parameter",
    ))
}

fn is_handler_attr(attr: &Attribute) -> bool {
    attr.path()
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "handler")
}

fn parse_lit_str_array(input: ParseStream<'_>) -> Result<Vec<LitStr>> {
    let content;
    bracketed!(content in input);
    let values = Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?;
    Ok(values.into_iter().collect())
}

fn parse_lit_int_array(input: ParseStream<'_>) -> Result<Vec<LitInt>> {
    let content;
    bracketed!(content in input);
    let values = Punctuated::<LitInt, Token![,]>::parse_terminated(&content)?;
    Ok(values.into_iter().collect())
}

fn assign_once<T>(slot: &mut Option<T>, key: Ident, value: T) -> Result<()> {
    if slot.is_some() {
        return Err(Error::new(
            key.span(),
            format!("duplicate `{key}` argument"),
        ));
    }
    *slot = Some(value);
    Ok(())
}

fn required_field<T>(slot: Option<T>, input: ParseStream<'_>, message: &str) -> Result<T> {
    slot.ok_or_else(|| Error::new(input.span(), message))
}

fn validate_plugin_name(lit: &LitStr, name: &str) -> Result<()> {
    if name.len() > PLUGIN_NAME_MAX_BYTES || !matches_plugin_name(name) {
        return Err(Error::new_spanned(
            lit,
            format!(
                "plugin name must match {PLUGIN_NAME_PATTERN} and be at most {PLUGIN_NAME_MAX_BYTES} bytes"
            ),
        ));
    }
    Ok(())
}

fn validate_plugin_version(lit: &LitStr, version: &str) -> Result<()> {
    if version.is_empty() {
        return Err(Error::new_spanned(lit, "plugin version must not be empty"));
    }
    if version.len() > PLUGIN_VERSION_MAX_BYTES {
        return Err(Error::new_spanned(
            lit,
            format!("plugin version must be at most {PLUGIN_VERSION_MAX_BYTES} bytes"),
        ));
    }
    Ok(())
}

fn validate_required_capabilities(requires: &[LitStr]) -> Result<Vec<String>> {
    let mut seen = BTreeSet::new();
    let mut values = Vec::new();

    for lit in requires {
        let value = lit.value();
        if value.len() > CAPABILITY_NAME_MAX_BYTES || !matches_capability_name(&value) {
            return Err(Error::new_spanned(
                lit,
                format!(
                    "required capability must match {CAPABILITY_PATTERN} and be at most {CAPABILITY_NAME_MAX_BYTES} bytes"
                ),
            ));
        }
        if !seen.insert(value.clone()) {
            return Err(Error::new_spanned(lit, "duplicate required capability"));
        }
        values.push(value);
    }

    Ok(values)
}

fn validate_handler_name(lit: &LitStr, name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::new_spanned(lit, "handler name must not be empty"));
    }
    Ok(())
}

fn validate_versions(versions: &[LitInt]) -> Result<Vec<u32>> {
    if versions.is_empty() {
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            "handler versions must include at least one version",
        ));
    }

    let mut seen = BTreeSet::new();
    let mut parsed = Vec::new();

    for version in versions {
        let value = version.base10_parse::<u32>()?;
        if !(VERSION_MIN..=VERSION_MAX).contains(&value) {
            return Err(Error::new_spanned(
                version,
                format!("handler versions must be between {VERSION_MIN} and {VERSION_MAX}"),
            ));
        }
        if !seen.insert(value) {
            return Err(Error::new_spanned(version, "duplicate handler version"));
        }
        parsed.push(value);
    }

    Ok(parsed)
}

fn reject_duplicate_handler_names(handlers: &[HandlerDescriptor]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for handler in handlers {
        if !seen.insert(handler.name.clone()) {
            return Err(Error::new_spanned(
                &handler.fn_ident,
                format!("duplicate handler name `{}`", handler.name),
            ));
        }
    }
    Ok(())
}

fn matches_plugin_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    first.is_ascii_lowercase()
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
}

fn matches_capability_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    first.is_ascii_lowercase()
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn combine_error(error: &mut Option<Error>, next_error: Error) {
    if let Some(error) = error {
        error.combine(next_error);
    } else {
        *error = Some(next_error);
    }
}

#[cfg(test)]
fn type_to_string(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::{parse_quote, parse_str};

    #[test]
    fn plugin_args_basic() {
        let args: PluginArgs = parse_str(r#"name = "sample-router", version = "1.2.3""#).unwrap();

        assert_eq!(args.name.value(), "sample-router");
        assert_eq!(args.version.value(), "1.2.3");
        assert!(args.requires.is_empty());
    }

    #[test]
    fn plugin_args_requires_array() {
        let args: PluginArgs =
            parse_str(r#"name = "sample-router", version = "1.2.3", requires = ["log", "clock"]"#)
                .unwrap();

        assert_eq!(
            args.requires.iter().map(LitStr::value).collect::<Vec<_>>(),
            ["log", "clock"]
        );
    }

    #[test]
    fn reject_unknown_attr() {
        let error = match parse_str::<PluginArgs>(r#"name = "p", version = "1", kind = "router""#) {
            Ok(_) => panic!("unknown plugin argument unexpectedly parsed"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("unknown #[plugin] argument"));
    }

    #[test]
    fn handler_versions_array() {
        let args: HandlerArgs = parse_str(r#"name = "route", versions = [1, 2, 3]"#).unwrap();
        let versions = validate_versions(&args.versions).unwrap();

        assert_eq!(args.name.value(), "route");
        assert_eq!(versions, [1, 2, 3]);
    }

    #[test]
    fn rejects_empty_handler_versions() {
        let args: HandlerArgs = parse_str(r#"name = "route", versions = []"#).unwrap();
        let error = validate_versions(&args.versions).unwrap_err();

        assert!(error.to_string().contains("at least one version"));
    }

    #[test]
    fn rejects_duplicate_plugin_args() {
        let error = match parse_str::<PluginArgs>(r#"name = "p", name = "q", version = "1""#) {
            Ok(_) => panic!("duplicate plugin argument unexpectedly parsed"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("duplicate `name`"));
    }

    #[test]
    fn plugin_descriptor_extracts_handler_metadata() {
        let args: PluginArgs =
            parse_str(r#"name = "sample-router", version = "1.0.0", requires = ["log"]"#).unwrap();
        let module: ItemMod = parse_quote! {
            mod plugin {
                #[handler(name = "route", versions = [1])]
                pub fn route(
                    request: cc_lb_plugin_wire::v1::route::RouteRequest
                ) -> extism_pdk::FnResult<cc_lb_plugin_wire::v1::route::RouteResponse> {
                    todo!()
                }
            }
        };

        let descriptor = parse_plugin_descriptor(&args, &module).unwrap();

        assert_eq!(descriptor.plugin_name, "sample-router");
        assert_eq!(descriptor.plugin_version, "1.0.0");
        assert_eq!(descriptor.required_capabilities, ["log"]);
        assert_eq!(descriptor.handlers.len(), 1);
        assert_eq!(descriptor.handlers[0].name, "route");
        assert_eq!(descriptor.handlers[0].versions, [1]);
        assert_eq!(descriptor.handlers[0].fn_ident, "route");
        assert_eq!(
            type_to_string(&descriptor.handlers[0].request_type),
            "cc_lb_plugin_wire :: v1 :: route :: RouteRequest"
        );
        assert_eq!(
            type_to_string(&descriptor.handlers[0].response_type),
            "cc_lb_plugin_wire :: v1 :: route :: RouteResponse"
        );
    }

    #[test]
    fn plugin_descriptor_accepts_qualified_handler_attribute() {
        let args: PluginArgs = parse_str(r#"name = "sample-router", version = "1.0.0""#).unwrap();
        let module: ItemMod = parse_quote! {
            mod plugin {
                #[cc_lb_pdk::handler(name = "shape", versions = [1])]
                pub fn shape(request: ShapeRequest) -> Result<ShapeResponse, String> {
                    todo!()
                }
            }
        };

        let descriptor = parse_plugin_descriptor(&args, &module).unwrap();

        assert_eq!(descriptor.handlers[0].name, "shape");
        assert_eq!(
            type_to_string(&descriptor.handlers[0].response_type),
            "ShapeResponse"
        );
    }

    #[test]
    fn rejects_module_without_handlers() {
        let args: PluginArgs = parse_str(r#"name = "sample-router", version = "1.0.0""#).unwrap();
        let module: ItemMod = parse_quote! {
            mod plugin {
                pub fn helper() {}
            }
        };

        let error = match parse_plugin_descriptor(&args, &module) {
            Ok(_) => panic!("module without handlers unexpectedly parsed"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("at least one function"));
    }

    #[test]
    fn rejects_handler_without_response_type() {
        let args: PluginArgs = parse_str(r#"name = "sample-router", version = "1.0.0""#).unwrap();
        let module: ItemMod = parse_quote! {
            mod plugin {
                #[handler(name = "route", versions = [1])]
                pub fn route(request: RouteRequest) {}
            }
        };

        let error = match parse_plugin_descriptor(&args, &module) {
            Ok(_) => panic!("handler without response unexpectedly parsed"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("must return a response type"));
    }

    #[test]
    fn compile_error_tokens_include_compile_error_macro() {
        let tokens = compile_error(Error::new(
            proc_macro2::Span::call_site(),
            "bad plugin attribute",
        ));

        assert!(tokens.to_string().contains("compile_error"));
    }
}
