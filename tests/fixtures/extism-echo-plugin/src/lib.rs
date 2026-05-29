use extism_pdk::{FnResult, plugin_fn};

#[plugin_fn]
pub fn route(input: String) -> FnResult<String> {
    Ok(input)
}
