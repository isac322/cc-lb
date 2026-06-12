use extism_pdk::{FnResult, plugin_fn};

#[plugin_fn]
pub fn filter(input: String) -> FnResult<String> {
    Ok(input)
}
