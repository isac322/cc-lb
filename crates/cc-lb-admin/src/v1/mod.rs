use axum::{
    http::{HeaderName, HeaderValue},
    response::Response,
};

pub mod oauth;
pub mod plugins;
pub mod plugins_wasm;
pub mod principals;
pub mod status;
pub mod upstreams;

use crate::AdminState;

pub const CC_LB_GENERATION_HEADER: HeaderName = HeaderName::from_static("x-cc-lb-generation");
pub const CC_LB_REBIND_STATUS_HEADER: HeaderName = HeaderName::from_static("x-cc-lb-rebind-status");

pub async fn apply_dynamic_view_after_mutation(state: &AdminState) -> anyhow::Result<u64> {
    let rebinder = state
        .config
        .dynamic_view_rebinder()
        .ok_or_else(|| anyhow::anyhow!("dynamic view rebinder unavailable"))?;
    let current_generation = state.dynamic_view.generation();
    let view = rebinder.rebuild_dynamic_view(current_generation).await?;
    let generation = view.generation;
    state.dynamic_view.store(view);
    Ok(generation)
}

pub async fn add_dynamic_rebind_headers(response: &mut Response, state: &AdminState) {
    match apply_dynamic_view_after_mutation(state).await {
        Ok(generation) => {
            if let Ok(value) = HeaderValue::from_str(&generation.to_string()) {
                response
                    .headers_mut()
                    .insert(CC_LB_GENERATION_HEADER, value);
            }
        }
        Err(error) => {
            tracing::error!(error = %error, "admin v1 dynamic view rebind deferred after mutation");
            response.headers_mut().insert(
                CC_LB_REBIND_STATUS_HEADER,
                HeaderValue::from_static("deferred"),
            );
        }
    }
}
