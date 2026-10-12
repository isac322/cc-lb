use axum::{
    http::{HeaderName, HeaderValue},
    response::Response,
};

pub(crate) mod api_key;
pub mod keys;
pub(crate) mod limit_resets;
pub mod oauth;
pub mod plugins;
pub mod plugins_wasm;
pub(crate) mod post_commit;
pub mod principal_cache_keepalive;
pub mod principals;
pub mod router;
pub mod status;
pub(crate) mod upstream_warmup;
pub mod upstreams;
mod wasm_cache;

use crate::AdminState;

pub const CC_LB_GENERATION_HEADER: HeaderName = HeaderName::from_static("x-cc-lb-generation");
pub const CC_LB_REBIND_STATUS_HEADER: HeaderName = HeaderName::from_static("x-cc-lb-rebind-status");

pub async fn apply_dynamic_view_after_mutation(state: &AdminState) -> anyhow::Result<u64> {
    let rebinder = state
        .dynamic_view_rebinder
        .clone()
        .ok_or_else(|| anyhow::anyhow!("dynamic view rebinder unavailable"))?;
    // Follow the holder's rebuild protocol: hold the rebuild lock across
    // the resident-generation read, the build, and the install so no other
    // live publisher can reuse this generation or replace this mutation's
    // view with an older build. The rebinder itself is build-only and must
    // not take the lock.
    let _rebuild_guard = state.dynamic_view.lock_rebuild().await;
    let current_generation = state.dynamic_view.generation();
    let view = rebinder.rebuild_dynamic_view(current_generation).await?;
    let attempted_generation = view.generation;
    // Defensive CAS on generation. With every live publisher following the
    // rebuild protocol, rejection only happens on generation saturation or
    // an out-of-protocol publication; report whichever generation is now
    // authoritative.
    if state.dynamic_view.try_store_if_newer(view) {
        Ok(attempted_generation)
    } else {
        Ok(state.dynamic_view.generation())
    }
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
