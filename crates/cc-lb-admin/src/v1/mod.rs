use axum::{
    http::{HeaderName, HeaderValue},
    response::Response,
};

pub mod keys;
pub mod oauth;
pub mod plugins;
pub mod plugins_wasm;
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

pub struct AdminCtx<'a> {
    pub state: &'a AdminState,
    pub scheduler: &'a cc_lb_scheduler::admin::SchedulerAdminHandle,
}

impl<'a> AdminCtx<'a> {
    pub fn from_state(state: &'a AdminState) -> anyhow::Result<Self> {
        let scheduler = state
            .scheduler
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("scheduler backend unavailable"))?;
        Ok(Self { state, scheduler })
    }
}

pub fn admin_ctx(state: &AdminState) -> anyhow::Result<AdminCtx<'_>> {
    AdminCtx::from_state(state)
}

pub async fn apply_dynamic_view_after_mutation(state: &AdminState) -> anyhow::Result<u64> {
    let rebinder = state
        .dynamic_view_rebinder
        .clone()
        .ok_or_else(|| anyhow::anyhow!("dynamic view rebinder unavailable"))?;
    let current_generation = state.dynamic_view.generation();
    let view = rebinder.rebuild_dynamic_view(current_generation).await?;
    let attempted_generation = view.generation;
    // CAS on generation so a slower reconcile / notify rebind can't
    // overwrite this admin-triggered rebuild. If a newer generation
    // is already resident (e.g. a concurrent admin mutation) the
    // config is still applied; return whichever generation is now
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
