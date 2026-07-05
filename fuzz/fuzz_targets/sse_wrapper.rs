#![no_main]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{SseBatchConfig, SseRelay};
use cc_lb_plugin_api::{
    DialectError, ObservabilityError, ObservabilityHook, ObserveEvent, Principal, RequestContext,
    ShapedRequest, ShapedRequestBuilder, Upstream, UpstreamDialect,
};
use http::StatusCode;
use http_body_util::BodyExt;
use libfuzzer_sys::fuzz_target;
use tokio::runtime::{Builder, Runtime};

fuzz_target!(|data: &[u8]| {
    let Some(runtime) = runtime() else {
        return;
    };
    runtime.block_on(async {
        let response = relay().into_response_from_body(Body::from(Bytes::copy_from_slice(data)));
        let _result = response.into_body().collect().await;
    });
});

fn runtime() -> Option<&'static Runtime> {
    static RUNTIME: OnceLock<Option<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| Builder::new_current_thread().enable_time().build().ok())
        .as_ref()
}

fn relay() -> SseRelay {
    SseRelay {
        obs: Arc::new(NoopHook),
        dialect: Arc::new(NoopDialect),
        batch: SseBatchConfig {
            max_events: 8,
            max_age: Duration::from_secs(60),
        },
        quota: None,
        principal_id: "fuzz-principal".to_owned(),
        reservation: None,
        error_normalizer: None,
        upstream_kind: None,
    }
}

struct NoopHook;

impl ObservabilityHook for NoopHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}

struct NoopDialect;

impl UpstreamDialect for NoopDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        _builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Err(DialectError::UnsupportedRequest {
            reason: "fuzz target exercises SSE relay only".to_owned(),
        })
    }
}
