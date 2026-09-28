#![no_main]

use std::sync::{Arc, OnceLock};

use axum::body::Body;
use bytes::Bytes;
use cc_lb_domain::{Principal, Upstream};
use cc_lb_engine::{RequestContext, SseRelay};
use cc_lb_upstream::{DialectError, ShapedRequest, ShapedRequestBuilder, UpstreamDialect};
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
        dialect: Arc::new(NoopDialect),
        quota: None,
        principal_id: "fuzz-principal".to_owned(),
        reservation: None,
        error_normalizer: None,
        upstream_kind: None,
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
