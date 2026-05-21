use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect,
};
use http::StatusCode;

use crate::compose_url;

/// Passthrough dialect for custom gateways that already speak Anthropic wire shape.
#[derive(Clone, Debug, Default)]
pub struct CustomAnthropicSpecDialect;

impl UpstreamDialect for CustomAnthropicSpecDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let Upstream::CustomAnthropicSpec { base_url } = upstream else {
            return Err(DialectError::UnsupportedRequest {
                reason: "CustomAnthropicSpecDialect requires Upstream::CustomAnthropicSpec"
                    .to_owned(),
            });
        };

        let url = compose_url(base_url, &ctx.path, ctx.query.as_deref());
        Ok(builder.shaped_request(
            url,
            ctx.method.clone(),
            ctx.downstream_headers.clone(),
            ctx.body_bytes.clone(),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}
