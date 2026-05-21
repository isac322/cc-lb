use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect,
};
use http::StatusCode;
use url::Url;

use crate::{compose_url, ANTHROPIC_API_BASE_URL};

/// Passthrough dialect for the official Anthropic API.
#[derive(Clone, Debug, Default)]
pub struct AnthropicDirectDialect;

impl UpstreamDialect for AnthropicDirectDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        if !matches!(upstream, Upstream::AnthropicDirect) {
            return Err(DialectError::UnsupportedRequest {
                reason: "AnthropicDirectDialect requires Upstream::AnthropicDirect".to_owned(),
            });
        }

        let base_url = Url::parse(ANTHROPIC_API_BASE_URL)
            .map_err(|source| DialectError::InvalidUrl { source })?;
        let url = compose_url(&base_url, &ctx.path, ctx.query.as_deref());
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
