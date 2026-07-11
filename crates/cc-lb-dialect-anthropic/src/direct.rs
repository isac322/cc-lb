use cc_lb_domain::{Principal, Upstream};
use cc_lb_plugin_api::{
    DialectError, RequestContext, ShapedRequest, ShapedRequestBuilder, UpstreamDialect,
};
use url::Url;

use crate::{ANTHROPIC_API_BASE_URL, compose_url};

/// Passthrough dialect for the official Anthropic API.
#[derive(Clone, Debug, Default)]
pub struct AnthropicDirectDialect {
    base_url: Option<Url>,
}

impl AnthropicDirectDialect {
    pub fn with_base_url(base_url: Option<Url>) -> Self {
        Self { base_url }
    }
}

impl UpstreamDialect for AnthropicDirectDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let upstream_base_url = match upstream {
            Upstream::AnthropicDirect { base_url } => base_url.as_ref(),
        };

        let base_url = match upstream_base_url.or(self.base_url.as_ref()) {
            Some(base_url) => base_url.clone(),
            None => Url::parse(ANTHROPIC_API_BASE_URL)
                .map_err(|source| DialectError::InvalidUrl { source })?,
        };
        let url = compose_url(&base_url, &ctx.path, ctx.query.as_deref());
        Ok(builder.shaped_request(
            url,
            ctx.method.clone(),
            ctx.downstream_headers.clone(),
            ctx.body_bytes.clone(),
        ))
    }
}
