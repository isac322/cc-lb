use cc_lb_domain::{Principal, Upstream};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, UpstreamDialect,
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
        context: &DialectShapeContext,
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
        let url = compose_url(&base_url, &context.path, context.query.as_deref());
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}
