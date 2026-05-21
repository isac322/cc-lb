use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect,
};
use http::StatusCode;
use url::Url;

const MANTLE_PATH_PREFIX: &str = "/anthropic";

#[derive(Clone, Debug, Default)]
pub struct BedrockMantleDialect {
    base_url: Option<Url>,
}

impl BedrockMantleDialect {
    pub fn with_base_url(base_url: Option<Url>) -> Self {
        Self { base_url }
    }
}

impl UpstreamDialect for BedrockMantleDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let Upstream::BedrockMantle { region } = upstream else {
            return Err(DialectError::UnsupportedRequest {
                reason: "BedrockMantleDialect requires Upstream::BedrockMantle".to_owned(),
            });
        };

        let base = match &self.base_url {
            Some(base_url) => base_url.clone(),
            None => {
                let mut base =
                    String::with_capacity("https://bedrock-mantle..api.aws".len() + region.len());
                base.push_str("https://bedrock-mantle.");
                base.push_str(region);
                base.push_str(".api.aws");
                Url::parse(&base).map_err(|source| DialectError::InvalidUrl { source })?
            }
        };
        let url = mantle_url(&base, &ctx.path, ctx.query.as_deref())?;
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

fn mantle_url(
    base_url: &Url,
    downstream_path: &str,
    query: Option<&str>,
) -> Result<Url, DialectError> {
    let mut url = base_url.clone();
    let downstream_path = downstream_path.trim_start_matches('/');
    let path = if downstream_path.is_empty() {
        MANTLE_PATH_PREFIX.to_owned()
    } else {
        let mut path = String::with_capacity(MANTLE_PATH_PREFIX.len() + 1 + downstream_path.len());
        path.push_str(MANTLE_PATH_PREFIX);
        path.push('/');
        path.push_str(downstream_path);
        path
    };
    url.set_path(&path);
    url.set_query(query);
    Ok(url)
}
