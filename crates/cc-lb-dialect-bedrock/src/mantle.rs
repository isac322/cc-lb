use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect,
};
use http::StatusCode;
use url::Url;

const MANTLE_PATH_PREFIX: &str = "/anthropic";

#[derive(Clone, Debug, Default)]
pub struct BedrockMantleDialect;

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

        let base = format!("https://bedrock-mantle.{region}.api.aws");
        let base = Url::parse(&base).map_err(|source| DialectError::InvalidUrl { source })?;
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
        format!("{MANTLE_PATH_PREFIX}/{downstream_path}")
    };
    url.set_path(&path);
    url.set_query(query);
    Ok(url)
}
