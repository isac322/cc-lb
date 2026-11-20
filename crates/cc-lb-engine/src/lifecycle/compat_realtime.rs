use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderValue};
use url::Url;

use crate::openai_compat::{self, CompatReject};
use crate::request_context::RequestContext;

use super::compat_request::PreparedCompatRequest;

/// Prepare an OAuth realtime call for the Codex public realtime endpoint.
///
/// The public SDP/multipart envelope is converted to the JSON shape expected by
/// the backend, while the OAuth endpoint and quicksilver routing metadata are
/// kept explicit and isolated from ordinary request preparation.
pub(super) async fn prepare_realtime_call(
    ctx: &RequestContext,
    codex_base: &Url,
) -> Result<PreparedCompatRequest, CompatReject> {
    let content_type = ctx
        .downstream_headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/sdp");
    let body =
        openai_compat::realtime::call_request_to_backend_json(content_type, ctx.body_bytes.clone())
            .await
            .map(Bytes::from)?;
    let mut url = codex_base.clone();
    url.set_path("/realtime/calls");
    url.set_query(Some(openai_compat::realtime::REALTIME_CALL_QUERY));
    Ok(PreparedCompatRequest {
        path: url.path().to_owned(),
        query: url.query().map(str::to_owned),
        body,
        response_plan: None,
    })
}

pub(super) fn realtime_call_headers() -> HeaderValue {
    HeaderValue::from_static(openai_compat::realtime::REALTIME_ALPHA_HEADER.1)
}
