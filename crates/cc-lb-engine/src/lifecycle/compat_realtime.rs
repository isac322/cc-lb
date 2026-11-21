use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderValue};

use crate::openai_compat::{self, CompatReject};
use crate::request_context::RequestContext;

use super::compat_request::PreparedCompatRequest;

/// Prepare an OAuth realtime call for the Codex public realtime endpoint.
///
/// The public SDP/multipart envelope is converted to the JSON shape expected by
/// the backend. The path stays the public `/v1/realtime/calls`, which the OAuth
/// dialect maps under the configured Codex base URL like every other operation;
/// only the quicksilver routing query is added.
pub(super) async fn prepare_realtime_call(
    ctx: &RequestContext,
) -> Result<PreparedCompatRequest, CompatReject> {
    let content_type = ctx
        .downstream_headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/sdp");
    let is_sdp = content_type
        .to_ascii_lowercase()
        .starts_with("application/sdp");
    let is_multipart = content_type
        .to_ascii_lowercase()
        .starts_with("multipart/form-data");
    let (query, body) = if is_sdp || is_multipart {
        (
            Some(openai_compat::realtime::REALTIME_CALL_QUERY.to_owned()),
            Bytes::from(
                openai_compat::realtime::call_request_to_backend_json(
                    content_type,
                    ctx.body_bytes.clone(),
                )
                .await?,
            ),
        )
    } else {
        (ctx.query.clone(), ctx.body_bytes.clone())
    };
    Ok(PreparedCompatRequest {
        path: ctx.path.clone(),
        query,
        body,
        response_plan: None,
    })
}

pub(super) fn realtime_call_headers() -> HeaderValue {
    HeaderValue::from_static(openai_compat::realtime::REALTIME_ALPHA_HEADER.1)
}
