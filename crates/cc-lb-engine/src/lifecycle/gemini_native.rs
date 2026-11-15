use super::{Body, RequestContext, Response, StorageUpstreamKind};

pub(super) const JSON_SSE_FRAMING_ERROR_MESSAGE: &str =
    "upstream Gemini SSE response used application/json instead of text/event-stream";

pub(super) fn rejects_json_success(
    context: &RequestContext,
    upstream_kind: StorageUpstreamKind,
    response: &Response<Body>,
) -> bool {
    upstream_kind == StorageUpstreamKind::GeminiApiKey
        && response.status().is_success()
        && requests_sse(&context.path, context.query.as_deref())
        && response
            .headers()
            .get(super::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn requests_sse(path: &str, query: Option<&str>) -> bool {
    path.ends_with(":streamGenerateContent")
        && query.is_some_and(|query| query.split('&').any(|pair| pair == "alt=sse"))
}

#[cfg(test)]
mod tests {
    use super::requests_sse;

    #[test]
    fn native_sse_requires_exact_action_and_query_pair() {
        let stream_path = "/v1beta/models/gemini-test:streamGenerateContent";
        for query in ["alt=sse", "key=ignored&alt=sse", "alt=sse&other=value"] {
            assert!(requests_sse(stream_path, Some(query)), "{query}");
        }
        for query in [
            None,
            Some("alt=json"),
            Some("other=alt=sse"),
            Some("alt=sse-extra"),
            Some("alt=sse=extra"),
            Some("alt=%73se"),
            Some("Alt=sse"),
        ] {
            assert!(!requests_sse(stream_path, query), "{query:?}");
        }
        assert!(!requests_sse(
            "/v1beta/models/gemini-test:generateContent",
            Some("alt=sse"),
        ));
        assert!(!requests_sse(
            "/v1beta/models/gemini-test:streamGenerateContent/extra",
            Some("alt=sse"),
        ));
    }
}
