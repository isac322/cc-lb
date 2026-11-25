use std::io::Read;

use axum::body::Body;
use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
use http::{HeaderMap, Method, Request, Response, StatusCode};

const INVALID_REQUEST_BODY_MESSAGE: &str = "invalid request body encoding";
/// RFC 9659 §3 limits the HTTP zstd decoder window to 8 MiB (`2^23`).
const ZSTD_WINDOW_LOG_MAX: u32 = 23;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ResponsesRequestBodyError {
    TooLarge,
    InvalidEncoding,
}

impl ResponsesRequestBodyError {
    fn internal_failure() -> cc_lb_engine::InternalFailure {
        cc_lb_engine::InternalFailure::body_read_failed(cc_lb_domain::InternalError {
            stage: cc_lb_domain::InternalErrorStage::Ingress,
            kind: cc_lb_domain::InternalErrorKind::InvalidInput,
            message: Some(INVALID_REQUEST_BODY_MESSAGE.to_owned()),
        })
    }

    pub(super) fn into_response(self) -> Response<Body> {
        match self {
            Self::TooLarge => super::body_too_large_response(cc_lb_domain::Provider::Openai),
            Self::InvalidEncoding => {
                let mut response = cc_lb_engine::provider_scope_error_response(
                    cc_lb_domain::Provider::Openai,
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    INVALID_REQUEST_BODY_MESSAGE,
                );
                response.extensions_mut().insert(Self::internal_failure());
                response
            }
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContentCoding {
    Identity,
    Zstd,
}

/// Normalize the authenticated OpenAI Responses request body when it uses zstd.
///
/// The authentication proof is intentionally required by this seam. Callers must
/// authenticate before asking this function to inspect or decode request bytes.
pub(super) fn normalize(
    request: Request<Bytes>,
    cap: usize,
    _auth: &cc_lb_engine::Authenticated,
    observer: Option<&cc_lb_engine::LifecycleContext>,
) -> Result<Request<Bytes>, ResponsesRequestBodyError> {
    if !should_normalize(&request) {
        return Ok(request);
    }

    let coding = match content_coding(request.headers()) {
        Ok(coding) => coding,
        Err(error) => {
            if let Some(context) = observer {
                context.terminate_failure(ResponsesRequestBodyError::internal_failure());
            }
            return Err(error);
        }
    };
    if coding == ContentCoding::Identity {
        return Ok(request);
    }

    let decoded = match decode_zstd(request.body().as_ref(), cap) {
        Ok(decoded) => decoded,
        Err(error @ ResponsesRequestBodyError::TooLarge) => {
            if let Some(context) = observer {
                context.terminate_body_too_large(cap as u64);
            }
            return Err(error);
        }
        Err(error) => {
            if let Some(context) = observer {
                context.terminate_failure(ResponsesRequestBodyError::internal_failure());
            }
            return Err(error);
        }
    };
    let (mut parts, _) = request.into_parts();
    parts.headers.remove(CONTENT_ENCODING);
    parts.headers.remove(CONTENT_LENGTH);
    Ok(Request::from_parts(parts, decoded))
}

fn should_normalize(request: &Request<Bytes>) -> bool {
    request.method() == Method::POST
        && request.uri().path() == "/v1/responses"
        && request
            .extensions()
            .get::<cc_lb_engine::ProviderScope>()
            .is_none_or(|scope| scope.0 == cc_lb_domain::Provider::Openai)
}

fn content_coding(headers: &HeaderMap) -> Result<ContentCoding, ResponsesRequestBodyError> {
    let mut values = headers.get_all(CONTENT_ENCODING).iter();
    let Some(value) = values.next() else {
        return Ok(ContentCoding::Identity);
    };
    if values.next().is_some() {
        return Err(ResponsesRequestBodyError::InvalidEncoding);
    }

    let value = value
        .to_str()
        .map_err(|_| ResponsesRequestBodyError::InvalidEncoding)?;
    let mut codings = value.split(',');
    let coding = codings
        .next()
        .map(str::trim)
        .filter(|coding| !coding.is_empty())
        .ok_or(ResponsesRequestBodyError::InvalidEncoding)?;
    if codings.next().is_some() {
        return Err(ResponsesRequestBodyError::InvalidEncoding);
    }

    if coding.eq_ignore_ascii_case("identity") {
        Ok(ContentCoding::Identity)
    } else if coding.eq_ignore_ascii_case("zstd") {
        Ok(ContentCoding::Zstd)
    } else {
        Err(ResponsesRequestBodyError::InvalidEncoding)
    }
}

fn decode_zstd(input: &[u8], cap: usize) -> Result<Bytes, ResponsesRequestBodyError> {
    let decoded_limit = cap
        .checked_add(1)
        .ok_or(ResponsesRequestBodyError::TooLarge)?;
    let decoded_limit =
        u64::try_from(decoded_limit).map_err(|_| ResponsesRequestBodyError::TooLarge)?;
    let mut decoder = zstd::stream::read::Decoder::with_buffer(input)
        .map_err(|_| ResponsesRequestBodyError::InvalidEncoding)?;
    decoder
        .window_log_max(ZSTD_WINDOW_LOG_MAX)
        .map_err(|_| ResponsesRequestBodyError::InvalidEncoding)?;

    let mut limited = decoder.take(decoded_limit);
    let mut decoded = Vec::new();
    limited
        .read_to_end(&mut decoded)
        .map_err(|_| ResponsesRequestBodyError::InvalidEncoding)?;
    if decoded.len() > cap {
        return Err(ResponsesRequestBodyError::TooLarge);
    }
    Ok(Bytes::from(decoded))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn rejects_stacked_and_duplicate_codings() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_ENCODING, "zstd, gzip".parse().unwrap());
        assert_eq!(
            content_coding(&headers),
            Err(ResponsesRequestBodyError::InvalidEncoding)
        );

        headers.insert(CONTENT_ENCODING, "zstd".parse().unwrap());
        headers.append(CONTENT_ENCODING, "zstd".parse().unwrap());
        assert_eq!(
            content_coding(&headers),
            Err(ResponsesRequestBodyError::InvalidEncoding)
        );
    }

    #[test]
    fn normalizes_only_unscoped_and_openai_routes() {
        let unscoped = Request::builder()
            .method(Method::POST)
            .uri("/v1/responses")
            .body(Bytes::new())
            .unwrap();
        assert!(should_normalize(&unscoped));

        let mut openai = Request::builder()
            .method(Method::POST)
            .uri("/v1/responses")
            .body(Bytes::new())
            .unwrap();
        openai
            .extensions_mut()
            .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
        assert!(should_normalize(&openai));

        for provider in [
            cc_lb_domain::Provider::Anthropic,
            cc_lb_domain::Provider::Gemini,
        ] {
            let mut scoped = Request::builder()
                .method(Method::POST)
                .uri("/v1/responses")
                .body(Bytes::new())
                .unwrap();
            scoped
                .extensions_mut()
                .insert(cc_lb_engine::ProviderScope(provider));
            assert!(!should_normalize(&scoped));
        }
    }

    #[test]
    fn accepts_identity_without_body_work() {
        let headers = HeaderMap::new();
        assert_eq!(content_coding(&headers), Ok(ContentCoding::Identity));

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_ENCODING, "identity".parse().unwrap());
        assert_eq!(content_coding(&headers), Ok(ContentCoding::Identity));
    }

    #[test]
    fn decodes_concatenated_frames() {
        let first = zstd::bulk::compress(b"first", 1).unwrap();
        let second = zstd::bulk::compress(b"second", 1).unwrap();
        let mut encoded = first;
        encoded.extend_from_slice(&second);
        assert_eq!(
            decode_zstd(&encoded, 32).unwrap(),
            Bytes::from_static(b"firstsecond")
        );
    }

    #[test]
    fn rejects_truncated_and_bad_checksum_frames() {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.write_all(b"request").unwrap();
        let encoded = encoder.finish().unwrap();
        assert!(matches!(
            decode_zstd(&encoded[..encoded.len() - 1], 32),
            Err(ResponsesRequestBodyError::InvalidEncoding)
        ));

        let mut corrupted = encoded;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x01;
        assert!(matches!(
            decode_zstd(&corrupted, 32),
            Err(ResponsesRequestBodyError::InvalidEncoding)
        ));
    }

    #[test]
    fn rejects_frames_with_windows_above_http_limit() {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.window_log(24).unwrap();
        encoder.write_all(b"request").unwrap();
        let encoded = encoder.finish().unwrap();
        assert_eq!(
            decode_zstd(&encoded, 32),
            Err(ResponsesRequestBodyError::InvalidEncoding)
        );
    }

    #[test]
    fn rejects_decoded_body_over_cap() {
        let encoded = zstd::bulk::compress(b"0123456789", 1).unwrap();
        assert_eq!(
            decode_zstd(&encoded, 9),
            Err(ResponsesRequestBodyError::TooLarge)
        );
    }

    #[test]
    fn rejects_cap_overflow_without_decoding() {
        assert_eq!(
            decode_zstd(&[], usize::MAX),
            Err(ResponsesRequestBodyError::TooLarge)
        );
    }

    #[tokio::test]
    async fn too_large_error_formats_openai_413_envelope() {
        use http_body_util::BodyExt;
        use serde_json::Value;

        let response = ResponsesRequestBodyError::TooLarge.into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            response.headers()[http::header::CONTENT_TYPE],
            "application/json; charset=utf-8"
        );
        let body = response
            .into_body()
            .collect()
            .await
            .expect("error body should collect")
            .to_bytes();
        let parsed: Value = serde_json::from_slice(&body).expect("error body should be valid JSON");
        assert_eq!(
            parsed,
            serde_json::json!({
                "error": {
                    "message": "request body exceeds configured cap",
                    "type": "body_too_large",
                    "code": "body_too_large",
                    "param": null,
                }
            })
        );
        assert!(parsed.get("type").is_none());
    }
}
