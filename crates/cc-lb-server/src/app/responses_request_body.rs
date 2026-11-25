use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

use axum::body::Body;
use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use tokio::sync::Semaphore;

const INVALID_REQUEST_BODY_MESSAGE: &str = "invalid request body encoding";
/// RFC 9659 §3 limits the HTTP zstd decoder window to 8 MiB (`2^23`).
const ZSTD_WINDOW_LOG_MAX: u32 = 23;
const ZSTD_DECODE_CONCURRENCY_LIMIT: usize = 16;
const ZSTD_DECODE_CHUNK_BYTES: usize = 16 * 1024;
static ZSTD_DECODE_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(ZSTD_DECODE_CONCURRENCY_LIMIT)));

struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

const INTERNAL_BODY_DECODE_MESSAGE: &str = "failed to decode request body";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ResponsesRequestBodyError {
    TooLarge,
    InvalidEncoding,
    Internal,
}

impl ResponsesRequestBodyError {
    fn internal_failure() -> cc_lb_engine::InternalFailure {
        cc_lb_engine::InternalFailure::body_read_failed(cc_lb_domain::InternalError {
            stage: cc_lb_domain::InternalErrorStage::Ingress,
            kind: cc_lb_domain::InternalErrorKind::InvalidInput,
            message: Some(INVALID_REQUEST_BODY_MESSAGE.to_owned()),
        })
    }

    fn internal_task_failure(join_error: &tokio::task::JoinError) -> cc_lb_engine::InternalFailure {
        cc_lb_engine::InternalFailure {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_code: cc_lb_engine::terminal_observer::error_codes::BODY_READ_FAILED,
            error: cc_lb_domain::InternalError {
                stage: cc_lb_domain::InternalErrorStage::Ingress,
                kind: cc_lb_domain::InternalErrorKind::Unavailable,
                message: Some(format!(
                    "zstd request body decode task failed: {join_error}"
                )),
            },
        }
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
            Self::Internal => cc_lb_engine::provider_scope_error_response(
                cc_lb_domain::Provider::Openai,
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                INTERNAL_BODY_DECODE_MESSAGE,
            ),
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
pub(super) async fn normalize(
    request: Request<Bytes>,
    cap: usize,
    auth: &cc_lb_engine::Authenticated,
    observer: Option<&cc_lb_engine::LifecycleContext>,
) -> Result<Request<Bytes>, ResponsesRequestBodyError> {
    normalize_with_semaphore(
        request,
        cap,
        auth,
        observer,
        Arc::clone(&ZSTD_DECODE_SEMAPHORE),
    )
    .await
}

async fn normalize_with_semaphore(
    request: Request<Bytes>,
    cap: usize,
    _auth: &cc_lb_engine::Authenticated,
    observer: Option<&cc_lb_engine::LifecycleContext>,
    semaphore: Arc<Semaphore>,
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

    let Ok(permit) = semaphore.acquire_owned().await else {
        return Err(ResponsesRequestBodyError::Internal);
    };
    let (mut parts, body) = request.into_parts();
    let cancel_guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
    let task_cancelled = Arc::clone(&cancel_guard.0);
    let decoded = match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        decode_zstd(body.as_ref(), cap, &task_cancelled)
    })
    .await
    {
        Ok(Ok(decoded)) => decoded,
        Ok(Err(error @ ResponsesRequestBodyError::TooLarge)) => {
            if let Some(context) = observer {
                context.terminate_body_too_large(cap as u64);
            }
            return Err(error);
        }
        Ok(Err(error)) => {
            if let Some(context) = observer {
                context.terminate_failure(ResponsesRequestBodyError::internal_failure());
            }
            return Err(error);
        }
        Err(join_error) => {
            tracing::error!(%join_error, "zstd request body decode task failed");
            if let Some(context) = observer {
                context.terminate_failure(ResponsesRequestBodyError::internal_task_failure(
                    &join_error,
                ));
            }
            return Err(ResponsesRequestBodyError::Internal);
        }
    };
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

fn decode_zstd(
    input: &[u8],
    cap: usize,
    cancelled: &AtomicBool,
) -> Result<Bytes, ResponsesRequestBodyError> {
    if cancelled.load(Ordering::Relaxed) {
        return Err(ResponsesRequestBodyError::Internal);
    }
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
    let mut chunk = [0_u8; ZSTD_DECODE_CHUNK_BYTES];
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(ResponsesRequestBodyError::Internal);
        }
        let read = limited
            .read(&mut chunk)
            .map_err(|_| ResponsesRequestBodyError::InvalidEncoding)?;
        if read == 0 {
            break;
        }
        decoded.extend_from_slice(&chunk[..read]);
        if decoded.len() > cap {
            return Err(ResponsesRequestBodyError::TooLarge);
        }
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
        let cancelled = AtomicBool::new(false);
        assert_eq!(
            decode_zstd(&encoded, 32, &cancelled).unwrap(),
            Bytes::from_static(b"firstsecond")
        );
    }

    #[test]
    fn rejects_truncated_and_bad_checksum_frames() {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.write_all(b"request").unwrap();
        let encoded = encoder.finish().unwrap();
        let cancelled = AtomicBool::new(false);
        assert!(matches!(
            decode_zstd(&encoded[..encoded.len() - 1], 32, &cancelled),
            Err(ResponsesRequestBodyError::InvalidEncoding)
        ));

        let mut corrupted = encoded;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x01;
        assert!(matches!(
            decode_zstd(&corrupted, 32, &cancelled),
            Err(ResponsesRequestBodyError::InvalidEncoding)
        ));
    }

    #[test]
    fn rejects_frames_with_windows_above_http_limit() {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.window_log(24).unwrap();
        encoder.write_all(b"request").unwrap();
        let encoded = encoder.finish().unwrap();
        let cancelled = AtomicBool::new(false);
        assert_eq!(
            decode_zstd(&encoded, 32, &cancelled),
            Err(ResponsesRequestBodyError::InvalidEncoding)
        );
    }

    #[test]
    fn rejects_decoded_body_over_cap() {
        let encoded = zstd::bulk::compress(b"0123456789", 1).unwrap();
        let cancelled = AtomicBool::new(false);
        assert_eq!(
            decode_zstd(&encoded, 9, &cancelled),
            Err(ResponsesRequestBodyError::TooLarge)
        );
    }

    #[test]
    fn rejects_cap_overflow_without_decoding() {
        let cancelled = AtomicBool::new(false);
        assert_eq!(
            decode_zstd(&[], usize::MAX, &cancelled),
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

    #[test]
    fn decode_aborts_immediately_when_cancelled() {
        let encoded = zstd::bulk::compress(b"cancelled-payload", 1).unwrap();
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            decode_zstd(&encoded, 64, &cancelled),
            Err(ResponsesRequestBodyError::Internal)
        );
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn normalize_bounds_zstd_concurrency_and_cancels_queued_work_on_drop() {
        let semaphore = Arc::new(Semaphore::new(1));
        let held_permit = semaphore.clone().acquire_owned().await.unwrap();

        let encoded = Bytes::from(zstd::bulk::compress(b"{\"hello\":\"world\"}", 1).unwrap());
        let auth = crate::app::tests::authenticate_test_request(&HeaderMap::new()).await;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/responses")
            .header(CONTENT_ENCODING, "zstd")
            .body(encoded)
            .unwrap();

        let sem_clone = Arc::clone(&semaphore);
        let mut fut =
            Box::pin(
                async move { normalize_with_semaphore(req, 1024, &auth, None, sem_clone).await },
            );

        // Poll once while semaphore is held: caller stays pending without spawning blocking decode.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut fut)
                .await
                .is_err(),
            "zstd normalization must wait for bounded decode semaphore permit"
        );
        assert_eq!(semaphore.available_permits(), 0);
        drop(held_permit);
        // Dropping the queued/woken future before polling to completion releases its permit.
        drop(fut);
        assert_eq!(semaphore.available_permits(), 1);
    }
}
