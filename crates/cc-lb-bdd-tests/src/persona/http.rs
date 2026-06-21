use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use bytes::Bytes;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::harness::TEST_ADMIN_TOKEN;
use crate::results::HttpResponse;

pub(crate) async fn admin_request(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> HttpResponse {
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(&format!("Bearer {TEST_ADMIN_TOKEN}")) {
        headers.insert(header::AUTHORIZATION, value);
    }
    router_request(router, method, path, body, headers).await
}

pub(crate) async fn proxy_request(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> HttpResponse {
    router_request(router, method, path, body, HeaderMap::new()).await
}

pub(crate) async fn proxy_request_with_headers(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
    headers: HeaderMap,
) -> HttpResponse {
    router_request(router, method, path, body, headers).await
}

pub(crate) fn json_response(status: StatusCode, body: Value) -> HttpResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    HttpResponse {
        status,
        headers,
        body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
    }
}

async fn router_request(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
    extra_headers: HeaderMap,
) -> HttpResponse {
    let mut builder = Request::builder().method(method).uri(path);
    for (name, value) in extra_headers {
        if let Some(name) = name {
            builder = builder.header(name, value);
        }
    }
    let request_body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&value).unwrap_or_default())
        }
        None => Body::empty(),
    };
    let request = match builder.body(request_body) {
        Ok(request) => request,
        Err(error) => {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "request_build_failed", "message": error.to_string() }),
            );
        }
    };
    let response = match router.oneshot(request).await {
        Ok(response) => response,
        Err(error) => match error {},
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = match response.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => Bytes::from(error.to_string()),
    };
    HttpResponse {
        status,
        headers,
        body,
    }
}
