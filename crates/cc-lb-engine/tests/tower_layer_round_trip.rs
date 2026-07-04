use std::convert::Infallible;

use http::header::HeaderValue;
use http::{Request, Response};
use tower::{Layer, ServiceExt, service_fn};

use cc_lb_engine::HopByHopStripLayer;

#[tokio::test]
async fn strips_hop_by_hop_on_request_and_response() {
    let layer = HopByHopStripLayer::new();
    let service = layer.layer(service_fn(|request: Request<()>| async move {
        let seen_headers = request
            .headers()
            .keys()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
            .join(",");

        let mut response = Response::new(());
        response.headers_mut().insert(
            "x-seen-headers",
            HeaderValue::from_str(&seen_headers).unwrap(),
        );
        response.headers_mut().insert(
            "connection",
            HeaderValue::from_static("keep-alive, X-Response-Hop"),
        );
        response
            .headers_mut()
            .insert("keep-alive", HeaderValue::from_static("timeout=5"));
        response
            .headers_mut()
            .insert("x-response-hop", HeaderValue::from_static("strip-me"));

        Ok::<_, Infallible>(response)
    }));

    let mut request = Request::new(());
    request.headers_mut().insert(
        "connection",
        HeaderValue::from_static("keep-alive, X-Request-Hop"),
    );
    request
        .headers_mut()
        .insert("keep-alive", HeaderValue::from_static("timeout=5"));
    request
        .headers_mut()
        .insert("x-request-hop", HeaderValue::from_static("strip-me"));
    request
        .headers_mut()
        .insert("x-request-ok", HeaderValue::from_static("preserve-me"));

    let response = service.oneshot(request).await.unwrap();

    let seen_headers = response
        .headers()
        .get("x-seen-headers")
        .and_then(|value| value.to_str().ok())
        .unwrap();
    assert!(!seen_headers.contains("connection"));
    assert!(!seen_headers.contains("keep-alive"));
    assert!(!seen_headers.contains("x-request-hop"));
    assert!(seen_headers.contains("x-request-ok"));

    assert!(!response.headers().contains_key("connection"));
    assert!(!response.headers().contains_key("keep-alive"));
    assert!(!response.headers().contains_key("x-response-hop"));
    assert_eq!(
        response
            .headers()
            .get("x-seen-headers")
            .unwrap()
            .to_str()
            .unwrap(),
        seen_headers
    );
}
