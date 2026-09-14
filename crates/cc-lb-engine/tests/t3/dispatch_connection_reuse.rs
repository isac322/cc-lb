use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use cc_lb_engine::{HyperDispatcher, UpstreamDispatch};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use http::{HeaderValue, Response, header::CONTENT_ENCODING};
use http_body_util::{BodyExt, Full};
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::common::signed_request;

#[tokio::test]
async fn t3__hyper_dispatcher_reuses_connection_without_corrupting_gzip_body() {
    let plaintext = Bytes::from_static(b"second response over the reused connection");
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&plaintext).expect("gzip write succeeds");
    let compressed = Bytes::from(encoder.finish().expect("gzip finish succeeds"));

    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("test listener binds");
    let address = listener.local_addr().expect("listener address");
    let accepted_connections = Arc::new(AtomicUsize::new(0));
    let response_index = Arc::new(AtomicUsize::new(0));
    let server_compressed = compressed.clone();
    let server_connections = Arc::clone(&accepted_connections);
    let server_responses = Arc::clone(&response_index);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("client connects");
        server_connections.fetch_add(1, Ordering::Relaxed);
        http1::Builder::new()
            .keep_alive(true)
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |_request| {
                    let index = server_responses.fetch_add(1, Ordering::Relaxed);
                    let body = if index == 0 {
                        Bytes::from_static(b"first response")
                    } else {
                        server_compressed.clone()
                    };
                    async move {
                        let mut response = Response::new(Full::new(body));
                        if index > 0 {
                            response
                                .headers_mut()
                                .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
                        }
                        Ok::<_, std::convert::Infallible>(response)
                    }
                }),
            )
            .await
            .expect("HTTP/1.1 connection serves both responses");
    });

    let dispatcher = HyperDispatcher::new();
    let base_url = format!("http://{address}/");
    let first = tokio::time::timeout(
        Duration::from_secs(5),
        dispatcher.dispatch(signed_request(&base_url).await),
    )
    .await
    .expect("first dispatch completes")
    .expect("first dispatch succeeds");
    let first_body = first
        .into_body()
        .collect()
        .await
        .expect("first body reads")
        .to_bytes();
    assert_eq!(first_body, Bytes::from_static(b"first response"));

    let second = tokio::time::timeout(
        Duration::from_secs(5),
        dispatcher.dispatch(signed_request(&base_url).await),
    )
    .await
    .expect("second dispatch completes on the reusable connection")
    .expect("second dispatch succeeds");
    assert_eq!(
        second.headers().get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip"))
    );
    let second_body = second
        .into_body()
        .collect()
        .await
        .expect("second body reads")
        .to_bytes();
    assert_eq!(second_body, compressed);

    let mut decoder = GzDecoder::new(second_body.as_ref());
    let mut decoded = Vec::new();
    decoder
        .read_to_end(&mut decoded)
        .expect("captured gzip body decompresses");
    assert_eq!(decoded, plaintext);
    assert_eq!(accepted_connections.load(Ordering::Relaxed), 1);
    assert_eq!(response_index.load(Ordering::Relaxed), 2);

    server.abort();
    let _ = server.await;
}
