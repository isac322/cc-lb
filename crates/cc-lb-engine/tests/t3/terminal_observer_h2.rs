use std::error::Error as StdError;
use std::time::Duration;

use http::{Request, StatusCode};
use http_body_util::{BodyExt as _, Full};
use hyper::client::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

const ERROR_CHAIN_MAX_DEPTH: usize = 8;

#[tokio::test]
async fn t3__classifies_real_hyper_http2_cancelled_body() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("HTTP/2 test listener binds");
    let address = listener.local_addr().expect("HTTP/2 listener address");
    let (headers_received_tx, headers_received_rx) = oneshot::channel();
    let (reset_observed_tx, reset_observed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("HTTP/2 client connects");
        let mut connection = h2::server::handshake(socket)
            .await
            .expect("HTTP/2 server handshake");
        let (_request, mut respond) = connection
            .accept()
            .await
            .expect("HTTP/2 request present")
            .expect("HTTP/2 request accepted");
        let response = http::Response::builder()
            .status(StatusCode::OK)
            .body(())
            .expect("HTTP/2 response builds");
        let mut body = respond
            .send_response(response, false)
            .expect("HTTP/2 response headers sent");
        tokio::select! {
            received = headers_received_rx => {
                received.expect("client confirms response headers");
            }
            _ = connection.accept() => {
                panic!("HTTP/2 connection ended before response-header acknowledgement");
            }
        }
        body.send_reset(h2::Reason::CANCEL);
        tokio::pin!(reset_observed_rx);
        loop {
            tokio::select! {
                observed = &mut reset_observed_rx => {
                    observed.expect("client observes RST_STREAM");
                    break;
                }
                accepted = connection.accept() => {
                    assert!(
                        accepted.is_some(),
                        "HTTP/2 connection closed before reset was observed"
                    );
                }
            }
        }
    });

    let socket = TcpStream::connect(address)
        .await
        .expect("HTTP/2 client connects");
    let (mut sender, connection) = http2::handshake(TokioExecutor::new(), TokioIo::new(socket))
        .await
        .expect("Hyper HTTP/2 client handshake");
    let client_connection = tokio::spawn(connection);
    let request = Request::builder()
        .uri(format!("http://{address}/cancel"))
        .body(Full::new(bytes::Bytes::new()))
        .expect("HTTP/2 request builds");
    let mut response = tokio::time::timeout(Duration::from_secs(5), sender.send_request(request))
        .await
        .expect("HTTP/2 response headers are flushed")
        .expect("response headers received");
    assert_eq!(response.status(), StatusCode::OK);
    headers_received_tx
        .send(())
        .expect("server waits for response-header acknowledgement");
    let error = tokio::time::timeout(Duration::from_secs(5), response.body_mut().frame())
        .await
        .expect("HTTP/2 reset is flushed")
        .expect("reset produces a body frame result")
        .expect_err("RST_STREAM CANCEL produces a Hyper body error");

    let h2_error = find_h2_error(&error).expect("Hyper error chain contains the HTTP/2 reset");
    assert!(h2_error.is_reset());
    assert_eq!(h2_error.reason(), Some(h2::Reason::CANCEL));
    assert_eq!(
        h2_error.reason().map(|reason| reason.to_string()),
        Some(h2::Reason::CANCEL.to_string())
    );

    reset_observed_tx
        .send(())
        .expect("server waits until Hyper exposes the reset");
    server.await.expect("HTTP/2 server task completes");
    client_connection.abort();
}

fn find_h2_error<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a h2::Error> {
    let mut current = Some(error);
    for _ in 0..ERROR_CHAIN_MAX_DEPTH {
        let source = current?;
        if let Some(error) = source.downcast_ref::<h2::Error>() {
            return Some(error);
        }
        current = source.source();
    }
    None
}
