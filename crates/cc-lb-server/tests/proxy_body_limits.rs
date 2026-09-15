use crate::common;

use std::error::Error;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const MESSAGES_CAP_BYTES: usize = 256;

#[tokio::test]
async fn oversized_content_length_returns_413_before_the_client_sends_a_body() -> TestResult<()> {
    // Given
    let server = common::spawn_test_server().await;
    let mut stream = connect_proxy(server.proxy_addr).await?;
    let request = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: {}\r\nx-api-key: {}\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        server.proxy_addr,
        server.managed_key.plaintext,
        MESSAGES_CAP_BYTES + 1,
    );

    // When
    stream.write_all(request.as_bytes()).await?;
    let response = read_response(&mut stream).await?;

    // Then
    assert_payload_too_large(&response);
    Ok(())
}

#[tokio::test]
async fn chunked_body_over_cap_returns_413_before_the_client_finishes_streaming() -> TestResult<()>
{
    // Given
    let server = common::spawn_test_server().await;
    let mut stream = connect_proxy(server.proxy_addr).await?;
    let request = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: {}\r\nx-api-key: {}\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
        server.proxy_addr, server.managed_key.plaintext,
    );
    stream.write_all(request.as_bytes()).await?;
    stream
        .write_all(b"80\r\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n")
        .await?;
    stream
        .write_all(b"81\r\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n")
        .await?;

    // When
    let response = read_response(&mut stream).await?;

    // Then
    assert_payload_too_large(&response);
    Ok(())
}

#[tokio::test]
async fn body_exactly_at_messages_cap_reaches_the_real_upstream() -> TestResult<()> {
    // Given
    let server = common::spawn_test_server().await;
    let body = messages_body_with_len(MESSAGES_CAP_BYTES);

    // When
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        &body,
        &[],
    )
    .await?;

    // Then
    assert_eq!(response.status, 200, "{}", response.body);
    assert!(response.body.contains("msg_fake_"));
    Ok(())
}

#[tokio::test]
async fn messages_and_files_paths_use_their_distinct_configured_caps() -> TestResult<()> {
    // Given
    let server = common::spawn_test_server().await;
    let body = messages_body_with_len(MESSAGES_CAP_BYTES + 1);

    // When
    let messages = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        &body,
        &[],
    )
    .await?;
    let files = common::http_post(
        server.proxy_addr,
        "/v1/files",
        &server.managed_key.plaintext,
        &body,
        &[],
    )
    .await?;

    // Then
    assert_eq!(messages.status, 413, "{}", messages.body);
    assert_eq!(files.status, 200, "{}", files.body);
    assert!(files.body.contains("file_abc123"));
    Ok(())
}

async fn connect_proxy(addr: std::net::SocketAddr) -> Result<TcpStream, std::io::Error> {
    TcpStream::connect(addr).await
}

async fn read_response(stream: &mut TcpStream) -> TestResult<String> {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut bytes)).await??;
    Ok(String::from_utf8(bytes)?)
}

fn assert_payload_too_large(response: &str) {
    assert!(
        response.starts_with("HTTP/1.1 413"),
        "expected 413 response, got {response:?}"
    );
    assert!(
        response.contains("body_too_large"),
        "expected standard body-too-large payload, got {response:?}"
    );
}

fn messages_body_with_len(len: usize) -> String {
    const PREFIX: &str =
        r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":""#;
    const SUFFIX: &str = r#""}],"max_tokens":16}"#;

    let padding_len = len
        .checked_sub(PREFIX.len() + SUFFIX.len())
        .expect("configured test cap must fit the fixed JSON envelope");
    let mut body = String::with_capacity(len);
    body.push_str(PREFIX);
    body.extend(std::iter::repeat_n('x', padding_len));
    body.push_str(SUFFIX);
    assert_eq!(body.len(), len);
    body
}
