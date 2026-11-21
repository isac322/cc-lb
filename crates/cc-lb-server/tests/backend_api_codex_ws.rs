use crate::{common, openai_oauth_public_media};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

#[tokio::test]
async fn backend_codex_websocket_preserves_frames_query_and_authentication() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = openai_oauth_public_media::spawn().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    openai_oauth_public_media::setup(&server, &fake, &client).await;
    let key = &server.managed_key.plaintext;

    let mut request = format!(
        "ws://{}/backend-api/codex/responses?trace=a%2Fb&beta=1",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-test","stream_id":"lane-1","input":"native"}"#.into(),
        ))
        .await
        .unwrap();
    let first_frame = match socket.next().await.unwrap().unwrap() {
        Message::Text(frame) => frame.to_string(),
        other => panic!("expected provider text frame, got {other:?}"),
    };
    let second_frame = match socket.next().await.unwrap().unwrap() {
        Message::Text(frame) => frame.to_string(),
        other => panic!("expected provider text frame, got {other:?}"),
    };
    assert_eq!(
        first_frame,
        r#"{"type":"response.created","response":{"id":"resp_tok","object":"response","status":"in_progress"}}"#
    );
    assert_eq!(
        second_frame,
        r#"{"type":"response.completed","response":{"id":"resp_tok","object":"response","status":"completed","usage":{"input_tokens":42,"output_tokens":0,"total_tokens":42}}}"#
    );

    let (body, query) = {
        let records = fake.state.0.lock().unwrap();
        let frame = records
            .iter()
            .find(|(path, transport, _, _)| {
                path == "/backend-api/codex/responses" && transport == "WS"
            })
            .expect("native alias frame reached OAuth backend");
        (
            String::from_utf8_lossy(&frame.3).into_owned(),
            frame
                .2
                .iter()
                .find(|(name, _)| name == "x-test-query")
                .map(|(_, value)| value.clone())
                .unwrap_or_default(),
        )
    };
    assert_eq!(query, "trace=a%2Fb&beta=1");
    assert!(body.contains("stream_id"));
    assert!(!body.contains("\"store\""));

    let unauthenticated = client
        .get(format!(
            "http://{}/backend-api/codex/models",
            server.proxy_addr
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), 401);
}

#[tokio::test]
async fn backend_codex_websocket_rejects_unauthenticated_upgrade() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = openai_oauth_public_media::spawn().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    openai_oauth_public_media::setup(&server, &fake, &client).await;
    let request = format!(
        "ws://{}/backend-api/codex/responses?trace=a%2Fb",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio_tungstenite::connect_async(request),
    )
    .await
    .expect("WebSocket auth handshake completed")
    .expect_err("unauthenticated WebSocket upgrade must fail");
    assert!(error.to_string().contains("401"));
    assert!(
        fake.state
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|(_, _, headers, _)| { !headers.iter().any(|(name, _)| name == "request-id") })
    );
}

#[tokio::test]
async fn backend_codex_websocket_rejects_traversal_without_upstream_contact() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = openai_oauth_public_media::spawn().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    openai_oauth_public_media::setup(&server, &fake, &client).await;
    let response = client
        .get(format!(
            "http://{}/backend-api/codex/..%2fresponses",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_client_error());
    assert!(
        fake.state
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|(path, _, headers, _)| {
                path != "/backend-api/codex/responses"
                    || !headers.iter().any(|(name, _)| name == "request-id")
            })
    );
}
