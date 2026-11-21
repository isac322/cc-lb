use crate::common;

use axum::{
    Router,
    body::to_bytes,
    extract::{
        OriginalUri, Request, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use reqwest::Method;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{net::TcpListener, task::JoinHandle};

/// One recorded upstream call: path, transport, headers, body.
type Recorded = (String, String, Vec<(String, String)>, Vec<u8>);

#[derive(Clone, Default)]
pub(crate) struct FakeState(pub(crate) Arc<Mutex<Vec<Recorded>>>);
pub(crate) struct Fake {
    pub(crate) base: String,
    pub(crate) state: FakeState,
    task: JoinHandle<Result<(), std::io::Error>>,
}
impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn ws_handler(
    State(state): State<FakeState>,
    OriginalUri(uri): OriginalUri,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let query = uri.query().unwrap_or_default().to_owned();
    ws.on_upgrade(move |mut socket: WebSocket| async move {
        if let Some(Ok(Message::Text(t))) = socket.recv().await {
            state.0.lock().unwrap().push((
                "/backend-api/codex/responses".into(),
                "WS".into(),
                vec![("x-test-query".into(), query)],
                t.as_bytes().to_vec(),
            ));
            let _ = socket
                .send(Message::Text(
                    r#"{"type":"response.created","response":{"id":"resp_tok","object":"response","status":"in_progress"}}"#.into(),
                ))
                .await;
            let _ = socket
                .send(Message::Text(
                    r#"{"type":"response.completed","response":{"id":"resp_tok","object":"response","status":"completed","usage":{"input_tokens":42,"output_tokens":0,"total_tokens":42}}}"#.into(),
                ))
                .await;
        }
    })
}
/// The provider joins a sideband only when `OpenAI-Alpha` matches the live
/// call, and otherwise rejects the handshake with a JSON error.
async fn sideband_handler(
    State(state): State<FakeState>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let alpha = headers
        .get("openai-alpha")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    state.0.lock().unwrap().push((
        "/v1/realtime".into(),
        "WS".into(),
        vec![("openai-alpha".into(), alpha.clone())],
        vec![],
    ));
    if alpha != "quicksilver=v2" {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error":{
                "message":"Sideband OpenAI-Alpha header does not match the live session.",
                "type":"invalid_request_error",
                "code":"observer_protocol_mismatch",
                "param":"OpenAI-Alpha"
            }})),
        )
            .into_response();
    }
    ws.on_upgrade(|mut socket: WebSocket| async move {
        let _ = socket
            .send(Message::Text(r#"{"type":"session.created"}"#.into()))
            .await;
    })
}
async fn fake(State(state): State<FakeState>, req: Request) -> Response {
    let _method = req.method().to_string();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let headers = req
        .headers()
        .iter()
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_owned())))
        .collect();
    let body = to_bytes(req.into_body(), 10_000_000)
        .await
        .unwrap()
        .to_vec();
    state
        .0
        .lock()
        .unwrap()
        .push((path.clone(), query, headers, body.clone()));
    match path.as_str() {
        "/backend-api/codex/images/edits" => {
            axum::Json(json!({"created":1,"data":[{"b64_json":"ZmFrZQ=="}]})).into_response()
        }
        "/backend-api/codex/images/generations" => {
            // The backend ignores `stream` and always answers with JSON.
            axum::Json(json!({"created":1,"data":[{"b64_json":"ZmFrZQ=="}]})).into_response()
        }
        "/backend-api/transcribe" => {
            axum::Json(json!({"text":"hello transcription"})).into_response()
        }
        "/backend-api/codex/realtime/calls" => (
            StatusCode::CREATED,
            [(header::LOCATION, "/v1/realtime/calls/rtc_test1")],
            "v=0\r\n",
        )
            .into_response(),
        "/v1/realtime/calls/rtc_test1/hangup" => StatusCode::NO_CONTENT.into_response(),
        _ => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"detail":"not found"})),
        )
            .into_response(),
    }
}
pub(crate) async fn spawn() -> Fake {
    let state = FakeState::default();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    let st = state.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            l,
            Router::new()
                .route(
                    "/backend-api/codex/responses",
                    axum::routing::get(ws_handler),
                )
                .route("/v1/realtime", axum::routing::get(sideband_handler))
                .fallback(fake)
                .with_state(st),
        )
        .await
    });
    Fake {
        base: format!("http://{a}/backend-api/codex"),
        state,
        task,
    }
}
async fn admin(
    c: &reqwest::Client,
    s: &common::TestServer,
    m: Method,
    p: &str,
    b: Option<Value>,
    etag: Option<&str>,
) -> (reqwest::StatusCode, Value, Option<String>) {
    let mut r = c
        .request(m, format!("http://{}{}", s.admin_addr, p))
        .bearer_auth("admin-token");
    if let Some(e) = etag {
        r = r.header("if-match", e)
    }
    if let Some(v) = b {
        r = r.json(&v)
    }
    let x = r.send().await.unwrap();
    let st = x.status();
    let e = x
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    (st, x.json().await.unwrap_or(json!({})), e)
}
pub(crate) async fn setup(s: &common::TestServer, f: &Fake, c: &reqwest::Client) -> String {
    let (_,v,_)=admin(c,s,Method::POST,"/admin/v1/upstreams",Some(json!({"name":"oauth-media","kind":"openai_oauth","base_url":f.base,"oauth_tokens":{"access_token":"test-access","refresh_token":"","expires_at_unix_secs":4102444800u64,"account_id":"acct-test","scopes":[],"never_refresh":true},"skip_preflight":true})),None).await;
    v["id"].as_str().unwrap().into()
}

#[tokio::test]
async fn openai_oauth_public_media_and_realtime_contract() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let f = spawn().await;
    let port = f.base.rsplit(':').next().unwrap();
    let s = common::spawn_test_server_with_openai_budget(&format!(
        "[openai_public_api]\nrealtime_origin = \"http://127.0.0.1:{port}/\"\n"
    ))
    .await;
    let c = reqwest::Client::new();
    setup(&s, &f, &c).await;
    let key = &s.managed_key.plaintext;
    let r = c
        .post(format!(
            "http://{}/openai/v1/responses/input_tokens",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({"model":"gpt-test","input":"hello"}))
        .send()
        .await
        .unwrap();
    let status = r.status();
    let text = r.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    let r_json: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(r_json["input_tokens"], 42);
    let r = c
        .post(format!(
            "http://{}/openai/v1/images/generations",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({"model":"x","prompt":"p","stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("image_generation.completed")
    );
    let r = c
        .post(format!("http://{}/openai/v1/realtime/calls", s.proxy_addr))
        .bearer_auth(key)
        .header("content-type", "application/sdp")
        .body("v=0...")
        .send()
        .await
        .unwrap();
    let status = r.status();
    let location = r.headers().get(header::LOCATION).cloned();
    let text = r.text().await.unwrap();
    assert_eq!(status, 201, "{text}");
    assert_eq!(
        location.as_ref().and_then(|v| v.to_str().ok()),
        Some("/v1/realtime/calls/rtc_test1")
    );
    // A public client joins the call's sideband without `OpenAI-Alpha`; cc-lb
    // must add the protocol header the subscription call was created with.
    {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = format!("ws://{}/openai/v1/realtime?call_id=rtc_test1", s.proxy_addr)
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!("Bearer {key}").parse().unwrap(),
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .expect("sideband handshake succeeds");
        let first = tokio::time::timeout(std::time::Duration::from_secs(10), ws.next())
            .await
            .expect("sideband event arrives")
            .expect("stream open")
            .expect("frame");
        assert_eq!(
            first.into_text().unwrap().as_str(),
            r#"{"type":"session.created"}"#
        );
        let rec = f.state.0.lock().unwrap();
        let sideband = rec
            .iter()
            .find(|x| x.0 == "/v1/realtime")
            .expect("sideband reached the provider");
        assert_eq!(sideband.2[0].1, "quicksilver=v2");
    }
    let r = c
        .post(format!(
            "http://{}/openai/v1/realtime/calls/rtc_test1/hangup",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let rec = f.state.0.lock().unwrap();
    assert!(rec.iter().any(|x| x.0 == "/backend-api/codex/responses"));
    assert!(
        rec.iter()
            .any(|x| x.0 == "/backend-api/codex/realtime/calls")
    );
}

#[tokio::test]
async fn openai_oauth_public_transcription_compatibility() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let f = spawn().await;
    let s = common::spawn_test_server_with_openai_budget("").await;
    let c = reqwest::Client::new();
    setup(&s, &f, &c).await;
    let key = &s.managed_key.plaintext;
    let form = reqwest::multipart::Form::new()
        .text("model", "whisper-1")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"audio".to_vec()).file_name("a.wav"),
        );
    let r = c
        .post(format!(
            "http://{}/openai/v1/audio/transcriptions",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert!(!r.status().is_success());
    assert!(
        !f.state
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|x| x.0 == "/backend-api/transcribe")
    );
    let principals = admin(&c, &s, Method::GET, "/admin/v1/principals", None, None)
        .await
        .1;
    let id = principals["principals"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (_, detail, etag) = admin(
        &c,
        &s,
        Method::GET,
        &format!("/admin/v1/principals/{id}"),
        None,
        None,
    )
    .await;
    let mut body = detail;
    body["openai_oauth_compat"] = json!({"transcription":"enabled"});
    let (st, _, _) = admin(
        &c,
        &s,
        Method::PUT,
        &format!("/admin/v1/principals/{id}"),
        Some(body),
        etag.as_deref(),
    )
    .await;
    assert_eq!(st, 200);
    let form = reqwest::multipart::Form::new()
        .text("model", "whisper-1")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"audio".to_vec()).file_name("a.wav"),
        );
    let r = c
        .post(format!(
            "http://{}/openai/v1/audio/transcriptions",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"text":"hello transcription"})
    );
    let form = reqwest::multipart::Form::new()
        .text("model", "whisper-1")
        .text("response_format", "srt")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"audio".to_vec()).file_name("a.wav"),
        );
    let r = c
        .post(format!(
            "http://{}/openai/v1/audio/transcriptions",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert!(
        f.state
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|x| x.0 == "/backend-api/transcribe")
    );
}
