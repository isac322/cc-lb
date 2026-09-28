#![allow(dead_code, clippy::manual_async_fn, clippy::too_many_arguments)]

use std::net::SocketAddr;

use axum::body::Bytes;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fake_anthropic::{AppConfig, OAuthRefreshPause, app as fake_anthropic_app};
use http::header::LOCATION;
use http::{HeaderMap, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;

use super::common::TestResult;

pub struct FakeAnthropic {
    base: String,
    pause: OAuthRefreshPause,
    server: JoinHandle<Result<(), std::io::Error>>,
}

impl FakeAnthropic {
    pub async fn spawn() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let pause = OAuthRefreshPause::new();
        let app = fake_anthropic_app(AppConfig {
            oauth_refresh_pause: Some(pause.clone()),
            ..AppConfig::default()
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        Ok(Self {
            base: format!("http://{addr}"),
            pause,
            server,
        })
    }

    pub fn auth_url(&self) -> Url {
        Url::parse(&format!("{}/oauth/authorize", self.base)).expect("fake auth URL parses")
    }

    pub fn token_url(&self) -> Url {
        Url::parse(&format!("{}/oauth/token", self.base)).expect("fake token URL parses")
    }

    pub async fn initial_tokens(&self) -> TestResult<InitialTokens> {
        let verifier = "verifier";
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let mut authorize_url = self.auth_url();
        authorize_url
            .query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", "test-client")
            .append_pair("redirect_uri", "http://localhost/callback")
            .append_pair("code_challenge", challenge.as_str())
            .append_pair("code_challenge_method", "S256")
            .append_pair("scope", "messages");
        let authorize = raw_http("GET", authorize_url.as_str(), &[], &[]).await?;
        if !authorize.status.is_redirection() {
            return Err(format!("authorize returned {}", authorize.status).into());
        }
        let location = authorize
            .headers
            .get(LOCATION)
            .ok_or("authorize response missing location")?
            .to_str()?;
        let code = Url::parse(location)?
            .query_pairs()
            .find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
            .ok_or("authorize redirect missing code")?;
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer.append_pair("grant_type", "authorization_code");
        serializer.append_pair("client_id", "test-client");
        serializer.append_pair("redirect_uri", "http://localhost/callback");
        serializer.append_pair("code", &code);
        serializer.append_pair("code_verifier", verifier);
        let body = serializer.finish();
        let token = raw_http(
            "POST",
            self.token_url().as_str(),
            &[("content-type", "application/x-www-form-urlencoded")],
            body.as_bytes(),
        )
        .await?;
        if !token.status.is_success() {
            return Err(format!("token returned {}", token.status).into());
        }
        Ok(serde_json::from_slice(&token.body)?)
    }

    pub async fn wait_for_refresh_request(&self) -> TestResult<()> {
        tokio::time::timeout(super::common::WAIT_TIMEOUT, self.pause.wait_until_entered())
            .await
            .map_err(|_| "timed out waiting for fake refresh request")?;
        Ok(())
    }

    pub fn release_refresh_response(&self) {
        self.pause.release();
    }

    pub async fn refresh_history_len(&self) -> TestResult<usize> {
        let response =
            raw_http("GET", &format!("{}/__refresh_history", self.base), &[], &[]).await?;
        if !response.status.is_success() {
            return Err(format!("history returned {}", response.status).into());
        }
        let body: Value = serde_json::from_slice(&response.body)?;
        let refreshes = body["refreshes"]
            .as_array()
            .ok_or("history response missing refreshes")?;
        Ok(refreshes.len())
    }
}

impl Drop for FakeAnthropic {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[derive(Debug, Deserialize)]
pub struct InitialTokens {
    pub access_token: String,
    pub refresh_token: String,
}

pub struct RawHttpResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

pub async fn raw_http(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<RawHttpResponse> {
    let url = Url::parse(url).expect("test URL parses");
    let host = url.host_str().expect("test URL host");
    let port = url.port_or_known_default().expect("test URL port");
    let addr = SocketAddr::new(
        host.parse()
            .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    );
    let mut target = url.path().to_owned();
    if let Some(query) = url.query() {
        target.push('?');
        target.push_str(query);
    }
    let authority = if url.port().is_some() {
        format!("{host}:{port}")
    } else {
        host.to_owned()
    };
    let mut request = format!(
        "{method} {target} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    parse_raw_response(&bytes)
}

fn parse_raw_response(bytes: &[u8]) -> std::io::Result<RawHttpResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing headers"))?;
    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing status"))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid status"))?;
    let mut headers = HeaderMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = http::header::HeaderName::from_bytes(name.trim().as_bytes())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            let value = http::HeaderValue::from_str(value.trim())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            headers.insert(name, value);
        }
    }
    let body = &bytes[header_end + 4..];
    let body = if headers
        .get(http::header::TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked(body)?
    } else {
        body.to_vec()
    };
    Ok(RawHttpResponse {
        status,
        headers,
        body: Bytes::from(body),
    })
}

fn decode_chunked(mut bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    loop {
        let line_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "chunk size"))?;
        let size_text = std::str::from_utf8(&bytes[..line_end])
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let size = usize::from_str_radix(size_text.trim(), 16)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        bytes = &bytes[line_end + 2..];
        if size == 0 {
            return Ok(decoded);
        }
        if bytes.len() < size + 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk body",
            ));
        }
        decoded.extend_from_slice(&bytes[..size]);
        bytes = &bytes[size + 2..];
    }
}
