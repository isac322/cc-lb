#![allow(dead_code)]

use std::fs;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_config::Config;
use cc_lb_server::{build_app_with_path, BuildError};
use fake_anthropic::{app as fake_anthropic_app, AppConfig};
use ring::digest::{digest, SHA256};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error, ProtocolVersion, SignatureScheme,
    SupportedProtocolVersion,
};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

pub const STREAM_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"stream through reload"}],"max_tokens":16,"stream":true}"#;

pub struct RunningTlsApp {
    pub proxy_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub cert_a_fingerprint: String,
    pub cert_b_fingerprint: String,
    pub cert_a_path: PathBuf,
    signals: cc_lb_server::signal::SignalHandle,
    server: Option<JoinHandle<Result<(), BuildError>>>,
    fake: Option<JoinHandle<Result<(), std::io::Error>>>,
    _dir: TempDir,
}

impl RunningTlsApp {
    pub async fn shutdown(mut self) {
        self.signals.start_shutdown();
        if let Some(server) = self.server.take() {
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .expect("server exits")
                .expect("server join")
                .expect("server ok");
        }
        if let Some(fake) = self.fake.take() {
            fake.abort();
        }
    }
}

impl Drop for RunningTlsApp {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        if let Some(fake) = self.fake.take() {
            fake.abort();
        }
    }
}

pub async fn start_tls_app(slow_mode_bps: u64) -> RunningTlsApp {
    let dir = tempfile::tempdir().expect("temp dir");
    let cert_path = dir.path().join("server-cert.pem");
    let key_path = dir.path().join("server-key.pem");
    fs::copy(fixture("cert-a.pem"), &cert_path).expect("copy cert a");
    fs::copy(fixture("key-a.pem"), &key_path).expect("copy key a");

    let fake_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake listener");
    let upstream_addr = fake_listener.local_addr().expect("fake addr");
    let fake_config = AppConfig {
        slow_mode_bps,
        files_cap_bytes: 104_857_600,
    };
    let fake =
        tokio::spawn(
            async move { axum::serve(fake_listener, fake_anthropic_app(fake_config)).await },
        );

    let proxy_addr = free_addr();
    let admin_addr = free_addr();
    let metrics_addr = free_addr();
    let config_path = dir.path().join("cc-lb.toml");
    write_config(
        &config_path,
        proxy_addr,
        admin_addr,
        metrics_addr,
        upstream_addr,
        &cert_path,
        &key_path,
    );
    let config = Config::load(&config_path).expect("load config");
    let app = build_app_with_path(config, Some(&config_path)).expect("build app");
    let signals = app.signal_handle();
    let server = tokio::spawn(async move { app.start().await });

    wait_tls_status(proxy_addr, &cert_path, "/healthz", 200).await;
    wait_plain_status(admin_addr, "/admin/health", 200).await;

    RunningTlsApp {
        proxy_addr,
        admin_addr,
        metrics_addr,
        cert_path,
        key_path,
        cert_a_fingerprint: fingerprint_for_cert(&fixture("cert-a.pem")),
        cert_b_fingerprint: fingerprint_for_cert(&fixture("cert-b.pem")),
        cert_a_path: fixture("cert-a.pem"),
        signals,
        server: Some(server),
        fake: Some(fake),
        _dir: dir,
    }
}

pub fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests dir")
        .parent()
        .expect("repo root")
        .join("crates")
        .join("cc-lb-server")
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

pub fn overwrite_pair(cert_path: &Path, key_path: &Path, cert_name: &str, key_name: &str) {
    fs::copy(fixture(cert_name), cert_path).expect("overwrite cert");
    fs::copy(fixture(key_name), key_path).expect("overwrite key");
}

pub async fn send_sighup() {
    let status = Command::new("kill")
        .arg("-HUP")
        .arg(std::process::id().to_string())
        .status()
        .await
        .expect("send SIGHUP");
    assert!(status.success(), "kill -HUP failed: {status}");
}

pub async fn wait_for_reloaded_cert(
    addr: SocketAddr,
    trust_cert: &Path,
    expected_fingerprint: &str,
) -> TlsResponse {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let last = match tls_get(addr, trust_cert, "/v1/models").await {
            Ok(response)
                if response.status == 200 && response.peer_fingerprint == expected_fingerprint =>
            {
                return response;
            }
            Ok(response) => format!(
                "status={} peer_fingerprint={}",
                response.status, response.peer_fingerprint
            ),
            Err(error) => error,
        };

        assert!(
            Instant::now() < deadline,
            "new certificate not observed; last={last}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn wait_tls_status(addr: SocketAddr, trust_cert: &Path, path: &str, status: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let last = match tls_get(addr, trust_cert, path).await {
            Ok(response) if response.status == status => return,
            Ok(response) => format!("status={} body={}", response.status, response.body),
            Err(error) => error,
        };
        assert!(
            Instant::now() < deadline,
            "TLS server not ready; last={last}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn wait_plain_status(addr: SocketAddr, path: &str, status: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let last = match plain_get(addr, path).await {
            Ok(response) if response.status == status => return,
            Ok(response) => format!("status={} body={}", response.status, response.body),
            Err(error) => error.to_string(),
        };
        assert!(
            Instant::now() < deadline,
            "plain HTTP server not ready; last={last}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn tls_get(
    addr: SocketAddr,
    trust_cert: &Path,
    path: &str,
) -> Result<TlsResponse, String> {
    tls_get_with_versions(addr, trust_cert, path, rustls::DEFAULT_VERSIONS).await
}

pub async fn tls_get_with_versions(
    addr: SocketAddr,
    trust_cert: &Path,
    path: &str,
    versions: &[&'static SupportedProtocolVersion],
) -> Result<TlsResponse, String> {
    let mut stream = tls_connect(addr, trust_cert, versions).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nx-api-key: sk-ant-test\r\nConnection: close\r\n\r\n"
    );
    let peer_fingerprint = peer_fingerprint(&stream);
    let protocol_version = stream.get_ref().1.protocol_version();
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    Ok(TlsResponse {
        status: status_code(&text),
        body: body(&text),
        peer_fingerprint,
        protocol_version,
    })
}

pub async fn start_streaming_post(
    addr: SocketAddr,
    trust_cert: &Path,
) -> Result<StreamingTlsResponse, String> {
    let mut stream = tls_connect(addr, trust_cert, rustls::DEFAULT_VERSIONS).await?;
    let request = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nx-api-key: sk-ant-test\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\naccept: text/event-stream\r\nx-fake-mode: slow\r\ncontent-length: {}\r\nConnection: close\r\n\r\n{}",
        STREAM_BODY.len(),
        STREAM_BODY
    );
    let peer_fingerprint = peer_fingerprint(&stream);
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    Ok(StreamingTlsResponse {
        stream,
        bytes: Vec::new(),
        peer_fingerprint,
    })
}

pub struct StreamingTlsResponse {
    stream: TlsStream<TcpStream>,
    bytes: Vec<u8>,
    pub peer_fingerprint: String,
}

impl StreamingTlsResponse {
    pub async fn read_until(&mut self, needle: &str, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            let text = String::from_utf8_lossy(&self.bytes).to_string();
            if text.contains(needle) {
                return text;
            }
            assert!(Instant::now() < deadline, "stream did not contain {needle}");

            let mut buffer = [0_u8; 4096];
            let read =
                tokio::time::timeout(Duration::from_millis(500), self.stream.read(&mut buffer))
                    .await
                    .expect("stream read timed out")
                    .expect("stream read");
            assert!(read > 0, "stream ended before {needle}");
            self.bytes.extend_from_slice(&buffer[..read]);
        }
    }

    pub async fn read_to_end(mut self, timeout: Duration) -> String {
        tokio::time::timeout(timeout, self.stream.read_to_end(&mut self.bytes))
            .await
            .expect("stream completion timed out")
            .expect("stream completion");
        String::from_utf8_lossy(&self.bytes).to_string()
    }
}

pub struct TlsResponse {
    pub status: u16,
    pub body: String,
    pub peer_fingerprint: String,
    pub protocol_version: Option<ProtocolVersion>,
}

pub struct PlainResponse {
    pub status: u16,
    pub body: String,
}

pub fn fingerprint_for_cert(path: &Path) -> String {
    let mut certs = load_certs(path);
    let cert = certs.remove(0);
    fingerprint_der(cert.as_ref())
}

pub fn write_evidence(name: &str, contents: &str) {
    let path = evidence_path(name);
    fs::create_dir_all(path.parent().expect("evidence dir")).expect("create evidence dir");
    fs::write(path, contents).expect("write evidence");
}

pub fn evidence_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests dir")
        .parent()
        .expect("repo root")
        .join(".omo")
        .join("evidence")
        .join(name)
}

async fn tls_connect(
    addr: SocketAddr,
    trust_cert: &Path,
    versions: &[&'static SupportedProtocolVersion],
) -> Result<TlsStream<TcpStream>, String> {
    let connector = TlsConnector::from(Arc::new(client_config(trust_cert, versions)));
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|error| error.to_string())?;
    let server_name = ServerName::try_from("localhost")
        .expect("server name")
        .to_owned();
    connector
        .connect(server_name, stream)
        .await
        .map_err(|error| error.to_string())
}

fn client_config(
    trust_cert: &Path,
    versions: &[&'static SupportedProtocolVersion],
) -> ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(versions)
        .expect("protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(FingerprintVerifier {
            expected_fingerprint: fingerprint_for_cert(trust_cert),
            provider,
        }))
        .with_no_client_auth()
}

#[derive(Debug)]
struct FingerprintVerifier {
    expected_fingerprint: String,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        if fingerprint_der(end_entity.as_ref()) == self.expected_fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(Error::InvalidCertificate(CertificateError::UnknownIssuer))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn load_certs(path: &Path) -> Vec<CertificateDer<'static>> {
    let bytes = fs::read(path).expect("read cert");
    let mut reader = BufReader::new(bytes.as_slice());
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("parse certs")
}

fn peer_fingerprint(stream: &TlsStream<TcpStream>) -> String {
    let connection = stream.get_ref().1;
    let cert = connection
        .peer_certificates()
        .and_then(|certs| certs.first())
        .expect("peer certificate");
    fingerprint_der(cert.as_ref())
}

fn fingerprint_der(bytes: &[u8]) -> String {
    let digest = digest(&SHA256, bytes);
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

async fn plain_get(addr: SocketAddr, path: &str) -> std::io::Result<PlainResponse> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    Ok(PlainResponse {
        status: status_code(&text),
        body: body(&text),
    })
}

fn status_code(text: &str) -> u16 {
    text.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0)
}

fn body(text: &str) -> String {
    text.split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default()
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free addr");
    listener.local_addr().expect("free addr")
}

fn write_config(
    path: &Path,
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    metrics_addr: SocketAddr,
    upstream_addr: SocketAddr,
    cert_path: &Path,
    key_path: &Path,
) {
    unsafe {
        std::env::set_var(
            "CC_LB_MASTER_KEY",
            "0000000000000000000000000000000000000000000000000000000000000000",
        );
        std::env::set_var("CC_LB_ADMIN_TOKEN", "admin-token");
    }
    let storage_path = path.with_file_name("cc-lb.redb");
    let storage_path = storage_path.display();
    let config = format!(
        r#"[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "{admin_addr}"
metrics_addr = "{metrics_addr}"

[listener.tls]
cert_path = "{}"
key_path = "{}"
reload_on_sighup = true

[upstreams.fake]
kind = "custom"
base_url = "http://{upstream_addr}"
auth_strategy = "api_key"

[principals.api-key]
allowed_models = ["*"]

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "api-key"
upstream_kind = "anthropic_key"
upstream_credential_ref = "fake_anthropic"

[plugins]
observability_hooks = []

[storage]
kind = "redb"
path = "{storage_path}"

[aead]
key_env = "CC_LB_MASTER_KEY"

[admin]
token_env = "CC_LB_ADMIN_TOKEN"
"#,
        cert_path.display(),
        key_path.display()
    );
    fs::write(path, config).expect("write config");
}
