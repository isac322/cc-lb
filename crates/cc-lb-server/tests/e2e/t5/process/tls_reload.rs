use super::support::{self, ChildProcess, ServerAddrs, Signal};

use std::fs;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error, SignatureScheme,
    SupportedProtocolVersion,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

const STREAM_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"stream through reload"}],"max_tokens":16,"stream":true}"#;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const STREAM_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t5__process_reload_cert_sighup_keeps_inflight_stream() {
    let fixture = support::Fixture::new("tls-reload");
    let cert_a = tls_fixture("cert-a.pem");
    let key_a = tls_fixture("key-a.pem");
    let cert_b = tls_fixture("cert-b.pem");
    let key_b = tls_fixture("key-b.pem");
    let bad_cert = tls_fixture("cert-bad.pem");
    let active_cert = fixture.path("server-cert.pem");
    let active_key = fixture.path("server-key.pem");
    fs::copy(&cert_a, &active_cert).expect("install certificate A");
    fs::copy(&key_a, &active_key).expect("install key A");

    let cert_a_fingerprint = fingerprint_for_cert(&cert_a);
    let cert_b_fingerprint = fingerprint_for_cert(&cert_b);
    assert_ne!(cert_a_fingerprint, cert_b_fingerprint);

    let upstream = spawn_slow_fake_anthropic().await;
    let reservations = support::ReservedAddrs::bind().expect("reserve server addresses");
    let addrs = ServerAddrs {
        proxy: reservations.proxy.addr(),
        admin: reservations.admin.addr(),
        metrics: reservations.metrics.addr(),
    };
    let data_dir = fixture.path("data");
    fs::create_dir_all(&data_dir).expect("create runtime data directory");
    let storage_path = fixture.path("cc-lb.sqlite");
    let config_path = fixture.path("cc-lb.toml");
    let config = tls_server_config(addrs, &storage_path, &data_dir, &active_cert, &active_key);
    fs::write(&config_path, config).expect("write TLS process config");

    let released_addrs = reservations.release();
    assert_eq!(released_addrs.proxy, addrs.proxy);
    assert_eq!(released_addrs.admin, addrs.admin);
    assert_eq!(released_addrs.metrics, addrs.metrics);

    let mut command = support::serve_command(&config_path);
    command
        .env("CC_LB_MASTER_KEY", support::MASTER_KEY_HEX)
        .env("CC_LB_ADMIN_TOKEN", support::ADMIN_TOKEN)
        .env("RUST_LOG", "info,hyper=warn,hyper_util=warn,axum=warn");
    let mut child = ChildProcess::spawn(command).expect("spawn cc-lb TLS process");

    let admin_health = support::http_request("GET", addrs.admin, "/admin/health", &[], "");
    let admin_ready = support::wait_http_status(
        &mut child,
        addrs.admin,
        &admin_health,
        200,
        support::READY_TIMEOUT,
    )
    .await
    .expect("plaintext admin listener becomes ready");
    assert_eq!(admin_ready.status, 200);

    let metrics_request = support::http_request("GET", addrs.metrics, "/metrics", &[], "");
    let metrics_ready = support::wait_http_status(
        &mut child,
        addrs.metrics,
        &metrics_request,
        200,
        support::READY_TIMEOUT,
    )
    .await
    .expect("plaintext metrics listener becomes ready");
    assert_eq!(metrics_ready.status, 200);

    let proxy_health = wait_tls_response(
        &mut child,
        addrs.proxy,
        &cert_a,
        "/healthz",
        200,
        &cert_a_fingerprint,
        support::READY_TIMEOUT,
    )
    .await;
    assert_eq!(proxy_health.status, 200);
    assert_eq!(proxy_health.peer_fingerprint, cert_a_fingerprint);

    seed_runtime(&mut child, addrs.admin, upstream.addr).await;

    let before = wait_tls_response(
        &mut child,
        addrs.proxy,
        &cert_a,
        "/v1/models",
        200,
        &cert_a_fingerprint,
        support::READY_TIMEOUT,
    )
    .await;
    assert_eq!(before.status, 200);
    assert_eq!(before.peer_fingerprint, cert_a_fingerprint);

    let tls12_before = openssl_tls12_fingerprint(addrs.proxy, &cert_a).await;
    assert_eq!(tls12_before, cert_a_fingerprint);

    let mut stream = start_streaming_post(addrs.proxy, &cert_a)
        .await
        .expect("start in-flight cert-A stream");
    assert_eq!(stream.peer_fingerprint, cert_a_fingerprint);
    let partial = stream
        .read_until(&mut child, "content_block_delta", IO_TIMEOUT)
        .await;
    assert!(partial.contains(" 200 "));
    assert!(!partial.contains("message_stop"));

    fs::copy(&bad_cert, &active_cert).expect("install invalid certificate");
    child
        .send_signal(Signal::Hangup)
        .expect("send bad-certificate SIGHUP");
    let failed_metrics = wait_metric_at_least(
        &mut child,
        addrs.metrics,
        "failure",
        1.0,
        support::READY_TIMEOUT,
    )
    .await;
    assert_eq!(metric_value(&failed_metrics, "failure"), Some(1.0));

    let after_failed_reload = tls_get(addrs.proxy, &cert_a, "/v1/models")
        .await
        .expect("request after rejected certificate reload");
    assert_eq!(after_failed_reload.status, 200);
    assert_eq!(after_failed_reload.peer_fingerprint, cert_a_fingerprint);
    let tls12_after_failed_reload = openssl_tls12_fingerprint(addrs.proxy, &cert_a).await;
    assert_eq!(tls12_after_failed_reload, cert_a_fingerprint);

    fs::copy(&cert_b, &active_cert).expect("install certificate B");
    fs::copy(&key_b, &active_key).expect("install key B");
    child
        .send_signal(Signal::Hangup)
        .expect("send certificate-B SIGHUP");
    let successful_metrics = wait_metric_at_least(
        &mut child,
        addrs.metrics,
        "success",
        1.0,
        support::READY_TIMEOUT,
    )
    .await;
    assert_eq!(metric_value(&successful_metrics, "failure"), Some(1.0));
    assert_eq!(metric_value(&successful_metrics, "success"), Some(1.0));

    let after = tls_get(addrs.proxy, &cert_b, "/v1/models")
        .await
        .expect("post-reload request");
    assert_eq!(after.status, 200);
    assert_eq!(after.peer_fingerprint, cert_b_fingerprint);

    let tls12_after = openssl_tls12_fingerprint(addrs.proxy, &cert_b).await;
    assert_eq!(tls12_after, cert_b_fingerprint);

    let completed = stream.read_to_end(&mut child, STREAM_TIMEOUT).await;
    assert!(completed.contains("message_stop"));

    let admin_after = support::raw_http(addrs.admin, &admin_health, IO_TIMEOUT)
        .await
        .expect("plaintext admin remains reachable");
    assert_eq!(admin_after.status, 200);
    let metrics_after = support::raw_http(addrs.metrics, &metrics_request, IO_TIMEOUT)
        .await
        .expect("plaintext metrics remains reachable");
    assert_eq!(metrics_after.status, 200);
    assert_eq!(metric_value(&metrics_after.body, "failure"), Some(1.0));
    assert_eq!(metric_value(&metrics_after.body, "success"), Some(1.0));

    let proxy_health_after = tls_get(addrs.proxy, &cert_b, "/healthz")
        .await
        .expect("TLS proxy health remains reachable");
    assert_eq!(proxy_health_after.status, 200);
    assert_eq!(proxy_health_after.peer_fingerprint, cert_b_fingerprint);
    child
        .send_signal(Signal::Terminate)
        .expect("send graceful shutdown signal");
    let output = child
        .wait_for_exit(support::EXIT_TIMEOUT)
        .await
        .expect("cc-lb exits after SIGTERM");
    assert_eq!(
        output.status.code(),
        Some(0),
        "cc-lb exit status={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        output.stdout,
        output.stderr
    );
}

struct RunningUpstream {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for RunningUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn spawn_slow_fake_anthropic() -> RunningUpstream {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake Anthropic listener");
    let addr = listener.local_addr().expect("fake Anthropic address");
    let app = fake_anthropic_app(AppConfig {
        slow_mode_bps: 8_192,
        ..AppConfig::default()
    });
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve fake Anthropic");
    });
    RunningUpstream { addr, task }
}

fn tls_server_config(
    addrs: ServerAddrs,
    storage_path: &Path,
    data_dir: &Path,
    cert_path: &Path,
    key_path: &Path,
) -> String {
    let mut config =
        support::base_server_config(addrs, storage_path, data_dir, "CC_LB_MASTER_KEY", "api-key");
    config.push_str(&format!(
        r#"

[listener.tls]
cert_path = "{}"
key_path = "{}"
reload_on_sighup = true
"#,
        cert_path.display(),
        key_path.display()
    ));
    config
}

async fn seed_runtime(child: &mut ChildProcess, admin_addr: SocketAddr, upstream_addr: SocketAddr) {
    let authorization = format!("Bearer {}", support::ADMIN_TOKEN);
    let principal_request = support::http_request(
        "POST",
        admin_addr,
        "/admin/v1/principals",
        &[
            ("Authorization", authorization.as_str()),
            ("content-type", "application/json"),
        ],
        r#"{"name":"api-key","kind":"machine","allowed_models":["*"]}"#,
    );
    child.assert_running("seed principal");
    let principal = support::raw_http(admin_addr, &principal_request, IO_TIMEOUT)
        .await
        .expect("seed principal");
    assert!(
        matches!(principal.status, 201 | 409),
        "seed principal status={} body={}",
        principal.status,
        principal.body
    );

    let upstream_body = format!(
        r#"{{"name":"fake_anthropic","kind":"anthropic_api_key","base_url":"http://{upstream_addr}","api_key_value":"sk-ant-test"}}"#
    );
    let upstream_request = support::http_request(
        "POST",
        admin_addr,
        "/admin/v1/upstreams",
        &[
            ("Authorization", authorization.as_str()),
            ("content-type", "application/json"),
        ],
        &upstream_body,
    );
    child.assert_running("seed upstream");
    let upstream = support::raw_http(admin_addr, &upstream_request, IO_TIMEOUT)
        .await
        .expect("seed upstream");
    assert!(
        matches!(upstream.status, 201 | 409),
        "seed upstream status={} body={}",
        upstream.status,
        upstream.body
    );
}

async fn wait_tls_response(
    child: &mut ChildProcess,
    addr: SocketAddr,
    trust_cert: &Path,
    path: &str,
    expected_status: u16,
    expected_fingerprint: &str,
    timeout: Duration,
) -> TlsResponse {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(20));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        child.assert_running("wait for TLS response");
        let last = match tls_get(addr, trust_cert, path).await {
            Ok(response)
                if response.status == expected_status
                    && response.peer_fingerprint == expected_fingerprint =>
            {
                return response;
            }
            Ok(response) => format!(
                "status={} peer_fingerprint={} body={}",
                response.status, response.peer_fingerprint, response.body
            ),
            Err(error) => error,
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "TLS endpoint {addr} did not return status={expected_status} fingerprint={expected_fingerprint} within {timeout:?}; last={last}\nstdout:\n{}\nstderr:\n{}",
            child.stdout_snapshot(),
            child.stderr_snapshot()
        );
        poll.tick().await;
    }
}

async fn wait_metric_at_least(
    child: &mut ChildProcess,
    metrics_addr: SocketAddr,
    outcome: &str,
    expected: f64,
    timeout: Duration,
) -> String {
    let request = support::http_request("GET", metrics_addr, "/metrics", &[], "");
    let deadline = tokio::time::Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(20));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        child.assert_running("wait for TLS reload metric");
        let last = match support::raw_http(metrics_addr, &request, Duration::from_millis(500)).await
        {
            Ok(response) if response.status == 200 => {
                if metric_value(&response.body, outcome).is_some_and(|value| value >= expected) {
                    return response.body;
                }
                format!(
                    "outcome={outcome} value={:?}",
                    metric_value(&response.body, outcome)
                )
            }
            Ok(response) => format!("status={} body={}", response.status, response.body),
            Err(error) => error.to_string(),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "TLS reload metric outcome={outcome} did not reach {expected} within {timeout:?}; last={last}\nstdout:\n{}\nstderr:\n{}",
            child.stdout_snapshot(),
            child.stderr_snapshot()
        );
        poll.tick().await;
    }
}

fn metric_value(metrics: &str, outcome: &str) -> Option<f64> {
    let prefix = format!("cc_lb_tls_reload_total{{outcome=\"{outcome}\"}}");
    metrics.lines().find_map(|line| {
        line.strip_prefix(&prefix)
            .and_then(|value| value.trim().parse::<f64>().ok())
    })
}

async fn tls_get(addr: SocketAddr, trust_cert: &Path, path: &str) -> Result<TlsResponse, String> {
    let mut stream = tls_connect(addr, trust_cert, rustls::DEFAULT_VERSIONS).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nx-api-key: sk-ant-test\r\nConnection: close\r\n\r\n"
    );
    let peer_fingerprint = peer_fingerprint(&stream);
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    tokio::time::timeout(IO_TIMEOUT, stream.read_to_end(&mut bytes))
        .await
        .map_err(|_| "TLS response timed out".to_owned())?
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    Ok(TlsResponse {
        status: status_code(&text),
        body: response_body(&text),
        peer_fingerprint,
    })
}

async fn start_streaming_post(
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

struct StreamingTlsResponse {
    stream: TlsStream<TcpStream>,
    bytes: Vec<u8>,
    peer_fingerprint: String,
}

impl StreamingTlsResponse {
    async fn read_until(
        &mut self,
        child: &mut ChildProcess,
        needle: &str,
        timeout: Duration,
    ) -> String {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let text = String::from_utf8_lossy(&self.bytes).to_string();
            if text.contains(needle) {
                return text;
            }
            child.assert_running("read partial SSE frame");
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "stream did not contain {needle:?} within {timeout:?}\nstdout:\n{}\nstderr:\n{}",
                child.stdout_snapshot(),
                child.stderr_snapshot()
            );
            let mut buffer = [0_u8; 4096];
            match tokio::time::timeout(
                remaining.min(Duration::from_millis(500)),
                self.stream.read(&mut buffer),
            )
            .await
            {
                Err(_) => continue,
                Ok(Err(error)) => panic!("stream read before {needle:?}: {error}"),
                Ok(Ok(0)) => panic!("stream ended before {needle:?}"),
                Ok(Ok(read)) => self.bytes.extend_from_slice(&buffer[..read]),
            }
        }
    }

    async fn read_to_end(mut self, child: &mut ChildProcess, timeout: Duration) -> String {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            child.assert_running("complete in-flight SSE stream");
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "stream completion timed out after {timeout:?}\nstdout:\n{}\nstderr:\n{}",
                child.stdout_snapshot(),
                child.stderr_snapshot()
            );
            let mut buffer = [0_u8; 4096];
            match tokio::time::timeout(
                remaining.min(Duration::from_millis(500)),
                self.stream.read(&mut buffer),
            )
            .await
            {
                Err(_) => continue,
                Ok(Err(error)) => panic!("stream completion: {error}"),
                Ok(Ok(0)) => return String::from_utf8_lossy(&self.bytes).to_string(),
                Ok(Ok(read)) => self.bytes.extend_from_slice(&buffer[..read]),
            }
        }
    }
}

struct TlsResponse {
    status: u16,
    body: String,
    peer_fingerprint: String,
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
    tokio::time::timeout(IO_TIMEOUT, connector.connect(server_name, stream))
        .await
        .map_err(|_| "TLS handshake timed out".to_owned())?
        .map_err(|error| error.to_string())
}

fn client_config(
    trust_cert: &Path,
    versions: &[&'static SupportedProtocolVersion],
) -> ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(versions)
        .expect("TLS protocol versions")
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

async fn openssl_tls12_fingerprint(addr: SocketAddr, trust_cert: &Path) -> String {
    let address = addr.to_string();
    let trust_cert = trust_cert
        .to_str()
        .expect("TLS fixture certificate path is UTF-8");
    let mut child = Command::new("openssl")
        .args([
            "s_client",
            "-tls1_2",
            "-connect",
            &address,
            "-servername",
            "localhost",
            "-CAfile",
            trust_cert,
            "-verify_return_error",
            "-showcerts",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn openssl s_client");
    let mut stdin = child.stdin.take().expect("openssl s_client stdin");
    stdin
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write health request to openssl s_client");
    stdin
        .shutdown()
        .await
        .expect("close openssl s_client stdin");
    drop(stdin);
    let output = tokio::time::timeout(IO_TIMEOUT, child.wait_with_output())
        .await
        .expect("openssl s_client timed out")
        .expect("wait for openssl s_client");
    assert!(
        output.status.success(),
        "openssl s_client -tls1_2 failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("openssl s_client stdout is UTF-8");
    let certificate = first_pem_certificate(&stdout);

    let mut child = Command::new("openssl")
        .args(["x509", "-noout", "-fingerprint", "-sha256"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn openssl x509");
    child
        .stdin
        .take()
        .expect("openssl x509 stdin")
        .write_all(certificate.as_bytes())
        .await
        .expect("write certificate to openssl x509");
    let output = tokio::time::timeout(IO_TIMEOUT, child.wait_with_output())
        .await
        .expect("openssl x509 timed out")
        .expect("wait for openssl x509");
    assert!(
        output.status.success(),
        "openssl x509 failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let fingerprint = String::from_utf8(output.stdout).expect("openssl x509 stdout is UTF-8");
    fingerprint
        .trim()
        .split_once('=')
        .map(|(_, value)| value.trim().to_ascii_uppercase())
        .expect("openssl x509 fingerprint output")
}

fn first_pem_certificate(transcript: &str) -> &str {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let start = transcript
        .find(BEGIN)
        .expect("s_client returned a certificate");
    let end = transcript[start..]
        .find(END)
        .map(|offset| start + offset + END.len())
        .expect("s_client returned a complete certificate");
    &transcript[start..end]
}

fn tls_fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

fn fingerprint_for_cert(path: &Path) -> String {
    let bytes = fs::read(path).expect("read certificate");
    let mut reader = BufReader::new(bytes.as_slice());
    let certificate = rustls_pemfile::certs(&mut reader)
        .next()
        .expect("certificate PEM contains a certificate")
        .expect("parse certificate PEM");
    fingerprint_der(certificate.as_ref())
}

fn peer_fingerprint(stream: &TlsStream<TcpStream>) -> String {
    let certificate = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .expect("TLS peer certificate");
    fingerprint_der(certificate.as_ref())
}

fn fingerprint_der(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

fn status_code(response: &str) -> u16 {
    response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0)
}

fn response_body(response: &str) -> String {
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default()
}
