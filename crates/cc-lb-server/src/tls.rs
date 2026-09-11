use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::serve::Listener;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;

pub struct TlsState {
    pub config: ArcSwap<rustls::ServerConfig>,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    metrics: TlsReloadMetrics,
}

impl TlsState {
    pub fn from_paths(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, TlsError> {
        let cert_path = cert_path.as_ref().to_path_buf();
        let key_path = key_path.as_ref().to_path_buf();
        let config = load_certs(&cert_path, &key_path)?;

        Ok(Self {
            config: ArcSwap::from_pointee(config),
            cert_path,
            key_path,
            metrics: TlsReloadMetrics,
        })
    }

    pub fn reload(&self) -> Result<(), TlsError> {
        match load_certs(&self.cert_path, &self.key_path) {
            Ok(config) => {
                self.config.store(Arc::new(config));
                self.metrics.record("success");
                Ok(())
            }
            Err(error) => {
                self.metrics.record("failure");
                Err(error)
            }
        }
    }

    pub fn current(&self) -> Arc<rustls::ServerConfig> {
        self.config.load_full()
    }
}

#[derive(Clone)]
pub struct ReloadableAcceptor(Arc<TlsState>);

impl ReloadableAcceptor {
    pub fn new(state: Arc<TlsState>) -> Self {
        Self(state)
    }

    pub fn accept<IO>(&self, stream: IO) -> tokio_rustls::Accept<IO>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        tokio_rustls::TlsAcceptor::from(self.0.current()).accept(stream)
    }
}

pub struct ReloadableListener {
    listener: TcpListener,
    acceptor: ReloadableAcceptor,
}

impl ReloadableListener {
    pub fn new(listener: TcpListener, state: Arc<TlsState>) -> Self {
        Self {
            listener,
            acceptor: ReloadableAcceptor::new(state),
        }
    }
}

impl Listener for ReloadableListener {
    type Io = TlsStream<TcpStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.listener.accept().await {
                Ok(connection) => connection,
                Err(error) => {
                    handle_accept_error(error).await;
                    continue;
                }
            };

            match self.acceptor.accept(stream).await {
                Ok(stream) => return (stream, addr),
                Err(error) => {
                    tracing::warn!(%addr, error = %error, "TLS handshake failed");
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

const IN_MEMORY_CERT_PATH: &str = "<in-memory-cert-pem>";
const IN_MEMORY_KEY_PATH: &str = "<in-memory-key-pem>";

pub fn load_certs(cert_path: &Path, key_path: &Path) -> Result<rustls::ServerConfig, TlsError> {
    let cert_pem = read_tls_file(cert_path)?;
    let certs = parse_certificate_chain(&cert_pem, cert_path)?;
    let key_pem = read_tls_file(key_path)?;
    let key = parse_private_key(&key_pem, key_path)?;
    build_server_config(certs, key)
}

pub fn parse_certs_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<rustls::ServerConfig, TlsError> {
    parse_certs_pem_with_paths(
        cert_pem,
        key_pem,
        Path::new(IN_MEMORY_CERT_PATH),
        Path::new(IN_MEMORY_KEY_PATH),
    )
}

fn read_tls_file(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::ReadFile {
        path: path.to_path_buf(),
        source,
    })
}

fn parse_certs_pem_with_paths(
    cert_pem: &[u8],
    key_pem: &[u8],
    cert_path: &Path,
    key_path: &Path,
) -> Result<rustls::ServerConfig, TlsError> {
    let certs = parse_certificate_chain(cert_pem, cert_path)?;
    let key = parse_private_key(key_pem, key_path)?;
    build_server_config(certs, key)
}

fn build_server_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<rustls::ServerConfig, TlsError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|source| TlsError::BuildConfig { source })?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|source| TlsError::BuildConfig { source })?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

fn parse_certificate_chain(
    cert_pem: &[u8],
    path: &Path,
) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let mut reader = std::io::BufReader::new(cert_pem);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::ParseCert {
            path: path.to_path_buf(),
            source,
        })?;

    if certs.is_empty() {
        return Err(TlsError::NoCertificates {
            path: path.to_path_buf(),
        });
    }

    Ok(certs)
}

fn parse_private_key(key_pem: &[u8], path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    if let Some(key) = first_pkcs8_key(key_pem, path)? {
        return Ok(PrivateKeyDer::from(key));
    }

    if let Some(key) = first_rsa_key(key_pem, path)? {
        return Ok(PrivateKeyDer::from(key));
    }

    Err(TlsError::NoPrivateKey {
        path: path.to_path_buf(),
    })
}

fn first_pkcs8_key(
    bytes: &[u8],
    path: &Path,
) -> Result<Option<rustls::pki_types::PrivatePkcs8KeyDer<'static>>, TlsError> {
    let mut reader = std::io::BufReader::new(bytes);
    let key = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .next()
        .transpose()
        .map_err(|source| TlsError::ParseKey {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(key)
}

fn first_rsa_key(
    bytes: &[u8],
    path: &Path,
) -> Result<Option<rustls::pki_types::PrivatePkcs1KeyDer<'static>>, TlsError> {
    let mut reader = std::io::BufReader::new(bytes);
    let key = rustls_pemfile::rsa_private_keys(&mut reader)
        .next()
        .transpose()
        .map_err(|source| TlsError::ParseKey {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(key)
}

async fn handle_accept_error(error: io::Error) {
    if matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    ) {
        return;
    }

    tracing::error!(error = %error, "accept error");
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[derive(Clone, Copy)]
struct TlsReloadMetrics;

impl TlsReloadMetrics {
    fn record(self, outcome: &'static str) {
        metrics::counter!("cc_lb_tls_reload_total", "outcome" => outcome).increment(1);
    }
}

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("failed to read TLS file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse certificate PEM {path}: {source}")]
    ParseCert {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse private key PEM {path}: {source}")]
    ParseKey {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("certificate PEM contains no certificates: {path}")]
    NoCertificates { path: PathBuf },
    #[error("private key PEM contains no PKCS#8 or RSA private key: {path}")]
    NoPrivateKey { path: PathBuf },
    #[error("failed to build TLS server config: {source}")]
    BuildConfig {
        #[source]
        source: rustls::Error,
    },
}
