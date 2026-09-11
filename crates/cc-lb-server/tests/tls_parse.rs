use std::path::Path;

use cc_lb_server::tls::{TlsError, parse_certs_pem};

const CERT_A: &[u8] = include_bytes!("fixtures/tls/cert-a.pem");
const CERT_BAD: &[u8] = include_bytes!("fixtures/tls/cert-bad.pem");
const KEY_A: &[u8] = include_bytes!("fixtures/tls/key-a.pem");

#[derive(Debug, PartialEq, Eq)]
enum TlsErrorCategory {
    ParseCert,
    ParseKey,
    NoCertificates,
    NoPrivateKey,
    BuildConfig,
}

fn error_category(error: &TlsError) -> TlsErrorCategory {
    match error {
        TlsError::ParseCert { .. } => TlsErrorCategory::ParseCert,
        TlsError::ParseKey { .. } => TlsErrorCategory::ParseKey,
        TlsError::NoCertificates { .. } => TlsErrorCategory::NoCertificates,
        TlsError::NoPrivateKey { .. } => TlsErrorCategory::NoPrivateKey,
        TlsError::BuildConfig { .. } => TlsErrorCategory::BuildConfig,
        TlsError::ReadFile { .. } => panic!("byte parser must not return a file error"),
    }
}

#[test]
fn parse_certs_pem_builds_server_config_from_bytes() {
    let config = parse_certs_pem(CERT_A, KEY_A).unwrap();

    assert_eq!(
        config.alpn_protocols,
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    );
}

#[test]
fn parse_certs_pem_rejects_invalid_certificate() {
    let error = parse_certs_pem(CERT_BAD, KEY_A).expect_err("invalid certificate must fail");

    assert_eq!(error_category(&error), TlsErrorCategory::BuildConfig);
}

#[test]
fn parse_certs_pem_rejects_missing_private_key() {
    let error = parse_certs_pem(CERT_A, CERT_A)
        .expect_err("certificate-only PEM must not provide a private key");

    match error {
        TlsError::NoPrivateKey { path } => {
            assert_eq!(path, Path::new("<in-memory-key-pem>"));
        }
        other => panic!("expected missing private key error, got {other}"),
    }
}
