use crate::tls_common;

use std::path::Path;

use cc_lb_server::tls::{TlsError, load_certs, parse_certs_pem};

const CERT_A: &[u8] = include_bytes!("../fixtures/tls/cert-a.pem");
const CERT_BAD: &[u8] = include_bytes!("../fixtures/tls/cert-bad.pem");
const KEY_A: &[u8] = include_bytes!("../fixtures/tls/key-a.pem");

#[derive(Debug, PartialEq, Eq)]
enum TlsErrorCategory {
    ReadFile,
    ParseCert,
    ParseKey,
    NoCertificates,
    NoPrivateKey,
    BuildConfig,
}

fn error_category(error: &TlsError) -> TlsErrorCategory {
    match error {
        TlsError::ReadFile { .. } => TlsErrorCategory::ReadFile,
        TlsError::ParseCert { .. } => TlsErrorCategory::ParseCert,
        TlsError::ParseKey { .. } => TlsErrorCategory::ParseKey,
        TlsError::NoCertificates { .. } => TlsErrorCategory::NoCertificates,
        TlsError::NoPrivateKey { .. } => TlsErrorCategory::NoPrivateKey,
        TlsError::BuildConfig { .. } => TlsErrorCategory::BuildConfig,
    }
}

#[test]
fn t3__load_certs_ok() {
    let config = load_certs(
        &tls_common::fixture("cert-a.pem"),
        &tls_common::fixture("key-a.pem"),
    )
    .unwrap();

    assert_eq!(
        config.alpn_protocols,
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    );
}

#[test]
fn t3__byte_and_path_loads_have_observable_parity() {
    let byte_config = parse_certs_pem(CERT_A, KEY_A).unwrap();
    let path_config = load_certs(
        &tls_common::fixture("cert-a.pem"),
        &tls_common::fixture("key-a.pem"),
    )
    .unwrap();
    assert_eq!(byte_config.alpn_protocols, path_config.alpn_protocols);

    let byte_bad_cert =
        parse_certs_pem(CERT_BAD, KEY_A).expect_err("invalid certificate bytes must fail");
    let path_bad_cert = load_certs(
        &tls_common::fixture("cert-bad.pem"),
        &tls_common::fixture("key-a.pem"),
    )
    .expect_err("invalid certificate file must fail");
    assert_eq!(
        error_category(&byte_bad_cert),
        error_category(&path_bad_cert)
    );

    let byte_missing_key =
        parse_certs_pem(CERT_A, CERT_A).expect_err("certificate-only key bytes must fail");
    let path_missing_key = load_certs(
        &tls_common::fixture("cert-a.pem"),
        &tls_common::fixture("cert-a.pem"),
    )
    .expect_err("certificate-only key file must fail");
    assert_eq!(
        error_category(&byte_missing_key),
        error_category(&path_missing_key)
    );
}

#[test]
fn t3__load_certs_preserves_key_read_before_certificate_validation() {
    let error = load_certs(
        &tls_common::fixture("cert-bad.pem"),
        &tls_common::fixture("missing-key.pem"),
    )
    .expect_err("missing key must fail before certificate validation");

    assert_eq!(error_category(&error), TlsErrorCategory::ReadFile);
}

#[test]
fn t3__load_certs_preserves_source_paths_in_errors() {
    let cert_without_certificate_path = tls_common::fixture("key-a.pem");
    let error = load_certs(
        &cert_without_certificate_path,
        &tls_common::fixture("key-a.pem"),
    )
    .expect_err("certificate path containing only a key must fail");
    match error {
        TlsError::NoCertificates { path } => {
            assert_eq!(path, cert_without_certificate_path);
        }
        other => panic!("expected missing certificate error, got {other}"),
    }

    let key_without_private_key_path = tls_common::fixture("cert-b.pem");
    let error = load_certs(
        &tls_common::fixture("cert-a.pem"),
        &key_without_private_key_path,
    )
    .expect_err("key path containing only a certificate must fail");
    match error {
        TlsError::NoPrivateKey { path } => {
            assert_eq!(path, key_without_private_key_path);
        }
        other => panic!("expected missing private key error, got {other}"),
    }

    let missing_key_path = tls_common::fixture("missing-key.pem");
    let error = load_certs(&tls_common::fixture("cert-a.pem"), &missing_key_path)
        .expect_err("missing key file must fail");
    match error {
        TlsError::ReadFile { path, .. } => assert_eq!(path, missing_key_path),
        other => panic!("expected key read error, got {other}"),
    }

    let missing_cert_path = tls_common::fixture("missing-cert.pem");
    let error = load_certs(&missing_cert_path, Path::new("unread-key.pem"))
        .expect_err("missing certificate file must fail first");
    match error {
        TlsError::ReadFile { path, .. } => assert_eq!(path, missing_cert_path),
        other => panic!("expected certificate read error, got {other}"),
    }
}
