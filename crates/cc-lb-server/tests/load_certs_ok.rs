mod tls_common;

use cc_lb_server::tls::load_certs;

#[test]
fn load_certs_ok() {
    let config = load_certs(
        &tls_common::fixture("cert-a.pem"),
        &tls_common::fixture("key-a.pem"),
    )
    .unwrap();

    assert_eq!(
        config.alpn_protocols,
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    );
    println!(
        "load_certs_ok PASSED: cert-a.pem loaded with ALPN {:?}",
        config.alpn_protocols
    );
}
