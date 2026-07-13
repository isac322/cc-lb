use crate::tls_common;

use std::sync::Arc;

use cc_lb_server::tls::{TlsState, load_certs};

#[test]
fn load_bad_cert_keeps_current_snapshot() {
    let guard = tls_common::init_metrics();
    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = tls_common::copy_pair(&dir, "cert-a.pem", "key-a.pem");
    let state = TlsState::from_paths(&cert_path, &key_path).unwrap();
    let before = state.current();

    std::fs::copy(tls_common::fixture("cert-bad.pem"), &cert_path).unwrap();

    assert!(load_certs(&cert_path, &key_path).is_err());
    assert!(state.reload().is_err());
    let after = state.current();
    let unchanged = Arc::ptr_eq(&before, &after);
    assert!(unchanged);
    let failure_count = tls_common::tls_reload_counter(&guard, "failure");
    assert_eq!(failure_count, 1.0);

    println!(
        "load_bad_cert PASSED: original ServerConfig pointer unchanged={unchanged}; cc_lb_tls_reload_total failure={failure_count}"
    );
}
