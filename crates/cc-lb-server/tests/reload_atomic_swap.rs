mod tls_common;

use std::sync::Arc;

use cc_lb_server::tls::TlsState;

#[test]
fn reload_atomic_swap_keeps_old_snapshot_valid() {
    let guard = tls_common::init_metrics();
    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = tls_common::copy_pair(&dir, "cert-a.pem", "key-a.pem");
    let state = TlsState::from_paths(&cert_path, &key_path).unwrap();
    let before = state.current();
    let before_ptr = format!("{:p}", Arc::as_ptr(&before));

    tls_common::overwrite_pair(&cert_path, &key_path, "cert-b.pem", "key-b.pem");
    state.reload().unwrap();

    let after = state.current();
    let after_ptr = format!("{:p}", Arc::as_ptr(&after));
    assert!(!Arc::ptr_eq(&before, &after));
    let old_snapshot_still_valid = rustls::ServerConnection::new(before.clone()).is_ok();
    assert!(old_snapshot_still_valid);
    let success_count = tls_common::tls_reload_counter(&guard, "success");
    assert_eq!(success_count, 1.0);

    println!(
        "reload_atomic_swap PASSED: before_ptr={before_ptr} after_ptr={after_ptr}; old_snapshot_still_valid={old_snapshot_still_valid}; cc_lb_tls_reload_total success={success_count}"
    );
}
