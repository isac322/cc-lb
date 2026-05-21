mod common;

use std::sync::Arc;

use cc_lb_signer_gcp::{GcpOAuthSigner, StaticGcpTokenProvider};

#[test]
fn debug_redacts_token() {
    let token = common::token("ya29.debug-secret", 3600);
    let provider = Arc::new(StaticGcpTokenProvider::new(token.clone()));
    let signer = GcpOAuthSigner::with_scopes(common::scopes(), provider.clone());

    let debug = format!("{token:?} {provider:?} {signer:?}");

    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("ya29.debug-secret"));
}
