use std::sync::Arc;

use cc_lb_signer_aws::{AwsSigV4Signer, StaticCredentialsProvider};

#[test]
fn debug_redacts_secret() {
    let signer = AwsSigV4Signer::new(
        "us-east-1",
        Arc::new(StaticCredentialsProvider::new(
            "AKIADEBUG",
            "debug-secret-access-key",
            Some("debug-session-token".to_owned()),
        )),
    );

    let debug = format!("{signer:?}");

    assert!(debug.contains("AKIADEBUG"));
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("debug-secret-access-key"));
    assert!(!debug.contains("debug-session-token"));
}
