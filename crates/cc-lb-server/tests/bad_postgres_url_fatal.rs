#[cfg(feature = "postgres")]
use std::process::Command;

#[cfg(feature = "postgres")]
#[test]
fn bad_postgres_url_fatal() {
    let dir = tempfile::tempdir().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .arg("serve")
        .env("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:0")
        .env("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:0")
        .env("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:0")
        .env("CC_LB_STORAGE__KIND", "postgres")
        .env(
            "CC_LB_STORAGE__URL",
            "postgres://user:secret@127.0.0.2:65499/testdb",
        )
        .env("CC_LB_STORAGE__POOL__ACQUIRE_TIMEOUT_SECS", "3")
        .env("CC_LB_DATA_DIR", dir.path().display().to_string())
        .env("CC_LB_AEAD__KEY_ENV", "CC_LB_AEAD_KEY")
        .env(
            "CC_LB_AEAD_KEY",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .env_remove("CC_LB_MASTER_KEY")
        .output()
        .unwrap();

    assert!(!output.status.success(), "status={:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("secret"), "stderr={stderr}");
    assert!(stderr.contains("127.0.0.2"), "stderr={stderr}");
}
