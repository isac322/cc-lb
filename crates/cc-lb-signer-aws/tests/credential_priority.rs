use std::env;
use std::ffi::OsString;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use cc_lb_signer_aws::credentials::{
    AwsCredentialsProvider, ChainCredentialsProvider, EnvCredentialsProvider,
    ProfileCredentialsProvider, StaticCredentialsProvider,
};

static ENV_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[tokio::test]
async fn credential_priority() {
    let _guard = ENV_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let _env = EnvGuard::capture(&[
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ]);
    clear_aws_env();

    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("credentials");
    write_profile(&path);
    let chain = ChainCredentialsProvider::new(vec![
        Arc::new(EnvCredentialsProvider::new()),
        Arc::new(StaticCredentialsProvider::new(
            "AKIACONFIG",
            "config-secret",
            None,
        )),
        Arc::new(ProfileCredentialsProvider::with_path("default", &path)),
    ]);

    env::set_var("AWS_ACCESS_KEY_ID", "AKIAENV");
    env::set_var("AWS_SECRET_ACCESS_KEY", "env-secret");
    env::set_var("AWS_SESSION_TOKEN", "env-token");
    let env_credentials = chain.resolve().await.expect("env credentials");
    assert_eq!(env_credentials.access_key_id, "AKIAENV");

    clear_aws_env();
    let config_credentials = chain.resolve().await.expect("config credentials");
    assert_eq!(config_credentials.access_key_id, "AKIACONFIG");

    let profile_chain = ChainCredentialsProvider::new(vec![
        Arc::new(EnvCredentialsProvider::new()),
        Arc::new(ProfileCredentialsProvider::with_path("default", &path)),
    ]);
    let profile_credentials = profile_chain.resolve().await.expect("profile credentials");
    assert_eq!(profile_credentials.access_key_id, "AKIAPROFILE");
}

fn write_profile(path: &Path) {
    std::fs::write(
        path,
        "[default]\naws_access_key_id = AKIAPROFILE\naws_secret_access_key = profile-secret\naws_session_token = profile-token\n",
    )
    .expect("write profile");
}

fn clear_aws_env() {
    env::remove_var("AWS_ACCESS_KEY_ID");
    env::remove_var("AWS_SECRET_ACCESS_KEY");
    env::remove_var("AWS_SESSION_TOKEN");
}

struct EnvGuard {
    saved: Vec<(&'static str, Option<OsString>)>,
}

impl EnvGuard {
    fn capture(names: &[&'static str]) -> Self {
        Self {
            saved: names
                .iter()
                .map(|name| (*name, env::var_os(name)))
                .collect(),
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (name, value) in &self.saved {
            match value {
                Some(value) => env::set_var(name, value),
                None => env::remove_var(name),
            }
        }
    }
}
