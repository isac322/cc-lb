use std::net::TcpListener as StdTcpListener;
use std::path::Path;
use std::process::Command;

use cc_lb_storage_api::{
    BackendKind, PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalCreate,
    PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
};
use serde_json::json;
use uuid::Uuid;

use super::support::{
    ADMIN_TOKEN, CapturedOutput, ChildProcess, EXIT_TIMEOUT, Fixture, MASTER_KEY_HEX,
    READY_TIMEOUT, ReservedAddrs, ServerAddrs, Signal, base_server_config, cc_lb_command,
    http_request, open_initialized_sqlite, serve_command, spawn_fake_anthropic, wait_http_status,
    wait_stdout_contains,
};

const BOOT_KEY_ENV: &str = "CC_LB_T5_BOOT_KEY";
const CUSTOM_KEY_ENV: &str = "CC_LB_T5_CUSTOM_AEAD_KEY";
const DEFAULT_PRINCIPAL: &str = "api-key";

#[tokio::test]
async fn t5__process_boot_default_config() {
    let fixture = Fixture::new("boot-default");
    let upstream = spawn_fake_anthropic().await;
    let reserved = ReservedAddrs::bind().expect("reserve cc-lb listener addresses");
    let addrs = ServerAddrs {
        proxy: reserved.proxy.addr(),
        admin: reserved.admin.addr(),
        metrics: reserved.metrics.addr(),
    };
    let storage_path = fixture.path("storage.sqlite");
    let data_dir = fixture.path("runtime");
    std::fs::create_dir_all(&data_dir).expect("create runtime data directory");
    let config = base_server_config(
        addrs,
        &storage_path,
        &data_dir,
        BOOT_KEY_ENV,
        DEFAULT_PRINCIPAL,
    );
    let config_path = fixture.write("cc-lb.toml", config);

    let released = reserved.release();
    assert_eq!(released.proxy, addrs.proxy);
    assert_eq!(released.admin, addrs.admin);
    assert_eq!(released.metrics, addrs.metrics);

    let mut command = serve_command(&config_path);
    command
        .env(BOOT_KEY_ENV, MASTER_KEY_HEX)
        .env_remove("CC_LB_MASTER_KEY")
        .env("CC_LB_ADMIN_TOKEN", ADMIN_TOKEN);
    let mut child = ChildProcess::spawn(command).expect("spawn cc-lb serve process");

    wait_stdout_contains(
        &mut child,
        "preflight: plugin_blob_missing_count: 0",
        READY_TIMEOUT,
    )
    .await
    .unwrap_or_else(|error| panic!("default boot preflight output: {error}"));
    let expected_preflight = concat!(
        "preflight: ok\n",
        "preflight: upstream_count: 0\n",
        "preflight: upstream_warnings: 0\n",
        "preflight: principal_count: 0\n",
        "preflight: principal_disabled_count: 0\n",
        "preflight: plugin_chain_entry_count: 0\n",
        "preflight: plugin_blob_missing_count: 0\n",
    );
    let stdout = child.stdout_snapshot();
    let preflight_offset = stdout
        .find(expected_preflight)
        .unwrap_or_else(|| panic!("stdout={stdout} stderr={}", child.stderr_snapshot()));
    assert_eq!(
        &stdout[preflight_offset..preflight_offset + expected_preflight.len()],
        expected_preflight,
        "stdout={stdout}"
    );

    let health = http_request("GET", addrs.admin, "/admin/health", &[], "");
    wait_http_status(&mut child, addrs.admin, &health, 200, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("admin listener readiness: {error}"));
    let bind_error = StdTcpListener::bind(addrs.proxy).expect_err("proxy address is held by cc-lb");
    assert_eq!(
        bind_error.kind(),
        std::io::ErrorKind::AddrInUse,
        "unexpected bind error: {bind_error}"
    );

    let admin_headers = [
        (
            "authorization",
            concat!("Bearer ", "t5-process-admin-token"),
        ),
        ("content-type", "application/json"),
    ];
    let upstream_body = json!({
        "name": "fake_anthropic",
        "kind": "anthropic_api_key",
        "base_url": format!("http://{}/", upstream.addr),
        "api_key_value": "sk-ant-test"
    })
    .to_string();
    let create_upstream = http_request(
        "POST",
        addrs.admin,
        "/admin/v1/upstreams",
        &admin_headers,
        &upstream_body,
    );
    let upstream_response = wait_http_status(
        &mut child,
        addrs.admin,
        &create_upstream,
        201,
        READY_TIMEOUT,
    )
    .await
    .unwrap_or_else(|error| panic!("admin upstream seed: {error}"));
    assert!(
        upstream_response
            .body
            .contains(r#""name":"fake_anthropic""#),
        "body={}",
        upstream_response.body
    );

    let principal_body = json!({
        "name": DEFAULT_PRINCIPAL,
        "kind": "machine",
        "allowed_models": [],
        "allowed_upstreams": [],
        "default_limits": []
    })
    .to_string();
    let create_principal = http_request(
        "POST",
        addrs.admin,
        "/admin/v1/principals",
        &admin_headers,
        &principal_body,
    );
    let principal_response = wait_http_status(
        &mut child,
        addrs.admin,
        &create_principal,
        201,
        READY_TIMEOUT,
    )
    .await
    .unwrap_or_else(|error| panic!("admin principal seed: {error}"));
    assert!(
        principal_response.body.contains(r#""name":"api-key""#),
        "body={}",
        principal_response.body
    );

    let messages_body = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#;
    let proxy_request = http_request(
        "POST",
        addrs.proxy,
        "/v1/messages",
        &[
            ("x-api-key", "sk-ant-test"),
            ("anthropic-version", "2023-06-01"),
            ("content-type", "application/json"),
        ],
        messages_body,
    );
    let proxy_response =
        wait_http_status(&mut child, addrs.proxy, &proxy_request, 200, READY_TIMEOUT)
            .await
            .unwrap_or_else(|error| {
                panic!("proxy request after deterministic admin seed: {error}")
            });
    assert!(
        proxy_response.body.contains(r#""type":"message""#),
        "body={}",
        proxy_response.body
    );
    assert!(storage_path.exists(), "configured SQLite path was not used");

    child
        .send_signal(Signal::Terminate)
        .expect("deliver SIGTERM to cc-lb");
    let output = child
        .wait_for_exit(EXIT_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("SIGTERM exit: {error}"));
    assert_eq!(
        output.status.code(),
        Some(0),
        "status={} stdout={} stderr={}",
        output.status,
        output.stdout,
        output.stderr
    );
    assert!(
        !output.stderr.contains("panicked at") && !output.stderr.contains("thread panicked"),
        "stderr={}",
        output.stderr
    );
}

#[derive(Clone, Copy, Debug)]
enum FatalCase {
    MissingAeadKey,
    BackendKindMismatch,
    StrictPreflightWarning,
    ValidateOk,
    ValidateMissingTls,
    CustomAeadEnvRequired,
    MissingConfigFile,
    MissingConfigArgument,
    #[cfg(feature = "postgres")]
    BadPostgresUrl,
}

impl FatalCase {
    const fn label(self) -> &'static str {
        match self {
            Self::MissingAeadKey => "missing-aead-key",
            Self::BackendKindMismatch => "backend-kind-mismatch",
            Self::StrictPreflightWarning => "strict-preflight-warning",
            Self::ValidateOk => "validate-ok",
            Self::ValidateMissingTls => "validate-missing-tls",
            Self::CustomAeadEnvRequired => "custom-aead-env-required",
            Self::MissingConfigFile => "missing-config-file",
            Self::MissingConfigArgument => "missing-config-argument",
            #[cfg(feature = "postgres")]
            Self::BadPostgresUrl => "bad-postgres-url",
        }
    }
}

#[tokio::test]
async fn t5__process_boot_fatal_table() {
    #[cfg(not(feature = "postgres"))]
    let cases = [
        FatalCase::MissingAeadKey,
        FatalCase::BackendKindMismatch,
        FatalCase::StrictPreflightWarning,
        FatalCase::ValidateOk,
        FatalCase::ValidateMissingTls,
        FatalCase::CustomAeadEnvRequired,
        FatalCase::MissingConfigFile,
        FatalCase::MissingConfigArgument,
    ];
    #[cfg(feature = "postgres")]
    let cases = [
        FatalCase::MissingAeadKey,
        FatalCase::BackendKindMismatch,
        FatalCase::StrictPreflightWarning,
        FatalCase::ValidateOk,
        FatalCase::ValidateMissingTls,
        FatalCase::CustomAeadEnvRequired,
        FatalCase::MissingConfigFile,
        FatalCase::MissingConfigArgument,
        FatalCase::BadPostgresUrl,
    ];

    for case in cases {
        let fixture = Fixture::new(case.label());
        match case {
            FatalCase::MissingAeadKey => {
                let storage_path = fixture.path("missing-aead-key.sqlite");
                let config_path = fixture.write(
                    "cc-lb.toml",
                    sqlite_serve_config(&storage_path, "CC_LB_AEAD_KEY"),
                );
                let mut command = serve_command(&config_path);
                command
                    .env_remove("CC_LB_MASTER_KEY")
                    .env_remove("CC_LB_AEAD_KEY");
                let output = run_command(command).await;
                assert_code(&output, 2, case);
                assert_eq!(
                    output.stderr, "storage master key env CC_LB_AEAD_KEY is missing\n",
                    "case={case:?}"
                );
            }
            FatalCase::BackendKindMismatch => {
                let storage_path = fixture.path("backend-kind-mismatch.sqlite");
                let storage = open_initialized_sqlite(&storage_path, BackendKind::Postgres)
                    .await
                    .expect("stamp SQLite storage as postgres");
                drop(storage);
                let config_path = fixture.write(
                    "cc-lb.toml",
                    sqlite_serve_config(&storage_path, "CC_LB_AEAD_KEY"),
                );
                let mut command = serve_command(&config_path);
                command
                    .env("CC_LB_AEAD_KEY", MASTER_KEY_HEX)
                    .env_remove("CC_LB_MASTER_KEY");
                let output = run_command(command).await;
                assert_code(&output, 2, case);
                assert_eq!(
                    output.stderr, "backend kind mismatch: stored=Postgres, configured=Sqlite\n",
                    "case={case:?}"
                );
            }
            FatalCase::StrictPreflightWarning => {
                let storage_path = fixture.path("strict-preflight.sqlite");
                seed_strict_preflight_warning(&storage_path).await;
                let config_path = fixture.write(
                    "cc-lb.toml",
                    strict_preflight_config(&storage_path, fixture.root()),
                );
                let mut command = serve_command(&config_path);
                command
                    .arg("--strict-preflight")
                    .env("CC_LB_AEAD_KEY", MASTER_KEY_HEX)
                    .env_remove("CC_LB_MASTER_KEY");
                let output = run_command(command).await;
                assert_code(&output, 1, case);
                assert!(
                    output.stdout.contains("preflight: ok"),
                    "stdout={}",
                    output.stdout
                );
                assert!(
                    output.stdout.contains("preflight: warning:")
                        && output.stdout.contains("unsupported slot router"),
                    "stdout={}",
                    output.stdout
                );
                assert_eq!(
                    output.stderr, "preflight: strict mode failed with 1 warning(s)\n",
                    "case={case:?}"
                );
            }
            FatalCase::ValidateOk => {
                let config_path = fixture.write("cc-lb.toml", "[listener]\n");
                let mut command = cc_lb_command();
                command
                    .args(["config", "validate", "--config"])
                    .arg(&config_path);
                let output = run_command(command).await;
                assert_code(&output, 0, case);
                assert!(
                    output.stdout.contains("validation: ok"),
                    "stdout={}",
                    output.stdout
                );
                assert!(
                    output.stdout.contains("preflight: ok"),
                    "stdout={}",
                    output.stdout
                );
            }
            FatalCase::ValidateMissingTls => {
                let config_path = fixture.write(
                    "cc-lb.toml",
                    r#"[listener]

[listener.tls]
cert_path = "/definitely/missing/cert.pem"
key_path = "/definitely/missing/key.pem"
"#,
                );
                let mut command = cc_lb_command();
                command
                    .args(["config", "validate", "--config"])
                    .arg(&config_path);
                let output = run_command(command).await;
                assert_code(&output, 2, case);
                assert!(
                    output.stderr.contains("validation: failed"),
                    "stderr={}",
                    output.stderr
                );
                assert!(
                    output.stderr.contains("listener.tls.cert_path"),
                    "stderr={}",
                    output.stderr
                );
            }
            FatalCase::CustomAeadEnvRequired => {
                let storage_path = fixture.path("custom-key.sqlite");
                let config_path = fixture.write(
                    "cc-lb.toml",
                    sqlite_serve_config(&storage_path, CUSTOM_KEY_ENV),
                );
                let mut command = serve_command(&config_path);
                command
                    .env("CC_LB_MASTER_KEY", MASTER_KEY_HEX)
                    .env_remove(CUSTOM_KEY_ENV);
                let output = run_command(command).await;
                assert_code(&output, 2, case);
                assert_eq!(
                    output.stderr,
                    format!("storage master key env {CUSTOM_KEY_ENV} is missing\n"),
                    "the configured custom key env must be required instead of the default"
                );
            }
            FatalCase::MissingConfigFile => {
                let config_path = fixture.path("does-not-exist.toml");
                let output = run_command(serve_command(&config_path)).await;
                assert_code(&output, 1, case);
                assert!(output.stdout.is_empty(), "stdout={}", output.stdout);
                assert_eq!(
                    output.stderr,
                    format!(
                        "failed to load config: No such file or directory (os error 2)\n in {} TOML file\n",
                        config_path.display()
                    )
                );
            }
            FatalCase::MissingConfigArgument => {
                let mut command = cc_lb_command();
                command.arg("serve");
                let output = run_command(command).await;
                assert_code(&output, 2, case);
                assert!(output.stdout.is_empty(), "stdout={}", output.stdout);
                assert_eq!(
                    output.stderr,
                    concat!(
                        "error: the following required arguments were not provided:\n",
                        "  --config <PATH>\n\n",
                        "Usage: cc-lb serve --config <PATH>\n\n",
                        "For more information, try '--help'.\n",
                    )
                );
            }
            #[cfg(feature = "postgres")]
            FatalCase::BadPostgresUrl => {
                let config_path = fixture.write(
                    "cc-lb.toml",
                    r#"[storage]
kind = "postgres"
url = "postgres://user:secret@127.0.0.2:65499/testdb"

[listener]
proxy_addr = "127.0.0.1:0"
admin_addr = "127.0.0.1:0"
metrics_addr = "127.0.0.1:0"

[cluster]
instance_url = "http://127.0.0.1:9090"

[storage.pool]
acquire_timeout_secs = 3

[aead]
key_env = "CC_LB_AEAD_KEY"
"#,
                );
                let mut command = serve_command(&config_path);
                command
                    .env("CC_LB_CLUSTER_TOKEN", "test-cluster-token")
                    .env("CC_LB_AEAD_KEY", MASTER_KEY_HEX)
                    .env_remove("CC_LB_MASTER_KEY");
                let output = run_command(command).await;
                assert_code(&output, 1, case);
                assert!(
                    !output.stderr.contains("secret"),
                    "stderr={}",
                    output.stderr
                );
                assert!(
                    output.stderr.contains("127.0.0.2"),
                    "stderr={}",
                    output.stderr
                );
            }
        }
    }
}

#[tokio::test]
async fn t5__process_cli_doctor_runs() {
    let fixture = Fixture::new("doctor-empty-storage");
    let storage_path = fixture.path("selected-by-env.sqlite");
    let storage = open_initialized_sqlite(&storage_path, BackendKind::Sqlite)
        .await
        .expect("initialize empty doctor storage");
    drop(storage);

    let mut command = cc_lb_command();
    command
        .args(["doctor", "list-abandoned-chain-entries"])
        .env("CC_LB_STORAGE_PATH", &storage_path)
        .env("HOME", fixture.path("home-without-default-storage"));
    let output = run_command(command).await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "status={} stdout={} stderr={}",
        output.status,
        output.stdout,
        output.stderr
    );
    assert_eq!(output.stdout, "{\"abandoned_chain_entries\":[]}\n");
    assert!(output.stderr.is_empty(), "stderr={}", output.stderr);
}

async fn run_command(command: Command) -> CapturedOutput {
    let mut child = ChildProcess::spawn(command).expect("spawn cc-lb command");
    child
        .wait_for_exit(EXIT_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("cc-lb command exit: {error}"))
}

fn assert_code(output: &CapturedOutput, expected: i32, case: FatalCase) {
    assert_eq!(
        output.status.code(),
        Some(expected),
        "case={case:?} status={} stdout={} stderr={}",
        output.status,
        output.stdout,
        output.stderr
    );
}

fn sqlite_serve_config(storage_path: &Path, key_env: &str) -> String {
    format!(
        r#"[listener]
proxy_addr = "127.0.0.1:0"
admin_addr = "127.0.0.1:0"
metrics_addr = "127.0.0.1:0"

[storage]
kind = "sqlite"
path = "{}"

[aead]
key_env = "{key_env}"
"#,
        storage_path.display()
    )
}

fn strict_preflight_config(storage_path: &Path, data_dir: &Path) -> String {
    format!(
        r#"[listener]
proxy_addr = "127.0.0.1:0"
admin_addr = "127.0.0.1:0"
metrics_addr = "127.0.0.1:0"

[runtime]
data_dir = "{data_dir}"

[storage]
kind = "sqlite"
path = "{storage_path}"

[aead]
key_env = "CC_LB_AEAD_KEY"

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "strict-principal"
upstream_kind = "anthropic_key"
"#,
        data_dir = data_dir.display(),
        storage_path = storage_path.display(),
    )
}

async fn seed_strict_preflight_warning(storage_path: &Path) {
    let storage = open_initialized_sqlite(storage_path, BackendKind::Sqlite)
        .await
        .expect("initialize strict preflight storage");
    let principal = PrincipalStore::create(
        &storage,
        PrincipalCreate {
            name: "strict-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1_800_000_000,
    )
    .await
    .expect("seed strict preflight principal");
    let wasm = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
    let (registry, _) = PluginRegistryStore::persist_wasm_upload(
        &storage,
        WasmBlob {
            sha256: [17; 32],
            size_bytes: wasm.len() as u64,
            bytes: wasm,
            parse_validated_at_unix_secs: 1_800_000_000,
        },
        WasmRegistryEntryInput {
            name: "strict-observe-only".to_owned(),
            version: None,
            original_filename: "strict-observe-only.wasm".to_owned(),
            label: None,
            uploaded_at_unix_secs: 1_800_000_000,
            uploaded_by_admin_id: Uuid::from_u128(17),
            description: "strict preflight warning fixture".to_owned(),
            usage: "process test".to_owned(),
            hook_metadata: Default::default(),
            supported_slots: vec![PluginSlotKind::ObservabilityHook],
            schema_hash: None,
        },
    )
    .await
    .expect("seed strict preflight registry entry");
    PluginRegistryStore::insert_chain_entry(
        &storage,
        PluginChainEntryInput {
            principal_id: principal.id,
            slot: PluginSlotKind::Router,
            order: 1_000,
            wasm_registry_id: registry.id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
        },
    )
    .await
    .expect("seed unsupported router chain entry");
}
