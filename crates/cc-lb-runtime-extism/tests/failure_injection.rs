mod common;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use bytes::Bytes;
use cc_lb_plugin_api::{AuthnError, PluginRuntime, RequestContext};
use cc_lb_runtime_extism::ExtismRuntime;
use http::{HeaderMap, HeaderValue, Method};

const SECRET_HEADER_TOKEN: &str = "sk-ant-oat01-task46-secret";
const SECRET_HEADER_VALUE: &str = "Bearer sk-ant-oat01-task46-secret";
const SECRET_BODY_TOKEN: &str = "sk-ant-oat01-task46-body-secret";
const SECRET_BEARER_TOKEN: &str = "task46-body-secret";
const SECRET_BODY_RAW: &[u8] =
    br#"{"api_key":"sk-ant-oat01-task46-body-secret","authorization":"Bearer task46-body-secret"}"#;

#[tokio::test]
async fn guest_trap_is_runtime_error_redacted_and_host_survives() {
    assert_failure_mode(FailureMode::GuestTrap).await;
}

#[tokio::test]
async fn oom_is_runtime_error_redacted_and_host_survives() {
    assert_failure_mode(FailureMode::Oom).await;
}

#[tokio::test]
async fn fuel_exhaustion_is_runtime_error_redacted_and_host_survives() {
    assert_failure_mode(FailureMode::Fuel).await;
}

#[tokio::test]
async fn failed_plugin_does_not_corrupt_other_plugins() {
    let failure_wat = FailureMode::GuestTrap.fixture_wat();
    let failure_fixture = common::fixture(
        "task46-cross-trap",
        &failure_wat,
        FailureMode::GuestTrap.metadata(),
    );
    let healthy_fixture = common::fixture(
        "task46-cross-healthy",
        &common::module_with_authn(&common::authn_response("cross_host_alive")),
        common::metadata(&[]),
    );
    let runtime = ExtismRuntime::new();
    let failing_plugin = runtime
        .instantiate(&failure_fixture.manifest)
        .expect("failing plugin instantiates");
    let healthy_plugin = runtime
        .instantiate(&healthy_fixture.manifest)
        .expect("healthy plugin instantiates");

    let process_id_before = std::process::id();
    let before_failure = healthy_plugin
        .authenticate(&secret_ctx())
        .await
        .expect("healthy plugin works before failure");
    assert_eq!(before_failure.principal.id, "cross_host_alive");

    let error = match failing_plugin.authenticate(&secret_ctx()).await {
        Ok(_) => panic!("guest trap unexpectedly succeeded"),
        Err(error) => error,
    };
    let reason = runtime_reason(error);
    assert_reason_has_markers(&reason, FailureMode::GuestTrap);
    assert_secret_not_in_failure(&reason);

    let after_failure = healthy_plugin
        .authenticate(&secret_ctx())
        .await
        .expect("healthy plugin works after failure");
    assert_eq!(after_failure.principal.id, "cross_host_alive");
    assert_eq!(std::process::id(), process_id_before);
    println!(
        "task46_cross_plugin boundary=AuthnError::Runtime mode={} redacted=true pid_unchanged=true host_alive={}",
        FailureMode::GuestTrap.name(),
        after_failure.principal.id
    );
}

#[derive(Clone, Copy)]
enum FailureMode {
    GuestTrap,
    Oom,
    Fuel,
}

impl FailureMode {
    fn name(self) -> &'static str {
        match self {
            Self::GuestTrap => "guest_trap",
            Self::Oom => "oom",
            Self::Fuel => "fuel_exhaustion",
        }
    }

    fn fixture_name(self) -> &'static str {
        match self {
            Self::GuestTrap => "task46-guest-trap",
            Self::Oom => "task46-oom",
            Self::Fuel => "task46-fuel",
        }
    }

    fn fixture_wat(self) -> String {
        match self {
            Self::GuestTrap => common::panic_module(),
            Self::Oom => common::memory_pressure_module(),
            Self::Fuel => common::infinite_loop_module(),
        }
    }

    fn metadata(self) -> std::collections::BTreeMap<String, serde_json::Value> {
        match self {
            Self::GuestTrap => common::metadata(&[("max_call_duration_ms", 500)]),
            Self::Oom => {
                common::metadata(&[("memory_max_pages", 2), ("max_call_duration_ms", 500)])
            }
            Self::Fuel => common::metadata(&[("fuel_max", 20_000), ("max_call_duration_ms", 200)]),
        }
    }

    fn expected_markers(self) -> &'static [&'static str] {
        match self {
            Self::GuestTrap => &["plugin call failed", "wasm backtrace"],
            Self::Oom => &["plugin call failed", "oom"],
            Self::Fuel => &["plugin call failed", "fuel"],
        }
    }
}

async fn assert_failure_mode(mode: FailureMode) {
    let failure_wat = mode.fixture_wat();
    let failure_fixture = common::fixture(mode.fixture_name(), &failure_wat, mode.metadata());
    let healthy_fixture = common::fixture(
        &format!("{}-healthy", mode.fixture_name()),
        &common::module_with_authn(&common::authn_response("host_alive")),
        common::metadata(&[]),
    );
    let runtime = ExtismRuntime::new();
    let failing_plugin = runtime
        .instantiate(&failure_fixture.manifest)
        .expect("failing plugin instantiates");
    let process_id_before = std::process::id();

    let error = match failing_plugin.authenticate(&secret_ctx()).await {
        Ok(_) => panic!("failure injection unexpectedly succeeded"),
        Err(error) => error,
    };
    let reason = runtime_reason(error);
    assert_reason_has_markers(&reason, mode);
    assert_secret_not_in_failure(&reason);

    let healthy_plugin = runtime
        .instantiate(&healthy_fixture.manifest)
        .expect("healthy plugin instantiates after failure");
    let outcome = healthy_plugin
        .authenticate(&secret_ctx())
        .await
        .expect("host remains stable after plugin failure");
    assert_eq!(outcome.principal.id, "host_alive");
    assert_eq!(std::process::id(), process_id_before);

    println!(
        "task46_failure mode={} boundary=AuthnError::Runtime reason={} redacted=true pid_unchanged=true host_alive={}",
        mode.name(),
        reason.replace('\n', "\\n"),
        outcome.principal.id
    );
}

fn runtime_reason(error: AuthnError) -> String {
    match error {
        AuthnError::Runtime { reason } => reason,
        AuthnError::InvalidCredentials { .. } | AuthnError::Denied { .. } => {
            panic!("failure did not surface as AuthnError::Runtime")
        }
    }
}

fn assert_reason_has_markers(reason: &str, mode: FailureMode) {
    let lower_reason = reason.to_ascii_lowercase();
    for marker in mode.expected_markers() {
        assert!(
            lower_reason.contains(marker),
            "failure reason missing expected classification marker"
        );
    }
}

fn assert_secret_not_in_failure(reason: &str) {
    let forbidden = [
        SECRET_HEADER_TOKEN.to_owned(),
        SECRET_HEADER_VALUE.to_owned(),
        SECRET_BODY_TOKEN.to_owned(),
        SECRET_BEARER_TOKEN.to_owned(),
        BASE64.encode(SECRET_HEADER_TOKEN.as_bytes()),
        BASE64.encode(SECRET_HEADER_VALUE.as_bytes()),
        BASE64.encode(SECRET_BODY_RAW),
    ];
    for secret in forbidden {
        assert!(
            !reason.contains(&secret),
            "failure reason leaked secret material"
        );
    }
}

fn secret_ctx() -> RequestContext {
    let mut downstream_headers = HeaderMap::new();
    downstream_headers.insert(
        "authorization",
        HeaderValue::from_static(SECRET_HEADER_VALUE),
    );
    downstream_headers.insert("x-api-key", HeaderValue::from_static(SECRET_HEADER_TOKEN));
    RequestContext {
        request_id: "req-task46-secret".to_owned(),
        downstream_headers,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: Some("mode=task46".to_owned()),
        body_bytes: Bytes::from_static(SECRET_BODY_RAW),
    }
}
