use std::panic::{self, AssertUnwindSafe};

use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::wire_function::{FallbackPolicy, WireFunction};
use extism::Plugin;
use serde_json::Value;
use thiserror::Error;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DispatchOutcome<R> {
    Ok(R),
    Fallback(FallbackPolicy),
}

pub fn dispatch_wire_call<F: WireFunction>(
    plugin: &mut Plugin,
    metadata: &AugmentedMetadata,
    request: F::Request,
) -> DispatchOutcome<F::Response> {
    match panic::catch_unwind(AssertUnwindSafe(|| {
        dispatch_wire_call_inner::<F>(plugin, metadata, request)
    })) {
        Ok(Ok(response)) => DispatchOutcome::Ok(response),
        Ok(Err(error)) => fallback::<F, F::Response>(error),
        Err(_) => fallback::<F, F::Response>(DispatchError::Panic),
    }
}

fn dispatch_wire_call_inner<F: WireFunction>(
    plugin: &mut Plugin,
    metadata: &AugmentedMetadata,
    request: F::Request,
) -> Result<F::Response, DispatchError> {
    let negotiated_version = *metadata
        .negotiated_functions
        .get(F::NAME)
        .ok_or(DispatchError::MissingNegotiatedVersion)?;

    let request_value =
        serde_json::to_value(request).map_err(|source| DispatchError::RequestValue {
            reason: source.to_string(),
        })?;
    let Value::Object(mut request_map) = request_value else {
        return Err(DispatchError::RequestNotObject);
    };
    request_map.insert("_v".to_owned(), Value::from(negotiated_version));

    let input = serde_json::to_string(&Value::Object(request_map)).map_err(|source| {
        DispatchError::SerializeRequestEnvelope {
            reason: source.to_string(),
        }
    })?;
    let output = plugin
        .call::<String, String>(F::NAME, input)
        .map_err(|source| DispatchError::PluginCall {
            reason: source.to_string(),
        })?;

    let response_value: Value = serde_json::from_str(&output).map_err(|source| {
        DispatchError::DeserializeResponseEnvelope {
            reason: source.to_string(),
        }
    })?;
    let Value::Object(mut response_map) = response_value else {
        return Err(DispatchError::ResponseNotObject);
    };
    let response_version = response_map
        .remove("_v")
        .ok_or(DispatchError::ResponseVersionMissing)?;
    let actual_version = response_version
        .as_u64()
        .ok_or(DispatchError::ResponseVersionInvalid)?;
    if actual_version != u64::from(negotiated_version) {
        return Err(DispatchError::ResponseVersionMismatch {
            expected: negotiated_version,
            actual: actual_version,
        });
    }

    serde_json::from_value(Value::Object(response_map)).map_err(|source| {
        DispatchError::ResponseDecode {
            reason: source.to_string(),
        }
    })
}

fn fallback<F: WireFunction, R>(error: DispatchError) -> DispatchOutcome<R> {
    let stage_label = error.stage_label();
    metrics::counter!(
        "cc_lb_plugin_dispatch_errors_total",
        "function" => F::NAME,
        "stage" => stage_label
    )
    .increment(1);
    tracing::warn!(
        target: "cc_lb_plugin.dispatch",
        function = F::NAME,
        error = ?error,
        "dispatch error, applying fallback {:?}",
        F::FALLBACK
    );
    DispatchOutcome::Fallback(F::FALLBACK)
}

#[derive(Debug, Error)]
enum DispatchError {
    #[error("negotiated version missing")]
    MissingNegotiatedVersion,
    #[error("request value serialization failed: {reason}")]
    RequestValue { reason: String },
    #[error("request serialized to non-object JSON")]
    RequestNotObject,
    #[error("request envelope serialization failed: {reason}")]
    SerializeRequestEnvelope { reason: String },
    #[error("plugin call failed: {reason}")]
    PluginCall { reason: String },
    #[error("response envelope deserialization failed: {reason}")]
    DeserializeResponseEnvelope { reason: String },
    #[error("response envelope was not a JSON object")]
    ResponseNotObject,
    #[error("response envelope missing _v")]
    ResponseVersionMissing,
    #[error("response envelope _v was not an unsigned integer")]
    ResponseVersionInvalid,
    #[error("response envelope version mismatch: expected {expected}, actual {actual}")]
    ResponseVersionMismatch { expected: u32, actual: u64 },
    #[error("response decode failed: {reason}")]
    ResponseDecode { reason: String },
    #[error("dispatch panicked")]
    Panic,
}

impl DispatchError {
    fn stage_label(&self) -> &'static str {
        match self {
            Self::MissingNegotiatedVersion => "version_lookup",
            Self::RequestValue { .. } | Self::RequestNotObject => "request_envelope",
            Self::SerializeRequestEnvelope { .. } => "serialize_request",
            Self::PluginCall { .. } => "plugin_call",
            Self::DeserializeResponseEnvelope { .. } => "deserialize_response",
            Self::ResponseNotObject
            | Self::ResponseVersionMissing
            | Self::ResponseVersionInvalid
            | Self::ResponseVersionMismatch { .. } => "response_envelope",
            Self::ResponseDecode { .. } => "response_decode",
            Self::Panic => "panic",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
    use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, PluginIdentity};
    use cc_lb_plugin_wire::v1::sign::{SignFn, SignResponse};
    use cc_lb_plugin_wire::wire_function::{FallbackPolicy, WireFunction};
    use extism::{Manifest, PluginBuilder, Wasm};
    use metrics::{
        Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Level, Metadata as TracingMetadata, Subscriber};

    use super::*;

    #[test]
    fn successful_call_decodes_versioned_response() {
        let mut plugin = plugin_with_output(
            <SignFn as WireFunction>::NAME,
            r#"{"_v":1,"url":null,"method":null,"headers":null,"body_base64":null}"#,
        );
        let metadata = metadata_with_function(<SignFn as WireFunction>::NAME);

        let outcome = dispatch_wire_call::<SignFn>(
            &mut plugin,
            &metadata,
            <SignFn as WireFunction>::dry_run_request(),
        );

        assert_eq!(outcome, DispatchOutcome::Ok(SignResponse::dry_run_sample()));
    }

    #[test]
    fn sign_fallback_is_fail_request_const() {
        assert_eq!(
            <SignFn as WireFunction>::FALLBACK,
            FallbackPolicy::FailRequest
        );
        let mut plugin = plugin_with_output(
            <SignFn as WireFunction>::NAME,
            r#"{"_v":1,"url":null,"method":null,"headers":null,"body_base64":null}"#,
        );
        let metadata = metadata_with_function("shape");

        let outcome = dispatch_wire_call::<SignFn>(
            &mut plugin,
            &metadata,
            <SignFn as WireFunction>::dry_run_request(),
        );

        assert_eq!(
            outcome,
            DispatchOutcome::Fallback(FallbackPolicy::FailRequest)
        );
    }

    #[test]
    fn response_version_mismatch_falls_back_to_function_const() {
        let mut plugin = plugin_with_output(
            <SignFn as WireFunction>::NAME,
            r#"{"_v":2,"url":null,"method":null,"headers":null,"body_base64":null}"#,
        );
        let metadata = metadata_with_function(<SignFn as WireFunction>::NAME);

        let outcome = dispatch_wire_call::<SignFn>(
            &mut plugin,
            &metadata,
            <SignFn as WireFunction>::dry_run_request(),
        );

        assert_eq!(
            outcome,
            DispatchOutcome::Fallback(FallbackPolicy::FailRequest)
        );
    }

    #[test]
    fn error_logs_emitted() {
        let logs = CapturingSubscriber::default();
        let captured_logs = logs.events.clone();
        let metrics = CapturingMetrics::default();
        let captured_metric_total = metrics.total.clone();
        let captured_metric_keys = metrics.keys.clone();
        let mut plugin = plugin_with_output(
            <SignFn as WireFunction>::NAME,
            r#"{"_v":1,"url":null,"method":null,"headers":null,"body_base64":null}"#,
        );
        let metadata = metadata_with_function("shape");

        let outcome = metrics::with_local_recorder(&metrics, || {
            tracing::subscriber::with_default(logs, || {
                dispatch_wire_call::<SignFn>(
                    &mut plugin,
                    &metadata,
                    <SignFn as WireFunction>::dry_run_request(),
                )
            })
        });

        assert_eq!(
            outcome,
            DispatchOutcome::Fallback(FallbackPolicy::FailRequest)
        );
        let rendered_logs = captured_logs.lock().expect("logs lock").join("\n");
        assert!(
            rendered_logs.contains("target=cc_lb_plugin.dispatch"),
            "log target missing from {rendered_logs}"
        );
        assert!(
            rendered_logs.contains("dispatch error, applying fallback FailRequest"),
            "message missing from {rendered_logs}"
        );
        assert!(
            rendered_logs.contains("function=\"sign\""),
            "function field missing from {rendered_logs}"
        );
        assert!(
            rendered_logs.contains("MissingNegotiatedVersion"),
            "error field missing from {rendered_logs}"
        );
        assert_eq!(captured_metric_total.load(Ordering::SeqCst), 1);
        let metric_keys = captured_metric_keys
            .lock()
            .expect("metric keys lock")
            .join("\n");
        assert!(
            metric_keys.contains("cc_lb_plugin_dispatch_errors_total"),
            "metric name missing from {metric_keys}"
        );
        assert!(
            metric_keys.contains("version_lookup"),
            "metric stage missing from {metric_keys}"
        );
    }

    fn metadata_with_function(function_name: &str) -> AugmentedMetadata {
        AugmentedMetadata {
            identity: PluginIdentity {
                magic: CC_LB_PLUGIN_MAGIC,
                abi_envelope: 1,
                plugin_name: "dispatch-test".to_owned(),
                plugin_version: "1.0.0".to_owned(),
            },
            negotiated_functions: BTreeMap::from([(function_name.to_owned(), 1)]),
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 1,
            self_check_passed: true,
            self_check_completed_at: 1,
            expires_at: 2,
        }
    }

    fn plugin_with_output(function_name: &str, output: &str) -> extism::Plugin {
        let output_bytes = output.as_bytes();
        let wat = format!(
            r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {output_helper}
  (func (export "{function_name}") (result i32)
    (call $output_set (call $dispatch_out) (i64.const {output_len}))
    (i32.const 0))
)
"#,
            output_helper = bytes_helper("dispatch_out", output_bytes),
            output_len = output_bytes.len(),
        );
        let wasm = wat::parse_str(wat).expect("dispatch fixture wat parses");
        let manifest = Manifest::new([Wasm::data(wasm)]).disallow_all_hosts();
        PluginBuilder::new(&manifest)
            .with_wasi(false)
            .with_cache_disabled()
            .build()
            .expect("dispatch fixture plugin builds")
    }

    fn bytes_helper(name: &str, bytes: &[u8]) -> String {
        let mut stores = String::new();
        for (index, byte) in bytes.iter().enumerate() {
            stores.push_str(&format!(
                "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
            ));
        }
        format!(
            r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
            len = bytes.len()
        )
    }

    #[derive(Clone, Default)]
    struct CapturingSubscriber {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl Subscriber for CapturingSubscriber {
        fn enabled(&self, metadata: &TracingMetadata<'_>) -> bool {
            metadata.target() == "cc_lb_plugin.dispatch" && metadata.level() <= &Level::WARN
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut visitor = EventVisitor::default();
            event.record(&mut visitor);
            self.events.lock().expect("events lock").push(format!(
                "target={} {}",
                event.metadata().target(),
                visitor.fields
            ));
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    #[derive(Default)]
    struct EventVisitor {
        fields: String,
    }

    impl Visit for EventVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            if !self.fields.is_empty() {
                self.fields.push(' ');
            }
            self.fields.push_str(&format!("{}={value:?}", field.name()));
        }
    }

    #[derive(Clone, Default)]
    struct CapturingMetrics {
        total: Arc<AtomicU64>,
        keys: Arc<Mutex<Vec<String>>>,
    }

    impl Recorder for CapturingMetrics {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            self.keys
                .lock()
                .expect("metric keys lock")
                .push(format!("{key:?}"));
            Counter::from_arc(Arc::new(TestCounter {
                total: self.total.clone(),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    struct TestCounter {
        total: Arc<AtomicU64>,
    }

    impl CounterFn for TestCounter {
        fn increment(&self, value: u64) {
            self.total.fetch_add(value, Ordering::SeqCst);
        }

        fn absolute(&self, value: u64) {
            self.total.store(value, Ordering::SeqCst);
        }
    }
}
