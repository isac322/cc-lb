use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use cc_lb_domain::Provider;
use cc_lb_engine::model_resolution::{cache_threshold_tokens, canonical_model_id};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt as _};
use tracing_subscriber::{Layer, Registry};

const UNKNOWN_ALIAS_MESSAGE: &str = "unknown model alias, returning unchanged";
const UNKNOWN_CACHE_THRESHOLD_MESSAGE: &str = "unknown model, defaulting cache threshold to 1024";

#[derive(Clone, Debug, Eq, PartialEq)]
struct CapturedWarning {
    message: String,
    model: Option<String>,
}

#[derive(Default)]
struct EventFields {
    message: Option<String>,
    model: Option<String>,
}

impl tracing::field::Visit for EventFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn Debug) {
        let value = format!("{value:?}");
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .map_or(value.clone(), str::to_owned);
        self.record(field, value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record(field, value.to_owned());
    }
}

impl EventFields {
    fn record(&mut self, field: &tracing::field::Field, value: String) {
        match field.name() {
            "message" => self.message = Some(value),
            "model" => self.model = Some(value),
            _ => {}
        }
    }
}

#[derive(Clone)]
struct WarningLayer {
    warnings: Arc<Mutex<Vec<CapturedWarning>>>,
}

impl<S> Layer<S> for WarningLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() != Level::WARN {
            return;
        }

        let mut fields = EventFields::default();
        event.record(&mut fields);
        let Some(message) = fields.message else {
            return;
        };
        self.warnings
            .lock()
            .expect("warning capture lock")
            .push(CapturedWarning {
                message,
                model: fields.model,
            });
    }
}

fn capture_warnings<R>(operation: impl FnOnce() -> R) -> (R, Vec<CapturedWarning>) {
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let layer = WarningLayer {
        warnings: Arc::clone(&warnings),
    };
    let result = tracing::subscriber::with_default(Registry::default().with(layer), operation);
    let warnings = warnings.lock().expect("warning capture lock").clone();
    (result, warnings)
}

fn warning(message: &str, model: &str) -> CapturedWarning {
    CapturedWarning {
        message: message.to_owned(),
        model: Some(model.to_owned()),
    }
}

#[test]
fn provider_aware_resolution_preserves_native_ids_and_diagnostics() {
    let (resolved, warnings) = capture_warnings(|| {
        [
            canonical_model_id(Provider::Openai, "gpt-5-mini"),
            canonical_model_id(Provider::Gemini, "gemini-3.5-flash-lite"),
            canonical_model_id(Provider::Gemini, "models/gemini-2.5-flash-lite"),
        ]
    });
    assert_eq!(
        resolved,
        [
            "gpt-5-mini",
            "gemini-3.5-flash-lite",
            "models/gemini-2.5-flash-lite",
        ]
    );
    assert!(
        warnings.is_empty(),
        "native IDs emitted warnings: {warnings:?}"
    );

    let (resolved, warnings) =
        capture_warnings(|| canonical_model_id(Provider::Anthropic, "claude-future-model"));
    assert_eq!(resolved, "claude-future-model");
    assert_eq!(
        warnings,
        vec![warning(UNKNOWN_ALIAS_MESSAGE, "claude-future-model")]
    );

    let (resolved, warnings) = capture_warnings(|| {
        (
            canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5"),
            canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5-20250929"),
        )
    });
    assert_eq!(
        resolved,
        ("claude-sonnet-4-5-20250929", "claude-sonnet-4-5-20250929")
    );
    assert!(
        warnings.is_empty(),
        "known rolling/dated Claude IDs emitted warnings: {warnings:?}"
    );

    let (threshold, warnings) =
        capture_warnings(|| cache_threshold_tokens("claude-future-threshold-model"));
    assert_eq!(threshold, 1024);
    assert_eq!(
        warnings,
        vec![warning(
            UNKNOWN_CACHE_THRESHOLD_MESSAGE,
            "claude-future-threshold-model"
        )]
    );
}
