//! Metric contract tests that assert against the rendered Prometheus
//! exposition, not against a second copy of the declared definitions.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use cc_lb_observability::{
    EngineMetricsHook, MetricDefinition, MetricKind, MetricsCrateHook, ProxyOnBodyChunk,
    ProxyOnResponse, cache_observation_dropped_reason, cache_observation_store_kind,
    inc_cache_observation_read_failed, inc_cache_observation_write_failed, metric_definitions,
    prometheus14_metric_definitions, register_metrics,
};
use http::{Response, StatusCode};
use metrics_exporter_prometheus::PrometheusBuilder;
use tower_http::trace::{OnBodyChunk as _, OnResponse as _};
use tracing::Span;

/// Label keys whose values identify who or what a request was attributed to.
const ATTRIBUTION_LABEL_KEYS: [&str; 5] =
    ["principal", "principal_id", "upstream", "model", "key_id"];

/// Labels the Prometheus exporter adds to histogram/summary series.
const EXPORTER_LABEL_KEYS: [&str; 2] = ["quantile", "le"];

#[derive(Debug)]
struct RenderedSample {
    family: String,
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
struct Exposition {
    samples: Vec<RenderedSample>,
    help: BTreeMap<String, String>,
    kinds: BTreeMap<String, String>,
}

impl Exposition {
    fn families(&self) -> BTreeSet<&str> {
        self.samples
            .iter()
            .map(|sample| sample.family.as_str())
            .collect()
    }

    fn samples_of<'a>(&'a self, family: &'a str) -> impl Iterator<Item = &'a RenderedSample> {
        self.samples
            .iter()
            .filter(move |sample| sample.family == family)
    }
}

fn definitions_by_name() -> BTreeMap<&'static str, MetricDefinition> {
    metric_definitions()
        .iter()
        .map(|definition| (definition.name, *definition))
        .collect()
}

fn render(record: impl FnOnce()) -> Exposition {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, record);
    parse_exposition(&handle.render(), &definitions_by_name())
}

fn parse_exposition(
    rendered: &str,
    definitions: &BTreeMap<&'static str, MetricDefinition>,
) -> Exposition {
    let mut exposition = Exposition::default();
    for line in rendered.lines().filter(|line| !line.trim().is_empty()) {
        if let Some(help) = line.strip_prefix("# HELP ") {
            let (name, text) = help.split_once(' ').unwrap_or((help, ""));
            exposition.help.insert(name.to_owned(), text.to_owned());
            continue;
        }
        if let Some(kind) = line.strip_prefix("# TYPE ") {
            let (name, kind) = kind.split_once(' ').expect("TYPE line has a kind");
            exposition.kinds.insert(name.to_owned(), kind.to_owned());
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let (series, labels) = match line.split_once('{') {
            Some((series, rest)) => {
                let end = rest.rfind('}').expect("label set closes");
                (series, parse_labels(&rest[..end]))
            }
            None => (
                line.split_once(' ').expect("sample has a value").0,
                BTreeMap::new(),
            ),
        };
        exposition.samples.push(RenderedSample {
            family: family_of(series, definitions),
            labels,
        });
    }
    exposition
}

/// Map a rendered series name to its metric family, folding the `_sum`,
/// `_count`, and `_bucket` series of a declared histogram into the histogram.
fn family_of(series: &str, definitions: &BTreeMap<&'static str, MetricDefinition>) -> String {
    if definitions.contains_key(series) {
        return series.to_owned();
    }
    for suffix in ["_sum", "_count", "_bucket"] {
        if let Some(base) = series.strip_suffix(suffix)
            && definitions
                .get(base)
                .is_some_and(|definition| definition.kind == MetricKind::Histogram)
        {
            return base.to_owned();
        }
    }
    series.to_owned()
}

fn parse_labels(raw: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    let mut rest = raw.trim_start_matches(',');
    while !rest.is_empty() {
        let (key, after_key) = rest.split_once("=\"").expect("label has a quoted value");
        let mut value = String::new();
        let mut chars = after_key.char_indices();
        let mut closing_quote = None;
        while let Some((index, character)) = chars.next() {
            match character {
                '\\' => {
                    if let Some((_, escaped)) = chars.next() {
                        value.push(if escaped == 'n' { '\n' } else { escaped });
                    }
                }
                '"' => {
                    closing_quote = Some(index);
                    break;
                }
                other => value.push(other),
            }
        }
        let closing_quote = closing_quote.expect("label value closes");
        labels.insert(key.to_owned(), value);
        rest = after_key[closing_quote + 1..].trim_start_matches(',');
    }
    labels
}

fn is_cc_lb_family(family: &str) -> bool {
    family.starts_with("cc_lb_") || family.starts_with("cclb_")
}

/// Assert every rendered `cc_lb_*`/`cclb_*` series against its declaration:
/// it is registered, its label keys equal the declared keys, its HELP text
/// and TYPE match the definition, and no attribution label is a placeholder.
fn assert_matches_declared_contract(exposition: &Exposition) {
    let definitions = definitions_by_name();
    let mut violations = Vec::new();
    for sample in &exposition.samples {
        if !is_cc_lb_family(&sample.family) {
            continue;
        }
        let Some(definition) = definitions.get(sample.family.as_str()) else {
            violations.push(format!("{} is emitted but not registered", sample.family));
            continue;
        };
        let emitted_keys = sample
            .labels
            .keys()
            .map(String::as_str)
            .filter(|key| !EXPORTER_LABEL_KEYS.contains(key))
            .collect::<BTreeSet<_>>();
        let declared_keys = definition.labels.iter().copied().collect::<BTreeSet<_>>();
        if emitted_keys != declared_keys {
            violations.push(format!(
                "{} emitted label keys {emitted_keys:?}, declared {declared_keys:?}",
                sample.family
            ));
        }
        for key in ATTRIBUTION_LABEL_KEYS {
            if sample.labels.get(key).map(String::as_str) == Some("unknown") {
                violations.push(format!(
                    "{} carries placeholder {key}=\"unknown\": {:?}",
                    sample.family, sample.labels
                ));
            }
        }
    }
    for family in exposition.families() {
        let Some(definition) = definitions.get(family) else {
            continue;
        };
        if exposition.help.get(family).map(String::as_str) != Some(definition.description) {
            violations.push(format!(
                "{family} HELP {:?} differs from declared description {:?}",
                exposition.help.get(family),
                definition.description
            ));
        }
        let expected_kinds: &[&str] = match definition.kind {
            MetricKind::Counter => &["counter"],
            MetricKind::Gauge => &["gauge"],
            MetricKind::Histogram => &["histogram", "summary"],
        };
        let rendered_kind = exposition.kinds.get(family).map(String::as_str);
        if !rendered_kind.is_some_and(|kind| expected_kinds.contains(&kind)) {
            violations.push(format!(
                "{family} TYPE {rendered_kind:?} does not match declared {:?}",
                definition.kind
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "rendered metrics violate their declared contract:\n{}",
        violations.join("\n")
    );
}

fn emit_representative_samples() {
    let response_hook = ProxyOnResponse;
    for status in [
        StatusCode::OK,
        StatusCode::NOT_FOUND,
        StatusCode::UNAUTHORIZED,
    ] {
        let response = Response::builder()
            .status(status)
            .body(())
            .expect("response builds");
        response_hook.on_response(&response, Duration::from_millis(3), &Span::none());
    }
    ProxyOnBodyChunk.on_body_chunk(
        &Bytes::from_static(b"data: {}\n\n"),
        Duration::from_millis(1),
        &Span::none(),
    );

    let engine_hook = MetricsCrateHook;
    engine_hook.record_cache_hit("upstream-a", "claude-test");
    engine_hook.record_cache_miss("upstream-a", "claude-test");
    engine_hook.record_cache_observation_dropped(cache_observation_dropped_reason::QUEUE_FULL);
    engine_hook.record_dropped_events_by("queue_full", 1);
    engine_hook.record_routing_tier_selection("known_base", "upstream-a", "principal-a");
    inc_cache_observation_write_failed(cache_observation_store_kind::SQLITE);
    inc_cache_observation_read_failed();
}

#[test]
fn registry_holds_every_definition_group_without_duplicates() {
    let definitions = metric_definitions();
    let names = definitions
        .iter()
        .map(|definition| definition.name)
        .collect::<Vec<_>>();
    let unique = names.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        unique.len(),
        names.len(),
        "duplicate metric names: {names:?}"
    );

    for definition in prometheus14_metric_definitions() {
        assert!(
            definitions.contains(definition),
            "{} is missing from metric_definitions()",
            definition.name
        );
    }
    for name in [
        "cc_lb_lifecycle_events_total",
        "cc_lb_limit_reservation_ttl_evicted_total",
        "cc_lb_request_stage_duration_seconds",
        "cc_lb_routing_tier_selections_total",
        "cc_lb_request_body_size_bytes",
        "cc_lb_request_unaccounted_duration_seconds",
        "cc_lb_request_diagnostics_lines_total",
    ] {
        assert!(unique.contains(name), "{name} is not registered");
    }
    assert_eq!(definitions.len(), 67);
}

#[test]
fn rendered_exposition_matches_declared_label_contract() {
    let exposition = render(|| {
        register_metrics();
        emit_representative_samples();
    });

    let families = exposition.families();
    for family in [
        "cc_lb_requests_started_total",
        "cc_lb_request_headers_duration_seconds",
        "cc_lb_response_body_chunks_total",
        "cc_lb_cache_hit_total",
        "cc_lb_routing_tier_selections_total",
    ] {
        assert!(families.contains(family), "{family} was not rendered");
    }
    let started_statuses = exposition
        .samples_of("cc_lb_requests_started_total")
        .filter_map(|sample| sample.labels.get("status").cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        started_statuses,
        BTreeSet::from(["200".to_owned(), "401".to_owned(), "404".to_owned()])
    );
    assert_matches_declared_contract(&exposition);
}

#[test]
fn zero_initialized_series_fabricate_no_attribution() {
    let exposition = render(register_metrics);

    assert_matches_declared_contract(&exposition);
    let placeholders = exposition
        .samples
        .iter()
        .filter(|sample| sample.labels.values().any(|value| value == "unknown"))
        .collect::<Vec<_>>();
    assert!(
        placeholders.is_empty(),
        "zero-init materialized placeholder series: {placeholders:?}"
    );
    let attributed = exposition
        .samples
        .iter()
        .filter(|sample| {
            ATTRIBUTION_LABEL_KEYS
                .iter()
                .any(|key| sample.labels.contains_key(*key))
        })
        .collect::<Vec<_>>();
    assert!(
        attributed.is_empty(),
        "zero-init must not pre-register attribution-labelled series: {attributed:?}"
    );
}

#[test]
fn request_diagnostics_series_are_zero_initialized_with_fixed_vocabulary() {
    let exposition = render(register_metrics);

    let outcomes = exposition
        .samples_of("cc_lb_request_diagnostics_lines_total")
        .filter_map(|sample| sample.labels.get("outcome").cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        outcomes,
        BTreeSet::from(["written", "encode_error", "write_error"].map(str::to_owned)),
        "every diagnostics line outcome is visible before the first request"
    );

    let reasons = exposition
        .samples_of("cc_lb_dropped_events_total")
        .filter_map(|sample| sample.labels.get("reason").cloned())
        .filter(|reason| reason.starts_with("request_diagnostics_"))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        reasons,
        BTreeSet::from(
            [
                "request_diagnostics_queue_full",
                "request_diagnostics_strip_queue_full",
                "request_diagnostics_strip_send_timeout",
                "request_diagnostics_queue_closed",
                "request_diagnostics_job_panic",
                "request_diagnostics_worker_panic",
                "request_diagnostics_shutdown_timeout",
            ]
            .map(str::to_owned)
        ),
        "every diagnostics drop reason is alertable from process start"
    );
}
