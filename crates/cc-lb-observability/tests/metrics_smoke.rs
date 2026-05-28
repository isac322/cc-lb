use std::sync::OnceLock;

use cc_lb_observability::{
    prometheus14_metric_definitions, register_metrics, touch_prometheus14_metrics,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();

fn prometheus_handle() -> &'static PrometheusHandle {
    PROMETHEUS.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("prometheus recorder installs for metrics smoke tests")
    })
}

#[test]
fn metric_appears() {
    let handle = prometheus_handle();
    register_metrics();
    touch_prometheus14_metrics();

    let rendered = handle.render();
    for definition in prometheus14_metric_definitions() {
        assert!(
            rendered.contains(definition.name),
            "missing metric {} in prometheus output:\n{}",
            definition.name,
            rendered
        );
        println!("OK {}", definition.name);
    }

    let sample = rendered
        .lines()
        .find(|line| line.starts_with("cclb_api_key_requests_total"))
        .expect("sample api key requests metric line");
    println!("sample {sample}");
}

#[test]
fn label_cardinality() {
    let handle = prometheus_handle();
    register_metrics();

    for index in 0..100 {
        metrics::counter!(
            "cclb_api_key_requests_total",
            "key_id" => format!("cardinality-key-{index}"),
            "principal_id" => format!("cardinality-principal-{index}"),
            "model" => format!("cardinality-model-{index}"),
            "upstream_kind" => "anthropic",
            "status" => "200"
        )
        .increment(1);
    }

    let rendered = handle.render();
    for index in 0..100 {
        assert!(rendered.contains(&format!("cardinality-key-{index}")));
        assert!(rendered.contains(&format!("cardinality-principal-{index}")));
        assert!(rendered.contains(&format!("cardinality-model-{index}")));
    }
}
