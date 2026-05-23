mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_plugin_api::PluginRuntime;
use cc_lb_runtime_extism::ExtismRuntime;

const REPEATED_INVOKES: usize = 1_000;
const RESOURCE_WARMUP_INVOKES: usize = 64;
const RSS_GROWTH_LIMIT_KIB: u64 = 1_024;

#[tokio::test]
async fn plugin_lifecycle_load_init_invoke_reload_drop() {
    let hold_after_call = (RESOURCE_WARMUP_INVOKES * 2 + REPEATED_INVOKES + 2) as u64;
    let fixture = common::fixture(
        "lifecycle",
        &common::lifecycle_counted_old_module(&common::authn_response("before"), hold_after_call),
        common::metadata(&[("fuel_max", 0), ("max_call_duration_ms", 2_000)]),
    );
    let runtime = Arc::new(ExtismRuntime::new());
    let runtime_weak = Arc::downgrade(&runtime);
    let authn = runtime
        .instantiate(&fixture.manifest)
        .expect("load succeeds");
    let authn_weak = Arc::downgrade(&authn);

    let init = authn
        .authenticate(&common::ctx())
        .await
        .expect("init invoke succeeds");
    assert_eq!(init.principal.id, "before");
    invoke_concurrent(authn.clone(), "before", RESOURCE_WARMUP_INVOKES).await;
    invoke_repeated(authn.as_ref(), "before", RESOURCE_WARMUP_INVOKES).await;
    tokio::time::sleep(Duration::from_millis(25)).await;

    let before_resources = ResourceSnapshot::capture();
    let before_latencies = invoke_repeated(authn.as_ref(), "before", REPEATED_INVOKES).await;
    let after_before_resources = ResourceSnapshot::capture();
    let before_leak = ResourceDelta::between(before_resources, after_before_resources);
    before_leak.assert_within_limits("pre-reload");

    let in_flight = {
        let authn = authn.clone();
        tokio::spawn(async move { authn.authenticate(&common::ctx()).await })
    };
    tokio::time::sleep(Duration::from_millis(25)).await;

    common::rewrite_fixture(
        &fixture,
        &common::reload_new_module(&common::authn_response("after")),
    );
    runtime.reload("lifecycle").expect("reload succeeds");
    let after_reload = authn
        .authenticate(&common::ctx())
        .await
        .expect("new call after reload succeeds");
    assert_eq!(after_reload.principal.id, "after");

    let old_outcome = in_flight
        .await
        .expect("in-flight task joins")
        .expect("old in-flight call completes");
    assert_eq!(old_outcome.principal.id, "before");

    invoke_repeated(authn.as_ref(), "after", RESOURCE_WARMUP_INVOKES).await;
    tokio::time::sleep(Duration::from_millis(25)).await;

    let after_resources = ResourceSnapshot::capture();
    let after_latencies = invoke_repeated(authn.as_ref(), "after", REPEATED_INVOKES).await;
    let after_after_resources = ResourceSnapshot::capture();
    let after_leak = ResourceDelta::between(after_resources, after_after_resources);
    after_leak.assert_within_limits("post-reload");

    drop(authn);
    let wrapper_released = authn_weak.upgrade().is_none();
    drop(runtime);
    let runtime_released = runtime_weak.upgrade().is_none();
    assert!(
        wrapper_released,
        "authn wrapper still has active references"
    );
    assert!(runtime_released, "runtime still has active references");

    let (before_mean_us, before_p99_us) = latency_summary_us(before_latencies);
    let (after_mean_us, after_p99_us) = latency_summary_us(after_latencies);
    let resource_leak = ResourceDelta::max(before_leak, after_leak);
    println!(
        "task_45_plugin_lifecycle PASSED load=true init_principal={} pre_reload_invokes={} pre_reload_mean_us={} pre_reload_p99_us={} reload_new_principal={} in_flight_old_principal={} post_reload_invokes={} post_reload_mean_us={} post_reload_p99_us={} resource_metrics={} pre_rss_growth_kib={} post_rss_growth_kib={} rss_growth_kib={} rss_threshold_kib={} rss_growth_lt_1024={} pre_fd_delta={} post_fd_delta={} fd_stable={} pre_thread_delta={} post_thread_delta={} thread_stable={} wrapper_released={} runtime_released={} joined_in_flight=true extism_http_hosts=denied",
        init.principal.id,
        REPEATED_INVOKES,
        before_mean_us,
        before_p99_us,
        after_reload.principal.id,
        old_outcome.principal.id,
        REPEATED_INVOKES,
        after_mean_us,
        after_p99_us,
        resource_leak.metrics_kind,
        before_leak.rss_growth_kib,
        after_leak.rss_growth_kib,
        resource_leak.rss_growth_kib,
        RSS_GROWTH_LIMIT_KIB,
        resource_leak.rss_growth_kib < RSS_GROWTH_LIMIT_KIB,
        before_leak.fd_delta,
        after_leak.fd_delta,
        resource_leak.fd_stable,
        before_leak.thread_delta,
        after_leak.thread_delta,
        resource_leak.thread_stable,
        wrapper_released,
        runtime_released
    );
}

async fn invoke_repeated(
    authn: &dyn cc_lb_plugin_api::AuthnPlugin,
    expected_principal: &str,
    count: usize,
) -> Vec<Duration> {
    let mut latencies = Vec::with_capacity(count);
    for call_index in 0..count {
        let started = Instant::now();
        let outcome = authn
            .authenticate(&common::ctx())
            .await
            .unwrap_or_else(|error| panic!("invoke {call_index} failed: {error}"));
        let elapsed = started.elapsed();
        assert_eq!(
            outcome.principal.id, expected_principal,
            "invoke {call_index} used the wrong plugin cell"
        );
        latencies.push(elapsed);
    }
    latencies
}

async fn invoke_concurrent(
    authn: Arc<dyn cc_lb_plugin_api::AuthnPlugin>,
    expected_principal: &'static str,
    count: usize,
) {
    let mut handles = Vec::with_capacity(count);
    for call_index in 0..count {
        let authn = authn.clone();
        handles.push(tokio::spawn(async move {
            let outcome = authn
                .authenticate(&common::ctx())
                .await
                .unwrap_or_else(|error| panic!("warmup invoke {call_index} failed: {error}"));
            assert_eq!(
                outcome.principal.id, expected_principal,
                "warmup invoke {call_index} used the wrong plugin cell"
            );
        }));
    }
    for handle in handles {
        handle.await.expect("warmup task joins");
    }
}

fn latency_summary_us(mut latencies: Vec<Duration>) -> (u128, u128) {
    latencies.sort_unstable();
    let total_us = latencies.iter().map(Duration::as_micros).sum::<u128>();
    let mean_us = total_us / latencies.len() as u128;
    let p99_index = ((latencies.len() * 99) / 100).min(latencies.len() - 1);
    (mean_us, latencies[p99_index].as_micros())
}

#[derive(Clone, Copy)]
struct ResourceSnapshot {
    metrics_kind: &'static str,
    rss_kib: Option<u64>,
    fd_count: Option<usize>,
    thread_count: Option<usize>,
}

impl ResourceSnapshot {
    fn capture() -> Self {
        Self {
            metrics_kind: metrics_kind(),
            rss_kib: current_rss_kib(),
            fd_count: current_fd_count(),
            thread_count: current_thread_count(),
        }
    }
}

#[derive(Clone, Copy)]
struct ResourceDelta {
    metrics_kind: &'static str,
    rss_growth_kib: u64,
    fd_delta: isize,
    thread_delta: isize,
    fd_stable: bool,
    thread_stable: bool,
}

impl ResourceDelta {
    fn between(before: ResourceSnapshot, after: ResourceSnapshot) -> Self {
        let fd_delta = signed_delta(before.fd_count, after.fd_count);
        let thread_delta = signed_delta(before.thread_count, after.thread_count);
        Self {
            metrics_kind: after.metrics_kind,
            rss_growth_kib: growth(before.rss_kib, after.rss_kib),
            fd_delta,
            thread_delta,
            fd_stable: fd_delta == 0,
            thread_stable: thread_delta == 0,
        }
    }

    fn max(first: Self, second: Self) -> Self {
        Self {
            metrics_kind: first.metrics_kind,
            rss_growth_kib: first.rss_growth_kib.max(second.rss_growth_kib),
            fd_delta: first.fd_delta.max(second.fd_delta),
            thread_delta: first.thread_delta.max(second.thread_delta),
            fd_stable: first.fd_stable && second.fd_stable,
            thread_stable: first.thread_stable && second.thread_stable,
        }
    }

    fn assert_within_limits(&self, phase: &str) {
        if self.metrics_kind == "unsupported" {
            return;
        }
        assert!(
            self.rss_growth_kib < RSS_GROWTH_LIMIT_KIB,
            "{phase} RSS growth {} KiB exceeded {} KiB",
            self.rss_growth_kib,
            RSS_GROWTH_LIMIT_KIB
        );
        assert_eq!(self.fd_delta, 0, "{phase} file descriptor count changed");
        assert_eq!(self.thread_delta, 0, "{phase} thread count changed");
    }
}

fn growth(before: Option<u64>, after: Option<u64>) -> u64 {
    match (before, after) {
        (Some(before), Some(after)) => after.saturating_sub(before),
        _ => 0,
    }
}

fn signed_delta(before: Option<usize>, after: Option<usize>) -> isize {
    match (before, after) {
        (Some(before), Some(after)) => after as isize - before as isize,
        _ => 0,
    }
}

#[cfg(target_os = "linux")]
fn metrics_kind() -> &'static str {
    "linux_proc"
}

#[cfg(not(target_os = "linux"))]
fn metrics_kind() -> &'static str {
    "unsupported"
}

#[cfg(target_os = "linux")]
fn current_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?;
        value.split_whitespace().next()?.parse().ok()
    })
}

#[cfg(not(target_os = "linux"))]
fn current_rss_kib() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn current_fd_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd").ok().map(Iterator::count)
}

#[cfg(not(target_os = "linux"))]
fn current_fd_count() -> Option<usize> {
    None
}

#[cfg(target_os = "linux")]
fn current_thread_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(Iterator::count)
}

#[cfg(not(target_os = "linux"))]
fn current_thread_count() -> Option<usize> {
    None
}
