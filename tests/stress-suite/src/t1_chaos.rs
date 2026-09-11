use std::net::TcpListener;
use std::path::Path;
use std::process::ExitCode;

use serde::Serialize;

const OBSERVED_LATENCY_DELTA_MS: u64 = 150;
const LATENCY_MIN_MS: u64 = 450;
const LATENCY_MAX_MS: u64 = 1_500;
const DROP_MIN: u64 = 60;
const DROP_MAX: u64 = 140;
const RST_BODY_MAX_BYTES: usize = 255;
const EXPECTED_TRUNCATED_EVENTS: usize = 2;

#[derive(Serialize)]
struct T1ChaosEvidence {
    control_ms: u64,
    chaos_ms: u64,
    chaos_observed: bool,
    control_status: Option<u16>,
    chaos_status: Option<u16>,
    drop_attempts: u64,
    dropped: u64,
    passed: u64,
    drop_distribution_observed: bool,
    rst_status: Option<u16>,
    rst_body_bytes: Option<usize>,
    rst_terminated_early: bool,
    truncate_status: Option<u16>,
    truncate_data_events: Option<usize>,
    truncate_has_message_stop: Option<bool>,
    all_faults_observed: bool,
    fake_port: Option<u16>,
    control_proxy_port: Option<u16>,
    chaos_proxy_port: Option<u16>,
    error: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) struct Ports {
    pub(super) fake: u16,
    pub(super) control: ServerPorts,
    pub(super) latency: ServerPorts,
    pub(super) drop: ServerPorts,
    pub(super) rst: ServerPorts,
    pub(super) truncate: ServerPorts,
}

#[derive(Clone, Copy)]
pub(super) struct ServerPorts {
    pub(super) proxy: u16,
    pub(super) admin: u16,
    pub(super) metrics: u16,
}

pub const fn chaos_observed(control_ms: u64, chaos_ms: u64) -> bool {
    chaos_ms.saturating_sub(control_ms) >= OBSERVED_LATENCY_DELTA_MS
}

pub fn run(output: &Path) -> ExitCode {
    let evidence = match Ports::allocate() {
        Ok(ports) => match crate::t1_chaos_runtime::exercise(ports) {
            Ok(measurements) => from_measurements(ports, measurements),
            Err(error) => failed(Some(ports), error),
        },
        Err(error) => failed(None, error),
    };
    if let Err(error) = crate::evidence_redaction::write_redacted_json(output, &evidence) {
        eprintln!("write T1 chaos evidence {}: {error}", output.display());
        return ExitCode::FAILURE;
    }
    match (evidence.error, evidence.all_faults_observed) {
        (Some(error), _) => {
            eprintln!("T1 chaos exercise failed: {error}");
            ExitCode::FAILURE
        }
        (None, true) => ExitCode::SUCCESS,
        (None, false) => {
            eprintln!("T1 chaos fault modes were not all observed");
            ExitCode::FAILURE
        }
    }
}

fn from_measurements(
    ports: Ports,
    measurements: crate::t1_chaos_runtime::Measurements,
) -> T1ChaosEvidence {
    let latency_observed = chaos_observed(
        measurements.control.elapsed_ms,
        measurements.latency.elapsed_ms,
    );
    let latency_budget_observed = measurements.latency.elapsed_ms >= LATENCY_MIN_MS
        && measurements.latency.elapsed_ms < LATENCY_MAX_MS;
    let drop_distribution_observed = (DROP_MIN..=DROP_MAX).contains(&measurements.drop.dropped)
        && measurements.drop.dropped + measurements.drop.passed == measurements.drop.attempts;
    let rst_observed =
        measurements.rst.body_bytes <= RST_BODY_MAX_BYTES && measurements.rst.terminated_early;
    let truncate_observed = measurements.truncate.status == 200
        && measurements.truncate.data_events == EXPECTED_TRUNCATED_EVENTS
        && !measurements.truncate.has_message_stop;
    let error = validation_error(
        &measurements,
        latency_budget_observed,
        drop_distribution_observed,
        rst_observed,
        truncate_observed,
    );

    T1ChaosEvidence {
        control_ms: measurements.control.elapsed_ms,
        chaos_ms: measurements.latency.elapsed_ms,
        chaos_observed: latency_observed,
        control_status: Some(measurements.control.status),
        chaos_status: Some(measurements.latency.status),
        drop_attempts: measurements.drop.attempts,
        dropped: measurements.drop.dropped,
        passed: measurements.drop.passed,
        drop_distribution_observed,
        rst_status: measurements.rst.status,
        rst_body_bytes: Some(measurements.rst.body_bytes),
        rst_terminated_early: measurements.rst.terminated_early,
        truncate_status: Some(measurements.truncate.status),
        truncate_data_events: Some(measurements.truncate.data_events),
        truncate_has_message_stop: Some(measurements.truncate.has_message_stop),
        all_faults_observed: error.is_none(),
        fake_port: Some(ports.fake),
        control_proxy_port: Some(ports.control.proxy),
        chaos_proxy_port: Some(ports.latency.proxy),
        error,
    }
}

fn validation_error(
    measurements: &crate::t1_chaos_runtime::Measurements,
    latency_budget_observed: bool,
    drop_distribution_observed: bool,
    rst_observed: bool,
    truncate_observed: bool,
) -> Option<String> {
    let mut failures = Vec::new();
    if measurements.control.status != 200 || measurements.latency.status != 200 {
        failures.push(format!(
            "latency statuses control={} chaos={} (expected 200/200)",
            measurements.control.status, measurements.latency.status
        ));
    }
    if !latency_budget_observed {
        failures.push(format!(
            "chaos latency was {}ms (expected {LATENCY_MIN_MS}ms..{LATENCY_MAX_MS}ms)",
            measurements.latency.elapsed_ms
        ));
    }
    if !drop_distribution_observed {
        failures.push(format!(
            "drop distribution dropped={} passed={} attempts={} (expected dropped in {DROP_MIN}..={DROP_MAX})",
            measurements.drop.dropped, measurements.drop.passed, measurements.drop.attempts
        ));
    }
    if !rst_observed {
        failures.push(format!(
            "RST observation status={:?} body_bytes={} terminated_early={} (expected <= {RST_BODY_MAX_BYTES}, true)",
            measurements.rst.status,
            measurements.rst.body_bytes,
            measurements.rst.terminated_early
        ));
    }
    if !truncate_observed {
        failures.push(format!(
            "SSE truncation status={} data_events={} message_stop={} (expected 200, {EXPECTED_TRUNCATED_EVENTS}, false)",
            measurements.truncate.status,
            measurements.truncate.data_events,
            measurements.truncate.has_message_stop
        ));
    }
    (!failures.is_empty()).then(|| failures.join("; "))
}

fn failed(ports: Option<Ports>, error: String) -> T1ChaosEvidence {
    T1ChaosEvidence {
        control_ms: 0,
        chaos_ms: 0,
        chaos_observed: false,
        control_status: None,
        chaos_status: None,
        drop_attempts: 0,
        dropped: 0,
        passed: 0,
        drop_distribution_observed: false,
        rst_status: None,
        rst_body_bytes: None,
        rst_terminated_early: false,
        truncate_status: None,
        truncate_data_events: None,
        truncate_has_message_stop: None,
        all_faults_observed: false,
        fake_port: ports.map(|ports| ports.fake),
        control_proxy_port: ports.map(|ports| ports.control.proxy),
        chaos_proxy_port: ports.map(|ports| ports.latency.proxy),
        error: Some(error),
    }
}

impl Ports {
    fn allocate() -> Result<Self, String> {
        let listeners = (0..16)
            .map(|_| TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let values = listeners
            .iter()
            .map(|listener| listener.local_addr().map(|address| address.port()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        if values.len() != 16 {
            return Err("failed to allocate T1 chaos ports".to_owned());
        }
        Ok(Self {
            fake: values[0],
            control: server_ports(&values, 1),
            latency: server_ports(&values, 4),
            drop: server_ports(&values, 7),
            rst: server_ports(&values, 10),
            truncate: server_ports(&values, 13),
        })
    }
}

fn server_ports(values: &[u16], offset: usize) -> ServerPorts {
    ServerPorts {
        proxy: values[offset],
        admin: values[offset + 1],
        metrics: values[offset + 2],
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    #[allow(non_snake_case)]
    fn tx__chaos_fault_modes_end_to_end() {
        let ports = Ports::allocate().expect("allocate chaos ports");
        let measurements = crate::t1_chaos_runtime::exercise(ports).expect("exercise chaos modes");
        let evidence = from_measurements(ports, measurements);

        assert_eq!(evidence.control_status, Some(200));
        assert_eq!(evidence.chaos_status, Some(200));
        assert!(
            evidence.chaos_observed,
            "evidence={}",
            evidence.error.as_deref().unwrap_or("none")
        );
        assert!(
            (LATENCY_MIN_MS..LATENCY_MAX_MS).contains(&evidence.chaos_ms),
            "chaos_ms={}",
            evidence.chaos_ms
        );
        assert_eq!(evidence.drop_attempts, 200);
        assert!(
            (DROP_MIN..=DROP_MAX).contains(&evidence.dropped),
            "dropped={} passed={}",
            evidence.dropped,
            evidence.passed
        );
        assert_eq!(evidence.dropped + evidence.passed, evidence.drop_attempts);
        assert!(
            evidence
                .rst_body_bytes
                .is_some_and(|bytes| bytes <= RST_BODY_MAX_BYTES),
            "rst_body_bytes={:?}",
            evidence.rst_body_bytes
        );
        assert!(evidence.rst_terminated_early);
        assert_eq!(evidence.truncate_status, Some(200));
        assert_eq!(
            evidence.truncate_data_events,
            Some(EXPECTED_TRUNCATED_EVENTS)
        );
        assert_eq!(evidence.truncate_has_message_stop, Some(false));
        assert!(
            evidence.all_faults_observed,
            "evidence={}",
            evidence.error.as_deref().unwrap_or("none")
        );
    }
}
