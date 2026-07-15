use std::net::TcpListener;
use std::path::Path;
use std::process::ExitCode;

use serde::Serialize;

const OBSERVED_LATENCY_DELTA_MS: u64 = 150;

#[derive(Serialize)]
struct T1ChaosEvidence {
    control_ms: u64,
    chaos_ms: u64,
    chaos_observed: bool,
    control_status: Option<u16>,
    chaos_status: Option<u16>,
    fake_port: Option<u16>,
    control_proxy_port: Option<u16>,
    chaos_proxy_port: Option<u16>,
    error: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) struct Ports {
    pub(super) fake: u16,
    pub(super) control: ServerPorts,
    pub(super) chaos: ServerPorts,
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
            Ok(measurements) => T1ChaosEvidence {
                control_ms: measurements.control.elapsed_ms,
                chaos_ms: measurements.chaos.elapsed_ms,
                chaos_observed: chaos_observed(
                    measurements.control.elapsed_ms,
                    measurements.chaos.elapsed_ms,
                ),
                control_status: Some(measurements.control.status),
                chaos_status: Some(measurements.chaos.status),
                fake_port: Some(ports.fake),
                control_proxy_port: Some(ports.control.proxy),
                chaos_proxy_port: Some(ports.chaos.proxy),
                error: None,
            },
            Err(error) => failed(Some(ports), error),
        },
        Err(error) => failed(None, error),
    };
    if let Err(error) = crate::evidence_redaction::write_redacted_json(output, &evidence) {
        eprintln!("write T1 chaos evidence {}: {error}", output.display());
        return ExitCode::FAILURE;
    }
    match (evidence.error, evidence.chaos_observed) {
        (Some(error), _) => {
            eprintln!("T1 chaos exercise failed: {error}");
            ExitCode::FAILURE
        }
        (None, true) => ExitCode::SUCCESS,
        (None, false) => {
            eprintln!("T1 chaos latency was not observed");
            ExitCode::FAILURE
        }
    }
}

fn failed(ports: Option<Ports>, error: String) -> T1ChaosEvidence {
    T1ChaosEvidence {
        control_ms: 0,
        chaos_ms: 0,
        chaos_observed: false,
        control_status: None,
        chaos_status: None,
        fake_port: ports.map(|ports| ports.fake),
        control_proxy_port: ports.map(|ports| ports.control.proxy),
        chaos_proxy_port: ports.map(|ports| ports.chaos.proxy),
        error: Some(error),
    }
}

impl Ports {
    fn allocate() -> Result<Self, String> {
        let listeners = (0..7)
            .map(|_| TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let values = listeners
            .iter()
            .map(|listener| listener.local_addr().map(|address| address.port()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        let [
            fake,
            control_proxy,
            control_admin,
            control_metrics,
            chaos_proxy,
            chaos_admin,
            chaos_metrics,
        ] = values.as_slice()
        else {
            return Err("failed to allocate T1 ports".to_owned());
        };
        Ok(Self {
            fake: *fake,
            control: ServerPorts {
                proxy: *control_proxy,
                admin: *control_admin,
                metrics: *control_metrics,
            },
            chaos: ServerPorts {
                proxy: *chaos_proxy,
                admin: *chaos_admin,
                metrics: *chaos_metrics,
            },
        })
    }
}
