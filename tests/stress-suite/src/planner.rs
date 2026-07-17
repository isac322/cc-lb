use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng};

use crate::manifest::{
    DirectedEdge, DirectedTopology, FakeScriptReference, GeneratorIdentity, Manifest,
    ManifestError, ManifestLifecycle, NetemScheduleEntry, SCHEMA_VERSION, StorageSchedule,
    SymbolicNode, Wave,
};
use crate::traffic_generator::TrafficCatalog;
use crate::verdict::{EvidenceSkeleton, Verdict};

pub use crate::manifest::Profile;

const GENERATOR_DESCRIPTOR: &str = "cc-lb-stress-suite-manifest-generator/v2";

#[derive(Clone, Copy, Debug)]
pub struct PlanInput {
    seed: u64,
    profile: Profile,
    generated_at: u64,
}

impl PlanInput {
    pub const fn new(seed: u64, profile: Profile, generated_at: u64) -> Self {
        Self {
            seed,
            profile,
            generated_at,
        }
    }
}

pub fn plan(input: PlanInput) -> Result<Manifest, ManifestError> {
    let mut rng = StdRng::seed_from_u64(input.seed);
    let wave_schedule = waves(input.profile);
    let traffic = TrafficCatalog::new();
    let fake_scripts = fake_scripts();
    let requests = traffic.plan_requests(&wave_schedule, input.profile, &mut rng);
    let netem_schedule = netem_schedule(&wave_schedule, &mut rng);
    let mut manifest = Manifest {
        schema_version: SCHEMA_VERSION,
        generated_at: Some(input.generated_at),
        generator: GeneratorIdentity {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            content_hash: Manifest::sha256_hex(GENERATOR_DESCRIPTOR.as_bytes()),
        },
        seed: input.seed,
        profile: input.profile,
        run_id: format!("stress-{}-{:016x}", input.profile.as_str(), input.seed),
        topology: planned_topology(),
        topology_decision: None,
        wave_schedule,
        netem_schedule,
        requests,
        personas: traffic.personas,
        principals: traffic.principals,
        sessions: traffic.sessions,
        fake_scripts,
        storage_schedule: StorageSchedule {
            backend: "sqlite".to_owned(),
            reset_at_wave: "wave-1".to_owned(),
            collect_after_wave: "wave-2".to_owned(),
        },
        environment_compat_keys: environment_compat_keys(),
        evidence_skeleton: EvidenceSkeleton {
            schema_version: SCHEMA_VERSION,
            verdict: Verdict::NotComparable,
            manifest_schedule_hash: String::new(),
            labels: BTreeMap::new(),
        },
        schedule_hash: String::new(),
        integrity_hash: None,
    };
    manifest.schedule_hash = manifest.computed_schedule_hash()?;
    manifest.evidence_skeleton.manifest_schedule_hash = manifest.schedule_hash.clone();
    manifest.integrity_hash = Some(manifest.computed_integrity_hash()?);
    Ok(manifest)
}

fn planned_topology() -> DirectedTopology {
    DirectedTopology {
        lifecycle: ManifestLifecycle::Planned,
        symbolic_nodes: vec![
            SymbolicNode::Probe,
            SymbolicNode::CcLb,
            SymbolicNode::Postgres,
        ],
        edges: vec![
            DirectedEdge {
                edge_id: "probe-to-cc-lb".to_owned(),
                sender: SymbolicNode::Probe,
                receiver: SymbolicNode::CcLb,
                destination_ipv4: None,
                classid: None,
            },
            DirectedEdge {
                edge_id: "cc-lb-to-postgres".to_owned(),
                sender: SymbolicNode::CcLb,
                receiver: SymbolicNode::Postgres,
                destination_ipv4: None,
                classid: None,
            },
        ],
    }
}

fn waves(profile: Profile) -> Vec<Wave> {
    let mut starts_at_ms = 0;
    let mut waves = Vec::with_capacity(profile.wave_count());
    for number in 1..=profile.wave_count() {
        waves.push(Wave {
            wave_id: format!("wave-{number}"),
            starts_at_ms,
        });
        starts_at_ms += profile.wave_spacing_ms();
    }
    waves
}

fn netem_schedule(waves: &[Wave], rng: &mut StdRng) -> Vec<NetemScheduleEntry> {
    let mut schedule = Vec::with_capacity(waves.len() * 2);
    for wave in waves {
        for edge_id in ["probe-to-cc-lb", "cc-lb-to-postgres"] {
            schedule.push(NetemScheduleEntry {
                edge_id: edge_id.to_owned(),
                wave_id: wave.wave_id.clone(),
                trigger: "wave_boundary".to_owned(),
                seed: rng.random_range(1_u64..u64::MAX),
                command_reference: "topology_spec::render_for_finalized_edge".to_owned(),
            });
        }
    }
    schedule
}

fn fake_scripts() -> Vec<FakeScriptReference> {
    vec![
        FakeScriptReference {
            fake_script_id: "fake-alpha".to_owned(),
            path: "tests/fixtures/fake-anthropic/scripts/alpha.json".to_owned(),
            content_hash: Manifest::sha256_hex(b"fake-alpha-v1"),
        },
        FakeScriptReference {
            fake_script_id: "fake-beta".to_owned(),
            path: "tests/fixtures/fake-anthropic/scripts/beta.json".to_owned(),
            content_hash: Manifest::sha256_hex(b"fake-beta-v1"),
        },
    ]
}

fn environment_compat_keys() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("data_port_exposure".to_owned(), "none".to_owned()),
        ("fabric_mechanic".to_owned(), "netem-sidecar".to_owned()),
        ("storage_backend".to_owned(), "sqlite".to_owned()),
    ])
}

impl Profile {
    pub(crate) const fn wave_count(self) -> usize {
        match self {
            Self::Smoke => 2,
            Self::Soak => 4,
            Self::Burst => 3,
            Self::Leak => 5,
        }
    }

    pub(crate) const fn wave_spacing_ms(self) -> u64 {
        match self {
            Self::Smoke => 1_000,
            Self::Soak => 5_000,
            Self::Burst => 250,
            Self::Leak => 10_000,
        }
    }

    pub(crate) const fn requests_per_wave(self) -> usize {
        match self {
            Self::Smoke => 4,
            Self::Soak => 8,
            Self::Burst => 16,
            Self::Leak => 8,
        }
    }

    pub(crate) const fn send_window_ms(self) -> u64 {
        match self {
            Self::Smoke => 750,
            Self::Soak => 4_000,
            Self::Burst => 200,
            Self::Leak => 8_000,
        }
    }
}
