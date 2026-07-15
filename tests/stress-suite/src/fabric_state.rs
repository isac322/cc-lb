use crate::docker::{Docker, DockerOutput};
use crate::topology_decision::{
    BlockedStage, CleanupReceipt, NodeAddress, SCHEMA_VERSION, TopologyDecision, Verdict,
};

pub const ALPINE_IMAGE: &str = "alpine:3.21";
pub const POSTGRES_IMAGE: &str = "postgres:18";
pub const SUBNET: &str = "172.30.0.0/24";
pub const PROBE_IP: &str = "172.30.0.2";
pub const CCLB_IP: &str = "172.30.0.3";
pub const POSTGRES_IP: &str = "172.30.0.4";

const LABEL_KEY: &str = "com.cc-lb.stress.run";

pub struct CleanupTargets {
    pub label: String,
    pub containers: Vec<String>,
    pub network: String,
    pub helper_image: String,
}

pub struct ResourceNames {
    pub network: String,
    pub helper_image: String,
    pub probe: String,
    pub cc_lb: String,
    pub postgres: String,
    pub probe_sidecar: String,
    pub cc_lb_sidecar: String,
    pub postgres_counter: String,
}

impl ResourceNames {
    pub fn new(run_id: &str) -> Result<Self, String> {
        if run_id.is_empty() || run_id.len() > 32 || !run_id.chars().all(valid_run_id_character) {
            return Err(
                "run-id must be 1-32 ASCII letters, digits, dots, underscores, or dashes"
                    .to_owned(),
            );
        }
        let suffix = run_id.to_ascii_lowercase();
        Ok(Self {
            network: format!("ccstress_{suffix}"),
            helper_image: format!("cc-lb-stress-netem:{suffix}"),
            probe: format!("ccstress_{suffix}_probe"),
            cc_lb: format!("ccstress_{suffix}_cc_lb"),
            postgres: format!("ccstress_{suffix}_postgres"),
            probe_sidecar: format!("ccstress_{suffix}_netem_probe"),
            cc_lb_sidecar: format!("ccstress_{suffix}_netem_cc_lb"),
            postgres_counter: format!("ccstress_{suffix}_counter_postgres"),
        })
    }

    fn container_names(&self) -> Vec<String> {
        vec![
            self.probe.clone(),
            self.cc_lb.clone(),
            self.postgres.clone(),
            self.probe_sidecar.clone(),
            self.cc_lb_sidecar.clone(),
            self.postgres_counter.clone(),
        ]
    }
}

pub fn cleanup_targets(names: &ResourceNames, run_id: &str) -> CleanupTargets {
    CleanupTargets {
        label: label(run_id),
        containers: names.container_names(),
        network: names.network.clone(),
        helper_image: names.helper_image.clone(),
    }
}

pub struct FabricCleanup<'a> {
    docker: &'a Docker,
    names: &'a ResourceNames,
    run_id: &'a str,
    complete: bool,
}

impl<'a> FabricCleanup<'a> {
    pub fn new(docker: &'a Docker, names: &'a ResourceNames, run_id: &'a str) -> Self {
        Self {
            docker,
            names,
            run_id,
            complete: false,
        }
    }

    pub fn cleanup(&mut self) -> CleanupReceipt {
        if self.complete {
            return CleanupReceipt::empty();
        }
        self.complete = true;
        let targets = cleanup_targets(self.names, self.run_id);
        let mut container_ids = self
            .docker
            .run(&[
                "ps".to_owned(),
                "-aq".to_owned(),
                "--filter".to_owned(),
                format!("label={}", targets.label),
            ])
            .map(|output| output.stdout.lines().map(str::to_owned).collect::<Vec<_>>())
            .unwrap_or_default();
        container_ids.extend(targets.containers);
        container_ids.sort();
        container_ids.dedup();
        let mut containers_removed = 0;
        for container_id in &container_ids {
            if self
                .docker
                .run(&["rm".to_owned(), "-f".to_owned(), container_id.clone()])
                .is_ok()
            {
                containers_removed += 1;
            }
        }
        let network_removed = self
            .docker
            .run(&["network".to_owned(), "rm".to_owned(), targets.network])
            .is_ok();
        let image_removed = self
            .docker
            .run(&[
                "image".to_owned(),
                "rm".to_owned(),
                "-f".to_owned(),
                targets.helper_image,
            ])
            .is_ok();
        CleanupReceipt {
            attempted: true,
            containers_removed,
            network_removed,
            image_removed,
        }
    }
}

impl Drop for FabricCleanup<'_> {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

pub fn label(run_id: &str) -> String {
    format!("{LABEL_KEY}={run_id}")
}

pub fn label_filter(run_id: &str) -> String {
    format!("label={}", label(run_id))
}

pub fn docker_command(
    docker: &Docker,
    args: Vec<String>,
    operation: &str,
) -> Result<String, String> {
    docker
        .run(&args)
        .map(|output: DockerOutput| output.stdout)
        .map_err(|error| format!("{operation}: {error}"))
}

pub fn blocked(run_id: &str, reason: String, cleanup: CleanupReceipt) -> TopologyDecision {
    TopologyDecision {
        schema_version: SCHEMA_VERSION,
        verdict: Verdict::Blocked,
        run_id: run_id.to_owned(),
        chosen_fabric: None,
        mechanic: None,
        network: None,
        impaired_edges: Vec::new(),
        host_published_data_ports: Vec::new(),
        blocked_stage: Some(BlockedStage::Preflight),
        blocked_reason: Some(reason),
        sidecar_unworkable_reason: None,
        cleanup,
    }
}

pub fn node(name: &str, ipv4: &str) -> NodeAddress {
    NodeAddress {
        name: name.to_owned(),
        ipv4: ipv4.to_owned(),
    }
}

fn valid_run_id_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
}
