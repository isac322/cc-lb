use std::net::TcpListener;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct ReplicaPorts {
    pub proxy: u16,
    pub admin: u16,
    pub metrics: u16,
}

impl ReplicaPorts {
    pub const fn new(proxy: u16, admin: u16, metrics: u16) -> Self {
        Self {
            proxy,
            admin,
            metrics,
        }
    }

    pub fn allocate_many(replicas: usize) -> Result<Vec<Self>, String> {
        let total = replicas
            .checked_mul(3)
            .ok_or_else(|| "replica port count overflow".to_owned())?;
        let listeners = (0..total)
            .map(|_| TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let ports = listeners
            .iter()
            .map(|listener| listener.local_addr().map(|address| address.port()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        ports
            .chunks_exact(3)
            .map(|ports| Ok(Self::new(ports[0], ports[1], ports[2])))
            .collect()
    }

    #[cfg(test)]
    pub const fn all(self) -> [u16; 3] {
        [self.proxy, self.admin, self.metrics]
    }
}

pub struct ReplicaConfig<'a> {
    pub storage_url: &'a str,
    pub ports: ReplicaPorts,
    pub data_dir: &'a str,
    pub instance_url: &'a str,
}

#[cfg(test)]
pub fn render_replica_config(storage_url: &str, ports: ReplicaPorts) -> String {
    render_config(&ReplicaConfig {
        storage_url,
        ports,
        data_dir: "/tmp/cc-lb-stress",
        instance_url: "http://replica.invalid",
    })
}

pub fn render_config(input: &ReplicaConfig<'_>) -> String {
    format!(
        r#"[listener]
proxy_addr = "0.0.0.0:{proxy}"
admin_addr = "0.0.0.0:{admin}"
metrics_addr = "0.0.0.0:{metrics}"

[body]
messages_cap_bytes = 33554432
files_cap_bytes = 104857600

[timeouts]
request_header_secs = 10
request_body_chunk_secs = 30
idle_secs = 300
upstream_total_secs = 30
drain_secs = 5

[downstream_auth]
mode = "api_key"

[storage]
kind = "postgres"
url = "{storage_url}"

[storage.pool]
max_connections = 8
acquire_timeout_secs = 10
statement_timeout_secs = 25
sslmode = "disable"

[event_bus]
storage_tail_poll_interval_ms = 100

[cluster]
instance_url = "{instance_url}"

[aead]
key_env = "CC_LB_MASTER_KEY"

[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[runtime]
data_dir = "{data_dir}"

[egress]
"#,
        proxy = input.ports.proxy,
        admin = input.ports.admin,
        metrics = input.ports.metrics,
        storage_url = input.storage_url,
        instance_url = input.instance_url,
        data_dir = input.data_dir,
    )
}
