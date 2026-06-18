//! Configuration for the distributed scheduler.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub separate_pool: bool,
    pub max_connections: u32,
    pub min_connections: u32,
}
