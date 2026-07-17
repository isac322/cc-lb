use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Verdict {
    Pass,
    Fail,
    Blocked,
    NotComparable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EvidenceSkeleton {
    pub schema_version: u16,
    pub verdict: Verdict,
    pub manifest_schedule_hash: String,
    pub labels: BTreeMap<String, Verdict>,
}
