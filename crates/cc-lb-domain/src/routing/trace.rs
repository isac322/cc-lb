use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::SubscriptionPreferenceTrace;

/// Strategy for selecting a terminal upstream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TerminalStrategy {
    /// Select first available upstream.
    #[default]
    FirstPick,
    /// Select a router plugin at random.
    Random,
}

/// Decision made at a single routing stage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageDecision {
    /// Name of the routing stage.
    pub stage_name: String,
    /// Upstream candidate identifier if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    /// Reason for this stage's decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Time spent executing this routing stage, in microseconds.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub duration_us: u64,
    /// Optional subscription-preference trace payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_preference: Option<SubscriptionPreferenceTrace>,
}

const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Terminal routing decision selecting an upstream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TerminalDecision {
    /// Selected upstream identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    /// Strategy used for selection.
    pub strategy: TerminalStrategy,
}

/// Complete routing trace for a request through all decision stages.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RoutingTrace {
    /// Sequence of stage decisions made during routing.
    pub stages: Vec<StageDecision>,
    /// Final terminal routing decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_decision: Option<TerminalDecision>,
}
