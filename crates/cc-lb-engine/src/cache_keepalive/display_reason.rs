use std::fmt;
use std::time::Duration;

use serde_json::Value;

pub(crate) enum CacheKeepaliveDisplayReason {
    AgentToolUse {
        tool_name: String,
        first_renewal_delay: Duration,
    },
    AgentInTurn {
        first_renewal_delay: Duration,
    },
    UserTurn {
        stop_reason: String,
    },
    AmbiguousTurn,
    SnapshotTooLarge,
}

impl CacheKeepaliveDisplayReason {
    pub(crate) fn agent_in_turn(response: &Value, first_renewal_delay: Duration) -> Self {
        match first_tool_use_name(response) {
            Some(tool_name) => Self::AgentToolUse {
                tool_name,
                first_renewal_delay,
            },
            None => Self::AgentInTurn {
                first_renewal_delay,
            },
        }
    }

    pub(crate) fn user_turn(response: &Value) -> Self {
        let stop_reason = response
            .get("stop_reason")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        Self::UserTurn { stop_reason }
    }
}

impl fmt::Display for CacheKeepaliveDisplayReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AgentToolUse {
                tool_name,
                first_renewal_delay,
            } => write!(
                f,
                "agent-in-turn (tool_use: `{tool_name}`) — first renewal in {}",
                format_duration(*first_renewal_delay)
            ),
            Self::AgentInTurn {
                first_renewal_delay,
            } => write!(
                f,
                "agent-in-turn — first renewal in {}",
                format_duration(*first_renewal_delay)
            ),
            Self::UserTurn { stop_reason } => write!(f, "user turn (stop_reason={stop_reason})"),
            Self::AmbiguousTurn => f.write_str("ambiguous turn"),
            Self::SnapshotTooLarge => f.write_str("snapshot too large"),
        }
    }
}

fn first_tool_use_name(response: &Value) -> Option<String> {
    response
        .get("content")
        .and_then(Value::as_array)
        .and_then(|content| {
            content.iter().find_map(|block| {
                (block.get("type").and_then(Value::as_str) == Some("tool_use"))
                    .then(|| block.get("name").and_then(Value::as_str))
                    .flatten()
                    .map(ToOwned::to_owned)
            })
        })
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    match (minutes, remaining_seconds) {
        (0, _) => format!("{seconds}s"),
        (_, 0) => format!("{minutes}m"),
        _ => format!("{minutes}m {remaining_seconds}s"),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::CacheKeepaliveDisplayReason;

    #[test]
    fn agent_tool_use_reason_when_scheduling_first_renewal() {
        // Given: an agent response that invokes a named tool.
        let response = json!({
            "content": [{"type": "tool_use", "name": "bash"}]
        });

        // When: the initial renewal projection is rendered.
        let reason =
            CacheKeepaliveDisplayReason::agent_in_turn(&response, Duration::from_secs(270))
                .to_string();

        // Then: the persisted UI reason retains the tool and first-renewal delay.
        assert_eq!(
            reason,
            "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s"
        );
    }

    #[test]
    fn not_tracked_reason_when_user_ends_turn() {
        // Given: a response that explicitly ends the user turn.
        let response = json!({"stop_reason": "end_turn"});

        // When: the decision-only projection is rendered.
        let reason = CacheKeepaliveDisplayReason::user_turn(&response).to_string();

        // Then: the UI gets a human-readable decision rather than an enum token.
        assert_eq!(reason, "user turn (stop_reason=end_turn)");
    }
}
