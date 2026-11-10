use std::collections::HashSet;

use serde_json::Value;

use cc_lb_storage_api::ClassifierConfig;

use super::metrics;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnDecision {
    UserTurn,
    AgentInTurn,
    Ambiguous,
}

impl TurnDecision {
    fn as_metric_label(self) -> &'static str {
        match self {
            Self::UserTurn => "user_turn",
            Self::AgentInTurn => "agent_in_turn",
            Self::Ambiguous => "ambiguous",
        }
    }
}

const BUILTIN_WAIT_FOR_USER_TOOLS: &[&str] = &[
    "AskUserQuestion",
    "ExitPlanMode",
    "ask_followup_question",
    "ask_question",
    "cursor_ask_question",
    "question",
    "ask",
    "attempt_completion",
    "submit_and_exit",
    "finish",
    "plan_mode_respond",
    "act_mode_respond",
    "report_bug",
    "StructuredOutput",
];

pub struct HeuristicClassifier<'a> {
    config: &'a ClassifierConfig,
    wait_for_user: HashSet<&'a str>,
}

impl<'a> HeuristicClassifier<'a> {
    pub fn new(config: &'a ClassifierConfig) -> Self {
        let mut wait_for_user: HashSet<&str> =
            BUILTIN_WAIT_FOR_USER_TOOLS.iter().copied().collect();
        for extra in &config.extra_wait_for_user_tools {
            wait_for_user.insert(extra.as_str());
        }
        Self {
            config,
            wait_for_user,
        }
    }

    pub fn classify(&self, _request_body: &Value, response_body: &Value) -> TurnDecision {
        let decision = self.classify_inner(response_body);
        metrics::record_classifier_decision(decision.as_metric_label());
        decision
    }

    pub(crate) fn is_compaction_boundary(response_body: &Value) -> bool {
        let stop_reason = response_body.get("stop_reason").and_then(Value::as_str);
        let content = response_body.get("content").and_then(Value::as_array);
        stop_reason == Some("compaction") || has_compaction_block(content)
    }

    fn classify_inner(&self, response_body: &Value) -> TurnDecision {
        let stop_reason = response_body.get("stop_reason").and_then(Value::as_str);
        let content = response_body.get("content").and_then(Value::as_array);
        if Self::is_compaction_boundary(response_body) {
            return TurnDecision::UserTurn;
        }
        match stop_reason {
            Some("pause_turn") => TurnDecision::AgentInTurn,
            Some("refusal") | Some("model_context_window_exceeded") => TurnDecision::UserTurn,
            Some("end_turn") => self.classify_end_turn(),
            Some("tool_use") => self.classify_tool_use(content),
            Some("max_tokens") | Some("stop_sequence") => TurnDecision::Ambiguous,
            _ => TurnDecision::UserTurn,
        }
    }

    fn classify_end_turn(&self) -> TurnDecision {
        if self.config.treat_end_turn_as_ambiguous {
            TurnDecision::Ambiguous
        } else {
            TurnDecision::UserTurn
        }
    }

    fn classify_tool_use(&self, content: Option<&Vec<Value>>) -> TurnDecision {
        let client_tool_names = collect_client_tool_use_names(content);
        if client_tool_names.is_empty() {
            return TurnDecision::UserTurn;
        }
        if client_tool_names
            .iter()
            .all(|name| self.wait_for_user.contains(name.as_str()))
        {
            return TurnDecision::UserTurn;
        }
        TurnDecision::AgentInTurn
    }
}

fn has_compaction_block(content: Option<&Vec<Value>>) -> bool {
    let Some(content) = content else {
        return false;
    };
    content
        .iter()
        .any(|block| block.get("type").and_then(Value::as_str) == Some("compaction"))
}

fn collect_client_tool_use_names(content: Option<&Vec<Value>>) -> Vec<String> {
    let Some(content) = content else {
        return Vec::new();
    };
    content
        .iter()
        .filter(|block| {
            matches!(
                block.get("type").and_then(Value::as_str),
                Some("tool_use") | Some("mcp_tool_use")
            )
        })
        .filter_map(|block| block.get("name").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn default_config() -> ClassifierConfig {
        ClassifierConfig::default()
    }

    fn classify(cfg: &ClassifierConfig, req: Value, resp: Value) -> TurnDecision {
        HeuristicClassifier::new(cfg).classify(&req, &resp)
    }

    #[test]
    fn pause_turn_is_agent_in_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({"stop_reason": "pause_turn", "content": []}),
        );
        assert_eq!(d, TurnDecision::AgentInTurn);
    }

    #[test]
    fn compaction_stop_reason_is_user_turn_boundary() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({"stop_reason": "compaction", "content": []}),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn compaction_block_is_user_turn_boundary_for_continuations() {
        for stop_reason in ["end_turn", "tool_use", "pause_turn"] {
            let d = classify(
                &default_config(),
                json!({}),
                json!({
                    "stop_reason": stop_reason,
                    "content": [
                        {"type": "compaction", "content": "summary"},
                        {"type": "tool_use", "name": "Bash", "id": "t1", "input": {}}
                    ],
                }),
            );
            assert_eq!(d, TurnDecision::UserTurn, "stop_reason={stop_reason}");
        }
    }

    #[test]
    fn ordinary_tool_use_remains_agent_in_turn_without_compaction() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "name": "Bash", "id": "t1", "input": {}}],
            }),
        );
        assert_eq!(d, TurnDecision::AgentInTurn);
    }

    #[test]
    fn refusal_is_user_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({"stop_reason": "refusal", "content": []}),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn model_context_window_exceeded_is_user_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({"stop_reason": "model_context_window_exceeded", "content": []}),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn max_tokens_is_ambiguous() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({"stop_reason": "max_tokens", "content": []}),
        );
        assert_eq!(d, TurnDecision::Ambiguous);
    }

    #[test]
    fn stop_sequence_is_ambiguous() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({"stop_reason": "stop_sequence", "content": []}),
        );
        assert_eq!(d, TurnDecision::Ambiguous);
    }

    #[test]
    fn missing_stop_reason_is_user_turn() {
        let d = classify(&default_config(), json!({}), json!({"content": []}));
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn end_turn_with_compaction_block_is_user_turn_boundary() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "end_turn",
                "content": [{"type": "compaction"}, {"type": "text", "text": "ok"}],
            }),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn end_turn_with_defined_completion_tool_is_still_user_turn() {
        for tool_name in ["ExitPlanMode", "attempt_completion", "StructuredOutput"] {
            let req = json!({"tools": [{"name": tool_name}]});
            let resp =
                json!({"stop_reason": "end_turn", "content": [{"type": "text", "text": "done"}]});
            assert_eq!(
                classify(&default_config(), req, resp),
                TurnDecision::UserTurn,
                "tool={tool_name}"
            );
        }
    }

    #[test]
    fn end_turn_with_completion_tool_invoked_is_user_turn() {
        let req = json!({
            "tools": [
                {"name": "read_file"},
                {"name": "attempt_completion"},
            ]
        });
        let resp = json!({
            "stop_reason": "end_turn",
            "content": [
                {"type": "text", "text": "done"},
                {"type": "tool_use", "name": "attempt_completion", "id": "t1", "input": {}}
            ]
        });
        let d = classify(&default_config(), req, resp);
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn end_turn_ambiguous_opt_in_returns_ambiguous() {
        let cfg = ClassifierConfig {
            treat_end_turn_as_ambiguous: true,
            ..ClassifierConfig::default()
        };
        let resp = json!({"stop_reason": "end_turn", "content": [{"type": "text", "text": "hi"}]});
        assert_eq!(classify(&cfg, json!({}), resp), TurnDecision::Ambiguous);
    }

    #[test]
    fn plain_end_turn_is_user_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "hello"}]
            }),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn tool_use_with_regular_client_tool_is_agent_in_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "name": "read_file", "id": "t1", "input": {}}],
            }),
        );
        assert_eq!(d, TurnDecision::AgentInTurn);
    }

    #[test]
    fn tool_use_with_bash_is_still_agent_in_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "name": "Bash", "id": "t1", "input": {}}],
            }),
        );
        assert_eq!(d, TurnDecision::AgentInTurn);
    }

    #[test]
    fn tool_use_with_only_structured_output_is_user_turn() {
        let d = classify(
            &default_config(),
            json!({"tools": [{"name": "StructuredOutput"}]}),
            json!({
                "stop_reason": "tool_use",
                "content": [
                    {"type": "tool_use", "name": "StructuredOutput", "id": "t1", "input": {}}
                ],
            }),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn tool_use_with_structured_output_and_regular_tool_is_agent_in_turn() {
        let d = classify(
            &default_config(),
            json!({"tools": [{"name": "StructuredOutput"}, {"name": "Bash"}]}),
            json!({
                "stop_reason": "tool_use",
                "content": [
                    {"type": "tool_use", "name": "StructuredOutput", "id": "a", "input": {}},
                    {"type": "tool_use", "name": "Bash", "id": "b", "input": {}}
                ],
            }),
        );
        assert_eq!(d, TurnDecision::AgentInTurn);
    }

    #[test]
    fn end_turn_with_defined_structured_output_is_user_turn() {
        let req = json!({"tools": [{"name": "StructuredOutput"}]});
        let resp = json!({
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": "thinking"}],
        });
        assert_eq!(
            classify(&default_config(), req, resp),
            TurnDecision::UserTurn
        );
    }

    #[test]
    fn tool_use_with_only_wait_for_user_tool_is_user_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "name": "attempt_completion", "id": "t1", "input": {}}],
            }),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn tool_use_mixed_wait_for_user_and_regular_is_agent_in_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [
                    {"type": "tool_use", "name": "attempt_completion", "id": "a", "input": {}},
                    {"type": "tool_use", "name": "read_file", "id": "b", "input": {}}
                ],
            }),
        );
        assert_eq!(d, TurnDecision::AgentInTurn);
    }

    #[test]
    fn tool_use_with_only_server_tool_use_is_user_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [{"type": "server_tool_use", "name": "web_search", "id": "s", "input": {}}],
            }),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn tool_use_with_ask_is_user_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "name": "ask", "id": "t1", "input": {}}],
            }),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn tool_use_with_ask_and_another_wait_tool_is_user_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [
                    {"type": "tool_use", "name": "ask", "id": "a", "input": {}},
                    {"type": "tool_use", "name": "attempt_completion", "id": "b", "input": {}}
                ],
            }),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }

    #[test]
    fn tool_use_with_ask_and_bash_is_agent_in_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [
                    {"type": "tool_use", "name": "ask", "id": "a", "input": {}},
                    {"type": "tool_use", "name": "bash", "id": "b", "input": {}}
                ],
            }),
        );
        assert_eq!(d, TurnDecision::AgentInTurn);
    }

    #[test]
    fn tool_use_with_case_sensitive_ask_is_agent_in_turn() {
        let d = classify(
            &default_config(),
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "name": "Ask", "id": "t1", "input": {}}],
            }),
        );
        assert_eq!(d, TurnDecision::AgentInTurn);
    }
    #[test]
    fn extra_wait_for_user_tools_are_honored() {
        let cfg = ClassifierConfig {
            extra_wait_for_user_tools: vec!["custom_wait".to_owned()],
            ..ClassifierConfig::default()
        };
        let d = classify(
            &cfg,
            json!({}),
            json!({
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "name": "custom_wait", "id": "t1", "input": {}}],
            }),
        );
        assert_eq!(d, TurnDecision::UserTurn);
    }
}
