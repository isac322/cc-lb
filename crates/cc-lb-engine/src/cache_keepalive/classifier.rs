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
    "attempt_completion",
    "submit_and_exit",
    "finish",
    "plan_mode_respond",
    "act_mode_respond",
    "report_bug",
];

const BUILTIN_COMPLETION_TOOLS: &[&str] = &[
    "attempt_completion",
    "submit_and_exit",
    "ExitPlanMode",
    "finish",
];

pub struct HeuristicClassifier<'a> {
    config: &'a ClassifierConfig,
    wait_for_user: HashSet<&'a str>,
    completion_tools: HashSet<&'a str>,
}

impl<'a> HeuristicClassifier<'a> {
    pub fn new(config: &'a ClassifierConfig) -> Self {
        let mut wait_for_user: HashSet<&str> =
            BUILTIN_WAIT_FOR_USER_TOOLS.iter().copied().collect();
        for extra in &config.extra_wait_for_user_tools {
            wait_for_user.insert(extra.as_str());
        }
        let completion_tools: HashSet<&str> = BUILTIN_COMPLETION_TOOLS.iter().copied().collect();
        Self {
            config,
            wait_for_user,
            completion_tools,
        }
    }

    pub fn classify(&self, request_body: &Value, response_body: &Value) -> TurnDecision {
        let decision = self.classify_inner(request_body, response_body);
        metrics::record_classifier_decision(decision.as_metric_label(), "heuristic");
        decision
    }

    fn classify_inner(&self, request_body: &Value, response_body: &Value) -> TurnDecision {
        let stop_reason = response_body.get("stop_reason").and_then(Value::as_str);
        let content = response_body.get("content").and_then(Value::as_array);
        match stop_reason {
            Some("pause_turn") | Some("compaction") => TurnDecision::AgentInTurn,
            Some("refusal") | Some("model_context_window_exceeded") => TurnDecision::UserTurn,
            Some("end_turn") => self.classify_end_turn(request_body, content),
            Some("tool_use") => self.classify_tool_use(content),
            Some("max_tokens") | Some("stop_sequence") => TurnDecision::Ambiguous,
            _ => TurnDecision::UserTurn,
        }
    }

    fn classify_end_turn(&self, req: &Value, content: Option<&Vec<Value>>) -> TurnDecision {
        if has_compaction_block(content) {
            return TurnDecision::AgentInTurn;
        }

        let defined_completion_tools = self.collect_defined_completion_tools(req);
        if !defined_completion_tools.is_empty() {
            let invoked_tools = collect_tool_use_names(content);
            if defined_completion_tools.is_disjoint(&invoked_tools) {
                return TurnDecision::AgentInTurn;
            }
        }

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

    fn collect_defined_completion_tools(&self, req: &Value) -> HashSet<String> {
        let mut out = HashSet::new();
        let Some(tools) = req.get("tools").and_then(Value::as_array) else {
            return out;
        };
        for tool in tools {
            let Some(name) = tool.get("name").and_then(Value::as_str) else {
                continue;
            };
            if self.completion_tools.contains(name) {
                out.insert(name.to_owned());
            }
        }
        out
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

fn collect_tool_use_names(content: Option<&Vec<Value>>) -> HashSet<String> {
    let mut out = HashSet::new();
    let Some(content) = content else {
        return out;
    };
    for block in content {
        let Some(kind) = block.get("type").and_then(Value::as_str) else {
            continue;
        };
        if !matches!(kind, "tool_use" | "mcp_tool_use") {
            continue;
        }
        if let Some(name) = block.get("name").and_then(Value::as_str) {
            out.insert(name.to_owned());
        }
    }
    out
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
    fn stop_reason_table() {
        struct Case {
            case: &'static str,
            response: Value,
            expected: TurnDecision,
        }

        let cases = [
            Case {
                case: "pause_turn_is_agent_in_turn",
                response: json!({"stop_reason": "pause_turn", "content": []}),
                expected: TurnDecision::AgentInTurn,
            },
            Case {
                case: "compaction_stop_reason_is_agent_in_turn",
                response: json!({"stop_reason": "compaction", "content": []}),
                expected: TurnDecision::AgentInTurn,
            },
            Case {
                case: "refusal_is_user_turn",
                response: json!({"stop_reason": "refusal", "content": []}),
                expected: TurnDecision::UserTurn,
            },
            Case {
                case: "model_context_window_exceeded_is_user_turn",
                response: json!({"stop_reason": "model_context_window_exceeded", "content": []}),
                expected: TurnDecision::UserTurn,
            },
            Case {
                case: "max_tokens_is_ambiguous",
                response: json!({"stop_reason": "max_tokens", "content": []}),
                expected: TurnDecision::Ambiguous,
            },
            Case {
                case: "stop_sequence_is_ambiguous",
                response: json!({"stop_reason": "stop_sequence", "content": []}),
                expected: TurnDecision::Ambiguous,
            },
            Case {
                case: "missing_stop_reason_is_user_turn",
                response: json!({"content": []}),
                expected: TurnDecision::UserTurn,
            },
            Case {
                case: "plain_end_turn_is_user_turn",
                response: json!({
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "hello"}]
                }),
                expected: TurnDecision::UserTurn,
            },
        ];

        for case in cases {
            assert_eq!(
                classify(&default_config(), json!({}), case.response),
                case.expected,
                "case={}",
                case.case
            );
        }
    }

    #[test]
    fn end_turn_table() {
        struct Case {
            case: &'static str,
            config: ClassifierConfig,
            request: Value,
            response: Value,
            expected: TurnDecision,
        }

        let cases = [
            Case {
                case: "end_turn_with_compaction_block_is_agent_in_turn",
                config: default_config(),
                request: json!({}),
                response: json!({
                    "stop_reason": "end_turn",
                    "content": [{"type": "compaction"}, {"type": "text", "text": "ok"}],
                }),
                expected: TurnDecision::AgentInTurn,
            },
            Case {
                case: "end_turn_without_completion_tool_invocation_is_agent_in_turn",
                config: default_config(),
                request: json!({
                    "tools": [
                        {"name": "read_file"},
                        {"name": "attempt_completion"},
                    ]
                }),
                response: json!({
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "done"}],
                }),
                expected: TurnDecision::AgentInTurn,
            },
            Case {
                case: "end_turn_with_only_non_completion_tool_invoked_is_agent_in_turn",
                config: default_config(),
                request: json!({
                    "tools": [
                        {"name": "read_file"},
                        {"name": "attempt_completion"},
                    ]
                }),
                response: json!({
                    "stop_reason": "end_turn",
                    "content": [
                        {"type": "tool_use", "name": "read_file", "id": "t1", "input": {}}
                    ]
                }),
                expected: TurnDecision::AgentInTurn,
            },
            Case {
                case: "end_turn_with_completion_tool_invoked_is_user_turn",
                config: default_config(),
                request: json!({
                    "tools": [
                        {"name": "read_file"},
                        {"name": "attempt_completion"},
                    ]
                }),
                response: json!({
                    "stop_reason": "end_turn",
                    "content": [
                        {"type": "text", "text": "done"},
                        {"type": "tool_use", "name": "attempt_completion", "id": "t1", "input": {}}
                    ]
                }),
                expected: TurnDecision::UserTurn,
            },
            Case {
                case: "end_turn_ambiguous_opt_in_returns_ambiguous",
                config: ClassifierConfig {
                    treat_end_turn_as_ambiguous: true,
                    ..ClassifierConfig::default()
                },
                request: json!({}),
                response: json!({
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "hi"}],
                }),
                expected: TurnDecision::Ambiguous,
            },
        ];

        for case in cases {
            assert_eq!(
                classify(&case.config, case.request, case.response),
                case.expected,
                "case={}",
                case.case
            );
        }
    }

    #[test]
    fn tool_use_table() {
        struct Case {
            case: &'static str,
            config: ClassifierConfig,
            content: Value,
            expected: TurnDecision,
        }

        let cases = [
            Case {
                case: "tool_use_with_regular_client_tool_is_agent_in_turn",
                config: default_config(),
                content: json!([
                    {"type": "tool_use", "name": "read_file", "id": "t1", "input": {}}
                ]),
                expected: TurnDecision::AgentInTurn,
            },
            Case {
                case: "tool_use_with_bash_is_still_agent_in_turn",
                config: default_config(),
                content: json!([
                    {"type": "tool_use", "name": "Bash", "id": "t1", "input": {}}
                ]),
                expected: TurnDecision::AgentInTurn,
            },
            Case {
                case: "tool_use_with_only_wait_for_user_tool_is_user_turn",
                config: default_config(),
                content: json!([
                    {"type": "tool_use", "name": "attempt_completion", "id": "t1", "input": {}}
                ]),
                expected: TurnDecision::UserTurn,
            },
            Case {
                case: "tool_use_mixed_wait_for_user_and_regular_is_agent_in_turn",
                config: default_config(),
                content: json!([
                    {"type": "tool_use", "name": "attempt_completion", "id": "a", "input": {}},
                    {"type": "tool_use", "name": "read_file", "id": "b", "input": {}}
                ]),
                expected: TurnDecision::AgentInTurn,
            },
            Case {
                case: "tool_use_with_only_server_tool_use_is_user_turn",
                config: default_config(),
                content: json!([
                    {"type": "server_tool_use", "name": "web_search", "id": "s", "input": {}}
                ]),
                expected: TurnDecision::UserTurn,
            },
            Case {
                case: "extra_wait_for_user_tools_are_honored",
                config: ClassifierConfig {
                    extra_wait_for_user_tools: vec!["custom_wait".to_owned()],
                    ..ClassifierConfig::default()
                },
                content: json!([
                    {"type": "tool_use", "name": "custom_wait", "id": "t1", "input": {}}
                ]),
                expected: TurnDecision::UserTurn,
            },
        ];

        for case in cases {
            assert_eq!(
                classify(
                    &case.config,
                    json!({}),
                    json!({"stop_reason": "tool_use", "content": case.content}),
                ),
                case.expected,
                "case={}",
                case.case
            );
        }
    }
}
