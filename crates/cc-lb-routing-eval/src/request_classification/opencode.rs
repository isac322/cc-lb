use super::{ClientRequestKind, RequestView};

const OPENCODE_MAIN_SYSTEM_PREFIXES: [&str; 3] = [
    "You are OpenCode, the best coding agent on the planet.",
    "You are opencode, an interactive CLI tool",
    "You are an AI agent running in OpenCode",
];
const OPENCODE_SESSION_TITLE_SYSTEM_PREFIX: &str =
    "You are a title generator. You output ONLY a thread title. Nothing else.";
const OPENCODE_COMPACTION_SYSTEM_PREFIX: &str =
    "You are an anchored context summarization assistant for coding sessions.";
const OPENCODE_COMPACTION_USER_PREFIX: &str = "Here is the conversation so far:\n\n<conversation>";

fn is_main(view: &RequestView<'_>) -> bool {
    OPENCODE_MAIN_SYSTEM_PREFIXES
        .iter()
        .any(|prefix| view.any_system_starts_with(prefix))
}

fn is_opencode_agent(view: &RequestView<'_>) -> bool {
    is_main(view) || view.any_system_starts_with("You are a file search specialist")
}

pub(crate) fn classify_auxiliary(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    view.session_id()?;

    if view.parent_session_id().is_some() && is_opencode_agent(view) {
        return Some(ClientRequestKind::Subagent);
    }
    if view.any_system_starts_with(OPENCODE_SESSION_TITLE_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::SessionTitle);
    }
    if view.any_system_starts_with(OPENCODE_COMPACTION_SYSTEM_PREFIX)
        || view.any_system_starts_with("Summarize what was done in this conversation")
        || (!view.has_tools()
            && (view.first_user_starts_with(OPENCODE_COMPACTION_USER_PREFIX)
                || view.last_user_starts_with(
                    "Provide a detailed prompt for continuing our conversation above",
                )))
    {
        return Some(ClientRequestKind::Compaction);
    }

    None
}

pub(crate) fn classify_main(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    (view.session_id().is_some() && is_main(view)).then_some(ClientRequestKind::Main)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::request_classification::classify_client_request_kind;

    fn classify(
        value: &serde_json::Value,
        session_id: Option<&str>,
        parent_session_id: Option<&str>,
    ) -> Option<&'static str> {
        classify_client_request_kind(Some(value), session_id, parent_session_id, false)
    }

    #[test]
    fn classifies_measured_opencode_request_families() {
        let cases = [
            (
                json!({
                    "system": [{"type": "text", "text": "You are OpenCode, the best coding agent on the planet.\n\nYou are an interactive CLI tool."}],
                    "messages": [{"role": "user", "content": "hello"}],
                    "tools": [{"name": "task"}]
                }),
                None,
                "main",
            ),
            (
                json!({
                    "system": [{"type": "text", "text": "You are OpenCode, the best coding agent on the planet.\n\nYou are an interactive CLI tool."}],
                    "messages": [{"role": "user", "content": "Reply with CHILD only."}],
                    "tools": [{"name": "task"}]
                }),
                Some("ses_parent"),
                "subagent",
            ),
            (
                json!({
                    "system": [{"type": "text", "text": "You are a title generator. You output ONLY a thread title. Nothing else.\n\n<task>Generate a title</task>"}],
                    "messages": [{"role": "user", "content": "fix the router"}]
                }),
                None,
                "session_title",
            ),
            (
                json!({
                    "system": [{"type": "text", "text": "You are an anchored context summarization assistant for coding sessions.\n\nSummarize only the conversation provided."}],
                    "messages": [{"role": "user", "content": "conversation"}]
                }),
                None,
                "compaction",
            ),
            (
                json!({
                    "messages": [{
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "Here is the conversation so far:\n\n<conversation>\n[User]: fix the router\n</conversation>\n\nCreate a new anchored summary from the conversation history."
                        }]
                    }],
                    "tools": []
                }),
                None,
                "compaction",
            ),
        ];

        for (value, parent_session_id, expected) in cases {
            assert_eq!(
                classify(&value, Some("ses_child_or_main"), parent_session_id),
                Some(expected)
            );
        }
    }

    #[test]
    fn refuses_opencode_body_markers_without_session_provenance() {
        let value = json!({
            "system": "You are OpenCode, the best coding agent on the planet.",
            "messages": [{"role": "user", "content": "hello"}]
        });

        assert_eq!(classify(&value, None, None), None);
    }

    #[test]
    fn main_request_quoting_current_compaction_prefix_stays_main() {
        let value = json!({
            "system": [{"type": "text", "text": "You are OpenCode, the best coding agent on the planet.\n\nYou are an interactive CLI tool."}],
            "messages": [{
                "role": "user",
                "content": "Here is the conversation so far:\n\n<conversation>\nquoted text"
            }],
            "tools": [{"name": "task"}]
        });

        assert_eq!(classify(&value, Some("ses_main"), None), Some("main"));
    }

    #[test]
    fn parent_session_does_not_reclassify_unrelated_requests() {
        let value = json!({
            "system": "You are a different coding agent.",
            "messages": [{"role": "user", "content": "hello"}]
        });

        assert_eq!(
            classify(&value, Some("ses_child"), Some("ses_parent")),
            Some("unknown")
        );
    }

    #[test]
    fn current_opencode_main_and_general_children_support_both_wire_formats() {
        for system in super::OPENCODE_MAIN_SYSTEM_PREFIXES {
            for leading in [false, true] {
                let value = if leading {
                    json!({"messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": "work"}
                    ]})
                } else {
                    json!({"system": system})
                };
                assert_eq!(classify(&value, Some("ses_main"), None), Some("main"));
                assert_eq!(
                    classify(&value, Some("ses_child"), Some("ses_parent")),
                    Some("subagent")
                );
                assert_eq!(classify(&value, None, None), None);
            }
        }
    }

    #[test]
    fn explore_agent_requires_parent_lineage() {
        let value = json!({"messages": [{
            "role": "system",
            "content": "You are a file search specialist. Find matching files."
        }]});
        assert_eq!(
            classify(&value, Some("ses_explore"), Some("ses_parent")),
            Some("subagent")
        );
        assert_eq!(classify(&value, Some("ses_explore"), None), Some("unknown"));
        assert_eq!(classify(&value, None, Some("ses_parent")), None);
    }

    #[test]
    fn current_compaction_markers_override_main_but_user_marker_requires_no_tools() {
        for leading in [false, true] {
            let value = if leading {
                json!({"messages": [{
                    "role": "system",
                    "content": "Summarize what was done in this conversation"
                }]})
            } else {
                json!({"system": "Summarize what was done in this conversation"})
            };
            assert_eq!(classify(&value, Some("ses_main"), None), Some("compaction"));
            assert_eq!(classify(&value, None, None), None);
        }
        let mut value = json!({
            "system": super::OPENCODE_MAIN_SYSTEM_PREFIXES[2],
            "messages": [
                {"role": "user", "content": "original task"},
                {"role": "assistant", "content": "work"},
                {"role": "user", "content": "Provide a detailed prompt for continuing our conversation above"},
                {"role": "assistant", "content": "summary prefix"}
            ],
            "tools": []
        });
        assert_eq!(classify(&value, Some("ses_main"), None), Some("compaction"));
        value["tools"] = json!([{"name": "task"}]);
        assert_eq!(classify(&value, Some("ses_main"), None), Some("main"));
    }
}
