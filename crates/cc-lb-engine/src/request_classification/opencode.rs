use super::{ClientRequestKind, RequestView};

const OPENCODE_MAIN_SYSTEM_PREFIX: &str = "You are OpenCode, the best coding agent on the planet.";
const OPENCODE_SESSION_TITLE_SYSTEM_PREFIX: &str =
    "You are a title generator. You output ONLY a thread title. Nothing else.";
const OPENCODE_COMPACTION_SYSTEM_PREFIX: &str =
    "You are an anchored context summarization assistant for coding sessions.";
const OPENCODE_COMPACTION_USER_PREFIX: &str = "Here is the conversation so far:\n\n<conversation>";

pub(crate) fn classify_auxiliary(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    view.session_id()?;

    if view.parent_session_id().is_some() && view.system_starts_with(OPENCODE_MAIN_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::Subagent);
    }
    if view.system_starts_with(OPENCODE_SESSION_TITLE_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::SessionTitle);
    }
    if view.system_starts_with(OPENCODE_COMPACTION_SYSTEM_PREFIX)
        || (!view.has_tools() && view.first_user_starts_with(OPENCODE_COMPACTION_USER_PREFIX))
    {
        return Some(ClientRequestKind::Compaction);
    }

    None
}

pub(crate) fn classify_main(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    (view.session_id().is_some() && view.system_starts_with(OPENCODE_MAIN_SYSTEM_PREFIX))
        .then_some(ClientRequestKind::Main)
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
}
