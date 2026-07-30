use super::{ClientRequestKind, RequestView};

const SENPI_MAIN_SYSTEM_PREFIXES: [&str; 2] = ["You are senpi,", "You are senpi on "];
const SENPI_SESSION_TITLE_SYSTEM_PREFIX: &str =
    "Generate a concise title for this coding-agent session.";
const SENPI_COMPACTION_SYSTEM_PREFIX: &str = "You are a context summarization assistant.";
const SENPI_LOOK_AT_SYSTEM_PREFIX: &str = "You analyze attached media for a downstream agent that cannot inspect the attachments directly.";
const SENPI_SUBAGENT_USER_PREFIX: &str = "Task: ";
const SENPI_SIDE_SESSION_MARKER: &str = ":btw:";
const SENPI_SIDE_SYSTEM_SUFFIX: &str = concat!(
    "The user is asking a side question about the conversation so far, outside the main task. ",
    "Answer it directly and concisely from the context above. ",
    "Do not continue any task, do not modify anything, and do not treat this as new work."
);

fn is_main(view: &RequestView<'_>) -> bool {
    SENPI_MAIN_SYSTEM_PREFIXES
        .iter()
        .any(|prefix| view.system_starts_with(prefix))
}

pub(crate) fn classify_main(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    is_main(view).then_some(ClientRequestKind::Main)
}

pub(crate) fn classify_auxiliary(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    if view
        .session_id()
        .is_some_and(|session_id| session_id.contains(SENPI_SIDE_SESSION_MARKER))
        || view.system_ends_with(SENPI_SIDE_SYSTEM_SUFFIX)
    {
        return Some(ClientRequestKind::Side);
    }
    if view.system_starts_with(SENPI_LOOK_AT_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::LookAt);
    }
    // Senpi's shipped opt-in subagent example launches a fresh
    // `senpi --no-session` process and encodes the delegated prompt as the
    // first user message.
    // In-memory sessions still carry a generated provider session ID, so the
    // stable wire marker is the `Task: ` prefix rather than header absence.
    // This intentionally fails toward `subagent`: an interactive first prompt
    // beginning with `Task: ` is indistinguishable on the wire.
    if is_main(view) && view.first_user_starts_with(SENPI_SUBAGENT_USER_PREFIX) {
        return Some(ClientRequestKind::Subagent);
    }
    if view.system_starts_with(SENPI_SESSION_TITLE_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::SessionTitle);
    }
    if view.system_starts_with(SENPI_COMPACTION_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::Compaction);
    }

    None
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::request_classification::{CLASSIFIER_TEXT_LIMIT, classify_client_request_kind};

    fn classify(value: &serde_json::Value, session_id: Option<&str>) -> Option<&'static str> {
        classify_client_request_kind(Some(value), session_id, None, false)
    }

    #[test]
    fn classifies_senpi_request_families_without_app_header() {
        let cases = [
            (
                json!({
                    "system": "You are senpi, a coding agent. Your work should be indistinguishable from a careful senior engineer's.\n\n## Intent Gate",
                    "messages": [{"role": "user", "content": "hello"}],
                    "tools": [{"name": "read"}]
                }),
                None,
                "main",
            ),
            (
                json!({
                    "system": "Generate a concise title for this coding-agent session.\n\nRules:\n- Use 3 to 6 words.",
                    "messages": [{"role": "user", "content": "fix the router"}],
                    "max_tokens": 64
                }),
                None,
                "session_title",
            ),
            (
                json!({
                    "system": "You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant.",
                    "messages": [{"role": "user", "content": "<conversation>work</conversation>"}]
                }),
                None,
                "compaction",
            ),
        ];

        for (value, session_id, expected) in cases {
            assert_eq!(classify(&value, session_id), Some(expected));
        }
    }

    #[test]
    fn classifies_current_senpi_prompt_preset_variants() {
        let systems = [
            "You are senpi, a coding agent. Ship work indistinguishable from a careful senior engineer's.",
            "You are senpi, a coding agent and autonomous deep worker: you receive goals, not step-by-step instructions.",
            "You are senpi, a coding agent running on Kimi K3 - decisive and evidence-first.",
            "You are senpi on Grok 4.5, acting as CEO and orchestrator: the single human-facing surface.",
        ];

        for system in systems {
            let main = json!({
                "system": system,
                "messages": [{"role": "user", "content": "hello"}],
                "tools": [{"name": "read"}]
            });
            assert_eq!(classify(&main, None), Some("main"));

            let subagent = json!({
                "system": system,
                "messages": [{"role": "user", "content": "Task: Review the auth change"}],
                "tools": [{"name": "read"}]
            });
            assert_eq!(classify(&subagent, None), Some("subagent"));
        }
    }

    #[test]
    fn side_query_suffix_beyond_prefix_scan_classifies_as_side() {
        let system = format!(
            "You are senpi, a coding agent.{}\n\n{}",
            "x".repeat(CLASSIFIER_TEXT_LIMIT),
            SENPI_SIDE_SYSTEM_SUFFIX
        );
        let value = json!({
            "system": system,
            "messages": [{"role": "user", "content": "what changed?"}],
            "tools": []
        });

        assert_eq!(classify(&value, None), Some("side"));
    }

    #[test]
    fn btw_session_id_classifies_custom_prompt_as_side() {
        let value = json!({
            "system": "Custom user-supplied system prompt",
            "messages": [{"role": "user", "content": "what changed?"}],
            "tools": []
        });

        assert_eq!(
            classify(
                &value,
                Some("session-1:btw:019c12d4-6d4a-73d9-bad7-3a612130883a")
            ),
            Some("side")
        );
    }

    #[test]
    fn classifies_shipped_subagent_invocation() {
        let value = json!({
            "system": concat!(
                "You are senpi, a coding agent. Your work should be indistinguishable from a careful senior engineer's.\n\n",
                "You are a focused code-review subagent."
            ),
            "messages": [{"role": "user", "content": "Task: Review the auth change"}],
            "tools": [{"name": "read"}]
        });

        assert_eq!(
            classify(&value, Some("019d20f2-6f15-7de2-a800-f3e97928a87c")),
            Some("subagent")
        );
    }

    #[test]
    fn classifies_look_at_request() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": concat!(
                    "You analyze attached media for a downstream agent that cannot inspect the attachments directly.\n\n",
                    "Extract only the information requested by the goal."
                )
            }],
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AA=="}},
                    {"type": "text", "text": "Goal:\nRead the label"}
                ]
            }]
        });

        assert_eq!(classify(&value, None), Some("look_at"));
    }

    #[test]
    fn classifies_look_at_after_anthropic_oauth_preamble() {
        let value = json!({
            "system": [
                {
                    "type": "text",
                    "text": "You are Claude Code, Anthropic's official CLI for Claude."
                },
                {
                    "type": "text",
                    "text": concat!(
                        "You analyze attached media for a downstream agent that cannot inspect the attachments directly.\n\n",
                        "Extract only the information requested by the goal."
                    )
                }
            ],
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AA=="}},
                    {"type": "text", "text": "Goal:\nRead the label"}
                ]
            }]
        });

        assert_eq!(classify(&value, None), Some("look_at"));
    }

    #[test]
    fn unrelated_request_is_not_senpi() {
        let value = json!({
            "system": "You are a coding assistant.",
            "messages": [{"role": "user", "content": "hello"}]
        });

        assert_eq!(classify(&value, None), None);
    }
}
