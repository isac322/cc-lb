use super::{ClientRequestKind, OMP_BILLING_ENTRYPOINT, RequestView};

const CLAUDE_MAIN_SYSTEM_MARKER: &str =
    "You are an interactive agent that helps users with software engineering tasks";
const CLAUDE_SESSION_TITLE_SYSTEM_MARKER: &str =
    "Generate a concise, sentence-case title (3-7 words)";
const CLAUDE_NOTIFICATION_SYSTEM_PREFIX: &str =
    "A user kicked off a Claude Code agent to do a coding task and walked away";
const CLAUDE_SIDE_QUESTION_PREFIX: &str = "<system-reminder>This is a side question from the user.";
const CLAUDE_TITLE_SCHEMA_POINTER: &str = "/output_config/format/schema/properties/title";

pub(crate) fn classify(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    if view.billing().is_some_and(|billing| billing.is_subagent) {
        return Some(ClientRequestKind::Subagent);
    }
    if !view.has_provenance() {
        return None;
    }

    // Side questions retain the main system prompt and tools, so this
    // auxiliary marker must be checked before either main rule.
    if view.last_user_starts_with(CLAUDE_SIDE_QUESTION_PREFIX) {
        return Some(ClientRequestKind::Side);
    }
    if view.system_contains(CLAUDE_SESSION_TITLE_SYSTEM_MARKER)
        && view.pointer(CLAUDE_TITLE_SCHEMA_POINTER).is_some()
    {
        return Some(ClientRequestKind::SessionTitle);
    }
    if view.system_starts_with(CLAUDE_NOTIFICATION_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::Notification);
    }
    if view.system_contains(CLAUDE_MAIN_SYSTEM_MARKER) {
        return Some(ClientRequestKind::Main);
    }

    // `--bare` and custom system prompts remove the stable main marker. Require
    // an inbound Claude Code billing entrypoint before using request structure.
    if is_claude_code_client(view) && view.has_tools() {
        return Some(ClientRequestKind::Main);
    }

    None
}

fn is_claude_code_client(view: &RequestView<'_>) -> bool {
    view.billing().is_some_and(|billing| {
        billing
            .entrypoint
            .is_some_and(|entrypoint| entrypoint != OMP_BILLING_ENTRYPOINT)
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn main_marker_with_session_provenance_classifies_as_main() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "\nYou are an interactive agent that helps users with software engineering tasks. Use the tools available."
            }],
            "messages": [{"role": "user", "content": "hello"}],
            "tools": [{"name": "Agent"}]
        });

        assert_eq!(
            classify(&RequestView::new(&value, Some("session-1"), None)),
            Some(ClientRequestKind::Main)
        );
    }

    #[test]
    fn bare_main_turn_without_prompt_marker_classifies_as_main() {
        let value = json!({
            "system": [
                {
                    "type": "text",
                    "text": "x-anthropic-billing-header: cc_version=2.1.219.c27; cc_entrypoint=sdk-cli;"
                },
                {
                    "type": "text",
                    "text": "You are a Claude agent, built on Anthropic's Claude Agent SDK."
                },
                {"type": "text", "text": "CWD: /tmp\nDate: 2026-07-25"}
            ],
            "messages": [{"role": "user", "content": "hello"}],
            "tools": [{"name": "Bash"}]
        });

        assert_eq!(
            classify(&RequestView::new(&value, Some("session-1"), None)),
            Some(ClientRequestKind::Main)
        );
    }

    #[test]
    fn side_question_classifies_as_side() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "\nYou are an interactive agent that helps users with software engineering tasks."
            }],
            "messages": [{
                "role": "user",
                "content": "<system-reminder>This is a side question from the user. You must answer this question directly in a single response.\n\nwhat is 2+2"
            }],
            "tools": [{"name": "Bash"}]
        });

        assert_eq!(
            classify(&RequestView::new(&value, Some("session-1"), None)),
            Some(ClientRequestKind::Side)
        );
    }

    #[test]
    fn notification_marker_needs_only_session_provenance() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "A user kicked off a Claude Code agent to do a coding task and walked away. Read the tail and decide whether to notify."
            }],
            "messages": [{"role": "user", "content": "tail"}],
            "max_tokens": 1024
        });

        assert_eq!(
            classify(&RequestView::new(&value, Some("session-1"), None)),
            Some(ClientRequestKind::Notification)
        );
    }

    #[test]
    fn billing_subagent_flag_without_header_classifies_as_subagent() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=2.1.219.669; cc_entrypoint=sdk-cli; cc_is_subagent=true;"
            }],
            "messages": [{"role": "user", "content": "work"}]
        });

        assert_eq!(
            classify(&RequestView::new(&value, None, None)),
            Some(ClientRequestKind::Subagent)
        );
    }

    #[test]
    fn title_marker_needs_only_session_provenance() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "Generate a concise, sentence-case title (3-7 words)"
            }],
            "messages": [{"role": "user", "content": "title this"}],
            "output_config": {
                "format": {
                    "schema": {
                        "properties": {
                            "title": {"type": "string"}
                        }
                    }
                }
            }
        });

        assert_eq!(
            classify(&RequestView::new(&value, Some("session-1"), None)),
            Some(ClientRequestKind::SessionTitle)
        );
    }

    #[test]
    fn toolless_unrecognized_request_returns_none() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=2.1.219.abc; cc_entrypoint=cli;"
            }, {
                "type": "text",
                "text": "unrecognized auxiliary request"
            }],
            "messages": [{"role": "user", "content": "work"}]
        });

        assert_eq!(classify(&RequestView::new(&value, None, None)), None);
    }

    #[test]
    fn omp_billing_tag_does_not_trigger_tools_fallback() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=0.3.165.ab1; cc_entrypoint=local-agent; cch=00000"
            }],
            "messages": [{"role": "user", "content": "work"}],
            "tools": [{"name": "bash"}]
        });

        assert_eq!(classify(&RequestView::new(&value, None, None)), None);
    }
}
