use super::{ClientRequestKind, OMP_BILLING_ENTRYPOINT, RequestView};

const CLAUDE_MAIN_SYSTEM_MARKER: &str =
    "You are an interactive agent that helps users with software engineering tasks";
const CLAUDE_SESSION_TITLE_SYSTEM_MARKER: &str =
    "Generate a concise, sentence-case title (3-7 words)";
const CLAUDE_NOTIFICATION_SYSTEM_PREFIX: &str =
    "A user kicked off a Claude Code agent to do a coding task and walked away";
const CLAUDE_SIDE_QUESTION_PREFIX: &str = "<system-reminder>This is a side question from the user.";
const CLAUDE_TITLE_SCHEMA_POINTER: &str = "/output_config/format/schema/properties/title";
const CLAUDE_NAMING_SYSTEM_PREFIX: &str =
    "You are naming a coding session so the user can pick it out of a long list of sessions.";

pub(crate) fn classify_auxiliary(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    if !view.has_provenance() {
        return None;
    }
    if view.last_user_starts_with(CLAUDE_SIDE_QUESTION_PREFIX) {
        return Some(ClientRequestKind::Side);
    }
    if ((view.any_system_contains(CLAUDE_SESSION_TITLE_SYSTEM_MARKER)
        || view.any_system_contains(CLAUDE_NAMING_SYSTEM_PREFIX))
        && view.pointer(CLAUDE_TITLE_SCHEMA_POINTER).is_some())
        || view.any_system_starts_with(CLAUDE_NAMING_SYSTEM_PREFIX)
        || view.any_system_starts_with(
            "Generate a short kebab-case name (2-4 words) that captures the main topic of this conversation.",
        )
    {
        return Some(ClientRequestKind::SessionTitle);
    }
    if view.any_system_starts_with(CLAUDE_NOTIFICATION_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::Notification);
    }
    if view.any_system_starts_with(
        "You are a helpful AI assistant tasked with summarizing conversations.",
    ) {
        return Some(ClientRequestKind::Compaction);
    }
    None
}

pub(crate) fn classify(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    if view.billing().is_some_and(|billing| billing.is_subagent) {
        return Some(ClientRequestKind::Subagent);
    }
    if !view.has_provenance() {
        return None;
    }

    if let Some(kind) = classify_auxiliary(view) {
        return Some(kind);
    }
    if view.any_system_contains(
        "You are an agent for Claude Code, Anthropic's official CLI for Claude.",
    ) {
        return Some(ClientRequestKind::Subagent);
    }
    // These tool-bearing helper calls can retain billing and main context.
    // Do not let either main marker or the bare-mode tools fallback claim them.
    if view.any_system_starts_with(
        "You run a web search for another Claude model, which is waiting on the result",
    ) || view.any_system_contains("Reply with ONLY the suggestion, no quotes or explanation.")
        || (view.user_contains("<exchanges>") && view.user_contains("</exchanges>"))
    {
        return None;
    }
    if view.any_system_contains(CLAUDE_MAIN_SYSTEM_MARKER)
        || view
            .any_system_contains("You are a Claude agent, built on Anthropic's Claude Agent SDK.")
    {
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

    #[test]
    fn current_claude_markers_support_top_level_and_leading_system() {
        let cases = [
            (
                "You are a Claude agent, built on Anthropic's Claude Agent SDK.",
                "main",
            ),
            (
                "You are an interactive agent that helps users with software engineering tasks.",
                "main",
            ),
            (
                "You are an agent for Claude Code, Anthropic's official CLI for Claude.",
                "subagent",
            ),
            (
                "You are naming a coding session so the user can pick it out of a long list of sessions.",
                "session_title",
            ),
            (
                "Generate a short kebab-case name (2-4 words) that captures the main topic of this conversation.",
                "session_title",
            ),
            (
                "You are a helpful AI assistant tasked with summarizing conversations.",
                "compaction",
            ),
        ];
        for (system, expected) in cases {
            for leading in [false, true] {
                let value = if leading {
                    json!({"messages": [{"role": "system", "content": system}]})
                } else {
                    json!({"system": system})
                };
                assert_eq!(
                    crate::request_classification::classify_client_request_kind(
                        Some(&value),
                        Some("claude-session"),
                        None,
                        false,
                    ),
                    Some(expected),
                    "system={system}, leading={leading}"
                );
                assert_eq!(classify(&RequestView::new(&value, None, None)), None);
            }
        }
    }

    #[test]
    fn title_schema_refines_embedded_naming_marker() {
        let mut value = json!({
            "system": format!("Naming helper:\n{CLAUDE_NAMING_SYSTEM_PREFIX}"),
            "output_config": {"format": {"schema": {"properties": {"title": {"type": "string"}}}}}
        });
        assert_eq!(
            classify_auxiliary(&RequestView::new(&value, Some("session"), None)),
            Some(ClientRequestKind::SessionTitle)
        );
        value.as_object_mut().unwrap().remove("output_config");
        assert_eq!(
            classify_auxiliary(&RequestView::new(&value, Some("session"), None)),
            None
        );
    }

    #[test]
    fn tool_bearing_auxiliary_calls_do_not_fall_through_to_main() {
        for (system, user) in [
            (
                "You run a web search for another Claude model, which is waiting on the result",
                "search",
            ),
            (
                "Reply with ONLY the suggestion, no quotes or explanation.",
                "suggest",
            ),
            (
                CLAUDE_MAIN_SYSTEM_MARKER,
                "<exchanges>conversation</exchanges>",
            ),
        ] {
            let value = json!({
                "system": [
                    {"text": "x-anthropic-billing-header: cc_version=2.1.312; cc_entrypoint=cli;"},
                    {"text": system},
                    {"text": CLAUDE_MAIN_SYSTEM_MARKER}
                ],
                "messages": [{"role": "user", "content": user}],
                "tools": [{"name": "web_search_tool"}]
            });
            assert_eq!(
                crate::request_classification::classify_client_request_kind(
                    Some(&value),
                    Some("session"),
                    None,
                    false,
                ),
                Some("unknown")
            );
        }
    }
}
