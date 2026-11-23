use serde_json::Value;

use super::{ClientRequestKind, RequestView};

/// Header and endpoint signals used to classify Codex CLI / Desktop / SDK
/// traffic on `/v1/responses` and `/v1/responses/compact`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CodexRequestSignals<'a> {
    pub(crate) subagent_header: Option<&'a str>,
    pub(crate) guardian_header: bool,
    pub(crate) has_parent_thread: bool,
    pub(crate) originator: Option<&'a str>,
    pub(crate) user_agent: Option<&'a str>,
    pub(crate) has_codex_turn_metadata: bool,
    pub(crate) is_compact_endpoint: bool,
}

fn ascii_contains_ignore_case(haystack: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn ascii_starts_with_ignore_case(haystack: &str, prefix: &str) -> bool {
    let haystack = haystack.trim();
    haystack.len() >= prefix.len()
        && haystack.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

fn classify_subagent_header(raw: &str) -> Option<ClientRequestKind> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.eq_ignore_ascii_case("compact") {
        return Some(ClientRequestKind::Compaction);
    }
    if trimmed.eq_ignore_ascii_case("review") {
        return Some(ClientRequestKind::Review);
    }
    if ascii_starts_with_ignore_case(trimmed, "memory") {
        return Some(ClientRequestKind::Memory);
    }
    if ascii_starts_with_ignore_case(trimmed, "guardian") {
        return Some(ClientRequestKind::Guardian);
    }
    Some(ClientRequestKind::Subagent)
}

fn has_codex_provenance(view: &RequestView<'_>, signals: &CodexRequestSignals<'_>) -> bool {
    signals
        .originator
        .is_some_and(|originator| ascii_starts_with_ignore_case(originator, "codex"))
        || signals
            .user_agent
            .is_some_and(|user_agent| ascii_contains_ignore_case(user_agent, "codex"))
        || signals.has_codex_turn_metadata
        || signals.has_parent_thread
        || view.any_system_contains("You are Codex, based on GPT")
        || view.any_system_contains("You are Codex, a coding agent")
}

fn classify_codex_prompts(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    if view.any_system_contains("You are performing a CONTEXT CHECKPOINT COMPACTION")
        || view.user_contains("You are performing a CONTEXT CHECKPOINT COMPACTION")
        || view.any_system_contains("Create a handoff summary for another LLM")
        || view.user_contains("Create a handoff summary for another LLM")
    {
        return Some(ClientRequestKind::Compaction);
    }
    if view.any_system_contains("You are acting as a reviewer for a proposed code change")
        || view.user_contains("You are acting as a reviewer for a proposed code change")
    {
        return Some(ClientRequestKind::Review);
    }
    if view.any_system_contains("guardian security reviewer")
        || (view.any_system_contains("You are a security reviewer")
            && (view.any_system_contains("approve") || view.user_contains("approve")))
    {
        return Some(ClientRequestKind::Guardian);
    }
    if view.any_system_contains("You are a Memory Writing Agent")
        || view.any_system_contains("consolidate raw memories")
        || view.any_system_contains("extract durable memories")
    {
        return Some(ClientRequestKind::Memory);
    }
    if !view.has_tools()
        && (view.any_system_contains("Generate a concise title")
            || view.user_contains("Generate a concise title")
            || view.any_system_contains("short title for this task")
            || view.user_contains("short title for this task")
            || view.any_system_contains("naming a coding session")
            || view.user_contains("naming a coding session"))
    {
        return Some(ClientRequestKind::SessionTitle);
    }
    None
}

/// Classify a request originating from OpenAI Codex CLI / Desktop / SDK.
/// Returns `None` when neither Codex headers, compact endpoint, nor Codex
/// system markers are present so generic OpenAI API requests remain unclassified.
pub(crate) fn classify(
    view: &RequestView<'_>,
    signals: Option<&CodexRequestSignals<'_>>,
) -> Option<ClientRequestKind> {
    let signals = signals.copied().unwrap_or_default();

    if signals.is_compact_endpoint {
        return Some(ClientRequestKind::Compaction);
    }
    if let Some(subagent) = signals.subagent_header
        && let Some(kind) = classify_subagent_header(subagent)
    {
        return Some(kind);
    }
    if signals.guardian_header {
        return Some(ClientRequestKind::Guardian);
    }
    if !has_codex_provenance(view, &signals) {
        return None;
    }
    if view.value().get("generate").and_then(Value::as_bool) == Some(false) {
        return Some(ClientRequestKind::Prewarm);
    }
    if let Some(kind) = classify_codex_prompts(view) {
        return Some(kind);
    }
    if signals.has_parent_thread {
        return Some(ClientRequestKind::Subagent);
    }
    Some(ClientRequestKind::Main)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn classify_case(value: Value, signals: CodexRequestSignals<'_>) -> Option<ClientRequestKind> {
        let view = RequestView::new(&value, Some("codex-session-1"), None);
        classify(&view, Some(&signals))
    }

    #[test]
    fn classifies_main_codex_session_from_originator_or_instructions() {
        let from_originator = classify_case(
            json!({"model": "gpt-5.4", "input": [{"role": "user", "content": "hi"}]}),
            CodexRequestSignals {
                originator: Some("codex_cli_rs"),
                ..Default::default()
            },
        );
        assert_eq!(from_originator, Some(ClientRequestKind::Main));

        let from_instructions = classify_case(
            json!({
                "model": "gpt-5.4",
                "instructions": "You are Codex, based on GPT-5.4, running as a coding agent.",
                "input": [{"role": "user", "content": "fix bug"}]
            }),
            CodexRequestSignals::default(),
        );
        assert_eq!(from_instructions, Some(ClientRequestKind::Main));
    }

    #[test]
    fn classifies_prewarm_when_generate_is_false() {
        let prewarm = classify_case(
            json!({
                "model": "gpt-5.4",
                "generate": false,
                "instructions": "You are Codex, based on GPT-5.4.",
                "input": []
            }),
            CodexRequestSignals {
                originator: Some("codex_cli_rs"),
                ..Default::default()
            },
        );
        assert_eq!(prewarm, Some(ClientRequestKind::Prewarm));
    }

    #[test]
    fn classifies_subagent_header_variants_and_compact_endpoint() {
        let body = json!({"model": "gpt-5.4", "input": []});
        assert_eq!(
            classify_case(
                body.clone(),
                CodexRequestSignals {
                    is_compact_endpoint: true,
                    ..Default::default()
                },
            ),
            Some(ClientRequestKind::Compaction)
        );

        assert_eq!(
            classify_case(
                body.clone(),
                CodexRequestSignals {
                    subagent_header: Some("compact"),
                    ..Default::default()
                }
            ),
            Some(ClientRequestKind::Compaction)
        );
        assert_eq!(
            classify_case(
                body.clone(),
                CodexRequestSignals {
                    subagent_header: Some("review"),
                    ..Default::default()
                }
            ),
            Some(ClientRequestKind::Review)
        );
        assert_eq!(
            classify_case(
                body.clone(),
                CodexRequestSignals {
                    subagent_header: Some("memory_consolidation"),
                    ..Default::default()
                }
            ),
            Some(ClientRequestKind::Memory)
        );
        assert_eq!(
            classify_case(
                body.clone(),
                CodexRequestSignals {
                    subagent_header: Some("guardian_security"),
                    ..Default::default()
                }
            ),
            Some(ClientRequestKind::Guardian)
        );
        assert_eq!(
            classify_case(
                body.clone(),
                CodexRequestSignals {
                    subagent_header: Some("collab_spawn"),
                    ..Default::default()
                }
            ),
            Some(ClientRequestKind::Subagent)
        );
        assert_eq!(
            classify_case(
                body,
                CodexRequestSignals {
                    guardian_header: true,
                    ..Default::default()
                }
            ),
            Some(ClientRequestKind::Guardian)
        );
    }

    #[test]
    fn classifies_codex_prompt_fallbacks_and_parent_thread() {
        let base_signals = CodexRequestSignals {
            user_agent: Some("codex_cli_rs/0.1.0"),
            ..Default::default()
        };
        assert_eq!(
            classify_case(
                json!({
                    "instructions": "You are acting as a reviewer for a proposed code change.",
                    "input": [{"role": "user", "content": "Review diff"}]
                }),
                CodexRequestSignals {
                    has_parent_thread: true,
                    ..base_signals
                },
            ),
            Some(ClientRequestKind::Review)
        );
        assert_eq!(
            classify_case(
                json!({
                    "instructions": "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary for another LLM.",
                    "input": []
                }),
                base_signals,
            ),
            Some(ClientRequestKind::Compaction)
        );
        assert_eq!(
            classify_case(
                json!({
                    "instructions": "You are a Memory Writing Agent tasked with memory consolidation.",
                    "input": []
                }),
                base_signals,
            ),
            Some(ClientRequestKind::Memory)
        );
        assert_eq!(
            classify_case(
                json!({
                    "instructions": "You are the guardian security reviewer. Decide whether to approve this command.",
                    "input": []
                }),
                base_signals,
            ),
            Some(ClientRequestKind::Guardian)
        );
        assert_eq!(
            classify_case(
                json!({
                    "instructions": "Generate a concise title for this coding task.",
                    "input": [{"role": "user", "content": "fix bug"}]
                }),
                base_signals,
            ),
            Some(ClientRequestKind::SessionTitle)
        );
        assert_eq!(
            classify_case(
                json!({
                    "instructions": "Explore the repository.",
                    "input": [{"role": "user", "content": "Find callers"}]
                }),
                CodexRequestSignals {
                    has_parent_thread: true,
                    ..base_signals
                },
            ),
            Some(ClientRequestKind::Subagent)
        );
    }

    #[test]
    fn leaves_generic_non_codex_openai_requests_unclassified() {
        let generic = json!({
            "model": "gpt-4o",
            "instructions": "You are a helpful assistant.",
            "input": [{"role": "user", "content": "Hello"}]
        });
        assert_eq!(classify_case(generic, CodexRequestSignals::default()), None);
    }
}
