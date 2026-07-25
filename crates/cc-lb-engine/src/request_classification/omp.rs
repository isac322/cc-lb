use super::{ClientRequestKind, RequestView};

const OMP_MAIN_SYSTEM_MARKER: &str = "operating in the Oh My Pi coding harness";
pub(crate) const OMP_ADVISOR_SYSTEM_MARKER: &str = "You shadow the main agent as a peer programmer";
const OMP_AUTO_THINKING_SYSTEM_PREFIX: &str = "You are a difficulty classifier for a coding agent.";
const OMP_SUBAGENT_PROMPT_PREFIX: &str = "Complete the assignment below, thoroughly:";

pub(crate) fn classify(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    if let Some(kind) = classify_advisor(view) {
        return Some(kind);
    }

    if !view.has_provenance() {
        return None;
    }

    if view.system_starts_with(OMP_AUTO_THINKING_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::AutoThinking);
    }
    if view.first_user_starts_with(OMP_SUBAGENT_PROMPT_PREFIX) {
        return Some(ClientRequestKind::Subagent);
    }
    if view
        .session_id()
        .is_some_and(|session_id| session_id.contains(":side:"))
    {
        return Some(ClientRequestKind::Side);
    }
    if view.system_contains(OMP_MAIN_SYSTEM_MARKER) {
        return Some(ClientRequestKind::Main);
    }

    None
}

pub(crate) fn classify_advisor(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    // OMP v17.1.3 gives Advisor its own provider session ID internally, but its
    // custom Anthropic request path emits neither the session header nor
    // metadata.user_id. The exact built-in Advisor system marker is therefore
    // the request's only OMP provenance.
    view.system_contains(OMP_ADVISOR_SYSTEM_MARKER)
        .then_some(ClientRequestKind::Advisor)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn classifies_omp_request_families() {
        let cases = [
            (
                json!({
                    "system": "operating in the Oh My Pi coding harness",
                    "messages": [{"role": "user", "content": "hello"}]
                }),
                "session-1",
                ClientRequestKind::Main,
            ),
            (
                json!({
                    "system": "You are a difficulty classifier for a coding agent. Read the request.",
                    "messages": [{"role": "user", "content": "fix this"}]
                }),
                "session-1",
                ClientRequestKind::AutoThinking,
            ),
            (
                json!({
                    "messages": [{
                        "role": "user",
                        "content": "Complete the assignment below, thoroughly:\n\nFix the issue"
                    }]
                }),
                "session-1",
                ClientRequestKind::Subagent,
            ),
            (
                json!({"messages": [{"role": "user", "content": "side"}]}),
                "session-1:side:question",
                ClientRequestKind::Side,
            ),
        ];

        for (value, session_id, expected) in cases {
            assert_eq!(
                classify(&RequestView::new(&value, Some(session_id))),
                Some(expected)
            );
        }
    }

    #[test]
    fn advisor_marker_classifies_without_session_id() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "You shadow the main agent as a peer programmer:"
            }],
            "messages": [{"role": "user", "content": "review"}]
        });

        assert_eq!(
            classify(&RequestView::new(&value, None)),
            Some(ClientRequestKind::Advisor)
        );
    }

    #[test]
    fn advisor_marker_wins_with_billing_and_compaction_transcript() {
        let value = json!({
            "system": [
                {
                    "type": "text",
                    "text": "x-anthropic-billing-header: cc_version=0.3.165.ab1; cc_entrypoint=local-agent; cch=00000"
                },
                {
                    "type": "text",
                    "text": "You shadow the main agent as a peer programmer:"
                }
            ],
            "messages": [{
                "role": "user",
                "content": "Review a transcript containing: Your task is to create a detailed summary of the conversation so far"
            }]
        });

        assert_eq!(
            classify(&RequestView::new(&value, None)),
            Some(ClientRequestKind::Advisor)
        );
    }
}
