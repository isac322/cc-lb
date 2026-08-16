use super::{ClientRequestKind, RequestView};

const HERMES_MAIN_SYSTEM_PREFIX: &str =
    "You are Hermes Agent, an intelligent AI assistant created by Nous Research.";
const HERMES_SUBAGENT_SYSTEM_PREFIX: &str =
    "You are a focused subagent working on a specific delegated task.";
const HERMES_SESSION_TITLE_SYSTEM_PREFIX: &str =
    "You name chat sessions. Given the user's opening message, write a title";
const HERMES_COMPACTION_USER_PREFIX: &str =
    "You are a summarization agent creating a context checkpoint.";

pub(crate) fn classify_auxiliary(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    view.session_id()?;

    if view.system_starts_with(HERMES_SUBAGENT_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::Subagent);
    }
    if view.system_starts_with(HERMES_SESSION_TITLE_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::SessionTitle);
    }
    if view.first_user_starts_with(HERMES_COMPACTION_USER_PREFIX) {
        return Some(ClientRequestKind::Compaction);
    }

    None
}

pub(crate) fn classify_main(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    (view.session_id().is_some() && view.system_starts_with(HERMES_MAIN_SYSTEM_PREFIX))
        .then_some(ClientRequestKind::Main)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::request_classification::classify_client_request_kind;

    fn classify(value: &serde_json::Value, session_id: Option<&str>) -> Option<&'static str> {
        classify_client_request_kind(Some(value), session_id, None, false)
    }

    #[test]
    fn classifies_measured_hermes_request_families() {
        let cases = [
            (
                json!({
                    "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.\n\nYou assist users with a wide range of tasks.",
                    "messages": [{"role": "user", "content": "hello"}],
                    "tools": [{"name": "terminal"}]
                }),
                "main",
            ),
            (
                json!({
                    "system": "You are a focused subagent working on a specific delegated task.\n\nYOUR TASK:\nReview the auth change",
                    "messages": [{"role": "user", "content": "Review the auth change"}],
                    "tools": [{"name": "terminal"}]
                }),
                "subagent",
            ),
            (
                json!({
                    "system": "You name chat sessions. Given the user's opening message, write a title that lets them find this conversation again in a list.",
                    "messages": [{"role": "user", "content": "fix the router"}]
                }),
                "session_title",
            ),
            (
                json!({
                    "messages": [{
                        "role": "user",
                        "content": "You are a summarization agent creating a context checkpoint.\n\nSummarize the conversation."
                    }]
                }),
                "compaction",
            ),
        ];

        for (value, expected) in cases {
            assert_eq!(
                classify(&value, Some("hermes-root-session")),
                Some(expected)
            );
        }
    }

    #[test]
    fn refuses_hermes_body_markers_without_session_provenance() {
        let value = json!({
            "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
            "messages": [{"role": "user", "content": "hello"}]
        });

        assert_eq!(classify(&value, None), None);
    }

    #[test]
    fn preserves_unknown_for_unrecognized_hermes_auxiliary_calls() {
        let value = json!({
            "system": "Extract structured data from the supplied document.",
            "messages": [{"role": "user", "content": "document"}]
        });

        assert_eq!(
            classify(&value, Some("hermes-root-session")),
            Some("unknown")
        );
    }
}
