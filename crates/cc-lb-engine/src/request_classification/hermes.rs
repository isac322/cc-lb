use super::{ClientRequestKind, RequestView};

const HERMES_MAIN_SYSTEM_PREFIXES: [&str; 2] = [
    "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
    "You are Hermes Agent, built by Nous Research.",
];
const HERMES_SUBAGENT_SYSTEM_PREFIX: &str =
    "You are a focused subagent working on a specific delegated task.";
const HERMES_SUBAGENT_SYSTEM_MARKER: &str = "\nPlatform: subagent\n";
const HERMES_SESSION_TITLE_SYSTEM_PREFIX: &str =
    "You name chat sessions. Given the user's opening message, write a title";
const HERMES_COMPACTION_USER_PREFIX: &str =
    "You are a summarization agent creating a context checkpoint.";

pub(crate) fn classify_auxiliary(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    view.session_id()?;

    if view.any_system_starts_with(HERMES_SUBAGENT_SYSTEM_PREFIX)
        || view.any_system_contains(HERMES_SUBAGENT_SYSTEM_PREFIX)
        || view.any_system_tail_contains(HERMES_SUBAGENT_SYSTEM_PREFIX)
        || view.any_system_contains(HERMES_SUBAGENT_SYSTEM_MARKER)
        || view.first_user_starts_with("You are running as Hermes' background skill CURATOR.")
    {
        return Some(ClientRequestKind::Subagent);
    }
    if view.last_user_starts_with(
        "The user asked a quick SIDE question with /btw while the main work continues in the original session.",
    ) || view.any_system_starts_with(
        "You are the same AI assistant that is currently working inside the conversation transcribed below.",
    ) {
        return Some(ClientRequestKind::Side);
    }
    if view.any_system_starts_with(HERMES_SESSION_TITLE_SYSTEM_PREFIX) {
        return Some(ClientRequestKind::SessionTitle);
    }
    if view.first_user_starts_with(HERMES_COMPACTION_USER_PREFIX)
        || view.first_user_starts_with(
            "You are a summarization agent creating a compact record of an ongoing conversation.",
        )
        || view.any_system_starts_with("You are a conversation summarization assistant.")
    {
        return Some(ClientRequestKind::Compaction);
    }
    if view.last_user_starts_with(
        "Review the conversation above and consider saving to memory if appropriate.",
    ) || view.last_user_starts_with(
        "Review the conversation above and update the skill library. Be ACTIVE",
    ) || view.last_user_starts_with("Review the conversation above and update two things:")
    {
        return Some(ClientRequestKind::Subagent);
    }

    None
}

pub(crate) fn classify_main(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    (view.session_id().is_some()
        && HERMES_MAIN_SYSTEM_PREFIXES
            .iter()
            .any(|prefix| view.any_system_starts_with(prefix)))
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
                    "system": [
                        {
                            "type": "text",
                            "text": "You are Hermes Agent, an intelligent AI assistant created by Nous Research."
                        },
                        {
                            "type": "text",
                            "text": "\n\nConversation started: Sunday, August 16, 2026 (UTC)\nModel: anthropic/claude-opus-5\nProvider: nous\nPlatform: subagent\n\nYou are a focused subagent working on a specific delegated task.\n\nYOUR TASK:\nReview the auth change"
                        }
                    ],
                    "messages": [{"role": "user", "content": "Review the auth change"}]
                }),
                "subagent",
            ),
            (
                json!({
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are Hermes Agent, an intelligent AI assistant created by Nous Research. You are helpful, knowledgeable, and direct."
                        },
                        {
                            "role": "user",
                            "content": "hello"
                        }
                    ],
                    "tools": [{"name": "memory"}]
                }),
                "main",
            ),
            (
                json!({
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.\n\nConversation started: Sunday, August 16, 2026 (UTC)\nModel: anthropic/claude-opus-5\nProvider: nous\nPlatform: subagent\n\nYou are a focused subagent working on a specific delegated task.\n\nYOUR TASK:\nReview the auth change"
                        },
                        {
                            "role": "user",
                            "content": "Review the auth change"
                        }
                    ]
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
                    "messages": [
                        {
                            "role": "system",
                            "content": "You name chat sessions. Given the user's opening message, write a title that lets them find this conversation again in a list."
                        },
                        {
                            "role": "user",
                            "content": "fix the router"
                        }
                    ]
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
            (
                json!({
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are Hermes Agent, an intelligent AI assistant created by Nous Research."
                        },
                        {
                            "role": "user",
                            "content": "You are a summarization agent creating a context checkpoint.\n\nSummarize the conversation."
                        }
                    ]
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

    #[test]
    fn current_hermes_contexts_support_both_wire_formats() {
        let cases = [
            (
                "You are Hermes Agent, built by Nous Research.",
                "hello",
                "main",
            ),
            (
                "You are a conversation summarization assistant.",
                "work",
                "compaction",
            ),
            (
                "You are Hermes Agent, built by Nous Research.",
                "You are a summarization agent creating a compact record of an ongoing conversation.",
                "compaction",
            ),
            (
                "You are Hermes Agent, built by Nous Research.",
                "The user asked a quick SIDE question with /btw while the main work continues in the original session.",
                "side",
            ),
            (
                "You are the same AI assistant that is currently working inside the conversation transcribed below.",
                "question",
                "side",
            ),
            (
                "You are Hermes Agent, built by Nous Research.",
                "You are running as Hermes' background skill CURATOR.",
                "subagent",
            ),
        ];
        for (system, user, expected) in cases {
            for leading in [false, true] {
                let value = if leading {
                    json!({"messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user}
                    ]})
                } else {
                    json!({"system": system, "messages": [{"role": "user", "content": user}]})
                };
                assert_eq!(classify(&value, Some("hermes-session")), Some(expected));
                assert_eq!(classify(&value, None), None);
            }
        }
    }

    #[test]
    fn runtime_environment_does_not_hide_appended_subagent_marker() {
        for padding in [0, crate::request_classification::CLASSIFIER_TEXT_LIMIT + 1] {
            let system = format!(
                "{}\n{}\n# Hermes runtime environment\nPlatform: subagent\nRuntime details\n{}",
                super::HERMES_MAIN_SYSTEM_PREFIXES[1],
                "é".repeat(padding),
                super::HERMES_SUBAGENT_SYSTEM_PREFIX
            );
            for leading in [false, true] {
                let value = if leading {
                    json!({"messages": [{"role": "system", "content": system}]})
                } else {
                    json!({"system": system})
                };
                assert_eq!(classify(&value, Some("hermes-child")), Some("subagent"));
            }
        }
    }

    #[test]
    fn platform_marker_identifies_subagent_with_custom_task_prompt() {
        let value = json!({
            "system": "You are Hermes Agent, built by Nous Research.\nPlatform: subagent\n# Hermes runtime environment\nReview the code."
        });
        assert_eq!(classify(&value, Some("hermes-child")), Some("subagent"));
    }

    #[test]
    fn background_reviews_classify_as_subagent_with_retained_main_context() {
        for review in [
            "Review the conversation above and consider saving to memory if appropriate.",
            "Review the conversation above and update the skill library. Be ACTIVE",
            "Review the conversation above and update two things:",
        ] {
            let value = json!({
                "system": super::HERMES_MAIN_SYSTEM_PREFIXES[1],
                "messages": [
                    {"role": "user", "content": "Original task"},
                    {"role": "assistant", "content": "Completed task"},
                    {"role": "user", "content": review},
                    {"role": "assistant", "content": "Review prefix"}
                ],
                "tools": [{"name": "memory"}]
            });
            assert_eq!(classify(&value, Some("hermes-session")), Some("subagent"));
        }
    }
}
