use super::{ClientRequestKind, RequestView};

const ASIDE_MAIN_SYSTEM_PREFIX: &str = "You are Aside, a helpful, proactive, and smart AI browser.";

pub(crate) fn classify_auxiliary(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    if view
        .session_id()
        .is_some_and(|id| id.starts_with("aside:worker:"))
        || view.any_system_starts_with("You are a child subagent spawned by a parent agent.")
        || view.any_system_starts_with("You are a forked worker process")
        || view.any_system_ends_with("</subagent_directive>")
        || view.any_system_tail_contains("<subagent_directive>")
        || view.last_user_starts_with("You are now acting as the memory extraction subagent.")
    {
        return Some(ClientRequestKind::Subagent);
    }
    if view.any_system_ends_with("</sidechat_directive>")
        || view.any_system_tail_contains("<sidechat_directive>")
    {
        return Some(ClientRequestKind::Side);
    }
    if view.any_system_contains(
        "Generate a brief title that would help the user find this conversation later.",
    ) {
        return Some(ClientRequestKind::SessionTitle);
    }
    if (view.any_system_contains("Do NOT continue the conversation")
        && view.any_system_contains("ONLY output the structured summary"))
        || view.user_contains(
            "Create a structured context checkpoint summary that another LLM will use",
        )
        || view
            .user_contains("The messages above are earlier context from an ongoing conversation.")
    {
        return Some(ClientRequestKind::Compaction);
    }
    if view
        .session_id()
        .is_some_and(|id| id.starts_with("aside:aux:"))
        || view.any_system_starts_with("You are Aside's memory dreaming pass.")
        || view.any_system_starts_with(
            "Read the distorted/styled text in this image from left to right.",
        )
    {
        return Some(ClientRequestKind::Unknown);
    }
    None
}

pub(crate) fn classify_main(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    view.any_system_starts_with(ASIDE_MAIN_SYSTEM_PREFIX)
        .then_some(ClientRequestKind::Main)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::request_classification::{CLASSIFIER_TEXT_LIMIT, classify_client_request_kind};

    #[test]
    fn classifies_aside_main_and_auxiliary_prompts_on_both_wire_formats() {
        let cases = [
            (ASIDE_MAIN_SYSTEM_PREFIX, "hello", "main"),
            (
                "You are a child subagent spawned by a parent agent.",
                "work",
                "subagent",
            ),
            (
                "You are a forked worker process reviewing code.",
                "work",
                "subagent",
            ),
            (
                ASIDE_MAIN_SYSTEM_PREFIX,
                "You are now acting as the memory extraction subagent.",
                "subagent",
            ),
            (
                "Generate a brief title that would help the user find this conversation later.",
                "work",
                "session_title",
            ),
            (
                "Do NOT continue the conversation. ONLY output the structured summary",
                "work",
                "compaction",
            ),
            (
                ASIDE_MAIN_SYSTEM_PREFIX,
                "Create a structured context checkpoint summary that another LLM will use",
                "compaction",
            ),
            (
                ASIDE_MAIN_SYSTEM_PREFIX,
                "The messages above are earlier context from an ongoing conversation.",
                "compaction",
            ),
            ("You are Aside's memory dreaming pass.", "work", "unknown"),
            (
                "Read the distorted/styled text in this image from left to right.",
                "work",
                "unknown",
            ),
        ];
        for (system, user, expected) in cases {
            for leading in [false, true] {
                let value = if leading {
                    json!({"messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                        {"role": "assistant", "content": "prior response"}
                    ]})
                } else {
                    json!({"system": system, "messages": [{"role": "user", "content": user}]})
                };
                assert_eq!(
                    classify_client_request_kind(Some(&value), None, None, false),
                    Some(expected),
                    "system={system}, leading={leading}"
                );
            }
        }
    }

    #[test]
    fn session_names_identify_workers_and_non_main_helpers() {
        let value = json!({"system": ASIDE_MAIN_SYSTEM_PREFIX});
        for (session, expected) in [
            ("aside:worker:child", "subagent"),
            ("aside:aux:taxonomy_migrate", "unknown"),
        ] {
            assert_eq!(
                classify_client_request_kind(Some(&value), Some(session), None, false),
                Some(expected)
            );
        }
    }

    #[test]
    fn appended_directives_beyond_prefix_scan_override_main() {
        for (directive, expected) in [
            ("subagent_directive", "subagent"),
            ("sidechat_directive", "side"),
        ] {
            for suffix in ["", "\nruntime context follows"] {
                let system = format!(
                    "{ASIDE_MAIN_SYSTEM_PREFIX}{}<{directive}>work</{directive}>{suffix}",
                    "é".repeat(CLASSIFIER_TEXT_LIMIT)
                );
                for leading in [false, true] {
                    let value = if leading {
                        json!({"messages": [{"role": "system", "content": system}]})
                    } else {
                        json!({"system": system})
                    };
                    assert_eq!(
                        classify_client_request_kind(Some(&value), None, None, false),
                        Some(expected)
                    );
                }
            }
        }
    }

    #[test]
    fn incomplete_compaction_marker_does_not_override_main() {
        let value = json!({"system": format!("{ASIDE_MAIN_SYSTEM_PREFIX}\nDo NOT continue the conversation")});
        assert_eq!(
            classify_client_request_kind(Some(&value), None, None, false),
            Some("main")
        );
    }
}
