//! Client-specific request-kind classification.
//!
//! `lifecycle.rs` parses each request body once and delegates classification
//! through [`classify_client_request_kind`].

pub(crate) mod aside;
pub(crate) mod claude_code;
pub(crate) mod hermes;
pub(crate) mod omp;
pub(crate) mod opencode;
pub(crate) mod senpi;

use serde_json::Value;

/// Maximum number of bytes inspected from each system or user text block.
pub(crate) const CLASSIFIER_TEXT_LIMIT: usize = 4_096;

/// Claude Code and OMP both send this attribution block in the inbound system prompt.
const BILLING_TAG_PREFIX: &str = "x-anthropic-billing-header:";
const BILLING_ENTRYPOINT_KEY: &str = "cc_entrypoint=";
const BILLING_SUBAGENT_FLAG: &str = "cc_is_subagent=true";
pub(crate) const OMP_BILLING_ENTRYPOINT: &str = "local-agent";
const SHARED_RECAP_PROMPT_PREFIX: &str =
    "The user stepped away and is coming back. Recap in under 40 words";
const SHARED_COMPACTION_SUMMARY_MARKER: &str =
    "Your task is to create a detailed summary of the conversation so far";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientRequestKind {
    Main,
    Advisor,
    Subagent,
    Recap,
    Compaction,
    SessionTitle,
    AutoThinking,
    Notification,
    Side,
    LookAt,
    Unknown,
}

impl ClientRequestKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Advisor => "advisor",
            Self::Subagent => "subagent",
            Self::Recap => "recap",
            Self::Compaction => "compaction",
            Self::SessionTitle => "session_title",
            Self::AutoThinking => "auto_thinking",
            Self::Notification => "notification",
            Self::Side => "side",
            Self::LookAt => "look_at",
            Self::Unknown => "unknown",
        }
    }
}

pub(crate) struct BillingAttribution<'a> {
    pub(crate) entrypoint: Option<&'a str>,
    pub(crate) is_subagent: bool,
}

/// Shared, borrow-only view over the already parsed request body.
pub(crate) struct RequestView<'a> {
    value: &'a Value,
    system: Option<&'a Value>,
    leading_system: Option<&'a Value>,
    first_user: Option<&'a Value>,
    last_user: Option<&'a Value>,
    billing: Option<BillingAttribution<'a>>,
    session_id: Option<&'a str>,
    parent_session_id: Option<&'a str>,
}

impl<'a> RequestView<'a> {
    pub(crate) fn new(
        value: &'a Value,
        session_id: Option<&'a str>,
        parent_session_id: Option<&'a str>,
    ) -> Self {
        let system = value
            .get("system")
            .or_else(|| value.get("instructions"))
            .or_else(|| value.get("systemInstruction"))
            .or_else(|| value.pointer("/generateContentRequest/systemInstruction"));
        Self {
            value,
            leading_system: leading_system_content(value),
            system,
            first_user: first_user_content(value),
            last_user: last_user_content(value),
            billing: billing_attribution(value.get("system")),
            session_id,
            parent_session_id,
        }
    }

    pub(crate) fn billing(&self) -> Option<&BillingAttribution<'a>> {
        self.billing.as_ref()
    }

    pub(crate) const fn session_id(&self) -> Option<&'a str> {
        self.session_id
    }

    pub(crate) const fn parent_session_id(&self) -> Option<&'a str> {
        self.parent_session_id
    }

    pub(crate) const fn has_provenance(&self) -> bool {
        self.session_id.is_some() || self.billing.is_some()
    }

    pub(crate) fn system_contains(&self, marker: &str) -> bool {
        self.system
            .is_some_and(|content| bounded_text_contains(content, marker))
    }

    pub(crate) fn system_starts_with(&self, marker: &str) -> bool {
        self.system
            .is_some_and(|content| text_block_starts_with(content, marker))
    }

    pub(crate) fn leading_system_contains(&self, marker: &str) -> bool {
        self.leading_system
            .is_some_and(|content| bounded_text_contains(content, marker))
    }

    pub(crate) fn leading_system_starts_with(&self, marker: &str) -> bool {
        self.leading_system
            .is_some_and(|content| text_block_starts_with(content, marker))
    }

    pub(crate) fn system_ends_with(&self, marker: &str) -> bool {
        self.system
            .is_some_and(|content| text_block_ends_with(content, marker))
    }

    pub(crate) fn leading_system_ends_with(&self, marker: &str) -> bool {
        self.leading_system
            .is_some_and(|content| text_block_ends_with(content, marker))
    }

    pub(crate) fn system_tail_contains(&self, marker: &str) -> bool {
        self.system
            .is_some_and(|content| bounded_tail_text_contains(content, marker))
    }

    pub(crate) fn leading_system_tail_contains(&self, marker: &str) -> bool {
        self.leading_system
            .is_some_and(|content| bounded_tail_text_contains(content, marker))
    }

    pub(crate) fn any_system_starts_with(&self, marker: &str) -> bool {
        self.system_starts_with(marker) || self.leading_system_starts_with(marker)
    }

    pub(crate) fn any_system_contains(&self, marker: &str) -> bool {
        self.system_contains(marker) || self.leading_system_contains(marker)
    }

    pub(crate) fn any_system_ends_with(&self, marker: &str) -> bool {
        self.system_ends_with(marker) || self.leading_system_ends_with(marker)
    }

    pub(crate) fn any_system_tail_contains(&self, marker: &str) -> bool {
        self.system_tail_contains(marker) || self.leading_system_tail_contains(marker)
    }

    pub(crate) fn user_contains(&self, marker: &str) -> bool {
        [self.first_user, self.last_user]
            .into_iter()
            .flatten()
            .any(|content| bounded_text_contains(content, marker))
    }

    pub(crate) fn user_starts_with(&self, marker: &str) -> bool {
        [self.first_user, self.last_user]
            .into_iter()
            .flatten()
            .any(|content| text_block_starts_with(content, marker))
    }

    pub(crate) fn first_user_starts_with(&self, marker: &str) -> bool {
        self.first_user
            .is_some_and(|content| text_block_starts_with(content, marker))
    }

    pub(crate) fn last_user_starts_with(&self, marker: &str) -> bool {
        self.last_user
            .is_some_and(|content| text_block_starts_with(content, marker))
    }

    pub(crate) fn has_tools(&self) -> bool {
        self.value
            .get("tools")
            .and_then(Value::as_array)
            .is_some_and(|tools| !tools.is_empty())
            || self
                .value
                .pointer("/generateContentRequest/tools")
                .and_then(Value::as_array)
                .is_some_and(|tools| !tools.is_empty())
    }

    pub(crate) fn pointer(&self, path: &str) -> Option<&'a Value> {
        self.value.pointer(path)
    }
}

fn classify_shared(view: &RequestView<'_>) -> Option<ClientRequestKind> {
    if !view.has_provenance() {
        return None;
    }
    if view.user_starts_with(SHARED_RECAP_PROMPT_PREFIX)
        || view.user_starts_with("<recap>\nUser stepped away; returning. Recap:")
        || view.user_starts_with("<recap>\r\nUser stepped away; returning. Recap:")
    {
        return Some(ClientRequestKind::Recap);
    }
    if view.user_contains(SHARED_COMPACTION_SUMMARY_MARKER)
        || view.any_system_starts_with(
            "You are a helpful AI assistant tasked with summarizing conversations.",
        )
    {
        return Some(ClientRequestKind::Compaction);
    }
    None
}

type FamilyClassifier = fn(&RequestView<'_>) -> Option<ClientRequestKind>;
const FAMILY_CLASSIFIERS: [FamilyClassifier; 6] = [
    aside::classify_main,
    hermes::classify_main,
    senpi::classify_main,
    opencode::classify_main,
    omp::classify,
    claude_code::classify,
];

#[cfg(test)]
pub(crate) fn classify_client_request_kind(
    value: Option<&Value>,
    observed_session_id: Option<&str>,
    parent_session_id: Option<&str>,
    has_claude_agent: bool,
) -> Option<&'static str> {
    classify_client_request_kind_with_class(
        value,
        observed_session_id,
        parent_session_id,
        has_claude_agent,
        None,
    )
}

fn classify_explicit_request_class(raw: &str) -> Option<Option<ClientRequestKind>> {
    let raw = raw.trim();
    // Compare case-insensitively without allocating a normalized header value.
    [
        ("main", Some(ClientRequestKind::Main)),
        ("subagent", Some(ClientRequestKind::Subagent)),
        ("workflow", Some(ClientRequestKind::Subagent)),
        ("compaction", Some(ClientRequestKind::Compaction)),
        ("side_reply", Some(ClientRequestKind::Side)),
        ("side", Some(ClientRequestKind::Side)),
        ("recap", Some(ClientRequestKind::Recap)),
        ("session_title", Some(ClientRequestKind::SessionTitle)),
        ("title", Some(ClientRequestKind::SessionTitle)),
        ("notification", Some(ClientRequestKind::Notification)),
        ("advisor", Some(ClientRequestKind::Advisor)),
        ("auto_thinking", Some(ClientRequestKind::AutoThinking)),
        ("look_at", Some(ClientRequestKind::LookAt)),
        ("auxiliary", None),
    ]
    .into_iter()
    .find_map(|(class, kind)| raw.eq_ignore_ascii_case(class).then_some(kind))
}

pub(crate) fn classify_client_request_kind_with_class(
    value: Option<&Value>,
    observed_session_id: Option<&str>,
    parent_session_id: Option<&str>,
    has_claude_agent: bool,
    claude_request_class: Option<&str>,
) -> Option<&'static str> {
    let explicit_class = claude_request_class.and_then(classify_explicit_request_class);
    if let Some(Some(kind)) = explicit_class {
        return Some(kind.as_str());
    }
    let Some(value) = value else {
        if has_claude_agent {
            return Some(ClientRequestKind::Subagent.as_str());
        }
        return explicit_class
            .is_some()
            .then(|| ClientRequestKind::Unknown.as_str());
    };
    let view = RequestView::new(value, observed_session_id, parent_session_id);
    let auxiliary_classifiers: [FamilyClassifier; 8] = [
        omp::classify_advisor,
        aside::classify_auxiliary,
        hermes::classify_auxiliary,
        senpi::classify_auxiliary,
        opencode::classify_auxiliary,
        omp::classify_auxiliary,
        claude_code::classify_auxiliary,
        classify_shared,
    ];
    for classify in auxiliary_classifiers {
        if let Some(kind) = classify(&view) {
            return Some(kind.as_str());
        }
    }
    if has_claude_agent {
        return Some(ClientRequestKind::Subagent.as_str());
    }
    for classify in FAMILY_CLASSIFIERS {
        if let Some(kind) = classify(&view) {
            return Some(kind.as_str());
        }
    }

    (view.has_provenance() || explicit_class.is_some()).then(|| ClientRequestKind::Unknown.as_str())
}

fn billing_attribution(system: Option<&Value>) -> Option<BillingAttribution<'_>> {
    let text = text_blocks(system?).find(|text| {
        bounded_prefix(text)
            .trim_start()
            .starts_with(BILLING_TAG_PREFIX)
    })?;
    let prefix = bounded_prefix(text);
    let entrypoint = prefix
        .split_once(BILLING_ENTRYPOINT_KEY)
        .map(|(_, rest)| rest.split(';').next().unwrap_or(rest).trim())
        .filter(|entrypoint| !entrypoint.is_empty());
    Some(BillingAttribution {
        entrypoint,
        is_subagent: prefix.contains(BILLING_SUBAGENT_FLAG),
    })
}

fn leading_system_content(value: &Value) -> Option<&Value> {
    if let Some(messages) = value.get("messages").and_then(Value::as_array)
        && let Some(message) = messages.first()
        && message.get("role").and_then(Value::as_str) == Some("system")
    {
        return message.get("content");
    }

    value
        .get("input")
        .and_then(Value::as_array)
        .and_then(|input| input.first())
        .filter(|item| {
            matches!(
                item.get("role").and_then(Value::as_str),
                Some("system" | "developer")
            )
        })
        .and_then(|item| item.get("content").or(Some(item)))
}

fn first_user_content(value: &Value) -> Option<&Value> {
    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        return messages
            .iter()
            .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
            .and_then(|message| message.get("content"));
    }

    if let Some(input) = value.get("input") {
        return match input {
            Value::String(_) => Some(input),
            Value::Array(input) => input
                .iter()
                .find(|item| item.get("role").and_then(Value::as_str) == Some("user"))
                .and_then(|item| item.get("content").or(Some(item))),
            _ => None,
        };
    }

    first_gemini_user_content(value)
}

fn last_user_content(value: &Value) -> Option<&Value> {
    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        return messages
            .iter()
            .rev()
            .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
            .and_then(|message| message.get("content"));
    }

    if let Some(input) = value.get("input") {
        return match input {
            Value::String(_) => Some(input),
            Value::Array(input) => input
                .iter()
                .rev()
                .find(|item| item.get("role").and_then(Value::as_str) == Some("user"))
                .and_then(|item| item.get("content").or(Some(item))),
            _ => None,
        };
    }

    last_gemini_user_content(value)
}

fn gemini_contents(value: &Value) -> Option<&[Value]> {
    value
        .get("contents")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| {
            value
                .pointer("/generateContentRequest/contents")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
        })
}

fn first_gemini_user_content(value: &Value) -> Option<&Value> {
    gemini_contents(value)?
        .iter()
        .find(|item| gemini_user_or_absent_role(item))
        .and_then(|item| item.get("parts"))
}

fn last_gemini_user_content(value: &Value) -> Option<&Value> {
    gemini_contents(value)?
        .iter()
        .rev()
        .find(|item| gemini_user_or_absent_role(item))
        .and_then(|item| item.get("parts"))
}

fn gemini_user_or_absent_role(item: &Value) -> bool {
    item.get("role")
        .is_none_or(|role| role.as_str() == Some("user"))
}

fn text_block_starts_with(content: &Value, marker: &str) -> bool {
    text_blocks(content).any(|text| text.trim_start().starts_with(marker))
}
fn text_block_ends_with(content: &Value, marker: &str) -> bool {
    text_blocks(content).any(|text| text.trim_end().ends_with(marker))
}

fn bounded_text_contains(content: &Value, marker: &str) -> bool {
    text_blocks(content).any(|text| bounded_prefix(text).contains(marker))
}

fn bounded_tail_text_contains(content: &Value, marker: &str) -> bool {
    text_blocks(content).any(|text| bounded_suffix(text).contains(marker))
}

fn text_blocks(content: &Value) -> impl Iterator<Item = &str> {
    let direct = match content {
        Value::String(text) => Some(text.as_str()),
        Value::Object(object) => object.get("text").and_then(Value::as_str),
        _ => None,
    };
    let blocks = match content {
        Value::Array(blocks) => Some(blocks.as_slice()),
        _ => None,
    };
    let parts = content
        .get("parts")
        .and_then(Value::as_array)
        .map(Vec::as_slice);

    direct
        .into_iter()
        .chain(blocks.into_iter().flatten().filter_map(text_block))
        .chain(parts.into_iter().flatten().filter_map(text_block))
}

fn text_block(block: &Value) -> Option<&str> {
    block.get("text").and_then(Value::as_str)
}
fn bounded_prefix(text: &str) -> &str {
    let mut end = text.len().min(CLASSIFIER_TEXT_LIMIT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn bounded_suffix(text: &str) -> &str {
    let mut start = text.len().saturating_sub(CLASSIFIER_TEXT_LIMIT);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn header_agent_short_circuits_to_subagent() {
        assert_eq!(
            classify_client_request_kind(None, None, None, true),
            Some("subagent")
        );
    }

    #[test]
    fn recap_prompt_classifies_before_family_rules() {
        let value = json!({
            "system": [{
                "type": "text",
                "text": "operating in the Oh My Pi coding harness"
            }],
            "messages": [{
                "role": "user",
                "content": "The user stepped away and is coming back. Recap in under 40 words. Lead with the goal."
            }]
        });

        assert_eq!(
            classify_client_request_kind(Some(&value), Some("session-1"), None, false),
            Some("recap")
        );
    }

    #[test]
    fn compaction_precedes_omp_main_marker() {
        let compaction = format!(
            "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\n\n{}",
            SHARED_COMPACTION_SUMMARY_MARKER
        );
        let value = json!({
            "system": [{
                "type": "text",
                "text": "operating in the Oh My Pi coding harness"
            }],
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": compaction}]
            }],
            "tools": [{"name": "Bash"}]
        });

        assert_eq!(
            classify_client_request_kind(Some(&value), Some("session-1"), None, false),
            Some("compaction")
        );
    }

    #[test]
    fn advisor_precedes_shared_compaction_markers() {
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
                "content": format!(
                    "Review this transcript quoting: {}",
                    SHARED_COMPACTION_SUMMARY_MARKER
                )
            }]
        });

        assert_eq!(
            classify_client_request_kind(Some(&value), None, None, false),
            Some("advisor")
        );
    }

    #[test]
    fn senpi_auxiliary_marker_precedes_shared_recap_marker() {
        let value = json!({
            "system": "Generate a concise title for this coding-agent session.\n\nRules:\n- Use 3 to 6 words.",
            "messages": [{
                "role": "user",
                "content": "The user stepped away and is coming back. Recap in under 40 words."
            }]
        });

        assert_eq!(
            classify_client_request_kind(Some(&value), None, None, false),
            Some("session_title")
        );
    }

    #[test]
    fn request_without_provenance_is_unclassified() {
        let value = json!({
            "messages": [{
                "role": "user",
                "content": "The user stepped away and is coming back. Recap in under 40 words."
            }]
        });

        assert_eq!(
            classify_client_request_kind(Some(&value), None, None, false),
            None
        );
    }

    #[test]
    fn request_view_supports_openai_instruction_and_input_fallbacks() {
        let precedence = json!({
            "system": "PRIMARY SYSTEM",
            "instructions": "SECONDARY SYSTEM",
            "systemInstruction": {"parts": [{"text": "TERTIARY SYSTEM"}]},
            "generateContentRequest": {
                "systemInstruction": {"parts": [{"text": "NESTED SYSTEM"}]}
            }
        });
        let view = RequestView::new(&precedence, None, None);
        assert!(view.system_starts_with("PRIMARY SYSTEM"));
        assert!(!view.system_contains("SECONDARY SYSTEM"));

        let instructions = json!({
            "instructions": [{"type": "text", "text": "OPENAI SYSTEM"}],
            "input": "OPENAI INPUT"
        });
        let view = RequestView::new(&instructions, None, None);
        assert!(view.system_contains("OPENAI SYSTEM"));
        assert!(view.first_user_starts_with("OPENAI INPUT"));
        assert!(view.last_user_starts_with("OPENAI INPUT"));

        let input = json!({
            "input": [
                {"role": "developer", "content": [{"type": "input_text", "text": "DEVELOPER SYSTEM"}]},
                {"role": "user", "content": [{"type": "input_text", "text": "FIRST INPUT"}]},
                {"role": "assistant", "content": [{"type": "input_text", "text": "ASSISTANT"}]},
                {"role": "user", "content": [{"type": "input_text", "text": "LAST INPUT"}]}
            ]
        });
        let view = RequestView::new(&input, None, None);
        assert!(view.leading_system_starts_with("DEVELOPER SYSTEM"));
        assert!(view.first_user_starts_with("FIRST INPUT"));
        assert!(view.last_user_starts_with("LAST INPUT"));
        let raw_item = json!({
            "input": [
                {"role": "user", "text": "RAW INPUT ITEM"}
            ]
        });
        let view = RequestView::new(&raw_item, None, None);
        assert!(view.first_user_starts_with("RAW INPUT ITEM"));
        assert!(view.last_user_starts_with("RAW INPUT ITEM"));
    }
    #[test]
    fn request_view_supports_gemini_system_contents_and_nested_request() {
        let value = json!({
            "systemInstruction": {
                "parts": [{"text": "GEMINI SYSTEM"}]
            },
            "contents": [
                {"role": "model", "parts": [{"text": "MODEL"}]},
                {"parts": [{"text": "FIRST GEMINI USER"}]},
                {"role": "user", "parts": [{"text": "LAST GEMINI USER"}]}
            ],
            "generateContentRequest": {
                "systemInstruction": {"parts": [{"text": "NESTED SYSTEM"}]},
                "contents": [{"role": "user", "parts": [{"text": "NESTED USER"}]}],
                "tools": [{"function": {"name": "lookup"}}]
            }
        });
        let view = RequestView::new(&value, None, None);
        assert!(view.system_starts_with("GEMINI SYSTEM"));
        assert!(view.first_user_starts_with("FIRST GEMINI USER"));
        assert!(view.last_user_starts_with("LAST GEMINI USER"));
        assert!(view.has_tools());

        let nested = json!({
            "generateContentRequest": {
                "systemInstruction": {"parts": [{"text": "NESTED SYSTEM"}]},
                "contents": [{"role": "user", "parts": [{"text": "NESTED USER"}]}]
            }
        });
        let view = RequestView::new(&nested, None, None);
        assert!(view.system_starts_with("NESTED SYSTEM"));
        assert!(view.first_user_starts_with("NESTED USER"));
        assert!(view.last_user_starts_with("NESTED USER"));
    }

    #[test]
    fn request_view_keeps_billing_attribution_on_system_only() {
        let value = json!({
            "instructions": [{
                "text": "x-anthropic-billing-header: cc_entrypoint=wrong"
            }],
            "systemInstruction": {
                "parts": [{"text": "x-anthropic-billing-header: cc_entrypoint=wrong"}]
            }
        });
        let view = RequestView::new(&value, None, None);
        assert!(view.billing().is_none());
        assert!(!view.has_provenance());
    }

    #[test]
    fn explicit_classes_override_body_markers_and_agent_header() {
        let value = json!({
            "system": "You shadow the main agent as a peer programmer",
            "messages": [{"role": "user", "content": "Complete assignment thoroughly:"}]
        });
        for (class, expected) in [
            (" MAIN ", "main"),
            ("subagent", "subagent"),
            ("workflow", "subagent"),
            ("compaction", "compaction"),
            ("side_reply", "side"),
            ("side", "side"),
            ("recap", "recap"),
            ("session_title", "session_title"),
            ("title", "session_title"),
            ("notification", "notification"),
            ("advisor", "advisor"),
            ("auto_thinking", "auto_thinking"),
            ("look_at", "look_at"),
        ] {
            for body in [None, Some(&value)] {
                assert_eq!(
                    classify_client_request_kind_with_class(body, None, None, true, Some(class),),
                    Some(expected),
                    "class={class}"
                );
            }
        }
    }

    #[test]
    fn auxiliary_class_is_recognized_but_body_rules_can_refine_it() {
        let value = json!({
            "system": "You are omp's trusted coding assistant.",
            "messages": [{"role": "user", "content": "<title-request>Name this session"}]
        });
        assert_eq!(
            classify_client_request_kind_with_class(
                Some(&value),
                Some("session"),
                None,
                true,
                Some("AUXILIARY"),
            ),
            Some("session_title")
        );
        let empty = json!({});
        for body in [None, Some(&empty)] {
            assert_eq!(
                classify_client_request_kind_with_class(
                    body,
                    None,
                    None,
                    false,
                    Some(" auxiliary "),
                ),
                Some("unknown")
            );
            assert_eq!(
                classify_client_request_kind_with_class(body, None, None, true, Some("auxiliary"),),
                Some("subagent")
            );
            assert_eq!(
                classify_client_request_kind_with_class(
                    body,
                    None,
                    None,
                    false,
                    Some("unrecognized"),
                ),
                None
            );
        }
    }

    #[test]
    fn body_auxiliaries_precede_agent_header_but_main_does_not() {
        for (system, user, expected) in [
            (
                "You shadow the main agent as a peer programmer",
                "work",
                "advisor",
            ),
            (
                "You are Aside, a helpful, proactive, and smart AI browser.<sidechat_directive>question</sidechat_directive>",
                "work",
                "side",
            ),
            (
                "You name chat sessions. Given the user's opening message, write a title",
                "work",
                "session_title",
            ),
            (
                "Generate a concise title for this coding-agent session.",
                "work",
                "session_title",
            ),
            (
                "You are a title generator. You output ONLY a thread title. Nothing else.",
                "work",
                "session_title",
            ),
            (
                "You are omp's trusted coding assistant.",
                "<recap>\nUser stepped away; returning. Recap:",
                "recap",
            ),
            (
                "A user kicked off a Claude Code agent to do a coding task and walked away",
                "work",
                "notification",
            ),
            (
                "You are a helpful AI assistant tasked with summarizing conversations.",
                "work",
                "compaction",
            ),
            (
                "You are omp's trusted coding assistant.",
                "work",
                "subagent",
            ),
        ] {
            let value = json!({
                "system": system,
                "messages": [{"role": "user", "content": user}]
            });
            assert_eq!(
                classify_client_request_kind(Some(&value), Some("session"), None, true),
                Some(expected),
                "system={system}"
            );
        }
    }

    #[test]
    fn last_user_is_found_before_trailing_assistant_and_tool_messages() {
        let value = json!({"messages": [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "answer"},
            {"role": "user", "content": "last"},
            {"role": "assistant", "content": "tool call"},
            {"role": "tool", "content": "tool result"}
        ]});
        let view = RequestView::new(&value, None, None);
        assert!(view.first_user_starts_with("first"));
        assert!(view.last_user_starts_with("last"));
        assert!(!view.last_user_starts_with("tool"));
    }

    #[test]
    fn suffix_scan_is_utf8_safe_and_bounded() {
        for text in [
            "",
            "short",
            &"é".repeat(CLASSIFIER_TEXT_LIMIT),
            &"🦀".repeat(CLASSIFIER_TEXT_LIMIT),
        ] {
            let suffix = bounded_suffix(text);
            assert!(suffix.len() <= CLASSIFIER_TEXT_LIMIT);
            assert!(text.ends_with(suffix));
        }
        let text = format!(
            "head-marker{}tail-marker",
            "é".repeat(CLASSIFIER_TEXT_LIMIT)
        );
        let content = json!([{"text": text}]);
        assert!(bounded_tail_text_contains(&content, "tail-marker"));
        assert!(!bounded_tail_text_contains(&content, "head-marker"));
        assert!(bounded_text_contains(&content, "head-marker"));
        assert!(!bounded_text_contains(&content, "tail-marker"));
        let split_boundary = format!("€{}", "x".repeat(CLASSIFIER_TEXT_LIMIT - 1));
        assert_eq!(
            bounded_suffix(&split_boundary),
            "x".repeat(CLASSIFIER_TEXT_LIMIT - 1)
        );
    }

    #[test]
    fn agent_header_still_classifies_unrecognized_body_as_subagent() {
        let value = json!({"messages": [{"role": "user", "content": "unrecognized"}]});
        assert_eq!(
            classify_client_request_kind(Some(&value), None, None, true),
            Some("subagent")
        );
    }

    #[test]
    fn shared_compaction_still_precedes_billing_subagent_flag() {
        let value = json!({
            "system": "x-anthropic-billing-header: cc_entrypoint=cli; cc_is_subagent=true;",
            "messages": [{"role": "user", "content": SHARED_COMPACTION_SUMMARY_MARKER}]
        });
        assert_eq!(
            classify_client_request_kind(Some(&value), None, None, false),
            Some("compaction")
        );
    }
}
