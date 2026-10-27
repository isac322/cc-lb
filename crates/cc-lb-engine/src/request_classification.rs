//! Client-specific request-kind classification.
//!
//! `lifecycle.rs` parses each request body once and delegates classification
//! through [`classify_client_request_kind`].

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
    if view.user_starts_with(SHARED_RECAP_PROMPT_PREFIX) {
        return Some(ClientRequestKind::Recap);
    }
    if view.user_contains(SHARED_COMPACTION_SUMMARY_MARKER) {
        return Some(ClientRequestKind::Compaction);
    }
    None
}

type FamilyClassifier = fn(&RequestView<'_>) -> Option<ClientRequestKind>;
const FAMILY_CLASSIFIERS: [FamilyClassifier; 5] = [
    hermes::classify_main,
    senpi::classify_main,
    opencode::classify_main,
    omp::classify,
    claude_code::classify,
];

pub(crate) fn classify_client_request_kind(
    value: Option<&Value>,
    observed_session_id: Option<&str>,
    parent_session_id: Option<&str>,
    has_claude_agent: bool,
) -> Option<&'static str> {
    if has_claude_agent {
        return Some(ClientRequestKind::Subagent.as_str());
    }

    let value = value?;
    let view = RequestView::new(value, observed_session_id, parent_session_id);
    if let Some(kind) = omp::classify_advisor(&view) {
        return Some(kind.as_str());
    }
    if let Some(kind) = hermes::classify_auxiliary(&view) {
        return Some(kind.as_str());
    }
    if let Some(kind) = senpi::classify_auxiliary(&view) {
        return Some(kind.as_str());
    }
    if let Some(kind) = opencode::classify_auxiliary(&view) {
        return Some(kind.as_str());
    }
    if let Some(kind) = classify_shared(&view) {
        return Some(kind.as_str());
    }
    for classify in FAMILY_CLASSIFIERS {
        if let Some(kind) = classify(&view) {
            return Some(kind.as_str());
        }
    }

    view.has_provenance()
        .then(|| ClientRequestKind::Unknown.as_str())
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
            .last()
            .filter(|message| message.get("role").and_then(Value::as_str) == Some("user"))
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
}
