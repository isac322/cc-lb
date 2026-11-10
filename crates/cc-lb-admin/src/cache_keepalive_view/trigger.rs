use super::status::CacheKeepaliveViewState;

const TRIGGER_ORDER: [&str; 6] = ["Bash", "Write", "Edit", "Read", "_advise", "Agent in-turn"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheKeepaliveTriggerKind {
    Tool,
    State,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CacheKeepaliveTriggerDescriptor {
    pub(crate) label: String,
    pub(crate) kind: CacheKeepaliveTriggerKind,
}

pub(crate) fn extract_trigger_descriptor(
    display_reason: Option<&str>,
    reason: &str,
    state: CacheKeepaliveViewState,
) -> CacheKeepaliveTriggerDescriptor {
    let display_reason = display_reason.filter(|value| !value.trim().is_empty());
    let source_text = match display_reason {
        Some(display_reason) if display_reason != reason => {
            format!("{display_reason} · {reason}")
        }
        Some(display_reason) => display_reason.to_owned(),
        None => reason.to_owned(),
    };

    if let Some(tool) =
        capture_backtick_tool(&source_text).or_else(|| capture_plain_tool(&source_text))
    {
        return CacheKeepaliveTriggerDescriptor {
            label: normalize_tool_trigger(tool),
            kind: CacheKeepaliveTriggerKind::Tool,
        };
    }
    if let Some(stop_reason) = capture_token_after(&source_text, "stop_reason=") {
        return CacheKeepaliveTriggerDescriptor {
            label: format!("stop_reason={stop_reason}"),
            kind: CacheKeepaliveTriggerKind::State,
        };
    }
    if let Some(unsupported) =
        capture_until_separator(&source_text, "zero-token renewal unsupported:")
    {
        return CacheKeepaliveTriggerDescriptor {
            label: format!("Unsupported: {}", unsupported.trim()),
            kind: CacheKeepaliveTriggerKind::State,
        };
    }
    if contains_ascii_case_insensitive(&source_text, "cached prefix too small") {
        return state_descriptor("Prefix < min tokens");
    }
    if contains_ascii_case_insensitive(&source_text, "snapshot too large") {
        return state_descriptor("Snapshot too large");
    }
    if contains_ascii_case_insensitive(&source_text, "ambiguous turn") {
        return state_descriptor("Ambiguous turn");
    }
    if contains_ascii_case_insensitive(&source_text, "agent-in-turn") {
        return state_descriptor("Agent in-turn");
    }
    if state != CacheKeepaliveViewState::NotTracked {
        return state_descriptor("Agent in-turn");
    }
    state_descriptor("Other")
}

fn state_descriptor(label: &str) -> CacheKeepaliveTriggerDescriptor {
    CacheKeepaliveTriggerDescriptor {
        label: label.to_owned(),
        kind: CacheKeepaliveTriggerKind::State,
    }
}

fn normalize_tool_trigger(raw: &str) -> String {
    let trimmed = raw.trim();
    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "bash" => "Bash".to_owned(),
        "write" | "write_file" => "Write".to_owned(),
        "edit" | "edit_file" => "Edit".to_owned(),
        "read" | "read_file" => "Read".to_owned(),
        _ => TRIGGER_ORDER
            .iter()
            .find(|known| known.to_ascii_lowercase() == lower)
            .map_or_else(|| trimmed.to_owned(), |known| (*known).to_owned()),
    }
}

fn capture_backtick_tool(source: &str) -> Option<&str> {
    let (_, after_prefix) = find_ascii_case_insensitive(source, "tool_use:")?;
    let after_prefix = after_prefix.trim_start();
    let after_prefix = after_prefix.strip_prefix('`')?;
    let end = after_prefix.find('`')?;
    (!after_prefix[..end].is_empty()).then_some(&after_prefix[..end])
}

fn capture_plain_tool(source: &str) -> Option<&str> {
    let (_, after_prefix) = find_ascii_case_insensitive(source, "tool_use:")?;
    let after_prefix = after_prefix.trim_start();
    let end = after_prefix
        .char_indices()
        .find_map(|(index, character)| {
            (!character.is_ascii_alphanumeric()
                && !matches!(character, '.' | '_' | ':' | '/' | '-'))
            .then_some(index)
        })
        .unwrap_or(after_prefix.len());
    (!after_prefix[..end].is_empty()).then_some(&after_prefix[..end])
}

fn capture_token_after<'a>(source: &'a str, prefix: &str) -> Option<&'a str> {
    let (_, after_prefix) = find_ascii_case_insensitive(source, prefix)?;
    let after_prefix = after_prefix.trim_start();
    let end = after_prefix
        .char_indices()
        .find_map(|(index, character)| (!is_token_character(character)).then_some(index))
        .unwrap_or(after_prefix.len());
    (!after_prefix[..end].is_empty()).then_some(&after_prefix[..end])
}

fn capture_until_separator<'a>(source: &'a str, prefix: &str) -> Option<&'a str> {
    let (_, after_prefix) = find_ascii_case_insensitive(source, prefix)?;
    let end = after_prefix
        .char_indices()
        .find_map(|(index, character)| matches!(character, '—' | '·' | '\n').then_some(index))
        .unwrap_or(after_prefix.len());
    (!after_prefix[..end].trim().is_empty()).then_some(&after_prefix[..end])
}

fn is_token_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | ':' | '-')
}

fn contains_ascii_case_insensitive(source: &str, needle: &str) -> bool {
    find_ascii_case_insensitive(source, needle).is_some()
}

fn find_ascii_case_insensitive<'a>(source: &'a str, needle: &str) -> Option<(usize, &'a str)> {
    let source_lower = source.to_ascii_lowercase();
    let needle_lower = needle.to_ascii_lowercase();
    let start = source_lower.find(&needle_lower)?;
    Some((start, &source[start + needle.len()..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(
        reason: &str,
        display_reason: Option<&str>,
        state: CacheKeepaliveViewState,
    ) -> (String, CacheKeepaliveTriggerKind) {
        let descriptor = extract_trigger_descriptor(display_reason, reason, state);
        (descriptor.label, descriptor.kind)
    }

    #[test]
    fn blank_display_reason_falls_back_to_reason() {
        for blank in [Some(""), Some("   ")] {
            assert_eq!(
                descriptor(
                    "cached prefix too small (5000 tokens < minimum 10000)",
                    blank,
                    CacheKeepaliveViewState::NotTracked,
                ),
                (
                    "Prefix < min tokens".to_owned(),
                    CacheKeepaliveTriggerKind::State
                )
            );
            assert_eq!(
                descriptor(
                    "agent-in-turn (tool_use: `Bash`) — first renewal",
                    blank,
                    CacheKeepaliveViewState::NotTracked,
                ),
                ("Bash".to_owned(), CacheKeepaliveTriggerKind::Tool)
            );
        }
    }

    #[test]
    fn parses_all_trigger_fallbacks() {
        assert_eq!(
            descriptor(
                "agent-in-turn (tool_use: `write_file`)",
                None,
                CacheKeepaliveViewState::NotTracked
            )
            .0,
            "Write"
        );
        assert_eq!(
            descriptor("tool_use: Read", None, CacheKeepaliveViewState::NotTracked).0,
            "Read"
        );
        assert_eq!(
            descriptor(
                "stop_reason=end_turn",
                None,
                CacheKeepaliveViewState::NotTracked
            )
            .0,
            "stop_reason=end_turn"
        );
        assert_eq!(
            descriptor(
                "zero-token renewal unsupported: enabled thinking — details",
                None,
                CacheKeepaliveViewState::NotTracked
            )
            .0,
            "Unsupported: enabled thinking"
        );
        assert_eq!(
            descriptor(
                "snapshot too large",
                None,
                CacheKeepaliveViewState::NotTracked
            )
            .0,
            "Snapshot too large"
        );
        assert_eq!(
            descriptor("ambiguous turn", None, CacheKeepaliveViewState::NotTracked).0,
            "Ambiguous turn"
        );
        assert_eq!(
            descriptor("agent-in-turn", None, CacheKeepaliveViewState::NotTracked).0,
            "Agent in-turn"
        );
        assert_eq!(
            descriptor("anything else", None, CacheKeepaliveViewState::NotTracked).0,
            "Other"
        );
        assert_eq!(
            descriptor("anything else", None, CacheKeepaliveViewState::Renewed).0,
            "Agent in-turn"
        );
    }
}
