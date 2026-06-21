use cc_lb_bdd_tests::HttpResponse;
use cc_lb_storage_api::AuditEntry;
use serde_json::json;
use uuid::Uuid;

pub(crate) fn audit_has_action(entries: &[AuditEntry], action: &str) -> bool {
    let snake_action = action_snake_case(action);
    entries.iter().any(|entry| {
        entry.kind.as_deref() == Some(action)
            || entry
                .admin_action
                .as_deref()
                .is_some_and(|seen| seen.contains(action) || seen.contains(&snake_action))
    })
}

pub(crate) fn principal_id_from(response: &HttpResponse) -> Option<Uuid> {
    response
        .body_json()
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
}

pub(crate) fn principal_name(marker: &str) -> String {
    format!(
        "w1-{}-{}",
        marker.to_ascii_lowercase().replace('.', "-"),
        Uuid::new_v4().simple()
    )
}

pub(crate) fn message_body(marker: &str) -> serde_json::Value {
    json!({
        "model": "claude-sonnet-bdd",
        "max_tokens": 8,
        "messages": [{"role": "user", "content": format!("W1 probe {marker}")}]
    })
}

fn action_snake_case(action: &str) -> String {
    let mut converted = String::new();
    for (index, ch) in action.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                converted.push('_');
            }
            converted.push(ch.to_ascii_lowercase());
        } else {
            converted.push(ch);
        }
    }
    converted
}
