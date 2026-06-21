pub(super) fn entity_job_type_from_idempotency_key(idempotency_key: &str) -> Option<&'static str> {
    let mut parts = idempotency_key.split(':');
    if parts.next()? != "entity" {
        return None;
    }
    let entity_kind = parts.next()?;
    parts.next()?;

    match entity_kind {
        "warmup" => Some("entity:warmup"),
        "oauth_refresh" => Some("entity:oauth_refresh"),
        "oauth_usage_poll" => Some("entity:oauth_usage_poll"),
        "anthropic_compat_refresh" => Some("entity:anthropic_compat_refresh"),
        "metadata_refresh" => Some("entity:metadata_refresh"),
        _other => None,
    }
}

pub(super) fn is_reconcile_upstream_entity_job_type(job_type: &str) -> bool {
    matches!(
        job_type,
        "entity:warmup" | "entity:oauth_refresh" | "entity:oauth_usage_poll"
    )
}
