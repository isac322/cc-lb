use super::ValidationError;

pub fn validate_raw_toml(raw_toml: &str) -> Result<(), ValidationError> {
    let Ok(table) = raw_toml.parse::<toml::Table>() else {
        return Ok(());
    };

    if table.get("plugins").is_some() || table.get("principals").is_some() {
        return Err(ValidationError::new(
            "config",
            "v2 removed `plugins.authn_plugin` / `principals.*.quotas`; use `downstream_auth.mode` + `principals.*.default_limits` (sk-cclb-* API keys)",
        ));
    }

    if let Some(storage) = table.get("storage").and_then(|v| v.as_table()) {
        let has_kind = storage.contains_key("kind");
        let has_legacy_storage_path = storage.contains_key("storage_path");
        let has_legacy_aead = storage.contains_key("oauth_aead_key_env");
        if has_kind && (has_legacy_storage_path || has_legacy_aead) {
            let mut keys = Vec::new();
            if has_legacy_storage_path {
                keys.push("storage_path");
            }
            if has_legacy_aead {
                keys.push("oauth_aead_key_env");
            }
            return Err(ValidationError::new(
                "storage",
                format!(
                    "conflicting [storage] keys: `kind` cannot be mixed with legacy [{}]",
                    keys.join(", ")
                ),
            ));
        }
    }

    if let Some(field) = legacy_none_mode_upstream_credential_ref_field(&table) {
        return Err(ValidationError::new(
            field,
            "upstream_credential_ref was removed; use principal.allowed_upstreams and router selection",
        ));
    }

    Ok(())
}

fn legacy_none_mode_upstream_credential_ref_field(table: &toml::Table) -> Option<&'static str> {
    if table
        .get("downstream_auth")
        .and_then(|value| value.as_table())
        .and_then(|downstream_auth| downstream_auth.get("none_mode"))
        .and_then(|value| value.as_table())
        .is_some_and(|none_mode| none_mode.contains_key("upstream_credential_ref"))
    {
        return Some("downstream_auth.none_mode.upstream_credential_ref");
    }

    if table
        .get("none_mode")
        .and_then(|value| value.as_table())
        .is_some_and(|none_mode| none_mode.contains_key("upstream_credential_ref"))
    {
        return Some("none_mode.upstream_credential_ref");
    }

    None
}

pub fn migrate_legacy_storage_toml(raw_toml: &str) -> Result<String, ValidationError> {
    let Ok(mut root) = raw_toml.parse::<toml::Table>() else {
        return Ok(raw_toml.to_owned());
    };

    let mut legacy_storage_path: Option<toml::Value> = None;
    let mut legacy_aead_env: Option<toml::Value> = None;
    let mut storage_was_legacy_only = false;

    if let Some(storage) = root.get_mut("storage").and_then(|v| v.as_table_mut()) {
        let had_legacy =
            storage.contains_key("storage_path") || storage.contains_key("oauth_aead_key_env");
        let had_kind = storage.contains_key("kind");
        legacy_storage_path = storage.remove("storage_path");
        legacy_aead_env = storage.remove("oauth_aead_key_env");
        if had_legacy && !had_kind {
            storage_was_legacy_only = true;
        }
    }

    if storage_was_legacy_only
        && let Some(storage) = root.get_mut("storage").and_then(|v| v.as_table_mut())
    {
        storage.insert("kind".to_owned(), toml::Value::String("sqlite".to_owned()));
        if let Some(path) = legacy_storage_path.take() {
            storage.insert("path".to_owned(), path);
        }
    }

    if let Some(env) = legacy_aead_env {
        let aead = root
            .entry("aead".to_owned())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        if let Some(aead_table) = aead.as_table_mut()
            && !aead_table.contains_key("key_env")
        {
            aead_table.insert("key_env".to_owned(), env);
        }
    }

    toml::to_string(&root).map_err(|err| {
        ValidationError::new(
            "storage",
            format!("failed to migrate legacy [storage] block: {err}"),
        )
    })
}
