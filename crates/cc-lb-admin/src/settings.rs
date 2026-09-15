use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use cc_lb_config::{
    ADMIN_AUTH_PROVIDERS_JSON_ENV, Config, ConfigEnvOverride, ConfigError, ConfigOverrides,
    DEFAULT_SQLITE_PATH, STORAGE_URL_REDACTION_SENTINEL, StorageConfig,
};
use cc_lb_storage_api::{ConfigDraftState, HistoryEntry, Storage, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use thiserror::Error;

use crate::{AdminState, StartupConfigOverrides};

const INVALID_DRAFT_TTL_SECS: u64 = 24 * 60 * 60;
const DATA_DIR_ENV: &str = "CC_LB_DATA_DIR";

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("storage unavailable")]
    StorageUnavailable,
    #[error(transparent)]
    Storage(StorageError),
    #[error("stale draft revision")]
    StaleDraftRevision { current: u64 },
    #[error("draft revision has not been validated")]
    DraftNotValidated,
    #[error("config file changed")]
    FileChanged { current_fingerprint: Option<String> },
    #[error("config file is not writable: {0}")]
    FileNotWritable(String),
    #[error("config path is not configured")]
    ConfigPathMissing,
    #[error("saving would remove or change the active admin authentication provider")]
    SelfLockoutConfirmationRequired,
    #[error("configuration validation failed")]
    Validation(Box<ConfigValidationReport>),
    #[error("config I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("schema serialization failed: {0}")]
    Schema(#[from] serde_json::Error),
}

impl From<StorageError> for SettingsError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigFileMode {
    Writable,
    ReadOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigFileInfo {
    pub path: String,
    pub exists: bool,
    pub mode: ConfigFileMode,
    pub reason: Option<String>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigOverrideInfo {
    pub path: String,
    pub source: String,
    pub name: String,
    pub sensitive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_value: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigValidationSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValidationIssue {
    pub path: String,
    pub code: String,
    pub message: String,
    pub severity: ConfigValidationSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValidationResult {
    pub valid: bool,
    pub issues: Vec<ConfigValidationIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValidationReport {
    pub revision: u64,
    pub file: ConfigValidationResult,
    pub effective: ConfigValidationResult,
    pub filesystem: Vec<ConfigValidationIssue>,
    pub overrides: Vec<ConfigValidationIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigEditorResponse {
    pub schema: Value,
    pub default_config: Value,
    pub file_config: Value,
    pub effective_config: Value,
    pub draft: Option<Value>,
    pub revision: u64,
    pub last_validated_revision: Option<u64>,
    pub last_validation: Option<Value>,
    pub saved_at_unix_secs: Option<u64>,
    pub file: ConfigFileInfo,
    pub overrides: Vec<ConfigOverrideInfo>,
    pub restart_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDraftResponse {
    pub draft: Option<Value>,
    pub revision: u64,
    pub last_validated_revision: Option<u64>,
    pub last_validation: Option<Value>,
    pub saved_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutConfigDraftRequest {
    pub draft: Value,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutConfigDraftResponse {
    pub revision: u64,
    pub saved_at_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateConfigDraftRequest {
    pub expected_revision: u64,
    #[serde(default)]
    pub storage_url_replacement: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaveConfigFileRequest {
    pub expected_revision: u64,
    pub expected_fingerprint: Option<String>,
    #[serde(default)]
    pub storage_url_replacement: Option<String>,
    #[serde(default)]
    pub confirm_self_lockout: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadConfigDraftRequest {
    pub expected_revision: u64,
    #[serde(default)]
    pub storage_url_replacement: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaveConfigFileResponse {
    pub revision: u64,
    pub saved_at_unix_secs: u64,
    pub restart_required: bool,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigHistoryResponse {
    pub entries: Vec<ConfigHistoryItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigHistoryItem {
    pub revision: u64,
    pub saved_at_unix_secs: u64,
}

struct FileSnapshot {
    bytes: Option<Vec<u8>>,
    info: ConfigFileInfo,
}

struct MaterializedDraft {
    bytes: Vec<u8>,
    file_json: Value,
    report: ConfigValidationReport,
}

pub async fn editor_response(state: &AdminState) -> Result<ConfigEditorResponse, SettingsError> {
    let storage = config_storage(state)?;
    let snapshot = file_snapshot(state.config_path.as_deref())?;
    let raw_toml = snapshot_text(&snapshot)?;
    let mut file_config = partial_file_json(raw_toml)?;
    redact_storage_url(&mut file_config);

    let effective = effective_config_from_state(state);
    let mut effective_config = serde_json::to_value(&effective)?;
    redact_storage_url(&mut effective_config);
    let mut default_config = serde_json::to_value(Config::default())?;
    redact_storage_url(&mut default_config);

    let draft_state = get_unexpired_draft(storage, &*state.clock).await?;
    let mut draft = draft_state.draft;
    if let Some(value) = &mut draft {
        redact_storage_url(value);
    }
    let overrides = override_provenance(&effective_config, &state.startup_config_overrides);

    Ok(ConfigEditorResponse {
        schema: serde_json::to_value(Config::json_schema())?,
        default_config,
        file_config,
        effective_config,
        draft,
        revision: draft_state.revision,
        last_validated_revision: draft_state.last_validated_revision,
        last_validation: draft_state.last_validation,
        saved_at_unix_secs: draft_state.saved_at_unix_secs,
        file: snapshot.info,
        overrides,
        restart_required: true,
    })
}

pub async fn get_draft(
    storage: &dyn Storage,
    clock: &dyn cc_lb_clock::Clock,
) -> Result<ConfigDraftResponse, SettingsError> {
    draft_response(get_unexpired_draft(storage, clock).await?)
}

pub async fn put_draft(
    storage: &dyn Storage,
    request: PutConfigDraftRequest,
    saved_at_unix_secs: u64,
) -> Result<PutConfigDraftResponse, SettingsError> {
    let current = storage.get_config_draft().await?;
    if current.revision != request.expected_revision {
        return Err(SettingsError::StaleDraftRevision {
            current: current.revision,
        });
    }
    let mut draft = request.draft;
    normalize_unset(&mut draft);
    redact_storage_url(&mut draft);
    let state = ConfigDraftState {
        draft: Some(draft),
        revision: 0,
        last_validated_revision: None,
        last_validation: None,
        saved_at_unix_secs: Some(saved_at_unix_secs),
    };
    let revision = storage
        .put_config_draft(state, request.expected_revision)
        .await?;
    Ok(PutConfigDraftResponse {
        revision,
        saved_at_unix_secs,
    })
}

pub async fn validate_draft(
    state: &AdminState,
    request: ValidateConfigDraftRequest,
) -> Result<ConfigValidationReport, SettingsError> {
    let storage = config_storage(state)?;
    let draft_state = storage.get_config_draft().await?;
    ensure_revision(&draft_state, request.expected_revision)?;
    let draft = draft_state
        .draft
        .ok_or_else(|| draft_missing_report(request.expected_revision))?;
    let materialized = materialize_and_validate(
        state,
        draft,
        request.expected_revision,
        request.storage_url_replacement.as_deref(),
    )?;
    let report = materialized.report;
    let valid = report_is_valid(&report);
    let validation = serde_json::to_value(&report)?;
    storage
        .set_config_validation(request.expected_revision, valid, validation)
        .await?;
    Ok(report)
}

pub async fn save_config_file(
    state: &AdminState,
    active_provider_id: &str,
    request: SaveConfigFileRequest,
    saved_at_unix_secs: u64,
) -> Result<SaveConfigFileResponse, SettingsError> {
    let storage = config_storage(state)?;
    let draft_state = storage.get_config_draft().await?;
    ensure_revision(&draft_state, request.expected_revision)?;
    if draft_state.last_validated_revision != Some(request.expected_revision) {
        return Err(SettingsError::DraftNotValidated);
    }
    let draft = draft_state
        .draft
        .ok_or_else(|| draft_missing_report(request.expected_revision))?;
    let path = state
        .config_path
        .as_deref()
        .ok_or(SettingsError::ConfigPathMissing)?;
    let before = file_snapshot(Some(path))?;
    ensure_expected_fingerprint(&before.info, request.expected_fingerprint.as_deref())?;
    if before.info.mode != ConfigFileMode::Writable {
        return Err(SettingsError::FileNotWritable(
            before
                .info
                .reason
                .unwrap_or_else(|| "atomic replacement is unavailable".to_owned()),
        ));
    }

    let materialized = materialize_and_validate(
        state,
        draft,
        request.expected_revision,
        request.storage_url_replacement.as_deref(),
    )?;
    if !report_is_valid(&materialized.report) {
        return Err(SettingsError::Validation(Box::new(materialized.report)));
    }
    if !request.confirm_self_lockout
        && active_provider_changed(
            before.bytes.as_deref(),
            &materialized.file_json,
            active_provider_id,
        )?
    {
        return Err(SettingsError::SelfLockoutConfirmationRequired);
    }

    let fingerprint = fingerprint_bytes(&materialized.bytes);
    atomic_replace(
        path,
        &materialized.bytes,
        request.expected_fingerprint.as_deref(),
    )?;
    let revision = match storage
        .put_config_draft(ConfigDraftState::default(), request.expected_revision)
        .await
    {
        Ok(revision) => {
            if let Err(error) = storage
                .append_config_history(revision, saved_at_unix_secs)
                .await
            {
                tracing::error!(
                    %error,
                    revision,
                    "config file was saved but config history metadata could not be recorded"
                );
            }
            revision
        }
        Err(error) => {
            tracing::error!(
                %error,
                revision = request.expected_revision,
                "config file was saved but the validated draft could not be cleared"
            );
            request.expected_revision
        }
    };

    Ok(SaveConfigFileResponse {
        revision,
        saved_at_unix_secs,
        restart_required: true,
        fingerprint,
    })
}

pub async fn download_config_draft(
    state: &AdminState,
    request: DownloadConfigDraftRequest,
) -> Result<Vec<u8>, SettingsError> {
    let storage = config_storage(state)?;
    let draft_state = storage.get_config_draft().await?;
    ensure_revision(&draft_state, request.expected_revision)?;
    if !draft_was_validated_for_download(&draft_state, request.expected_revision) {
        return Err(SettingsError::DraftNotValidated);
    }
    let draft = draft_state
        .draft
        .ok_or_else(|| draft_missing_report(request.expected_revision))?;
    let materialized = materialize_and_validate(
        state,
        draft,
        request.expected_revision,
        request.storage_url_replacement.as_deref(),
    )?;
    if !materialized.report.file.valid {
        return Err(SettingsError::Validation(Box::new(materialized.report)));
    }
    Ok(materialized.bytes)
}

pub async fn list_history(
    storage: &dyn Storage,
    limit: usize,
) -> Result<ConfigHistoryResponse, SettingsError> {
    let entries = storage
        .list_config_history(limit.min(100))
        .await?
        .into_iter()
        .map(history_item)
        .collect();
    Ok(ConfigHistoryResponse { entries })
}

fn config_storage(state: &AdminState) -> Result<&dyn Storage, SettingsError> {
    state
        .storage
        .as_deref()
        .ok_or(SettingsError::StorageUnavailable)
}

async fn get_unexpired_draft(
    storage: &dyn Storage,
    clock: &dyn cc_lb_clock::Clock,
) -> Result<ConfigDraftState, SettingsError> {
    let state = storage.get_config_draft().await?;
    if invalid_draft_expired(&state, cc_lb_clock::unix_secs(clock.now())) {
        storage
            .put_config_draft(ConfigDraftState::default(), state.revision)
            .await?;
        return Ok(storage.get_config_draft().await?);
    }
    Ok(state)
}

fn draft_response(mut state: ConfigDraftState) -> Result<ConfigDraftResponse, SettingsError> {
    if let Some(draft) = &mut state.draft {
        redact_storage_url(draft);
    }
    Ok(ConfigDraftResponse {
        draft: state.draft,
        revision: state.revision,
        last_validated_revision: state.last_validated_revision,
        last_validation: state.last_validation,
        saved_at_unix_secs: state.saved_at_unix_secs,
    })
}

fn invalid_draft_expired(state: &ConfigDraftState, now_unix_secs: u64) -> bool {
    state.draft.is_some()
        && state
            .last_validation
            .as_ref()
            .is_some_and(|validation| !stored_validation_is_valid(validation))
        && state
            .saved_at_unix_secs
            .is_some_and(|saved_at| now_unix_secs.saturating_sub(saved_at) > INVALID_DRAFT_TTL_SECS)
}

fn stored_validation_is_valid(validation: &Value) -> bool {
    serde_json::from_value::<ConfigValidationReport>(validation.clone())
        .is_ok_and(|report| report_is_valid(&report))
}

fn ensure_revision(state: &ConfigDraftState, expected: u64) -> Result<(), SettingsError> {
    if state.revision == expected {
        Ok(())
    } else {
        Err(SettingsError::StaleDraftRevision {
            current: state.revision,
        })
    }
}

fn draft_was_validated_for_download(state: &ConfigDraftState, revision: u64) -> bool {
    state.last_validated_revision == Some(revision)
        || state.last_validation.as_ref().is_some_and(|validation| {
            validation.get("revision").and_then(Value::as_u64) == Some(revision)
                && validation
                    .get("file")
                    .and_then(|file| file.get("valid"))
                    .and_then(Value::as_bool)
                    == Some(true)
        })
}
fn materialize_and_validate(
    state: &AdminState,
    mut draft: Value,
    revision: u64,
    storage_url_replacement: Option<&str>,
) -> Result<MaterializedDraft, SettingsError> {
    normalize_unset(&mut draft);
    let snapshot = file_snapshot(state.config_path.as_deref())?;
    let current_file = partial_file_json(snapshot_text(&snapshot)?)?;
    if let Err(issue) = restore_storage_url(&mut draft, &current_file, storage_url_replacement) {
        let report = ConfigValidationReport {
            revision,
            file: ConfigValidationResult {
                valid: false,
                issues: vec![issue],
            },
            effective: ConfigValidationResult {
                valid: false,
                issues: Vec::new(),
            },
            filesystem: filesystem_capability_issues(&snapshot.info),
            overrides: Vec::new(),
        };
        return Ok(MaterializedDraft {
            bytes: Vec::new(),
            file_json: json!({}),
            report,
        });
    }

    let draft_recurring_job_issues = unknown_recurring_job_issues(&draft);
    let toml = Config::partial_json_toml(draft)?;
    let file_json = partial_file_json(Some(&toml))?;
    let overrides = override_provenance_from_state(state);
    let mut file_validation = validate_file_config(&toml, &overrides);
    normalize_unknown_recurring_job_issues(&mut file_validation, &draft_recurring_job_issues);
    let (mut effective_validation, effective_config) =
        validate_effective_config(&toml, &state.startup_config_overrides);
    normalize_unknown_recurring_job_issues(&mut effective_validation, &draft_recurring_job_issues);
    let mut filesystem = filesystem_capability_issues(&snapshot.info);
    if let Some(config) = effective_config.as_ref() {
        filesystem.extend(config_filesystem_issues(config));
    }
    let report = ConfigValidationReport {
        revision,
        file: file_validation,
        effective: effective_validation,
        filesystem,
        overrides: override_dependency_issues(&overrides),
    };
    Ok(MaterializedDraft {
        bytes: toml.into_bytes(),
        file_json,
        report,
    })
}

fn validate_file_config(toml: &str, overrides: &[ConfigOverrideInfo]) -> ConfigValidationResult {
    match Config::from_stored_toml(toml) {
        Ok(config) => validate_editor_config(&config),
        Err(error) => {
            let mut issues = config_error_issues(error);
            for issue in &mut issues {
                if issue.code == "missing_field"
                    && overrides.iter().any(|source| source.path == issue.path)
                {
                    issue.code = "env_supplied_required_value".to_owned();
                    issue.message = format!(
                        "{} is supplied by an active override and is not stored in the file",
                        issue.path
                    );
                    issue.severity = ConfigValidationSeverity::Warning;
                }
            }
            ConfigValidationResult {
                valid: !issues
                    .iter()
                    .any(|issue| issue.severity == ConfigValidationSeverity::Error),
                issues,
            }
        }
    }
}

fn validate_effective_config(
    toml: &str,
    startup: &StartupConfigOverrides,
) -> (ConfigValidationResult, Option<Config>) {
    match Config::from_stored_toml_with_env(toml, ConfigOverrides::default()) {
        Ok(mut config) => {
            apply_startup_overrides(&mut config, startup);
            let result = validate_editor_config(&config);
            let keep = result.valid.then_some(config);
            (result, keep)
        }
        Err(error) => (
            ConfigValidationResult {
                valid: false,
                issues: config_error_issues(error),
            },
            None,
        ),
    }
}

fn unknown_recurring_job_issues(draft: &Value) -> Vec<ConfigValidationIssue> {
    let Some(jobs) = draft
        .get("scheduler")
        .and_then(|scheduler| scheduler.get("recurring_jobs"))
        .and_then(Value::as_object)
    else {
        return Vec::new();
    };
    let defaults = Config::default();
    jobs.keys()
        .filter(|job| !defaults.scheduler.recurring_jobs.contains_key(*job))
        .map(|job| {
            error_issue(
                format!("scheduler.recurring_jobs.{job}"),
                "unknown_recurring_job",
                format!("unknown recurring scheduler job `{job}`"),
            )
        })
        .collect()
}

fn normalize_unknown_recurring_job_issues(
    validation: &mut ConfigValidationResult,
    unknown_issues: &[ConfigValidationIssue],
) {
    if unknown_issues.is_empty() {
        return;
    }
    validation.issues.retain(|issue| {
        issue.code == "unknown_recurring_job"
            || !issue.message.contains("unknown scheduler recurring job")
    });
    for unknown_issue in unknown_issues {
        if !validation
            .issues
            .iter()
            .any(|issue| issue.path == unknown_issue.path && issue.code == unknown_issue.code)
        {
            validation.issues.push(unknown_issue.clone());
        }
    }
    validation.valid = false;
}

fn validate_editor_config(config: &Config) -> ConfigValidationResult {
    let mut issues = config
        .validate_without_filesystem()
        .err()
        .map(config_error_issues)
        .unwrap_or_default();
    let known_jobs = Config::default().scheduler.recurring_jobs;
    for job in config.scheduler.recurring_jobs.keys() {
        if !known_jobs.contains_key(job) {
            issues.push(error_issue(
                format!("scheduler.recurring_jobs.{job}"),
                "unknown_recurring_job",
                format!("unknown recurring scheduler job `{job}`"),
            ));
        }
    }
    ConfigValidationResult {
        valid: !issues
            .iter()
            .any(|issue| issue.severity == ConfigValidationSeverity::Error),
        issues,
    }
}

fn config_error_issues(error: ConfigError) -> Vec<ConfigValidationIssue> {
    match error {
        ConfigError::Figment(error) => (*error)
            .into_iter()
            .map(|error| {
                let mut path = error.path.join(".");
                let code = match &error.kind {
                    figment::error::Kind::MissingField(field) => {
                        if !path.is_empty() {
                            path.push('.');
                        }
                        path.push_str(field);
                        "missing_field"
                    }
                    figment::error::Kind::UnknownField(field, _) => {
                        if !path.is_empty() {
                            path.push('.');
                        }
                        path.push_str(field);
                        "unknown_field"
                    }
                    figment::error::Kind::InvalidType(_, _) => "invalid_type",
                    figment::error::Kind::InvalidValue(_, _) => "invalid_value",
                    figment::error::Kind::UnknownVariant(_, _) => "unknown_variant",
                    _ => "invalid_config",
                };
                error_issue(path, code, error.kind.to_string())
            })
            .collect(),
        ConfigError::Validation(error) => {
            vec![error_issue(error.field, "validation_failed", error.message)]
        }
        ConfigError::InvalidPostgresUrl { message } => {
            vec![error_issue("storage.url", "invalid_url", message)]
        }
        ConfigError::InvalidAdminAuthProvidersEnv { message, .. } => vec![error_issue(
            "admin.auth.providers",
            "invalid_override",
            message,
        )],
        ConfigError::StatementTimeoutExceedsRequestTimeout { statement, request } => {
            vec![error_issue(
                "storage.pool.statement_timeout_secs",
                "cross_field_validation",
                format!(
                    "postgres statement timeout {statement}s must be less than request timeout {request}s"
                ),
            )]
        }
        ConfigError::TomlParse(error) => vec![error_issue("", "invalid_toml", error.to_string())],
        ConfigError::TomlSerialize(error) => {
            vec![error_issue("", "invalid_toml_value", error.to_string())]
        }
    }
}

fn error_issue(
    path: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ConfigValidationIssue {
    ConfigValidationIssue {
        path: path.into(),
        code: code.into(),
        message: message.into(),
        severity: ConfigValidationSeverity::Error,
    }
}

fn warning_issue(
    path: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ConfigValidationIssue {
    ConfigValidationIssue {
        path: path.into(),
        code: code.into(),
        message: message.into(),
        severity: ConfigValidationSeverity::Warning,
    }
}

fn report_is_valid(report: &ConfigValidationReport) -> bool {
    report.file.valid
        && report.effective.valid
        && !report
            .filesystem
            .iter()
            .any(|issue| issue.severity == ConfigValidationSeverity::Error)
}

fn draft_missing_report(revision: u64) -> SettingsError {
    SettingsError::Validation(Box::new(ConfigValidationReport {
        revision,
        file: ConfigValidationResult {
            valid: false,
            issues: vec![error_issue(
                "",
                "draft_missing",
                "no configuration draft exists",
            )],
        },
        effective: ConfigValidationResult {
            valid: false,
            issues: Vec::new(),
        },
        filesystem: Vec::new(),
        overrides: Vec::new(),
    }))
}

fn partial_file_json(raw_toml: Option<&str>) -> Result<Value, SettingsError> {
    match raw_toml {
        Some(raw) if !raw.trim().is_empty() => Ok(Config::partial_toml_json(raw)?),
        _ => Ok(json!({})),
    }
}

fn snapshot_text(snapshot: &FileSnapshot) -> Result<Option<&str>, SettingsError> {
    snapshot
        .bytes
        .as_deref()
        .map(|bytes| {
            std::str::from_utf8(bytes).map_err(|error| {
                SettingsError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
            })
        })
        .transpose()
}

fn normalize_unset(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.retain(|_, child| !child.is_null());
            for child in object.values_mut() {
                normalize_unset(child);
            }
        }
        Value::Array(values) => {
            values.retain(|child| !child.is_null());
            for child in values {
                normalize_unset(child);
            }
        }
        _ => {}
    }
}

fn storage_url_mut(value: &mut Value) -> Option<&mut Value> {
    value
        .as_object_mut()?
        .get_mut("storage")?
        .as_object_mut()?
        .get_mut("url")
}

fn storage_url(value: &Value) -> Option<&str> {
    value
        .as_object()?
        .get("storage")?
        .as_object()?
        .get("url")?
        .as_str()
}

fn redact_storage_url(value: &mut Value) {
    if let Some(url) = storage_url_mut(value) {
        *url = Value::String(STORAGE_URL_REDACTION_SENTINEL.to_owned());
    }
}

fn restore_storage_url(
    draft: &mut Value,
    current_file: &Value,
    replacement: Option<&str>,
) -> Result<(), ConfigValidationIssue> {
    if let Some(replacement) = replacement
        && let Some(storage) = draft
            .as_object_mut()
            .and_then(|draft| draft.get_mut("storage"))
            .and_then(Value::as_object_mut)
    {
        storage.insert("url".to_owned(), Value::String(replacement.to_owned()));
        return Ok(());
    }
    let Some(url) = storage_url_mut(draft) else {
        return Ok(());
    };
    if url.as_str() != Some(STORAGE_URL_REDACTION_SENTINEL) {
        return Ok(());
    }
    if let Some(original) = storage_url(current_file) {
        *url = Value::String(original.to_owned());
        return Ok(());
    }
    Err(error_issue(
        "storage.url",
        "secret_source_missing",
        "storage.url is marked unchanged, but the current file has no value to restore",
    ))
}

fn effective_config_from_state(state: &AdminState) -> Config {
    let mut effective = (*state.config.current_config()).clone();
    apply_startup_overrides(&mut effective, &state.startup_config_overrides);
    effective
}

fn override_provenance(
    effective_config: &Value,
    startup: &StartupConfigOverrides,
) -> Vec<ConfigOverrideInfo> {
    let mut overrides = Config::environment_overrides()
        .into_iter()
        .map(|source| generic_override(source, effective_config))
        .collect::<Vec<_>>();
    if std::env::var_os(ADMIN_AUTH_PROVIDERS_JSON_ENV).is_some() {
        overrides.push(ConfigOverrideInfo {
            path: "admin.auth.providers".to_owned(),
            source: "special_env".to_owned(),
            name: ADMIN_AUTH_PROVIDERS_JSON_ENV.to_owned(),
            sensitive: false,
            effective_value: value_at_path(effective_config, "admin.auth.providers").cloned(),
        });
    }
    if special_env_runtime_data_dir().is_some() {
        overrides.push(ConfigOverrideInfo {
            path: "runtime.data_dir".to_owned(),
            source: "special_env".to_owned(),
            name: DATA_DIR_ENV.to_owned(),
            sensitive: false,
            effective_value: value_at_path(effective_config, "runtime.data_dir").cloned(),
        });
    } else if startup.runtime_data_dir.is_some() {
        overrides.push(ConfigOverrideInfo {
            path: "runtime.data_dir".to_owned(),
            source: "cli".to_owned(),
            name: "--data-dir".to_owned(),
            sensitive: false,
            effective_value: value_at_path(effective_config, "runtime.data_dir").cloned(),
        });
    }
    overrides.sort_by(|left, right| left.path.cmp(&right.path).then(left.name.cmp(&right.name)));
    overrides.dedup_by(|left, right| left.path == right.path && left.name == right.name);
    overrides
}

fn override_provenance_from_state(state: &AdminState) -> Vec<ConfigOverrideInfo> {
    let mut effective =
        serde_json::to_value(effective_config_from_state(state)).unwrap_or(Value::Null);
    redact_storage_url(&mut effective);
    override_provenance(&effective, &state.startup_config_overrides)
}

fn generic_override(source: ConfigEnvOverride, effective_config: &Value) -> ConfigOverrideInfo {
    ConfigOverrideInfo {
        effective_value: (!source.sensitive)
            .then(|| value_at_path(effective_config, &source.path).cloned())
            .flatten(),
        path: source.path,
        source: "env".to_owned(),
        name: source.name,
        sensitive: source.sensitive,
    }
}

fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(value, |value, segment| {
        value
            .as_object()?
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(segment))
            .map(|(_, value)| value)
    })
}

fn override_dependency_issues(overrides: &[ConfigOverrideInfo]) -> Vec<ConfigValidationIssue> {
    overrides
        .iter()
        .map(|source| {
            warning_issue(
                source.path.clone(),
                "active_override",
                format!(
                    "effective value is supplied by {} {} and is not controlled by this file",
                    source.source, source.name
                ),
            )
        })
        .collect()
}

fn apply_startup_overrides(config: &mut Config, startup: &StartupConfigOverrides) {
    if let Some(data_dir) = &startup.runtime_data_dir {
        config.runtime.data_dir = Some(data_dir.clone());
    }
    if let Some(data_dir) = special_env_runtime_data_dir() {
        config.runtime.data_dir = Some(data_dir);
    }
}

fn special_env_runtime_data_dir() -> Option<PathBuf> {
    std::env::var(DATA_DIR_ENV).ok().map(PathBuf::from)
}

fn filesystem_capability_issues(info: &ConfigFileInfo) -> Vec<ConfigValidationIssue> {
    if info.mode == ConfigFileMode::Writable {
        Vec::new()
    } else {
        vec![error_issue(
            "$file",
            "config_file_not_writable",
            info.reason
                .clone()
                .unwrap_or_else(|| "config file cannot be atomically replaced".to_owned()),
        )]
    }
}

fn config_filesystem_issues(config: &Config) -> Vec<ConfigValidationIssue> {
    let mut issues = Vec::new();
    if let Some(tls) = &config.listener.tls {
        check_required_file(
            &mut issues,
            "listener.tls.cert_path",
            tls.cert_path.as_deref(),
        );
        check_required_file(
            &mut issues,
            "listener.tls.key_path",
            tls.key_path.as_deref(),
        );
    }
    if let StorageConfig::Sqlite { path } = &config.storage
        && !(path == Path::new(DEFAULT_SQLITE_PATH) && !path.exists())
    {
        check_writable_file_path(&mut issues, "storage.path", path);
    }
    issues
}

fn check_required_file(issues: &mut Vec<ConfigValidationIssue>, field: &str, path: Option<&Path>) {
    let Some(path) = path else {
        issues.push(error_issue(
            field,
            "missing_path",
            "required path is not configured",
        ));
        return;
    };
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => issues.push(error_issue(
            field,
            "not_a_file",
            format!("not a file: {}", path.display()),
        )),
        Err(error) => issues.push(error_issue(
            field,
            "file_missing",
            format!("cannot access {}: {error}", path.display()),
        )),
    }
}

fn check_writable_file_path(issues: &mut Vec<ConfigValidationIssue>, field: &str, path: &Path) {
    if path.exists() {
        match fs::metadata(path) {
            Ok(metadata) if !metadata.is_file() => issues.push(error_issue(
                field,
                "not_a_file",
                format!("not a file: {}", path.display()),
            )),
            Ok(metadata) if metadata.permissions().readonly() => issues.push(error_issue(
                field,
                "read_only",
                format!("file is not writable: {}", path.display()),
            )),
            Err(error) => issues.push(error_issue(
                field,
                "filesystem_error",
                format!("cannot inspect {}: {error}", path.display()),
            )),
            _ => {}
        }
    }
    let parent = parent_directory(path);
    match fs::metadata(parent) {
        Ok(metadata) if !metadata.is_dir() => issues.push(error_issue(
            field,
            "parent_not_directory",
            format!("parent path is not a directory: {}", parent.display()),
        )),
        Ok(metadata) if metadata.permissions().readonly() => issues.push(error_issue(
            field,
            "parent_read_only",
            format!("parent directory is not writable: {}", parent.display()),
        )),
        Err(error) => issues.push(error_issue(
            field,
            "parent_missing",
            format!(
                "cannot access parent directory {}: {error}",
                parent.display()
            ),
        )),
        _ => {}
    }
}

fn file_snapshot(path: Option<&Path>) -> Result<FileSnapshot, SettingsError> {
    let Some(path) = path else {
        return Ok(FileSnapshot {
            bytes: None,
            info: ConfigFileInfo {
                path: String::new(),
                exists: false,
                mode: ConfigFileMode::ReadOnly,
                reason: Some("config path is not configured".to_owned()),
                fingerprint: None,
            },
        });
    };
    match fs::read(path) {
        Ok(bytes) => {
            let metadata = fs::metadata(path)?;
            let capability = atomic_replace_capability(path, true, Some(&metadata));
            Ok(FileSnapshot {
                info: ConfigFileInfo {
                    path: path.display().to_string(),
                    exists: true,
                    mode: capability.0,
                    reason: capability.1,
                    fingerprint: Some(fingerprint_bytes(&bytes)),
                },
                bytes: Some(bytes),
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let capability = atomic_replace_capability(path, false, None);
            Ok(FileSnapshot {
                bytes: None,
                info: ConfigFileInfo {
                    path: path.display().to_string(),
                    exists: false,
                    mode: capability.0,
                    reason: capability.1,
                    fingerprint: None,
                },
            })
        }
        Err(error) => Err(SettingsError::Io(error)),
    }
}

fn atomic_replace_capability(
    path: &Path,
    exists: bool,
    metadata: Option<&fs::Metadata>,
) -> (ConfigFileMode, Option<String>) {
    if exists && !metadata.is_some_and(fs::Metadata::is_file) {
        return (
            ConfigFileMode::ReadOnly,
            Some("config path is not a regular file".to_owned()),
        );
    }
    let parent = parent_directory(path);
    match fs::metadata(parent) {
        Ok(metadata) if !metadata.is_dir() => (
            ConfigFileMode::ReadOnly,
            Some("config parent path is not a directory".to_owned()),
        ),
        Ok(metadata) if metadata.permissions().readonly() => (
            ConfigFileMode::ReadOnly,
            Some("config parent directory is read-only".to_owned()),
        ),
        Ok(_) => probe_parent_write(parent),
        Err(error) => (
            ConfigFileMode::ReadOnly,
            Some(format!("config parent directory is unavailable: {error}")),
        ),
    }
}

fn probe_parent_write(parent: &Path) -> (ConfigFileMode, Option<String>) {
    let probe = parent.join(format!(".cc-lb-write-probe-{}", uuid::Uuid::now_v7()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    match options.open(&probe) {
        Ok(file) => {
            drop(file);
            match fs::remove_file(&probe) {
                Ok(()) => (ConfigFileMode::Writable, None),
                Err(error) => (
                    ConfigFileMode::ReadOnly,
                    Some(format!("cannot clean up config write probe: {error}")),
                ),
            }
        }
        Err(error) => (
            ConfigFileMode::ReadOnly,
            Some(format!(
                "config parent does not support atomic replacement: {error}"
            )),
        ),
    }
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn fingerprint_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn file_fingerprint(path: &Path) -> Result<Option<String>, SettingsError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(fingerprint_bytes(&bytes))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(SettingsError::Io(error)),
    }
}

fn ensure_expected_fingerprint(
    info: &ConfigFileInfo,
    expected: Option<&str>,
) -> Result<(), SettingsError> {
    if info.fingerprint.as_deref() == expected {
        Ok(())
    } else {
        Err(SettingsError::FileChanged {
            current_fingerprint: info.fingerprint.clone(),
        })
    }
}

fn atomic_replace(
    path: &Path,
    bytes: &[u8],
    expected_fingerprint: Option<&str>,
) -> Result<(), SettingsError> {
    let parent = parent_directory(path);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cc-lb.toml");
    let temp_path: PathBuf = parent.join(format!(".{name}.{}.tmp", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut temp = options.open(&temp_path)?;
        temp.write_all(bytes)?;
        temp.flush()?;
        if let Ok(metadata) = fs::metadata(path) {
            temp.set_permissions(metadata.permissions())?;
        }
        temp.sync_all()?;
        let current = file_fingerprint(path)?;
        if current.as_deref() != expected_fingerprint {
            return Err(SettingsError::FileChanged {
                current_fingerprint: current,
            });
        }
        fs::rename(&temp_path, path)?;
        if let Ok(parent_file) = fs::File::open(parent) {
            let _ = parent_file.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn active_provider_changed(
    current_bytes: Option<&[u8]>,
    next: &Value,
    active_provider_id: &str,
) -> Result<bool, SettingsError> {
    let Some(current_bytes) = current_bytes else {
        return Ok(false);
    };
    let current_toml = std::str::from_utf8(current_bytes).map_err(|error| {
        SettingsError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    })?;
    let current = partial_file_json(Some(current_toml))?;
    let before = admin_auth_provider(&current, active_provider_id);
    let after = admin_auth_provider(next, active_provider_id);
    Ok(before.is_some() && before != after)
}

fn admin_auth_provider<'a>(config: &'a Value, provider_id: &str) -> Option<&'a Value> {
    value_at_path(config, "admin.auth.providers")?
        .as_array()?
        .iter()
        .find(|provider| provider.get("id").and_then(Value::as_str) == Some(provider_id))
}

fn history_item(entry: HistoryEntry) -> ConfigHistoryItem {
    ConfigHistoryItem {
        revision: entry.revision,
        saved_at_unix_secs: entry.applied_at_unix_secs,
    }
}
