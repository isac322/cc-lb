#![cfg(unix)]

use std::env;
use std::fs;
use std::io;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use cc_lb_core::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_core::api_keys::secret;
use cc_lb_storage_redb::{
    api_key_storage_key, AuditEntry, KeyStatus, Limit, LimitKind, OAuthCredentials,
    PrincipalKindLite, Storage, UpstreamKind, API_KEYS_V1, AUDIT_LOG_V1, CURRENT_SCHEMA_VERSION,
    KEY_INDEX_BY_HASH_V1, OAUTH_CREDENTIALS_V1,
};
use redb::{ReadableDatabase, ReadableTable};

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

const CHILD_CASE_ENV: &str = "CC_LB_CRASH_CHILD_CASE";
const DB_PATH_ENV: &str = "CC_LB_CRASH_DB_PATH";
const CONTROL_DIR_ENV: &str = "CC_LB_CRASH_CONTROL_DIR";
const ITERATION_ENV: &str = "CC_LB_CRASH_ITERATION";
const TOTAL_ITERATIONS_ENV: &str = "CC_LB_CRASH_ITERATIONS";

const DEFAULT_ITERATIONS: usize = 100;
const COMMITTED_ROWS: usize = 32;
const PENDING_ROWS: usize = 10_000;
const MASTER_KEY: [u8; 32] = [48; 32];
const ANTHROPIC_OAUTH_PROVIDER: &str = "anthropic_oauth";
const STARTED_MARKER: &str = "pending_tx_started";
const CHILD_LINGER: Duration = Duration::from_secs(60);
const KEY_CREATE_SENTINEL_ENV: &str = "CC_LB_CRASH_SENTINEL_KEY_CREATE";
const KEY_REVOKE_SENTINEL_ENV: &str = "CC_LB_CRASH_SENTINEL_KEY_REVOKE";
const PRICE_CATALOG_SENTINEL_ENV: &str = "CC_LB_CRASH_SENTINEL_PRICE_CATALOG";
const REVOKE_KEY_ID_FILE: &str = "revoke_key_id";
const REVOKE_INDEX_HASH_FILE: &str = "revoke_index_hash";
const PRICE_CACHE_FILE: &str = "litellm-cache.json";
const OLD_PRICE_FETCHED_AT_MS: u64 = 1_765_000_100;
const NEW_PRICE_FETCHED_AT_MS: u64 = 1_765_000_200;
const OLD_PRICE_JSON: &[u8] =
    br#"{"claude-old":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000002}}"#;
const NEW_PRICE_JSON: &[u8] =
    br#"{"claude-new":{"input_cost_per_token":0.000003,"output_cost_per_token":0.000004}}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum CrashCase {
    OAuthAead,
    Audit,
    KeyIssuance,
    KeyRevoke,
    PriceCatalog,
}

impl CrashCase {
    fn as_str(self) -> &'static str {
        match self {
            Self::OAuthAead => "oauth_aead",
            Self::Audit => "audit",
            Self::KeyIssuance => "key_issuance",
            Self::KeyRevoke => "key_revoke",
            Self::PriceCatalog => "price_catalog",
        }
    }
}

#[derive(Debug)]
struct VerificationReport {
    schema_version: u32,
    committed_rows: usize,
    pending_rows: usize,
}

pub fn run_child_if_requested(case: CrashCase) -> Result<bool, Box<dyn std::error::Error>> {
    let Ok(child_case) = env::var(CHILD_CASE_ENV) else {
        return Ok(false);
    };
    if child_case != case.as_str() {
        return Ok(false);
    }

    let db_path = PathBuf::from(required_env(DB_PATH_ENV)?);
    let control_dir = PathBuf::from(required_env(CONTROL_DIR_ENV)?);
    let iteration = required_env(ITERATION_ENV)?.parse::<usize>()?;

    match case {
        CrashCase::OAuthAead => child_oauth_aead(&db_path, &control_dir, iteration)?,
        CrashCase::Audit => child_audit(&db_path, &control_dir, iteration)?,
        CrashCase::KeyIssuance => child_key_issuance(&db_path, iteration)?,
        CrashCase::KeyRevoke => child_key_revoke(&db_path, &control_dir, iteration)?,
        CrashCase::PriceCatalog => child_price_catalog(&db_path)?,
    }
    Ok(true)
}

pub fn run_parent(case: CrashCase, test_name: &str) -> TestResult {
    let iterations = iteration_count()?;
    println!(
        "start scenario={} iterations={} committed_rows={} pending_tx_rows={}",
        case.as_str(),
        iterations,
        COMMITTED_ROWS,
        PENDING_ROWS
    );

    for iteration in 0..iterations {
        run_iteration(case, test_name, iteration)?;
    }

    Ok(())
}

fn run_iteration(case: CrashCase, test_name: &str, iteration: usize) -> TestResult {
    let tempdir = tempfile::Builder::new()
        .prefix("cc-lb-task-48-")
        .tempdir()?;
    let db_path = tempdir.path().join("storage.redb");
    let control_dir = tempdir.path().join("control");
    fs::create_dir(&control_dir)?;

    let mut child = spawn_child(case, test_name, iteration, &db_path, &control_dir)?;
    let child_pid = child.id();

    if let Err(error) = wait_for_marker(&control_dir, STARTED_MARKER) {
        cleanup_child(&mut child);
        return Err(error);
    }

    thread::sleep(Duration::from_millis(5));
    child.kill()?;
    let status = child.wait()?;
    if status.signal() != Some(9) {
        return Err(io_error(format!(
            "child process was not terminated by SIGKILL: {status}"
        ))
        .into());
    }

    let report = match case {
        CrashCase::OAuthAead => verify_oauth_aead(&db_path, iteration)?,
        CrashCase::Audit => verify_audit(&db_path, iteration)?,
        CrashCase::KeyIssuance => verify_key_issuance(&db_path, iteration)?,
        CrashCase::KeyRevoke => verify_key_revoke(&db_path, &control_dir, iteration)?,
        CrashCase::PriceCatalog => verify_price_catalog(&db_path)?,
    };

    println!(
        "PASS scenario={} iteration={} child_pid={} child_killed=SIGKILL db_reopened=true verification_succeeded=true schema_version={} committed_entries={} pending_entries={}",
        case.as_str(),
        iteration,
        child_pid,
        report.schema_version,
        report.committed_rows,
        report.pending_rows
    );

    Ok(())
}

fn spawn_child(
    case: CrashCase,
    test_name: &str,
    iteration: usize,
    db_path: &Path,
    control_dir: &Path,
) -> Result<Child, Box<dyn std::error::Error>> {
    let mut command = Command::new(env::current_exe()?);
    command
        .env(CHILD_CASE_ENV, case.as_str())
        .env(DB_PATH_ENV, db_path)
        .env(CONTROL_DIR_ENV, control_dir)
        .env(ITERATION_ENV, iteration.to_string())
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture");
    match case {
        CrashCase::KeyIssuance => {
            command.env(KEY_CREATE_SENTINEL_ENV, "1");
        }
        CrashCase::KeyRevoke => {
            command.env(KEY_REVOKE_SENTINEL_ENV, "1");
        }
        CrashCase::PriceCatalog => {
            command.env(PRICE_CATALOG_SENTINEL_ENV, "1");
        }
        CrashCase::OAuthAead | CrashCase::Audit => {}
    }
    Ok(command.spawn()?)
}

fn cleanup_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn child_oauth_aead(path: &Path, control_dir: &Path, iteration: usize) -> TestResult {
    {
        let storage = Storage::open(path, MASTER_KEY)?;
        storage.set_killswitch_enabled(true)?;
        for index in 0..COMMITTED_ROWS {
            let principal_id = oauth_committed_principal(iteration, index);
            storage.put_oauth(
                &principal_id,
                ANTHROPIC_OAUTH_PROVIDER,
                &oauth_credentials(iteration, index),
            )?;
        }
    }

    let db = redb::Database::create(path)?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
        for index in 0..PENDING_ROWS {
            let key = oauth_pending_key(iteration, index);
            let value = [1_u8, 2, 3, 4, 5, 6, 7, 8];
            table.insert(key.as_slice(), value.as_slice())?;
            signal_after_first_pending_row(control_dir, index)?;
        }
    }
    sleep_before_unreachable_commit();
    write_txn.commit()?;
    Ok(())
}

fn child_audit(path: &Path, control_dir: &Path, iteration: usize) -> TestResult {
    {
        let storage = Storage::open(path, MASTER_KEY)?;
        storage.set_killswitch_enabled(true)?;
        for index in 0..COMMITTED_ROWS {
            storage.append_audit(&audit_entry(iteration, index, false))?;
        }
    }

    let db = redb::Database::create(path)?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(AUDIT_LOG_V1)?;
        for index in 0..PENDING_ROWS {
            let key = audit_pending_key(iteration, index).to_be_bytes();
            let payload = serde_json::to_vec(&audit_entry(iteration, index, true))?;
            table.insert(key.as_slice(), payload.as_slice())?;
            signal_after_first_pending_row(control_dir, index)?;
        }
    }
    sleep_before_unreachable_commit();
    write_txn.commit()?;
    Ok(())
}

fn child_key_issuance(path: &Path, iteration: usize) -> TestResult {
    let storage = Arc::new(Storage::open(path, MASTER_KEY)?);
    storage.set_killswitch_enabled(true)?;
    let key_store = KeyStore::new(storage);
    let _ = key_store.create(
        &key_issuance_principal(iteration),
        create_params(iteration, "issued during crash"),
    )?;
    Ok(())
}

fn child_key_revoke(path: &Path, control_dir: &Path, iteration: usize) -> TestResult {
    let storage = Arc::new(Storage::open(path, MASTER_KEY)?);
    storage.set_killswitch_enabled(true)?;
    let key_store = KeyStore::new(storage);
    let (_record, plaintext) = key_store.create(
        &key_revoke_principal(iteration),
        create_params(iteration, "revoked during crash"),
    )?;
    let (key_id, secret_b64_bytes) = secret::parse(plaintext.expose())?;
    let index_hash = secret::compute_index_hash(&secret_b64_bytes);

    fs::write(control_dir.join(REVOKE_KEY_ID_FILE), key_id.as_bytes())?;
    fs::write(control_dir.join(REVOKE_INDEX_HASH_FILE), index_hash)?;

    key_store.revoke(&key_revoke_principal(iteration), &key_id)?;
    Ok(())
}

fn child_price_catalog(path: &Path) -> TestResult {
    unsafe { env::remove_var(PRICE_CATALOG_SENTINEL_ENV) };
    let storage = Storage::open(path, MASTER_KEY)?;
    storage.set_killswitch_enabled(true)?;
    storage.put_price_snapshot(OLD_PRICE_JSON, OLD_PRICE_FETCHED_AT_MS)?;
    fs::write(price_cache_path(path), OLD_PRICE_JSON)?;

    unsafe { env::set_var(PRICE_CATALOG_SENTINEL_ENV, "1") };
    storage.put_price_snapshot(NEW_PRICE_JSON, NEW_PRICE_FETCHED_AT_MS)?;
    Ok(())
}

fn verify_oauth_aead(
    path: &Path,
    iteration: usize,
) -> Result<VerificationReport, Box<dyn std::error::Error>> {
    let schema_version;
    {
        let storage = Storage::open(path, MASTER_KEY)?;
        schema_version = verify_schema_and_killswitch(&storage)?;
        for index in 0..COMMITTED_ROWS {
            let principal_id = oauth_committed_principal(iteration, index);
            let actual = storage
                .get_oauth(&principal_id, ANTHROPIC_OAUTH_PROVIDER)?
                .ok_or_else(|| io_error(format!("missing oauth row {principal_id}")))?;
            if actual != oauth_credentials(iteration, index) {
                return Err(io_error(format!(
                    "oauth committed row mismatch at iteration {iteration} index {index}"
                ))
                .into());
            }
        }
        storage.put_oauth(
            "task-48-parent-oauth-probe",
            ANTHROPIC_OAUTH_PROVIDER,
            &OAuthCredentials {
                access_token: "parent-access".to_owned(),
                refresh_token: "parent-refresh".to_owned(),
                expires_at: 4_200_000_000,
                scopes: vec!["messages".to_owned()],
            },
        )?;
    }

    let pending_rows = count_oauth_prefix(path, oauth_pending_prefix(iteration).as_bytes())?;
    if pending_rows != 0 {
        return Err(io_error(format!(
            "oauth pending transaction left {pending_rows} rows"
        ))
        .into());
    }

    Ok(VerificationReport {
        schema_version,
        committed_rows: COMMITTED_ROWS,
        pending_rows,
    })
}

fn verify_audit(
    path: &Path,
    iteration: usize,
) -> Result<VerificationReport, Box<dyn std::error::Error>> {
    let schema_version;
    {
        let storage = Storage::open(path, MASTER_KEY)?;
        schema_version = verify_schema_and_killswitch(&storage)?;
        let entries = storage.query_audit(
            Some(&audit_principal(iteration)),
            audit_ts(iteration, 0),
            audit_ts(iteration, COMMITTED_ROWS - 1),
            COMMITTED_ROWS + 1,
        )?;
        if entries.len() != COMMITTED_ROWS {
            return Err(io_error(format!(
                "audit committed row count mismatch at iteration {iteration}: {}",
                entries.len()
            ))
            .into());
        }
        for (index, entry) in entries.iter().enumerate() {
            if entry.request_id != audit_request_id(iteration, index, false) {
                return Err(io_error(format!(
                    "audit committed row mismatch at iteration {iteration} index {index}"
                ))
                .into());
            }
        }
        storage.append_audit(&AuditEntry {
            ts: audit_ts(iteration, COMMITTED_ROWS + 1),
            request_id: format!("audit-parent-probe-{iteration}"),
            principal_id: "task-48-parent-probe".to_owned(),
            route: "messages".to_owned(),
            upstream: "anthropic_direct".to_owned(),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            input_tokens: Some(1),
            output_tokens: Some(1),
            duration_ms: 1,
            agent_label: Some("task-48-parent".to_owned()),
            ..Default::default()
        })?;
    }

    let pending_rows = count_audit_request_prefix(path, &audit_pending_request_prefix(iteration))?;
    if pending_rows != 0 {
        return Err(io_error(format!(
            "audit pending transaction left {pending_rows} rows"
        ))
        .into());
    }

    Ok(VerificationReport {
        schema_version,
        committed_rows: COMMITTED_ROWS,
        pending_rows,
    })
}

fn verify_key_issuance(
    path: &Path,
    iteration: usize,
) -> Result<VerificationReport, Box<dyn std::error::Error>> {
    let storage = Storage::open(path, MASTER_KEY)?;
    let schema_version = verify_schema_and_killswitch(&storage)?;
    let api_key_rows = count_api_key_rows(&storage)?;
    let index_rows = count_key_index_rows(&storage)?;

    match (api_key_rows, index_rows) {
        (0, 0) => {}
        (1, 1) => {
            let keys = storage.list_api_keys(&key_issuance_principal(iteration))?;
            if keys.len() != 1 {
                return Err(io_error(format!(
                    "issued api key row count mismatch at iteration {iteration}: {}",
                    keys.len()
                ))
                .into());
            }
        }
        (api_rows, key_index_rows) => {
            return Err(io_error(format!(
                "partial key issuance commit at iteration {iteration}: api_keys={api_rows} key_index={key_index_rows}"
            ))
            .into());
        }
    }

    Ok(VerificationReport {
        schema_version,
        committed_rows: api_key_rows,
        pending_rows: index_rows,
    })
}

fn verify_key_revoke(
    path: &Path,
    control_dir: &Path,
    iteration: usize,
) -> Result<VerificationReport, Box<dyn std::error::Error>> {
    let key_id = fs::read_to_string(control_dir.join(REVOKE_KEY_ID_FILE))?;
    let index_hash_bytes = fs::read(control_dir.join(REVOKE_INDEX_HASH_FILE))?;
    let index_hash: [u8; 32] = index_hash_bytes
        .try_into()
        .map_err(|_| io_error("stored revoke index hash was not 32 bytes"))?;
    let principal_id = key_revoke_principal(iteration);

    let storage = Storage::open(path, MASTER_KEY)?;
    let schema_version = verify_schema_and_killswitch(&storage)?;
    let record = storage
        .get_api_key(&principal_id, &key_id)?
        .ok_or_else(|| {
            io_error(format!(
                "missing revoked api key row {principal_id}/{key_id}"
            ))
        })?;
    let indexed_composite = storage.get_composite_by_index(&index_hash)?;
    let expected_composite = api_key_storage_key(&principal_id, &key_id);

    match record.status {
        KeyStatus::Active => {
            if indexed_composite.as_deref() != Some(expected_composite.as_slice()) {
                return Err(io_error(format!(
                    "active api key lost index at iteration {iteration}"
                ))
                .into());
            }
        }
        KeyStatus::Revoked => {
            if indexed_composite.is_some() {
                return Err(io_error(format!(
                    "revoked api key retained index at iteration {iteration}"
                ))
                .into());
            }
            if record.index_hash != [0; 32] {
                return Err(io_error(format!(
                    "revoked api key retained index hash at iteration {iteration}"
                ))
                .into());
            }
        }
        status => {
            return Err(io_error(format!(
                "unexpected api key status after crash at iteration {iteration}: {status:?}"
            ))
            .into());
        }
    }

    Ok(VerificationReport {
        schema_version,
        committed_rows: 1,
        pending_rows: usize::from(indexed_composite.is_some()),
    })
}

fn verify_price_catalog(path: &Path) -> Result<VerificationReport, Box<dyn std::error::Error>> {
    let storage = Storage::open(path, MASTER_KEY)?;
    let schema_version = verify_schema_and_killswitch(&storage)?;
    let snapshot = storage
        .get_price_snapshot()?
        .ok_or_else(|| io_error("missing price catalog snapshot after crash"))?;
    if !is_expected_price_snapshot(snapshot.json_bytes.as_slice(), snapshot.fetched_at_ms) {
        return Err(io_error(format!(
            "unexpected price catalog snapshot after crash: fetched_at_ms={}",
            snapshot.fetched_at_ms
        ))
        .into());
    }

    let cache_path = price_cache_path(path);
    if cache_path.exists() {
        let cache_bytes = fs::read(cache_path)?;
        serde_json::from_slice::<serde_json::Value>(&cache_bytes)?;
        if cache_bytes != OLD_PRICE_JSON && cache_bytes != NEW_PRICE_JSON {
            return Err(io_error("disk price cache was neither old nor new snapshot").into());
        }
    }

    Ok(VerificationReport {
        schema_version,
        committed_rows: 1,
        pending_rows: 0,
    })
}

fn verify_schema_and_killswitch(storage: &Storage) -> Result<u32, Box<dyn std::error::Error>> {
    let schema_version = storage.schema_version()?;
    if schema_version != CURRENT_SCHEMA_VERSION {
        return Err(io_error(format!(
            "schema version mismatch after reopen: {schema_version}"
        ))
        .into());
    }
    if !storage.killswitch_enabled()? {
        return Err(io_error("killswitch row was not readable after reopen").into());
    }
    Ok(schema_version)
}

fn count_oauth_prefix(path: &Path, prefix: &[u8]) -> Result<usize, Box<dyn std::error::Error>> {
    let db = redb::Database::create(path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(OAUTH_CREDENTIALS_V1)?;
    let mut count = 0;
    for row in table.iter()? {
        let (key, _) = row?;
        if key.value().starts_with(prefix) {
            count += 1;
        }
    }
    Ok(count)
}

fn count_audit_request_prefix(
    path: &Path,
    prefix: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let storage = Storage::open(path, MASTER_KEY)?;
    let mut count = 0;
    for entry in storage.query_audit(None, 0, u64::MAX, usize::MAX)? {
        if entry.request_id.starts_with(prefix) {
            count += 1;
        }
    }
    Ok(count)
}

fn count_api_key_rows(storage: &Storage) -> Result<usize, Box<dyn std::error::Error>> {
    let read_txn = storage.begin_read()?;
    let table = read_txn.open_table(API_KEYS_V1)?;
    let mut count = 0;
    for row in table.iter()? {
        let _ = row?;
        count += 1;
    }
    Ok(count)
}

fn count_key_index_rows(storage: &Storage) -> Result<usize, Box<dyn std::error::Error>> {
    let read_txn = storage.begin_read()?;
    let table = read_txn.open_table(KEY_INDEX_BY_HASH_V1)?;
    let mut count = 0;
    for row in table.iter()? {
        let _ = row?;
        count += 1;
    }
    Ok(count)
}

fn signal_after_first_pending_row(control_dir: &Path, index: usize) -> Result<(), io::Error> {
    if index == 0 {
        fs::write(control_dir.join(STARTED_MARKER), b"started")?;
    }
    if index.is_multiple_of(128) {
        thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

fn wait_for_marker(control_dir: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let path = control_dir.join(name);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(5));
    }
    Err(io_error(format!("timed out waiting for child marker {name}")).into())
}

fn sleep_before_unreachable_commit() {
    thread::sleep(CHILD_LINGER);
}

fn iteration_count() -> Result<usize, Box<dyn std::error::Error>> {
    match env::var(TOTAL_ITERATIONS_ENV) {
        Ok(value) => {
            let parsed = value.parse::<usize>()?;
            if parsed == 0 {
                return Err(io_error("CC_LB_CRASH_ITERATIONS must be greater than zero").into());
            }
            Ok(parsed)
        }
        Err(_) => Ok(DEFAULT_ITERATIONS),
    }
}

fn required_env(name: &str) -> Result<String, io::Error> {
    env::var(name).map_err(|_| io_error(format!("missing environment variable {name}")))
}

fn oauth_committed_principal(iteration: usize, index: usize) -> String {
    format!("task-48-oauth-committed-{iteration}-{index}")
}

fn oauth_pending_prefix(iteration: usize) -> String {
    format!("task-48-oauth-pending-{iteration}-")
}

fn oauth_pending_key(iteration: usize, index: usize) -> Vec<u8> {
    format!("{}{index}", oauth_pending_prefix(iteration)).into_bytes()
}

fn oauth_credentials(iteration: usize, index: usize) -> OAuthCredentials {
    OAuthCredentials {
        access_token: format!("access-token-{iteration}-{index}"),
        refresh_token: format!("refresh-token-{iteration}-{index}"),
        expires_at: 4_100_000_000 + iteration as u64,
        scopes: vec!["messages".to_owned(), format!("scope-{index}")],
    }
}

fn audit_principal(iteration: usize) -> String {
    format!("task-48-audit-principal-{iteration}")
}

fn audit_request_id(iteration: usize, index: usize, pending: bool) -> String {
    let state = if pending { "pending" } else { "committed" };
    format!("task-48-audit-{state}-{iteration}-{index}")
}

fn audit_pending_request_prefix(iteration: usize) -> String {
    format!("task-48-audit-pending-{iteration}-")
}

fn audit_ts(iteration: usize, index: usize) -> u64 {
    1_800_000_000 + iteration as u64 * 100_000 + index as u64
}

fn audit_pending_key(iteration: usize, index: usize) -> u64 {
    2_000_000_000_000_000_000 + iteration as u64 * PENDING_ROWS as u64 + index as u64
}

fn audit_entry(iteration: usize, index: usize, pending: bool) -> AuditEntry {
    AuditEntry {
        ts: audit_ts(iteration, index),
        request_id: audit_request_id(iteration, index, pending),
        principal_id: audit_principal(iteration),
        route: "messages".to_owned(),
        upstream: "anthropic_direct".to_owned(),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(index as u64 + 1),
        output_tokens: Some(index as u64 + 2),
        duration_ms: 3,
        agent_label: Some("task-48-crash-child".to_owned()),
        ..Default::default()
    }
}

fn create_params(iteration: usize, label: &str) -> CreateParams {
    CreateParams {
        upstream_kind: UpstreamKind::AnthropicKey,
        upstream_credential_ref: format!("task-30-upstream-{iteration}"),
        label: label.to_owned(),
        description: Some(format!("task-30 crash recovery iteration {iteration}")),
        expires_at_unix_secs: None,
        limit_overrides: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 1_000,
        }],
        principal_kind: PrincipalKindLite::Machine,
    }
}

fn key_issuance_principal(iteration: usize) -> String {
    format!("task-30-key-issuance-principal-{iteration}")
}

fn key_revoke_principal(iteration: usize) -> String {
    format!("task-30-key-revoke-principal-{iteration}")
}

fn price_cache_path(db_path: &Path) -> PathBuf {
    db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(PRICE_CACHE_FILE)
}

fn is_expected_price_snapshot(json_bytes: &[u8], fetched_at_ms: u64) -> bool {
    (json_bytes == OLD_PRICE_JSON && fetched_at_ms == OLD_PRICE_FETCHED_AT_MS)
        || (json_bytes == NEW_PRICE_JSON && fetched_at_ms == NEW_PRICE_FETCHED_AT_MS)
}

fn io_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
