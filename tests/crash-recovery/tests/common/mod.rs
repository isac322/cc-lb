#![cfg(unix)]

use std::env;
use std::fs;
use std::io;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use cc_lb_storage_redb::{
    AUDIT_LOG_V1, AuditEntry, BucketKind, CURRENT_SCHEMA_VERSION, OAUTH_CREDENTIALS_V1,
    OAuthCredentials, QUOTAS_BY_PRINCIPAL_V1, Storage,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum CrashCase {
    Quota,
    OAuthAead,
    Audit,
}

impl CrashCase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Quota => "quota",
            Self::OAuthAead => "oauth_aead",
            Self::Audit => "audit",
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
        CrashCase::Quota => child_quota(&db_path, &control_dir, iteration)?,
        CrashCase::OAuthAead => child_oauth_aead(&db_path, &control_dir, iteration)?,
        CrashCase::Audit => child_audit(&db_path, &control_dir, iteration)?,
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
        CrashCase::Quota => verify_quota(&db_path, iteration)?,
        CrashCase::OAuthAead => verify_oauth_aead(&db_path, iteration)?,
        CrashCase::Audit => verify_audit(&db_path, iteration)?,
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
    Ok(command.spawn()?)
}

fn cleanup_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn child_quota(path: &Path, control_dir: &Path, iteration: usize) -> TestResult {
    {
        let storage = Storage::open(path, MASTER_KEY)?;
        storage.set_killswitch_enabled(true)?;
        let principal_id = quota_committed_principal(iteration);
        for index in 0..COMMITTED_ROWS {
            storage.incr_quota(&principal_id, index as u64, BucketKind::Requests, 1)?;
        }
    }

    let db = redb::Database::create(path)?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
        let pending_principal = quota_pending_principal(iteration);
        let value = 1_u64.to_le_bytes();
        for index in 0..PENDING_ROWS {
            let key = cc_lb_storage_redb::quota_key(
                &pending_principal,
                index as u64,
                BucketKind::Requests,
            );
            table.insert(key.as_slice(), value.as_slice())?;
            signal_after_first_pending_row(control_dir, index)?;
        }
    }
    sleep_before_unreachable_commit();
    write_txn.commit()?;
    Ok(())
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

fn verify_quota(
    path: &Path,
    iteration: usize,
) -> Result<VerificationReport, Box<dyn std::error::Error>> {
    let schema_version;
    {
        let storage = Storage::open(path, MASTER_KEY)?;
        schema_version = verify_schema_and_killswitch(&storage)?;
        let principal_id = quota_committed_principal(iteration);
        for index in 0..COMMITTED_ROWS {
            let value = storage.get_quota(&principal_id, index as u64, BucketKind::Requests)?;
            if value != 1 {
                return Err(io_error(format!(
                    "quota committed row mismatch at iteration {iteration} index {index}: {value}"
                ))
                .into());
            }
        }
        let probe = storage.incr_quota(
            "task-48-parent-probe",
            iteration as u64,
            BucketKind::Requests,
            1,
        )?;
        if probe != 1 {
            return Err(io_error(format!("parent probe write returned {probe}")).into());
        }
    }

    let pending_rows = count_quota_prefix(path, quota_pending_principal(iteration).as_bytes())?;
    if pending_rows != 0 {
        return Err(io_error(format!(
            "quota pending transaction left {pending_rows} rows"
        ))
        .into());
    }

    Ok(VerificationReport {
        schema_version,
        committed_rows: COMMITTED_ROWS,
        pending_rows,
    })
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
            input_tokens: 1,
            output_tokens: 1,
            duration_ms: 1,
            agent_label: Some("task-48-parent".to_owned()),
            kind: None,
            payload: None,
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

fn count_quota_prefix(path: &Path, prefix: &[u8]) -> Result<usize, Box<dyn std::error::Error>> {
    let db = redb::Database::create(path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
    let mut count = 0;
    for row in table.iter()? {
        let (key, _) = row?;
        if key.value().starts_with(prefix) {
            count += 1;
        }
    }
    Ok(count)
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
    let db = redb::Database::create(path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(AUDIT_LOG_V1)?;
    let mut count = 0;
    for row in table.iter()? {
        let (_, value) = row?;
        let entry = serde_json::from_slice::<AuditEntry>(value.value())?;
        if entry.request_id.starts_with(prefix) {
            count += 1;
        }
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

fn quota_committed_principal(iteration: usize) -> String {
    format!("task-48-quota-committed-{iteration}")
}

fn quota_pending_principal(iteration: usize) -> String {
    format!("task-48-quota-pending-{iteration}")
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
        input_tokens: index as u64 + 1,
        output_tokens: index as u64 + 2,
        duration_ms: 3,
        agent_label: Some("task-48-crash-child".to_owned()),
        kind: None,
        payload: None,
    }
}

fn io_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
