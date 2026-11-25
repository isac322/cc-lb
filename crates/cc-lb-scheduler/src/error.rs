//! Error types for the scheduler.

use std::time::Duration;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("job error: {0}")]
    Job(String),

    #[error("scheduler task conflict: {0}")]
    Conflict(String),

    #[error("retry delay must be non-zero")]
    InvalidRetryDelay,

    /// Handler exceeded the per-job timeout enforced inside the dispatcher.
    ///
    /// This error MUST reach the apalis ack path so the row is marked `Failed`
    /// (or retried by the retry class layer) rather than being dropped mid-flight
    /// by a tower `TimeoutLayer`, which historically left rows stuck in
    /// `status='Running'` forever.
    #[error("job {job_kind} exceeded timeout of {}ms", .timeout.as_millis())]
    JobTimeout {
        job_kind: &'static str,
        timeout: Duration,
    },

    #[error("retries exhausted ({category}): {summary}")]
    RetriesExhausted {
        category: &'static str,
        summary: String,
    },
}

const MAX_FAILURE_SUMMARY_CHARS: usize = 256;

/// Bound and sanitize handler failure text persisted to `last_result` on retry exhaustion.
pub(crate) fn sanitize_failure_summary(raw: &str) -> String {
    let mut normalized = String::with_capacity(raw.len().min(MAX_FAILURE_SUMMARY_CHARS));
    let mut last_was_space = true;
    let mut in_query_string = false;
    for ch in raw.chars() {
        if ch.is_control() || ch.is_whitespace() {
            in_query_string = false;
            if !last_was_space {
                normalized.push(' ');
                last_was_space = true;
            }
            continue;
        }
        if in_query_string {
            continue;
        }
        if ch == '?' {
            in_query_string = true;
            last_was_space = false;
            continue;
        }
        normalized.push(ch);
        last_was_space = false;
    }
    let trimmed = normalized.trim_end();
    let redacted = redact_secret_tokens(trimmed);
    truncate_chars(&redacted, MAX_FAILURE_SUMMARY_CHARS)
}

fn redact_secret_tokens(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut bearer_next = false;
    for (idx, word) in input.split(' ').enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        if bearer_next {
            out.push_str("[REDACTED]");
            bearer_next = false;
            continue;
        }
        if word.eq_ignore_ascii_case("bearer") {
            out.push_str(word);
            bearer_next = true;
        } else if let Some(prefix) = word.split('=').next().filter(|p| {
            let lower = p.to_ascii_lowercase();
            lower.contains("token") || lower.contains("secret") || lower.contains("key")
        }) && word.contains('=')
        {
            out.push_str(prefix);
            out.push_str("=[REDACTED]");
        } else if word.starts_with("sk-") && word.len() > 8 {
            out.push_str("sk-[REDACTED]");
        } else {
            out.push_str(word);
        }
    }
    out
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    let count = input.chars().count();
    if count <= max_chars {
        return input.to_owned();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out: String = input.chars().take(keep).collect();
    out.push_str("...");
    out
}

impl SchedulerError {
    /// Returns `true` only when a task push error guarantees the database row
    /// was never committed (for example pool timeout before SQL submission,
    /// invalid payload encoding, or explicit pre-commit constraint/schema error).
    ///
    /// Transport/protocol failures, caller-side timeouts, and ambiguous
    /// `Database` errors (such as PgBouncer `08P01` disconnects or Postgres
    /// `40003` statement-completion-unknown) remain `false` because an
    /// autocommit statement may already have committed before the error arrived.
    pub fn is_confirmed_push_rejection(&self) -> bool {
        match self {
            Self::Job(_) | Self::Conflict(_) | Self::InvalidRetryDelay => true,
            Self::JobTimeout { .. } | Self::RetriesExhausted { .. } => false,
            Self::Database(db_err) => is_confirmed_push_rejection_sqlx(db_err),
        }
    }

    pub(crate) fn from_task_push_sqlx_error(error: sqlx::Error) -> Self {
        if is_unique_constraint_error(&error) {
            Self::Conflict(error.to_string())
        } else {
            Self::Database(error)
        }
    }
}

fn is_confirmed_push_rejection_sqlx(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Configuration(_)
        | sqlx::Error::Encode(_) => true,
        sqlx::Error::Database(database_error) => {
            is_confirmed_push_rejection_database(database_error.as_ref())
        }
        _ => false,
    }
}

fn is_confirmed_push_rejection_database(database_error: &dyn sqlx::error::DatabaseError) -> bool {
    if matches!(
        database_error.kind(),
        sqlx::error::ErrorKind::UniqueViolation
            | sqlx::error::ErrorKind::ForeignKeyViolation
            | sqlx::error::ErrorKind::NotNullViolation
            | sqlx::error::ErrorKind::CheckViolation
            | sqlx::error::ErrorKind::ExclusionViolation
    ) {
        return true;
    }

    let Some(code) = database_error.code() else {
        return false;
    };
    let code = code.as_ref();

    // Match 5-character PostgreSQL SQLSTATE codes first so 5-digit codes such
    // as `40003` or `23505` never collide with SQLite primary code masks.
    if code.len() == 5
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return is_confirmed_non_commit_pg_sqlstate(code);
    }

    if !code.starts_with('0')
        && code.len() <= 4
        && let Ok(raw_code) = code.parse::<u32>()
    {
        let primary = raw_code & 0xFF;
        return matches!(primary, 1 | 3 | 8 | 19 | 20 | 21 | 23 | 25);
    }

    false
}

fn is_confirmed_non_commit_pg_sqlstate(code: &str) -> bool {
    if matches!(&code[..2], "22" | "23" | "28" | "3D" | "3F" | "42" | "44") {
        return true;
    }
    matches!(code, "25006" | "25P02" | "40001" | "40002" | "40P01")
}

#[allow(clippy::collapsible_if)]
fn is_unique_constraint_error(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database_error) = error else {
        return false;
    };
    if let Some(code) = database_error.code() {
        if matches!(code.as_ref(), "23505" | "2067" | "1555") {
            return true;
        }
    }
    let message = database_error.message();
    message.contains("UNIQUE constraint failed")
        || message.contains("duplicate key value violates unique constraint")
}

pub type Result<T> = std::result::Result<T, SchedulerError>;

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt;

    use sqlx::error::{DatabaseError, ErrorKind};

    use super::*;

    #[derive(Debug)]
    struct MockDatabaseError {
        message: String,
        code: Option<String>,
        kind: ErrorKind,
    }

    impl MockDatabaseError {
        fn new(code: Option<&str>, kind: ErrorKind) -> Self {
            Self {
                message: format!("mock db error code={code:?} kind={kind:?}"),
                code: code.map(ToOwned::to_owned),
                kind,
            }
        }
    }

    impl fmt::Display for MockDatabaseError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.message)
        }
    }

    impl StdError for MockDatabaseError {}

    impl DatabaseError for MockDatabaseError {
        fn message(&self) -> &str {
            &self.message
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            self.code.as_deref().map(Cow::Borrowed)
        }

        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> ErrorKind {
            match &self.kind {
                ErrorKind::UniqueViolation => ErrorKind::UniqueViolation,
                ErrorKind::ForeignKeyViolation => ErrorKind::ForeignKeyViolation,
                ErrorKind::NotNullViolation => ErrorKind::NotNullViolation,
                ErrorKind::CheckViolation => ErrorKind::CheckViolation,
                ErrorKind::ExclusionViolation => ErrorKind::ExclusionViolation,
                _ => ErrorKind::Other,
            }
        }
    }

    fn db_err(code: Option<&str>, kind: ErrorKind) -> SchedulerError {
        SchedulerError::Database(sqlx::Error::Database(Box::new(MockDatabaseError::new(
            code, kind,
        ))))
    }

    #[test]
    fn confirmed_push_rejection_distinguishes_pre_commit_and_ambiguous_errors() {
        assert!(SchedulerError::Job("encode error".to_owned()).is_confirmed_push_rejection());
        assert!(SchedulerError::Conflict("duplicate".to_owned()).is_confirmed_push_rejection());
        assert!(SchedulerError::InvalidRetryDelay.is_confirmed_push_rejection());
        assert!(SchedulerError::Database(sqlx::Error::PoolTimedOut).is_confirmed_push_rejection());
        assert!(SchedulerError::Database(sqlx::Error::PoolClosed).is_confirmed_push_rejection());
        assert!(
            SchedulerError::Database(sqlx::Error::Configuration("bad url".into()))
                .is_confirmed_push_rejection()
        );

        assert!(
            !SchedulerError::JobTimeout {
                job_kind: "manual_warmup",
                timeout: Duration::from_secs(1),
            }
            .is_confirmed_push_rejection()
        );
        assert!(
            !SchedulerError::Database(sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset after commit",
            )))
            .is_confirmed_push_rejection()
        );
        assert!(
            !SchedulerError::Database(sqlx::Error::Protocol("eof".into()))
                .is_confirmed_push_rejection()
        );
        assert!(
            !SchedulerError::Database(sqlx::Error::WorkerCrashed).is_confirmed_push_rejection()
        );
    }

    #[test]
    fn confirmed_push_rejection_classifies_database_error_codes_conservatively() {
        // Ambiguous Postgres / PgBouncer / SQLite errors must stay `false` -> audited 504.
        for code in [
            None,
            Some("08P01"), // PgBouncer query timeout / client disconnect
            Some("08006"), // connection_failure
            Some("40003"), // statement_completion_unknown
            Some("53300"), // too_many_connections
            Some("57014"), // query_canceled
            Some("57P01"), // admin_shutdown
            Some("57P02"), // crash_shutdown
            Some("58030"), // io_error
            Some("XX000"), // internal_error
            Some("5"),     // SQLITE_BUSY
            Some("9"),     // SQLITE_INTERRUPT
            Some("10"),    // SQLITE_IOERR
            Some("13"),    // SQLITE_FULL
            Some("15"),    // SQLITE_PROTOCOL
        ] {
            assert!(
                !db_err(code, ErrorKind::Other).is_confirmed_push_rejection(),
                "database error {code:?} must be treated as ambiguous (504)"
            );
        }

        // Structured constraint ErrorKinds are definite rejections even without SQLSTATE code.
        for kind in [
            ErrorKind::UniqueViolation,
            ErrorKind::ForeignKeyViolation,
            ErrorKind::NotNullViolation,
            ErrorKind::CheckViolation,
            ErrorKind::ExclusionViolation,
        ] {
            let label = format!("{kind:?}");
            assert!(
                db_err(None, kind).is_confirmed_push_rejection(),
                "constraint kind {label} must be a confirmed push rejection"
            );
        }

        // Definite pre-commit Postgres SQLSTATE and SQLite result codes.
        for code in [
            "22P02", // invalid_text_representation
            "23502", // not_null_violation
            "25006", // read_only_sql_transaction
            "25P02", // in_failed_sql_transaction
            "28P01", // invalid_password
            "3D000", // invalid_catalog_name
            "3F000", // invalid_schema_name
            "40001", // serialization_failure
            "40002", // transaction_integrity_constraint_violation
            "40P01", // deadlock_detected
            "42501", // insufficient_privilege
            "42P01", // undefined_table
            "44000", // with_check_option_violation
            "1",     // SQLITE_ERROR
            "3",     // SQLITE_PERM
            "8",     // SQLITE_READONLY
            "264",   // SQLITE_READONLY_RECOVERY (264 & 0xFF == 8)
            "19",    // SQLITE_CONSTRAINT
            "1299",  // SQLITE_CONSTRAINT_NOTNULL (1299 & 0xFF == 19)
            "20",    // SQLITE_MISMATCH
            "21",    // SQLITE_MISUSE
            "23",    // SQLITE_AUTH
            "25",    // SQLITE_RANGE
        ] {
            assert!(
                db_err(Some(code), ErrorKind::Other).is_confirmed_push_rejection(),
                "database error {code} must be a confirmed push rejection"
            );
        }
    }
}
