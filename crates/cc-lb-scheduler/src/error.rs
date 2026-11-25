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
    pub(crate) fn from_task_push_sqlx_error(error: sqlx::Error) -> Self {
        if is_unique_constraint_error(&error) {
            Self::Conflict(error.to_string())
        } else {
            Self::Database(error)
        }
    }
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
