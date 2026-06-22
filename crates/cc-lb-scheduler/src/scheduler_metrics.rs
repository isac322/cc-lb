use std::sync::Once;
use std::time::Duration;

use metrics::Unit;

pub const JOBS_TOTAL: &str = "cclb_scheduler_jobs_total";
pub const JOB_DURATION_SECONDS: &str = "cclb_scheduler_job_duration_seconds";
pub const FAILURES_TOTAL: &str = "cclb_scheduler_failures_total";
pub const LEADER_ACQUIRED_TOTAL: &str = "cclb_scheduler_leader_acquired_total";
pub const LEADER_LOST_TOTAL: &str = "cclb_scheduler_leader_lost_total";
pub const INIT_FAILURE: &str = "cclb_scheduler_init_failure";
pub const LAZY_REFRESH_TIMEOUT_TOTAL: &str = "cclb_scheduler_lazy_refresh_timeout_total";
pub const PRUNE_ROWS_REMOVED_TOTAL: &str = "cclb_scheduler_prune_rows_removed_total";
pub const QUOTA_GC_ROWS_REMOVED_TOTAL: &str = "cclb_scheduler_quota_gc_rows_removed_total";
pub const PRICE_CATALOG_STATUS_TOTAL: &str = "cclb_scheduler_price_catalog_status_total";
pub const PROMPT_CACHE_PURGE_ROWS_REMOVED_TOTAL: &str =
    "cclb_scheduler_prompt_cache_purge_rows_removed_total";
pub const METADATA_REFRESH_STATUS_TOTAL: &str = "cclb_scheduler_metadata_refresh_status_total";

const JOB_TYPES: &[&str] = &[
    "upstream_warmup",
    "entity:oauth_refresh",
    "entity:oauth_usage_poll",
    "entity:metadata_refresh",
    "singleton:usage_rollup",
    "singleton:usage_prune",
    "singleton:quota_gc",
    "singleton:prompt_cache_purge",
    "singleton:price_catalog_refresh",
    "singleton:apalis_housekeeping",
    "singleton:warmup_watchdog",
    "singleton:oauth_refresh_watchdog",
    "singleton:oauth_usage_poll_watchdog",
    "singleton:anthropic_compat_refresh",
];

const JOB_STATUSES: &[&str] = &[
    "started",
    "done",
    "retry",
    "skip",
    "panicked",
    "duplicate_effect",
    "noop",
];

static DESCRIBE: Once = Once::new();

pub fn describe_scheduler_metrics() {
    DESCRIBE.call_once(|| {
        ::metrics::describe_counter!(
            JOBS_TOTAL,
            Unit::Count,
            "Scheduler job lifecycle events by registered job type and status."
        );
        ::metrics::describe_histogram!(
            JOB_DURATION_SECONDS,
            Unit::Seconds,
            "Scheduler job handler duration in seconds by registered job type."
        );
        ::metrics::describe_counter!(
            FAILURES_TOTAL,
            Unit::Count,
            "Scheduler terminal job failures by registered job type."
        );
        ::metrics::describe_counter!(
            LEADER_ACQUIRED_TOTAL,
            Unit::Count,
            "Scheduler leader lock acquisitions."
        );
        ::metrics::describe_counter!(
            LEADER_LOST_TOTAL,
            Unit::Count,
            "Scheduler leader lock losses."
        );
        ::metrics::describe_gauge!(INIT_FAILURE, Unit::Count, "Scheduler init failure state.");
        ::metrics::describe_counter!(
            LAZY_REFRESH_TIMEOUT_TOTAL,
            Unit::Count,
            "Lazy OAuth refresh waits that timed out."
        );
        ::metrics::describe_counter!(
            PRUNE_ROWS_REMOVED_TOTAL,
            Unit::Count,
            "Scheduler usage prune rows removed by table."
        );
        ::metrics::describe_counter!(
            QUOTA_GC_ROWS_REMOVED_TOTAL,
            Unit::Count,
            "Scheduler subscription quota GC rows removed."
        );
        ::metrics::describe_counter!(
            PRICE_CATALOG_STATUS_TOTAL,
            Unit::Count,
            "Scheduler price catalog refresh outcomes by status."
        );
        ::metrics::describe_counter!(
            PROMPT_CACHE_PURGE_ROWS_REMOVED_TOTAL,
            Unit::Count,
            "Scheduler prompt-cache observation purge rows removed."
        );
        ::metrics::describe_counter!(
            METADATA_REFRESH_STATUS_TOTAL,
            Unit::Count,
            "Scheduler metadata refresh outcomes by status."
        );
    });
}

pub fn touch_scheduler_metric_handles() {
    describe_scheduler_metrics();
    for job_type in JOB_TYPES {
        for status in JOB_STATUSES {
            ::metrics::counter!(JOBS_TOTAL, "job_type" => *job_type, "status" => *status)
                .increment(0);
        }
        record_scheduler_job_duration(job_type, Duration::ZERO);
        record_scheduler_failure(job_type, 0);
    }
    record_leader_acquired(0);
    record_leader_lost(0);
    set_scheduler_init_failure(false);
    ::metrics::counter!(LAZY_REFRESH_TIMEOUT_TOTAL).increment(0);
    ::metrics::counter!(PRUNE_ROWS_REMOVED_TOTAL, "table" => "request_events").increment(0);
    ::metrics::counter!(PRUNE_ROWS_REMOVED_TOTAL, "table" => "audit_log").increment(0);
    ::metrics::counter!(QUOTA_GC_ROWS_REMOVED_TOTAL).increment(0);
    ::metrics::counter!(PRICE_CATALOG_STATUS_TOTAL, "status" => "applied").increment(0);
    ::metrics::counter!(PRICE_CATALOG_STATUS_TOTAL, "status" => "noop").increment(0);
    ::metrics::counter!(PROMPT_CACHE_PURGE_ROWS_REMOVED_TOTAL).increment(0);
    ::metrics::counter!(METADATA_REFRESH_STATUS_TOTAL, "status" => "unknown").increment(0);
}

pub fn record_scheduler_job_status(job_type: &'static str, status: &'static str) {
    describe_scheduler_metrics();
    ::metrics::counter!(JOBS_TOTAL, "job_type" => job_type, "status" => status).increment(1);
}

pub fn record_scheduler_job_duration(job_type: &'static str, duration: Duration) {
    describe_scheduler_metrics();
    ::metrics::histogram!(JOB_DURATION_SECONDS, "job_type" => job_type)
        .record(duration.as_secs_f64());
}

pub fn record_scheduler_failure(job_type: &str, count: u64) {
    describe_scheduler_metrics();
    ::metrics::counter!(FAILURES_TOTAL, "job_type" => job_type.to_owned()).increment(count);
}

pub fn record_leader_acquired(count: u64) {
    describe_scheduler_metrics();
    ::metrics::counter!(LEADER_ACQUIRED_TOTAL).increment(count);
}

pub fn record_leader_lost(count: u64) {
    describe_scheduler_metrics();
    ::metrics::counter!(LEADER_LOST_TOTAL).increment(count);
}

pub fn set_scheduler_init_failure(failed: bool) {
    describe_scheduler_metrics();
    ::metrics::gauge!(INIT_FAILURE).set(if failed { 1.0 } else { 0.0 });
}
