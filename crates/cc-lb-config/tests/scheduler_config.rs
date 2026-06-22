mod common;

use cc_lb_config::{Config, SchedulerConfig};

#[test]
fn missing_scheduler_section_applies_default_config() {
    let (_dir, path) = common::temp_config("[listener]\n");

    let config = Config::load(&path).unwrap();

    assert_eq!(config.scheduler, SchedulerConfig::default());
}

#[test]
fn scheduler_toml_round_trips_overrides() {
    let config: Config = toml::from_str(
        r#"
[scheduler]
leader_lock_key = 42
dlq_retention_days = 7
entity_concurrency = 4
singleton_concurrency = 1
pgbouncer_transaction_mode = true

[scheduler.separate_pool]
max_connections = 3
min_connections = 1
acquire_timeout_secs = 2
idle_timeout_secs = 30
statement_timeout_secs = 9
sslmode = "require"

[scheduler.retry_classes.probe]
max_attempts = 2
base_secs = 1
max_secs = 4

[scheduler.retry_classes.adaptive]
max_attempts = 6
base_secs = 10
max_secs = 120

[scheduler.retry_classes.maintenance]
max_attempts = 1
base_secs = 60
max_secs = 60

[scheduler.recurring_jobs.usage_rollup]
enabled = false
interval_secs = 45
jitter_secs = 4

[scheduler.idempotency]
claim_ttl_secs = 45

[scheduler.staleness]
warmup_effect_retention_days = 14
"#,
    )
    .unwrap();

    let serialized = toml::to_string_pretty(&config).unwrap();
    let reparsed: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(reparsed.scheduler, config.scheduler);
    assert_eq!(reparsed.scheduler.leader_lock_key, 42);
    assert_eq!(reparsed.scheduler.separate_pool.max_connections, 3);
    assert_eq!(reparsed.scheduler.retry_classes.adaptive.max_attempts, 6);
    assert_eq!(
        reparsed
            .scheduler
            .recurring_jobs
            .get("usage_rollup")
            .unwrap()
            .interval_secs,
        45
    );
    assert!(reparsed.scheduler.pgbouncer_transaction_mode);
}

#[test]
fn scheduler_enabled_flag_is_rejected() {
    let error = toml::from_str::<Config>(
        r#"
[scheduler]
enabled = true
"#,
    )
    .unwrap_err();

    assert!(error.to_string().contains("unknown field `enabled`"));
}

#[test]
fn default_scheduler_config_matches_snapshot() {
    insta::assert_json_snapshot!(SchedulerConfig::default());
}
