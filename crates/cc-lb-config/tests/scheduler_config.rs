use cc_lb_config::{Config, SchedulerConfig};

#[test]
fn missing_scheduler_section_applies_default_config() {
    let (_dir, path) = crate::common::temp_config("[listener]\n");

    let config = Config::load(&path).unwrap();

    assert_eq!(config.scheduler, SchedulerConfig::default());
    assert_eq!(
        config
            .scheduler
            .recurring_jobs
            .get("upstream_affinity_purge")
            .expect("upstream affinity purge default"),
        &cc_lb_config::RecurringJobConfig {
            enabled: true,
            interval_secs: 600,
            jitter_secs: 30,
        }
    );
}

#[test]
fn scheduler_toml_round_trips_overrides() {
    let config: Config = toml::from_str(
        r#"
[scheduler]
dlq_retention_days = 7
entity_concurrency = 4
singleton_concurrency = 1

[scheduler.separate_pool]
max_connections = 3
min_connections = 1
acquire_timeout_secs = 2
idle_timeout_secs = 30
statement_timeout_secs = 9
sslmode = "require"

[scheduler.recurring_jobs.usage_rollup]
enabled = false
interval_secs = 45
jitter_secs = 4

"#,
    )
    .unwrap();

    let serialized = toml::to_string_pretty(&config).unwrap();
    let reparsed: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(reparsed.scheduler, config.scheduler);
    assert_eq!(reparsed.scheduler.separate_pool.max_connections, 3);
    assert_eq!(
        reparsed
            .scheduler
            .recurring_jobs
            .get("usage_rollup")
            .unwrap()
            .interval_secs,
        45
    );
}

#[test]
fn partial_recurring_job_override_preserves_other_defaults() {
    let toml = r#"
[scheduler.recurring_jobs.usage_rollup]
enabled = false
"#;
    let (_dir, path) = crate::common::temp_config(toml);

    for config in [
        Config::load(&path).expect("file config loads"),
        Config::from_stored_toml(toml).expect("stored config loads"),
    ] {
        assert!(!config.scheduler.recurring_jobs["usage_rollup"].enabled);
        assert_eq!(
            config.scheduler.recurring_jobs["usage_rollup"].interval_secs,
            30
        );
        assert_eq!(
            config.scheduler.recurring_jobs["usage_rollup"].jitter_secs,
            3
        );
        assert!(
            config
                .scheduler
                .recurring_jobs
                .contains_key("oauth_refresh_watchdog")
        );
        assert!(config.scheduler.recurring_jobs.contains_key("usage_prune"));
        assert_eq!(
            config.scheduler.recurring_jobs.len(),
            SchedulerConfig::default().recurring_jobs.len()
        );
    }
}

#[test]
fn unknown_recurring_job_is_rejected() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[scheduler.recurring_jobs.not_a_job]
enabled = true
"#,
    );

    let error = Config::load(&path).expect_err("unknown recurring job should fail");

    assert!(
        error
            .to_string()
            .contains("unknown scheduler recurring job `not_a_job`"),
        "{error}"
    );
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
