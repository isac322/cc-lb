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

/// POL-09: the API-key usage poll defaults to 300 s with 15 s jitter.
#[test]
fn pol09_api_key_usage_poll_defaults() {
    let job = &SchedulerConfig::default().recurring_jobs["api_key_usage_poll"];

    assert!(job.enabled);
    assert_eq!(job.interval_secs, 300);
    assert_eq!(job.jitter_secs, 15);
}

/// POL-09: 60 s is the smallest accepted interval.
#[test]
fn pol09_api_key_usage_poll_accepts_sixty_seconds() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[scheduler.recurring_jobs.api_key_usage_poll]
interval_secs = 60
jitter_secs = 59
"#,
    );

    let config = Config::load(&path).expect("60 s interval is accepted");

    assert_eq!(
        config.scheduler.recurring_jobs["api_key_usage_poll"].interval_secs,
        60
    );
}

/// POL-09: 604800 s (7 days) is the largest accepted interval.
#[test]
fn pol09_api_key_usage_poll_accepts_seven_days() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[scheduler.recurring_jobs.api_key_usage_poll]
interval_secs = 604800
jitter_secs = 604799
"#,
    );

    let config = Config::load(&path).expect("7-day interval is accepted");

    assert_eq!(
        config.scheduler.recurring_jobs["api_key_usage_poll"].interval_secs,
        604_800
    );
}

/// POL-09: an interval below 60 s or above 7 days and a jitter not below the
/// interval fail validation naming the field, for file and stored configs
/// alike.
#[test]
fn pol09_api_key_usage_poll_bounds_name_the_field() {
    for (toml, field) in [
        (
            "[scheduler.recurring_jobs.api_key_usage_poll]\ninterval_secs = 59\njitter_secs = 0\n",
            "scheduler.recurring_jobs.api_key_usage_poll.interval_secs",
        ),
        (
            "[scheduler.recurring_jobs.api_key_usage_poll]\ninterval_secs = 604801\njitter_secs = 0\n",
            "scheduler.recurring_jobs.api_key_usage_poll.interval_secs",
        ),
        (
            "[scheduler.recurring_jobs.api_key_usage_poll]\ninterval_secs = 9223372036854775807\njitter_secs = 0\n",
            "scheduler.recurring_jobs.api_key_usage_poll.interval_secs",
        ),
        (
            "[scheduler.recurring_jobs.api_key_usage_poll]\ninterval_secs = 300\njitter_secs = 300\n",
            "scheduler.recurring_jobs.api_key_usage_poll.jitter_secs",
        ),
    ] {
        let (_dir, path) = crate::common::temp_config(toml);
        let file_error = Config::load(&path).expect_err("out-of-bounds poll config fails");
        assert!(file_error.to_string().contains(field), "{file_error}");

        let stored = Config::from_stored_toml(toml).expect("stored toml parses");
        let stored_error = stored
            .validate_without_filesystem()
            .expect_err("stored out-of-bounds poll config fails");
        assert!(stored_error.to_string().contains(field), "{stored_error}");
    }
}

/// POL-09: unknown fields in the job block, including a removed
/// `api_key_env`, are rejected.
#[test]
fn pol09_api_key_usage_poll_rejects_unknown_fields() {
    for toml in [
        "[scheduler.recurring_jobs.api_key_usage_poll]\napi_key_env = \"ANTHROPIC_API_KEY\"\n",
        "[scheduler.recurring_jobs.api_key_usage_poll]\npage_size = 10\n",
    ] {
        let (_dir, path) = crate::common::temp_config(toml);
        let error = Config::load(&path).expect_err("unknown poll field fails");
        assert!(error.to_string().contains("unknown field"), "{error}");
    }
}

#[test]
fn default_scheduler_config_matches_snapshot() {
    insta::assert_json_snapshot!(SchedulerConfig::default());
}
