use std::convert::Infallible;
use std::error::Error;

use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollCronJob;
use cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJob;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeJob;
use cc_lb_scheduler::jobs::quota_gc::SubscriptionQuotaGcJob;
use cc_lb_scheduler::jobs::upstream_affinity_purge::UpstreamAffinityPurgeJob;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneJob;
use cc_lb_scheduler::jobs::usage_rollup::UsageRollupTask;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::middleware::SchedulerMetricsLayer;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::scheduler_metrics;
use cc_lb_scheduler::worker::{AdaptiveJob, CronJob};
use metrics_exporter_prometheus::PrometheusBuilder;
use tower::{Layer as _, ServiceExt as _, service_fn};
use uuid::Uuid;

#[test]
fn t2__scheduler_metrics_cover_job_lifecycle() -> Result<(), Box<dyn Error>> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            scheduler_metrics::touch_scheduler_metric_handles();
            record_adaptive_jobs().await;
            record_cron_jobs().await;

            let rendered = handle.render();
            assert_required_metric_names(&rendered);
            assert_counter_eq(
                &rendered,
                scheduler_metrics::JOBS_TOTAL,
                &[("job_type", "upstream_warmup"), ("status", "done")],
                1.0,
            );
            assert_counter_eq(
                &rendered,
                scheduler_metrics::JOBS_TOTAL,
                &[("job_type", "adaptive:oauth_refresh"), ("status", "done")],
                1.0,
            );
            assert_counter_eq(
                &rendered,
                scheduler_metrics::JOBS_TOTAL,
                &[("job_type", "adaptive:cache_keepalive"), ("status", "done")],
                1.0,
            );
            assert_counter_eq(
                &rendered,
                scheduler_metrics::JOBS_TOTAL,
                &[
                    ("job_type", "cron:upstream_affinity_purge"),
                    ("status", "done"),
                ],
                1.0,
            );
            Ok::<(), Box<dyn Error>>(())
        })
    })?;
    Ok(())
}

async fn record_adaptive_jobs() {
    for job in entity_jobs(Uuid::from_u128(1)) {
        SchedulerMetricsLayer::new()
            .layer(service_fn(|_job: AdaptiveJob| async {
                Ok::<_, Infallible>(JobOutcome::Done)
            }))
            .oneshot(job)
            .await
            .expect("infallible adaptive lifecycle service");
    }
}

async fn record_cron_jobs() {
    for job in singleton_jobs() {
        SchedulerMetricsLayer::new()
            .layer(service_fn(|_job: CronJob| async {
                Ok::<_, Infallible>(JobOutcome::Done)
            }))
            .oneshot(job)
            .await
            .expect("infallible cron lifecycle service");
    }
}

fn entity_jobs(upstream_id: Uuid) -> [AdaptiveJob; 4] {
    [
        AdaptiveJob::Warmup(UpstreamWarmupJob::new(upstream_id, 1)),
        AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(upstream_id, 1)),
        AdaptiveJob::CacheKeepalive(CacheKeepaliveJob {
            session_key_hash: "metric-session".to_owned(),
            generation: 1,
            principal_id: "principal".to_owned(),
            upstream_id,
            ttl: cc_lb_storage_api::CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 1_800_000_000,
            expires_at_unix_secs: 1_800_000_300,
            refresh_delay_secs: 270,
            max_refreshes: 12,
            max_total_duration_secs: 14_400,
            traceparent: None,
        }),
    ]
}

fn singleton_jobs() -> [CronJob; 8] {
    [
        CronJob::UsageRollup(UsageRollupTask::default()),
        CronJob::UsagePrune(UsagePruneJob::default()),
        CronJob::QuotaGc(SubscriptionQuotaGcJob::default()),
        CronJob::PromptCachePurge(PromptCacheObservationPurgeJob::default()),
        CronJob::UpstreamAffinityPurge(UpstreamAffinityPurgeJob::default()),
        CronJob::PriceCatalogRefresh(PriceCatalogRefreshJob::default()),
        CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()),
        CronJob::OAuthUsagePoll(OAuthUsagePollCronJob::new(1_800_000_000)),
    ]
}

fn assert_required_metric_names(rendered: &str) {
    for name in [
        scheduler_metrics::JOBS_TOTAL,
        scheduler_metrics::JOB_DURATION_SECONDS,
        scheduler_metrics::FAILURES_TOTAL,
        scheduler_metrics::INIT_FAILURE,
        scheduler_metrics::LAZY_REFRESH_TIMEOUT_TOTAL,
        scheduler_metrics::PRUNE_ROWS_REMOVED_TOTAL,
        scheduler_metrics::PRICE_CATALOG_STATUS_TOTAL,
    ] {
        assert!(
            rendered.contains(name),
            "missing {name} in prometheus output:\n{rendered}"
        );
    }
}

fn assert_counter_eq(rendered: &str, name: &str, labels: &[(&str, &str)], expected: f64) {
    let value = counter_value(rendered, name, labels);
    assert_eq!(
        value, expected,
        "counter {name} labels {labels:?}:\n{rendered}"
    );
}

fn counter_value(rendered: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let metric_prefix = format!("{name}{{");
    rendered
        .lines()
        .find(|line| {
            line.starts_with(&metric_prefix)
                && labels
                    .iter()
                    .all(|(label, value)| line.contains(&format!(r#"{label}="{value}""#)))
        })
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}
