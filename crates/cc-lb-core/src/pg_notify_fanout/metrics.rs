use crate::metrics_labels::{NotifyDropReason, NotifyHttpOutcome, NotifySentOutcome};

pub async fn record_queue_usage(pg_pool: &sqlx::PgPool) {
    match sqlx::query_scalar::<_, f64>("SELECT pg_notification_queue_usage()")
        .fetch_one(pg_pool)
        .await
    {
        Ok(usage) => metrics::gauge!("sse_notify_queue_usage_ratio").set(usage),
        Err(error) => tracing::warn!(%error, "pg notification queue usage query failed"),
    }
}

pub fn record_notify_sent(outcome: NotifySentOutcome) {
    metrics::counter!("sse_partial_notify_sent_total", "outcome" => outcome.as_str()).increment(1);
}

pub fn record_notify_dropped(reason: NotifyDropReason) {
    metrics::counter!("sse_partial_notify_dropped_total", "reason" => reason.as_str()).increment(1);
}

pub fn record_http_fetch(outcome: NotifyHttpOutcome) {
    metrics::counter!("sse_notify_http_fetches_total", "outcome" => outcome.as_str()).increment(1);
}
