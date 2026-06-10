use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default, Clone, Debug)]
pub struct RequestStageTimings {
    pub bulkhead_wait_ms: Option<u64>,
    pub dns_ms: Option<u64>,
    pub connect_ms: Option<u64>,
    pub connection_reused: Option<bool>,
}

tokio::task_local! {
    pub static REQUEST_STAGE_TIMINGS: Arc<Mutex<RequestStageTimings>>;
}

pub async fn with_timings<F, T>(fut: F) -> (T, RequestStageTimings)
where
    F: Future<Output = T>,
{
    let ctx = Arc::new(Mutex::new(RequestStageTimings::default()));
    let value = REQUEST_STAGE_TIMINGS.scope(ctx.clone(), fut).await;
    let snapshot = ctx.lock().unwrap().clone();

    (value, snapshot)
}

pub fn record_bulkhead_wait(elapsed: Duration) {
    let elapsed_ms = duration_to_ms(elapsed);
    update_current_timing(|timings| {
        timings.bulkhead_wait_ms = Some(elapsed_ms);
    });
}

pub fn record_dns(elapsed: Duration) {
    let elapsed_ms = duration_to_ms(elapsed);
    update_current_timing(|timings| {
        timings.dns_ms = Some(elapsed_ms);
    });
}

pub fn record_connect(elapsed: Duration) {
    let elapsed_ms = duration_to_ms(elapsed);
    update_current_timing(|timings| {
        timings.connect_ms = Some(elapsed_ms);
    });
}

pub fn mark_connector_called() {
    update_current_timing(|timings| {
        if timings.connection_reused.is_none() {
            timings.connection_reused = Some(false);
        }
    });
}

pub fn finalize_connection_reused_if_unset() {
    update_current_timing(|timings| {
        if timings.connection_reused.is_none() {
            timings.connection_reused = Some(true);
        }
    });
}

fn duration_to_ms(elapsed: Duration) -> u64 {
    elapsed.as_millis().min(u128::from(u64::MAX)) as u64
}

fn update_current_timing(update: impl FnOnce(&mut RequestStageTimings)) {
    let _ = REQUEST_STAGE_TIMINGS.try_with(|ctx| {
        let mut guard = ctx.lock().unwrap();
        update(&mut guard);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn default_carrier_is_all_none_and_reused_unknown() {
        let timings = RequestStageTimings::default();

        assert_eq!(timings.bulkhead_wait_ms, None);
        assert_eq!(timings.dns_ms, None);
        assert_eq!(timings.connect_ms, None);
        assert_eq!(timings.connection_reused, None);
    }

    #[tokio::test]
    async fn record_inside_scope_updates_all_fields() {
        let (_, timings) = with_timings(async {
            record_bulkhead_wait(Duration::from_millis(50));
            record_dns(Duration::from_millis(20));
            record_connect(Duration::from_millis(100));
        })
        .await;

        assert_eq!(timings.bulkhead_wait_ms, Some(50));
        assert_eq!(timings.dns_ms, Some(20));
        assert_eq!(timings.connect_ms, Some(100));
    }

    #[test]
    fn record_outside_scope_is_silent_noop() {
        record_bulkhead_wait(Duration::from_millis(50));
        record_dns(Duration::from_millis(20));
        record_connect(Duration::from_millis(100));
    }

    #[tokio::test]
    async fn mark_connector_called_then_finalize_yields_false() {
        let (_, timings) = with_timings(async {
            mark_connector_called();
            finalize_connection_reused_if_unset();
        })
        .await;

        assert_eq!(timings.connection_reused, Some(false));
    }

    #[tokio::test]
    async fn finalize_without_mark_yields_true() {
        let (_, timings) = with_timings(async {
            finalize_connection_reused_if_unset();
        })
        .await;

        assert_eq!(timings.connection_reused, Some(true));
    }

    #[tokio::test]
    async fn with_timings_returns_inner_value_and_snapshot() {
        let (value, timings) = with_timings(async { 42 }).await;

        assert_eq!(value, 42);
        assert_eq!(timings.bulkhead_wait_ms, None);
        assert_eq!(timings.dns_ms, None);
        assert_eq!(timings.connect_ms, None);
        assert_eq!(timings.connection_reused, None);
    }
}
