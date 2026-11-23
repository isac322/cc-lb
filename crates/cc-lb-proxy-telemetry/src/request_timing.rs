use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Outcome of a single DNS resolution attempt inside the connector.
///
/// Recorded explicitly so failure paths never have to infer "DNS ran" from
/// `dns_ms` being non-zero or non-None.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DnsResolutionOutcome {
    /// The resolver returned at least one address.
    Resolved,
    /// The resolver returned an error; no TCP connect was attempted.
    Failed,
}

#[derive(Default, Clone, Debug)]
pub struct RequestStageTimings {
    pub bulkhead_wait_ms: Option<u64>,
    pub dns_ms: Option<u64>,
    pub connect_ms: Option<u64>,
    pub connection_reused: Option<bool>,
    /// Outcome of the most recent DNS resolution in this scope, if any ran.
    pub(crate) dns_outcome: Option<DnsResolutionOutcome>,
    /// Number of DNS resolutions recorded in this scope. The connector diffs
    /// this counter around its inner call to attribute `dns_ms`/`dns_outcome`
    /// to the current connect attempt instead of a stale earlier resolution.
    pub(crate) dns_attempts: u64,
}

/// Point-in-time view of the DNS fields of [`RequestStageTimings`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DnsSnapshot {
    pub ms: Option<u64>,
    pub outcome: Option<DnsResolutionOutcome>,
    pub attempts: u64,
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

pub(crate) fn record_dns(elapsed: Duration, outcome: DnsResolutionOutcome) {
    let elapsed_ms = duration_to_ms(elapsed);
    update_current_timing(|timings| {
        timings.dns_ms = Some(elapsed_ms);
        timings.dns_outcome = Some(outcome);
        timings.dns_attempts = timings.dns_attempts.saturating_add(1);
    });
}

/// Snapshot of the DNS fields of the current scope's timings.
///
/// Outside a [`with_timings`] scope this returns the default (empty) snapshot.
pub(crate) fn dns_snapshot() -> DnsSnapshot {
    REQUEST_STAGE_TIMINGS
        .try_with(|ctx| {
            let timings = ctx.lock().unwrap();
            DnsSnapshot {
                ms: timings.dns_ms,
                outcome: timings.dns_outcome,
                attempts: timings.dns_attempts,
            }
        })
        .unwrap_or_default()
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
        assert_eq!(timings.dns_outcome, None);
        assert_eq!(timings.dns_attempts, 0);
    }

    #[tokio::test]
    async fn record_inside_scope_updates_all_fields() {
        let (_, timings) = with_timings(async {
            record_bulkhead_wait(Duration::from_millis(50));
            record_dns(Duration::from_millis(20), DnsResolutionOutcome::Resolved);
            record_connect(Duration::from_millis(100));
        })
        .await;

        assert_eq!(timings.bulkhead_wait_ms, Some(50));
        assert_eq!(timings.dns_ms, Some(20));
        assert_eq!(timings.connect_ms, Some(100));
        assert_eq!(timings.dns_outcome, Some(DnsResolutionOutcome::Resolved));
        assert_eq!(timings.dns_attempts, 1);
    }

    #[tokio::test]
    async fn record_dns_failure_records_elapsed_and_outcome() {
        let (_, timings) = with_timings(async {
            record_dns(Duration::from_millis(7), DnsResolutionOutcome::Failed);
        })
        .await;

        assert_eq!(timings.dns_ms, Some(7));
        assert_eq!(timings.dns_outcome, Some(DnsResolutionOutcome::Failed));
        assert_eq!(timings.dns_attempts, 1);
    }

    #[tokio::test]
    async fn dns_attempts_counts_each_resolution() {
        let (_, timings) = with_timings(async {
            record_dns(Duration::from_millis(3), DnsResolutionOutcome::Failed);
            record_dns(Duration::from_millis(5), DnsResolutionOutcome::Resolved);
        })
        .await;

        assert_eq!(timings.dns_attempts, 2);
        assert_eq!(timings.dns_ms, Some(5));
        assert_eq!(timings.dns_outcome, Some(DnsResolutionOutcome::Resolved));
    }

    #[tokio::test]
    async fn dns_snapshot_reflects_scope_and_defaults_outside() {
        assert_eq!(dns_snapshot(), DnsSnapshot::default());

        let (_, timings) = with_timings(async {
            record_dns(Duration::from_millis(9), DnsResolutionOutcome::Resolved);
            assert_eq!(
                dns_snapshot(),
                DnsSnapshot {
                    ms: Some(9),
                    outcome: Some(DnsResolutionOutcome::Resolved),
                    attempts: 1,
                }
            );
        })
        .await;

        assert_eq!(timings.dns_attempts, 1);
    }

    #[test]
    fn record_outside_scope_is_silent_noop() {
        record_bulkhead_wait(Duration::from_millis(50));
        record_dns(Duration::from_millis(20), DnsResolutionOutcome::Resolved);
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
