use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RateLimitKind, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: UpstreamRateLimitStateStore,
{
    put_then_list_for_upstream_ids_roundtrip(Arc::clone(&backend)).await?;
    latest_write_wins_within_same_key(Arc::clone(&backend)).await?;
    latest_write_wins_within_same_key_forward(Arc::clone(&backend)).await?;
    empty_list_for_unknown_id(backend).await?;
    Ok(())
}

macro_rules! scenario {
    ($name:ident, $body:expr) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: UpstreamRateLimitStateStore,
        {
            with_conformance_fixture(backend, $body).await
        }
    };
}

scenario!(
    put_then_list_for_upstream_ids_roundtrip,
    |storage| async move {
        let upstream_a = upstream_id(1);
        let upstream_b = upstream_id(2);
        let observations = vec![
            observation(upstream_a, "minute", RateLimitKind::Requests, 100, 90),
            observation(upstream_b, "minute", RateLimitKind::Requests, 101, 91),
            observation(upstream_a, "hour", RateLimitKind::InputTokens, 102, 92),
        ];

        for observation in &observations {
            storage.put_observation(observation).await?;
        }

        let listed = storage
            .list_for_upstream_ids(&[upstream_a, upstream_b])
            .await?;
        let mut expected = observations;
        sort_observations(&mut expected);
        ensure!(listed == expected, "listed observations should round-trip");
        Ok(())
    }
);

scenario!(latest_write_wins_within_same_key, |storage| async move {
    let upstream_id = upstream_id(3);
    let latest = observation(upstream_id, "minute", RateLimitKind::Requests, 100, 80);
    let stale = observation(upstream_id, "minute", RateLimitKind::Requests, 99, 79);

    storage.put_observation(&latest).await?;
    storage.put_observation(&stale).await?;

    let listed = storage.list_for_upstream_ids(&[upstream_id]).await?;
    ensure!(
        listed == [latest],
        "older observation must not replace newer one"
    );
    Ok(())
});

scenario!(
    latest_write_wins_within_same_key_forward,
    |storage| async move {
        let upstream_id = upstream_id(4);
        let stale = observation(upstream_id, "minute", RateLimitKind::Requests, 100, 70);
        let latest = observation(upstream_id, "minute", RateLimitKind::Requests, 200, 60);

        storage.put_observation(&stale).await?;
        storage.put_observation(&latest).await?;

        let listed = storage.list_for_upstream_ids(&[upstream_id]).await?;
        ensure!(
            listed == [latest],
            "newer observation should replace older one"
        );
        Ok(())
    }
);

scenario!(empty_list_for_unknown_id, |storage| async move {
    let known_upstream_id = upstream_id(5);
    let unknown_upstream_id = upstream_id(6);

    storage
        .put_observation(&observation(
            known_upstream_id,
            "minute",
            RateLimitKind::Requests,
            100,
            50,
        ))
        .await?;

    let listed = storage
        .list_for_upstream_ids(&[unknown_upstream_id])
        .await?;
    ensure!(
        listed.is_empty(),
        "unknown upstream should have no observations"
    );
    Ok(())
});

fn observation(
    upstream_id: Uuid,
    window: &str,
    kind: RateLimitKind,
    observed_at_unix_secs: u64,
    remaining: u64,
) -> UpstreamRateLimitObservationRecord {
    UpstreamRateLimitObservationRecord {
        upstream_id,
        window: window.to_owned(),
        kind,
        limit: Some(observed_at_unix_secs + 1_000),
        remaining: Some(remaining),
        reset: Some(format!("reset-{observed_at_unix_secs}")),
        observed_at_unix_secs,
    }
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn sort_observations(records: &mut [UpstreamRateLimitObservationRecord]) {
    records.sort_by(|left, right| {
        left.upstream_id
            .cmp(&right.upstream_id)
            .then_with(|| left.window.cmp(&right.window))
            .then_with(|| left.kind.as_str().cmp(right.kind.as_str()))
    });
}
