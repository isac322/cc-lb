use anyhow::{Result, ensure};
use cc_lb_storage_api::ThreadStickySubagentStore;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B: ConformanceBackend>(backend: std::sync::Arc<B>) -> Result<()> {
    with_conformance_fixture(backend, |storage| async move {
        // Miss before seeding.
        ensure!(
            storage
                .is_thread_sticky_subagent("p-sticky", "sess-1", 1_000)
                .await?
                .is_none()
        );

        // Seed with TTL 100 (expires at 1_100).
        storage
            .seed_thread_sticky_subagent("p-sticky", "sess-1", 1_000, 100)
            .await?;
        ensure!(
            storage
                .is_thread_sticky_subagent("p-sticky", "sess-1", 1_050)
                .await?
                == Some(1_100)
        );
        // Strict `>` on expires_at boundary: expired at 1_100.
        ensure!(
            storage
                .is_thread_sticky_subagent("p-sticky", "sess-1", 1_100)
                .await?
                .is_none()
        );
        // Re-seeding with an earlier expiry must not regress existing `expires_at` (1_100).
        storage
            .seed_thread_sticky_subagent("p-sticky", "sess-1", 1_020, 10)
            .await?;
        ensure!(
            storage
                .is_thread_sticky_subagent("p-sticky", "sess-1", 1_099)
                .await?
                == Some(1_100)
        );
        // Extending expiry moves it forward.
        storage
            .seed_thread_sticky_subagent("p-sticky", "sess-1", 1_080, 200)
            .await?;
        ensure!(
            storage
                .is_thread_sticky_subagent("p-sticky", "sess-1", 1_279)
                .await?
                == Some(1_280)
        );

        // Seed additional expired and active rows and verify bounded purge.
        storage
            .seed_thread_sticky_subagent("p-sticky", "sess-expired-1", 1_000, 50)
            .await?;
        storage
            .seed_thread_sticky_subagent("p-sticky", "sess-expired-2", 1_000, 60)
            .await?;
        let first_batch = storage
            .purge_expired_thread_sticky_subagents(1_100, 1)
            .await?;
        ensure!(
            first_batch == 1,
            "expected bounded purge of 1 row, got {first_batch}"
        );
        let second_batch = storage
            .purge_expired_thread_sticky_subagents(1_100, 10)
            .await?;
        ensure!(
            second_batch == 1,
            "expected second batch purge of 1 row, got {second_batch}"
        );
        ensure!(
            storage
                .is_thread_sticky_subagent("p-sticky", "sess-1", 1_100)
                .await?
                == Some(1_280),
            "unexpired sticky session must survive purge with preserved expiry"
        );

        Ok(())
    })
    .await
}
