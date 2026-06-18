use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cc_lb_scheduler::leader_election::LeaderElection;

#[cfg(feature = "postgres")]
const TEST_LOCK_KEY: i64 = 0x0000_CC1B_5CDE_1009_u64 as i64;

#[tokio::test]
async fn shared_modules_leader_election_sqlite_runs_work_as_leader() {
    let election = LeaderElection::sqlite();
    let ran = Arc::new(AtomicBool::new(false));
    let ran_clone = Arc::clone(&ran);

    election
        .run(|| async move {
            ran_clone.store(true, Ordering::SeqCst);
        })
        .await
        .expect("sqlite leader run succeeds");

    assert!(ran.load(Ordering::SeqCst));
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn shared_modules_leader_election_postgres_holds_advisory_lock_on_dedicated_connection() {
    let Some(database_url) = std::env::var("DATABASE_URL").ok() else {
        eprintln!("SKIP: DATABASE_URL not set; skipping postgres leader election test");
        return;
    };

    let first = LeaderElection::postgres(&database_url, TEST_LOCK_KEY)
        .await
        .expect("first leader connects");
    let second = LeaderElection::postgres(&database_url, TEST_LOCK_KEY)
        .await
        .expect("second leader connects");

    assert!(first.try_acquire().await.expect("first acquire succeeds"));
    assert!(!second.try_acquire().await.expect("second acquire succeeds"));
    first.heartbeat().await.expect("heartbeat succeeds");
    assert!(first.release().await.expect("release succeeds"));
    assert!(
        second
            .try_acquire()
            .await
            .expect("standby acquire succeeds")
    );
    assert!(second.release().await.expect("standby release succeeds"));
}
