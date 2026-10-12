//! A committed config draft PUT still writes its admin audit row when the
//! client disconnects while that audit write is pending.

use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{Request, header};
use cc_lb_clock::Clock;
use cc_lb_storage_api::{AuditEntry, AuditQueryScope, AuditStore, ConfigStore};
use cc_lb_storage_sqlite::SqliteStorage;
use serde_json::json;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tower::ServiceExt;

use crate::config_admin_common::{
    TOKEN, app, minimal_config, put_body, temp_storage, test_state_with_clock,
};

/// Fails the test instead of hanging when an expected event never happens.
const HANG_GUARD: Duration = Duration::from_secs(30);

const DRAFT_ROUTE: &str = "/admin/v1/config/draft";

/// Wall clock that, once armed, lets `passing_reads` reads through and then
/// blocks the next read until the test releases it. The admin audit writer
/// reads the clock right before inserting the audit row.
struct GateClock {
    remaining_reads: Mutex<Option<usize>>,
    reached: UnboundedSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl GateClock {
    fn arm_after(&self, passing_reads: usize) {
        *self.remaining_reads.lock().expect("gate counter lock") = Some(passing_reads);
    }
}

impl Clock for GateClock {
    fn now(&self) -> SystemTime {
        let hold = {
            let mut remaining = self.remaining_reads.lock().expect("gate counter lock");
            match *remaining {
                Some(0) => {
                    *remaining = None;
                    true
                }
                Some(reads) => {
                    *remaining = Some(reads - 1);
                    false
                }
                None => false,
            }
        };
        if hold {
            self.reached.send(()).expect("test awaits the gate");
            self.release
                .lock()
                .expect("gate receiver lock")
                .recv()
                .expect("test releases the gate");
        }
        SystemTime::now()
    }
}

async fn draft_put_audit_rows(storage: &SqliteStorage) -> Vec<AuditEntry> {
    storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 100, false)
        .await
        .expect("audit query")
        .into_iter()
        .filter(|entry| {
            entry.route == DRAFT_ROUTE && entry.admin_action.as_deref() == Some("config_draft_put")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_draft_put_writes_audit_after_request_is_dropped() {
    let (reached_tx, mut reached) = unbounded_channel();
    let (release, release_rx) = mpsc::channel();
    let clock = Arc::new(GateClock {
        remaining_reads: Mutex::new(None),
        reached: reached_tx,
        release: Mutex::new(release_rx),
    });
    let (_dir, storage) = temp_storage().await;
    let app = app(test_state_with_clock(
        minimal_config(),
        Some(storage.clone()),
        clock.clone(),
    ));

    // The handler reads the clock once for the draft timestamp before the
    // commit; the next read is the audit writer's.
    clock.arm_after(1);
    let request = Request::builder()
        .method("PUT")
        .uri(DRAFT_ROUTE)
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            put_body(json!({ "not_config": true }), 0).to_string(),
        ))
        .expect("request builds");
    let request = tokio::spawn(app.oneshot(request));
    tokio::time::timeout(HANG_GUARD, reached.recv())
        .await
        .expect("audit write reaches the gate")
        .expect("gate reports");

    let draft = storage.get_config_draft().await.expect("draft read");
    assert_eq!(
        draft.revision, 1,
        "the draft committed before the audit gate"
    );
    assert!(draft_put_audit_rows(&storage).await.is_empty());

    // Hold every pool connection so the audit insert, once released from the
    // gate, waits for a connection without sending anything to SQLite.
    let pool = storage.pool();
    let mut held = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        held.push(
            tokio::time::timeout(HANG_GUARD, pool.acquire())
                .await
                .expect("pool connection is free")
                .expect("pool connection"),
        );
    }

    // The client disconnects: the request future is dropped.
    request.abort();
    release.send(()).expect("gate is waiting");
    let joined = request.await;
    assert!(
        joined.is_err_and(|error| error.is_cancelled()),
        "the request future was dropped"
    );
    drop(held);

    let rows = tokio::time::timeout(HANG_GUARD, async {
        loop {
            let rows = draft_put_audit_rows(&storage).await;
            if !rows.is_empty() {
                return rows;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the dropped draft PUT still writes its audit row");
    assert_eq!(rows.len(), 1);
}
