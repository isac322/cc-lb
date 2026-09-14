use std::sync::Arc;

use cc_lb_clock::TestClock;
use cc_lb_storage_api::{AuditEntry, AuditStore, BackendKind, MetaStore};

const NOW: u64 = 10_000;
const AUTHORITY: &str = "https://identity.example";
const SUBJECT: &str = "alice";

async fn storage() -> (tempfile::TempDir, cc_lb_storage_sqlite::SqliteStorage) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        directory.path().join("audit.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(TestClock::new_at_secs(NOW)))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("migrate sqlite");
    (directory, storage)
}

fn audit_entry(request_id: &str, ts: u64, authority: &str, subject: &str) -> AuditEntry {
    AuditEntry {
        ts,
        request_id: request_id.to_owned(),
        principal_id: "target-principal".to_owned(),
        route: "/admin/v1/audit-test".to_owned(),
        upstream: "admin".to_owned(),
        status: 200,
        actor: Some(format!("{subject}@example.com")),
        actor_authority: Some(authority.to_owned()),
        actor_subject: Some(subject.to_owned()),
        actor_kind: Some("human".to_owned()),
        actor_email: Some(format!("{subject}@example.com")),
        ..Default::default()
    }
}

#[tokio::test]
async fn query_audit_by_actor_filters_before_limit() {
    let (_directory, storage) = storage().await;
    let entries = [
        audit_entry("other-subject-older", NOW, AUTHORITY, "bob"),
        audit_entry(
            "other-authority-older",
            NOW + 1,
            "https://other.example",
            SUBJECT,
        ),
        audit_entry("alice-first", NOW + 2, AUTHORITY, SUBJECT),
        audit_entry("other-subject-newer", NOW + 3, AUTHORITY, "bob"),
        audit_entry("alice-second", NOW + 4, AUTHORITY, SUBJECT),
        audit_entry("alice-third", NOW + 5, AUTHORITY, SUBJECT),
    ];
    for entry in &entries {
        storage.append_audit(entry).await.expect("append audit");
    }

    let rows = storage
        .query_audit_by_actor(AUTHORITY, SUBJECT, NOW, NOW + 5, 2)
        .await
        .expect("query audit by actor");

    assert_eq!(
        rows.iter()
            .map(|entry| entry.request_id.as_str())
            .collect::<Vec<_>>(),
        ["alice-first", "alice-second"]
    );
    assert!(rows.iter().all(|entry| {
        entry.actor_authority.as_deref() == Some(AUTHORITY)
            && entry.actor_subject.as_deref() == Some(SUBJECT)
            && entry.actor_kind.as_deref() == Some("human")
            && entry.actor_email.as_deref() == Some("alice@example.com")
    }));
}
