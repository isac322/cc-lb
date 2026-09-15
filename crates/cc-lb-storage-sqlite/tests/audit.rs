use std::sync::Arc;

use cc_lb_clock::TestClock;
use cc_lb_storage_api::{AuditEntry, AuditQueryScope, AuditStore, BackendKind, MetaStore};

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

fn request_ids(entries: &[AuditEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.request_id.as_str())
        .collect()
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

#[tokio::test]
async fn query_audit_preserves_append_order() {
    let (_directory, storage) = storage().await;
    let entries = [
        audit_entry("first-newer", NOW + 2, AUTHORITY, SUBJECT),
        audit_entry("second-backfilled", NOW, AUTHORITY, SUBJECT),
        audit_entry("third-same-timestamp", NOW + 2, AUTHORITY, SUBJECT),
    ];
    storage
        .append_audit_entries(&entries)
        .await
        .expect("append audit entries");

    let rows = storage
        .query_audit(None, NOW, NOW + 2, 10)
        .await
        .expect("query audit");

    assert_eq!(
        request_ids(&rows),
        ["first-newer", "second-backfilled", "third-same-timestamp"]
    );
}

#[tokio::test]
async fn query_recent_audit_limits_after_latest_ordering() {
    let (_directory, storage) = storage().await;
    let mut entries = Vec::with_capacity(208);
    for index in 0..203 {
        entries.push(audit_entry(
            &format!("request-{index:03}"),
            NOW + index,
            AUTHORITY,
            SUBJECT,
        ));
    }
    entries.push(audit_entry("late-backfill", NOW + 1, AUTHORITY, SUBJECT));
    entries.push(audit_entry(
        "latest-tie-first",
        NOW + 202,
        AUTHORITY,
        SUBJECT,
    ));
    entries.push(audit_entry(
        "latest-tie-second",
        NOW + 202,
        AUTHORITY,
        SUBJECT,
    ));
    entries.push(audit_entry("before-range", NOW - 1, AUTHORITY, SUBJECT));
    entries.push(audit_entry("after-range", NOW + 203, AUTHORITY, SUBJECT));
    storage
        .append_audit_entries(&entries)
        .await
        .expect("append audit entries");

    let rows = storage
        .query_recent_audit(AuditQueryScope::All, NOW, NOW + 202, 200)
        .await
        .expect("query recent audit");

    assert_eq!(rows.len(), 200);
    assert_eq!(
        &request_ids(&rows)[..3],
        ["latest-tie-second", "latest-tie-first", "request-202"]
    );
    assert_eq!(rows.last().unwrap().request_id, "request-005");
    assert!(rows.iter().all(|entry| {
        !matches!(
            entry.request_id.as_str(),
            "late-backfill" | "before-range" | "after-range"
        )
    }));
}

#[tokio::test]
async fn query_recent_audit_filters_before_limit() {
    let (_directory, storage) = storage().await;
    let mut other_principal_actor_match = audit_entry("actor-match-old", NOW, AUTHORITY, SUBJECT);
    other_principal_actor_match.principal_id = "other-principal".to_owned();
    let principal_match_old = audit_entry("principal-match-old", NOW + 1, AUTHORITY, "bob");
    let mut other_authority =
        audit_entry("other-authority", NOW + 2, "https://other.example", SUBJECT);
    other_authority.principal_id = "other-principal".to_owned();
    let principal_match_new = audit_entry("principal-match-new", NOW + 3, AUTHORITY, "bob");
    let mut other_principal_actor_match_new =
        audit_entry("actor-match-new", NOW + 4, AUTHORITY, SUBJECT);
    other_principal_actor_match_new.principal_id = "other-principal".to_owned();
    let mut newest_decoy = audit_entry("newest-decoy", NOW + 5, AUTHORITY, "bob");
    newest_decoy.principal_id = "other-principal".to_owned();
    storage
        .append_audit_entries(&[
            other_principal_actor_match,
            principal_match_old,
            other_authority,
            principal_match_new,
            other_principal_actor_match_new,
            newest_decoy,
        ])
        .await
        .expect("append audit entries");

    let principal_rows = storage
        .query_recent_audit(
            AuditQueryScope::Principal("target-principal"),
            NOW,
            NOW + 5,
            2,
        )
        .await
        .expect("query recent audit by principal");
    assert_eq!(
        request_ids(&principal_rows),
        ["principal-match-new", "principal-match-old"]
    );

    let actor_rows = storage
        .query_recent_audit(
            AuditQueryScope::Actor {
                authority: AUTHORITY,
                subject: SUBJECT,
            },
            NOW,
            NOW + 5,
            2,
        )
        .await
        .expect("query recent audit by actor");
    assert_eq!(
        request_ids(&actor_rows),
        ["actor-match-new", "actor-match-old"]
    );
}

#[tokio::test]
async fn query_recent_audit_returns_empty_for_zero_limit_or_inverted_range() {
    let (_directory, storage) = storage().await;
    storage
        .append_audit(&audit_entry("present", NOW, AUTHORITY, SUBJECT))
        .await
        .expect("append audit");

    let zero_limit = storage
        .query_recent_audit(AuditQueryScope::All, u64::MAX, u64::MAX, 0)
        .await
        .expect("query recent audit with zero limit");
    assert!(zero_limit.is_empty());

    let inverted = storage
        .query_recent_audit(AuditQueryScope::All, NOW + 1, NOW, 10)
        .await
        .expect("query recent audit with inverted range");
    assert!(inverted.is_empty());
}
