use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use cc_lb_clock::TestClock;
use cc_lb_storage_api::{AuditEntry, AuditQueryScope, AuditStore, BackendKind, MetaStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

const NOW: u64 = 10_000;
const AUTHORITY: &str = "https://identity.example";
const SUBJECT: &str = "alice";

#[test]
fn query_audit_by_actor_filters_before_limit() {
    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        eprintln!(
            "skip: CI_POSTGRES_URL or PG_URL not set; requires isolated local/test postgres DSN"
        );
        return;
    };
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let fixture = Fixture::create(&url).await?;
            let result = run_contract(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop audit actor query schema")?;
            result
        })
        .expect("PostgreSQL audit actor query contract");
}

#[test]
fn query_recent_audit_orders_before_limit_and_filters_exactly() {
    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        eprintln!(
            "skip: CI_POSTGRES_URL or PG_URL not set; requires isolated local/test postgres DSN"
        );
        return;
    };
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let fixture = Fixture::create(&url).await?;
            let result = run_recent_contract(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop recent audit query schema")?;
            result
        })
        .expect("PostgreSQL recent audit query contract");
}

async fn run_contract(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(TestClock::new_at_secs(NOW)));
    storage.initialize(BackendKind::Postgres).await?;
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
        storage.append_audit(entry).await?;
    }

    let rows = storage
        .query_audit_by_actor(AUTHORITY, SUBJECT, NOW, NOW + 5, 2)
        .await?;

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
    Ok(())
}

async fn run_recent_contract(pool: &PgPool) -> Result<()> {
    let storage = PostgresStorage::new(pool.clone(), Arc::new(TestClock::new_at_secs(NOW)));
    storage.initialize(BackendKind::Postgres).await?;
    let mut entries = [
        recent_audit_entry("matching-old", NOW, "target-principal", AUTHORITY, SUBJECT),
        recent_audit_entry(
            "other-principal-newest-match",
            NOW + 9,
            "other-principal",
            AUTHORITY,
            SUBJECT,
        ),
        recent_audit_entry(
            "wrong-authority-newer",
            NOW + 11,
            "target-principal",
            "https://other.example",
            SUBJECT,
        ),
        recent_audit_entry(
            "wrong-subject-newer",
            NOW + 10,
            "target-principal",
            AUTHORITY,
            "bob",
        ),
        recent_audit_entry(
            "matching-tie-first",
            NOW + 8,
            "target-principal",
            AUTHORITY,
            SUBJECT,
        ),
        recent_audit_entry(
            "matching-tie-second",
            NOW + 8,
            "target-principal",
            AUTHORITY,
            SUBJECT,
        ),
        recent_audit_entry(
            "late-inserted-old-timestamp",
            NOW + 2,
            "target-principal",
            AUTHORITY,
            SUBJECT,
        ),
        recent_audit_entry(
            "admin-action-old",
            NOW,
            "target-principal",
            AUTHORITY,
            SUBJECT,
        ),
        recent_audit_entry(
            "admin-kind-only",
            NOW + 1,
            "other-principal",
            AUTHORITY,
            "bob",
        ),
    ];
    entries[7].admin_action = Some("principal_update".to_owned());
    entries[8].kind = Some("admin".to_owned());
    for entry in &entries {
        storage.append_audit(entry).await?;
    }

    let all_rows = storage
        .query_recent_audit(AuditQueryScope::All, NOW, NOW + 11, 5, false)
        .await?;
    assert_request_ids(
        &all_rows,
        &[
            "wrong-authority-newer",
            "wrong-subject-newer",
            "other-principal-newest-match",
            "matching-tie-second",
            "matching-tie-first",
        ],
    );

    let all_with_late_row = storage
        .query_recent_audit(AuditQueryScope::All, NOW, NOW + 11, 6, false)
        .await?;
    assert_eq!(
        all_with_late_row
            .last()
            .map(|entry| entry.request_id.as_str()),
        Some("late-inserted-old-timestamp")
    );

    let principal_rows = storage
        .query_recent_audit(
            AuditQueryScope::Principal("target-principal"),
            NOW,
            NOW + 11,
            4,
            false,
        )
        .await?;
    assert_request_ids(
        &principal_rows,
        &[
            "wrong-authority-newer",
            "wrong-subject-newer",
            "matching-tie-second",
            "matching-tie-first",
        ],
    );

    let actor_rows = storage
        .query_recent_audit(
            AuditQueryScope::Actor {
                authority: AUTHORITY,
                subject: SUBJECT,
            },
            NOW,
            NOW + 11,
            3,
            false,
        )
        .await?;
    assert_request_ids(
        &actor_rows,
        &[
            "other-principal-newest-match",
            "matching-tie-second",
            "matching-tie-first",
        ],
    );

    let tied_boundary_rows = storage
        .query_recent_audit(AuditQueryScope::All, NOW + 8, NOW + 8, 10, false)
        .await?;
    assert_request_ids(
        &tied_boundary_rows,
        &["matching-tie-second", "matching-tie-first"],
    );

    assert!(
        storage
            .query_recent_audit(AuditQueryScope::All, NOW, NOW + 11, 0, true)
            .await?
            .is_empty()
    );
    assert!(
        storage
            .query_recent_audit(AuditQueryScope::All, NOW + 1, NOW, 10, true)
            .await?
            .is_empty()
    );

    // admin_only applies its predicate before LIMIT: with these limits the
    // newest non-admin rows would exhaust the window and hide every admin row
    // if the predicate ran after LIMIT.
    let admin_rows = storage
        .query_recent_audit(AuditQueryScope::All, NOW, NOW + 11, 5, true)
        .await?;
    assert_request_ids(&admin_rows, &["admin-kind-only", "admin-action-old"]);

    let principal_admin_rows = storage
        .query_recent_audit(
            AuditQueryScope::Principal("target-principal"),
            NOW,
            NOW + 11,
            3,
            true,
        )
        .await?;
    assert_request_ids(&principal_admin_rows, &["admin-action-old"]);

    let actor_admin_rows = storage
        .query_recent_audit(
            AuditQueryScope::Actor {
                authority: AUTHORITY,
                subject: SUBJECT,
            },
            NOW,
            NOW + 11,
            3,
            true,
        )
        .await?;
    assert_request_ids(&actor_admin_rows, &["admin-action-old"]);

    let bounded_admin_rows = storage
        .query_recent_audit(AuditQueryScope::All, NOW + 1, NOW + 1, 200, true)
        .await?;
    assert_request_ids(&bounded_admin_rows, &["admin-kind-only"]);
    Ok(())
}

fn assert_request_ids(rows: &[AuditEntry], expected: &[&str]) {
    let actual = rows
        .iter()
        .map(|entry| entry.request_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(actual.as_slice(), expected);
}

fn recent_audit_entry(
    request_id: &str,
    ts: u64,
    principal_id: &str,
    authority: &str,
    subject: &str,
) -> AuditEntry {
    AuditEntry {
        principal_id: principal_id.to_owned(),
        ..audit_entry(request_id, ts, authority, subject)
    }
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

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("audit_actor_query_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        Ok(Self {
            schema,
            admin_pool,
            pool,
        })
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_')
    );
    format!("\"{identifier}\"")
}
