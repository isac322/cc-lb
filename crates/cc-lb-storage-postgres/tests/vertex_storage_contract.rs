use std::{error::Error, str::FromStr, sync::Arc, time::Duration};

use cc_lb_aead::AeadService;
use cc_lb_storage_api::{
    MetaStore, StorageError, UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStatusUpdate,
    UpstreamStore, UpstreamUpdate,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, Connection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct Fixture {
    schema: String,
    admin: PgPool,
    options: PgConnectOptions,
}

impl Fixture {
    async fn create() -> TestResult<Option<Self>> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skipped: CI_POSTGRES_URL unset; Vertex PostgreSQL QA did not run");
            return Ok(None);
        };
        let schema = format!("vertex_storage_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await?;
        let options = PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
        Ok(Some(Self {
            schema,
            admin,
            options,
        }))
    }

    async fn pool(&self, max_connections: u32) -> TestResult<PgPool> {
        Ok(PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(self.options.clone())
            .await?)
    }

    async fn drop(self) -> TestResult {
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await?;
        self.admin.close().await;
        Ok(())
    }
}
async fn create_vertex(storage: &PostgresStorage, name: &str) -> TestResult<UpstreamRecord> {
    Ok(storage
        .create(UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::VertexCredential,
            base_url: Some("https://vertex.example.test".parse()?),
            api_key_ciphertext: Some(vec![1]),
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: Some("workspace-qa".to_owned()),
            vertex: Some(cc_lb_domain::VertexUpstreamConfig {
                target: cc_lb_domain::VertexTarget::Regional {
                    project_id: "project-12345".into(),
                    location: "us-central1".into(),
                },
                api_version: cc_lb_domain::VertexApiVersion::V1Beta1,
            }),
            vertex_credential_type: Some(cc_lb_domain::VertexCredentialType::ServiceAccount),
            vertex_quota_project_id: Some("quota-project-123".to_owned()),
            api_key_access: None,
        })
        .await?)
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn qa_sto_2_postgres_0174_to_0175_applies_kind_check_and_honors_one_second_lock_timeout()
-> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.pool(4).await?;
        let mut connection = sqlx::PgConnection::connect_with(&fixture.options).await?;
        sqlx::migrate!("./migrations").run_direct(Some(174), &mut connection, false).await?;
        connection.close().await?;
        let id = Uuid::from_u128(0x1731);
        sqlx::query(
            "INSERT INTO upstream_spec_v1
             (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,workspace_id,owner_epoch)
             VALUES ($1,'qa-sto-2-source','gemini_api_key',TRUE,FALSE,4,NOW(),NOW(),'workspace-qa',8)",
        )
        .bind(id)
        .execute(&pool)
        .await?;
        let mut blocker = pool.begin().await?;
        sqlx::query("LOCK TABLE upstream_spec_v1 IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await?;
        let mut blocked = sqlx::PgConnection::connect_with(&fixture.options).await?;
        let started = std::time::Instant::now();
        let error = sqlx::migrate!("./migrations").run_direct(None, &mut blocked, false).await
            .expect_err("0175 must fail closed when the one-second lock timeout expires");
        assert!(started.elapsed() < Duration::from_secs(5), "lock timeout was not bounded: {error}");
        assert!(error.to_string().contains("lock timeout"), "unexpected lock error: {error}");
        blocked.close().await?;
        blocker.rollback().await?;
        let mut apply = sqlx::PgConnection::connect_with(&fixture.options).await?;
        sqlx::migrate!("./migrations").run_direct(None, &mut apply, false).await?;
        apply.close().await?;
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        let row: (i64, String, String, i64) = sqlx::query_as(
            "SELECT spec_revision,workspace_id,kind,owner_epoch FROM upstream_spec_v1 WHERE id=$1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(row, (4, "workspace-qa".to_owned(), "gemini_api_key".to_owned(), 8));
        let constraints: Vec<String> = sqlx::query_scalar(
            "SELECT conname FROM pg_constraint WHERE conrelid='upstream_spec_v1'::regclass AND conname LIKE '%vertex%'",
        )
        .fetch_all(&pool)
        .await?;
        assert!(constraints.iter().any(|name| name == "upstream_spec_v1_vertex_config_check"));
        storage.verify_schema_current().await?;
        pool.close().await;
        Ok(())
    }.await;
    let teardown = fixture.drop().await;
    result?;
    teardown
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn qa_sto_3_postgres_vertex_check_rejects_invalid_rows_and_accepts_positive_locations()
-> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.pool(4).await?;
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        MetaStore::initialize(&storage).await?;
        for (index, location) in ["global", "us", "eu", "us-central1", "europe-west4"].into_iter().enumerate() {
            let record = storage.create(UpstreamCreate {
                id: Uuid::new_v4(), name: format!("qa-sto-3-valid-{index}"), kind: UpstreamKind::VertexApiKey,
                base_url: None, api_key_ciphertext: Some(vec![1]), oauth_tokens: None,
                warmup_enabled: false, warmup_dialect_plugin: None, workspace_id: None,
                vertex: Some(cc_lb_domain::VertexUpstreamConfig {
                    target: cc_lb_domain::VertexTarget::Regional { project_id: "project-12345".into(), location: location.into() },
                    api_version: cc_lb_domain::VertexApiVersion::V1Beta1,
                }), vertex_credential_type: None, vertex_quota_project_id: None, api_key_access: None,
            }).await?;
            assert_eq!(record.kind, UpstreamKind::VertexApiKey);
        }
        let invalid_sql = [
            "UPDATE upstream_spec_v1 SET vertex_api_version='v2' WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
            "UPDATE upstream_spec_v1 SET vertex_location='US-central1' WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
            "UPDATE upstream_spec_v1 SET vertex_project_id='Bad.Project' WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
            "UPDATE upstream_spec_v1 SET vertex_express=TRUE, vertex_project_id='project-12345' WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
            "UPDATE upstream_spec_v1 SET vertex_project_id=NULL WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
            "UPDATE upstream_spec_v1 SET vertex_location=NULL WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_project_id,vertex_location) VALUES ('00000000-0000-0000-0000-000000009997','bad-missing-version','vertex_api_key',TRUE,FALSE,1,NOW(),NOW(),'project-12345','us-central1')",
            "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_api_version) VALUES ('00000000-0000-0000-0000-000000009999','bad-nonvertex','gemini_api_key',TRUE,FALSE,1,NOW(),NOW(),'v1')",
            "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_project_id,vertex_location,vertex_api_version,vertex_express) VALUES ('00000000-0000-0000-0000-000000009990','bad-null-express','vertex_api_key',TRUE,FALSE,1,NOW(),NOW(),'project-12345','us-central1','v1',NULL)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_project_id,vertex_location,vertex_api_version,vertex_express) VALUES ('00000000-0000-0000-0000-000000009991','bad-null-version','vertex_api_key',TRUE,FALSE,1,NOW(),NOW(),'project-12345','us-central1',NULL,FALSE)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_project_id,vertex_location,vertex_api_version,vertex_express) VALUES ('00000000-0000-0000-0000-000000009992','bad-credential-type','vertex_credential',TRUE,FALSE,1,NOW(),NOW(),'project-12345','us-central1','v1',FALSE)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_api_version,vertex_express,vertex_credential_type) VALUES ('00000000-0000-0000-0000-000000009993','bad-credential-express','vertex_credential',TRUE,FALSE,1,NOW(),NOW(),'v1',TRUE,'service_account')",
            "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_credential_type) VALUES ('00000000-0000-0000-0000-000000009994','bad-nonvertex-credential-type','gemini_api_key',TRUE,FALSE,1,NOW(),NOW(),'service_account')",
        ];
        for sql in invalid_sql {
            assert!(sqlx::query(sql).execute(&pool).await.is_err(), "must reject: {sql}");
        }
        pool.close().await;
        Ok(())
    }.await;
    let teardown = fixture.drop().await;
    result?;
    teardown
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn qa_sto_4_postgres_vertex_record_round_trips_without_configuration_loss() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.pool(4).await?;
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        MetaStore::initialize(&storage).await?;
        let created = storage
            .create(UpstreamCreate {
                id: Uuid::new_v4(),
                name: "qa-sto-4-postgres-roundtrip".to_owned(),
                kind: UpstreamKind::VertexCredential,
                base_url: Some("https://vertex.example.test".parse()?),
                api_key_ciphertext: Some(vec![1]),
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: Some("workspace-qa".to_owned()),
                vertex: Some(cc_lb_domain::VertexUpstreamConfig {
                    target: cc_lb_domain::VertexTarget::Regional {
                        project_id: "project-12345".into(),
                        location: "us-central1".into(),
                    },
                    api_version: cc_lb_domain::VertexApiVersion::V1,
                }),
                vertex_credential_type: Some(cc_lb_domain::VertexCredentialType::AuthorizedUser),
                vertex_quota_project_id: Some("quota-project-123".to_owned()),
                api_key_access: None,
            })
            .await?;
        let loaded = storage
            .get_by_id(created.id)
            .await?
            .expect("created record");
        assert_eq!(loaded, created);
        let raw: (String, String, String, String, bool, String) = sqlx::query_as(
            "SELECT kind, base_url, vertex_project_id, vertex_location,
                    vertex_express, vertex_api_version
             FROM upstream_spec_v1 WHERE id=$1",
        )
        .bind(created.id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            raw,
            (
                "vertex_credential".to_owned(),
                "https://vertex.example.test/".to_owned(),
                "project-12345".to_owned(),
                "us-central1".to_owned(),
                false,
                "v1".to_owned(),
            )
        );
        pool.close().await;
        Ok(())
    }
    .await;
    let teardown = fixture.drop().await;
    result?;
    teardown
}
#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn qa_sto_5_through_8_postgres_epochs_status_latch_rebuild_and_peer_material() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.pool(4).await?;
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        MetaStore::initialize(&storage).await?;
        let created = create_vertex(&storage, "qa-sto-5-8-postgres").await?;
        let version_only = storage
            .update_spec(
                created.id,
                created.revision,
                UpstreamUpdate {
                    vertex: Some(Some(cc_lb_domain::VertexUpstreamConfig {
                        target: cc_lb_domain::VertexTarget::Regional {
                            project_id: "project-12345".into(),
                            location: "us-central1".into(),
                        },
                        api_version: cc_lb_domain::VertexApiVersion::V1,
                    })),
                    ..UpstreamUpdate::default()
                },
            )
            .await?;
        assert_eq!(version_only.owner_epoch, 0);
        storage
            .set_status(
                version_only.id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some(
                        cc_lb_domain::VERTEX_CREDENTIAL_INVALID_GRANT.to_owned(),
                    )),
                    expected_spec_revision: Some(version_only.revision),
                    ..UpstreamStatusUpdate::default()
                },
            )
            .await?;
        let stale = storage
            .set_status(
                version_only.id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some("stale".to_owned())),
                    expected_spec_revision: Some(version_only.revision - 1),
                    ..UpstreamStatusUpdate::default()
                },
            )
            .await
            .expect_err("stale status must fail");
        assert!(matches!(stale, StorageError::Conflict { .. }));
        storage
            .set_status(
                version_only.id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some("transient".to_owned())),
                    expected_spec_revision: Some(version_only.revision),
                    ..UpstreamStatusUpdate::default()
                },
            )
            .await?;
        let rebuilt = storage.get_by_id(version_only.id).await?.expect("rebuilt");
        assert_eq!(
            rebuilt.last_apply_error.as_deref(),
            Some(cc_lb_domain::VERTEX_CREDENTIAL_INVALID_GRANT)
        );
        assert_eq!(rebuilt.revision, version_only.revision);
        assert_ne!(
            serde_json::to_vec(&version_only)?,
            serde_json::to_vec(&rebuilt)?
        );
        pool.close().await;
        Ok(())
    }
    .await;
    let teardown = fixture.drop().await;
    result?;
    teardown
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn qa_sec_5_postgres_database_dump_contains_no_vertex_secret_plaintext() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.pool(2).await?;
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        MetaStore::initialize(&storage).await?;
        let secret = "qa-sec-5-postgres-api-key";
        let encrypted = AeadService::from_master_key([93; 32]).encrypt(
            format!("{secret}\nqa-sec-5-private-key\nqa-sec-5-refresh-token").as_bytes(),
            b"qa-sec-5",
        )?;
        storage
            .create(UpstreamCreate {
                id: Uuid::new_v4(),
                name: "qa-sec-5-postgres".to_owned(),
                kind: UpstreamKind::VertexCredential,
                base_url: Some("https://vertex.example.test".parse()?),
                api_key_ciphertext: Some(encrypted),
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                vertex: Some(cc_lb_domain::VertexUpstreamConfig {
                    target: cc_lb_domain::VertexTarget::Regional {
                        project_id: "project-12345".into(),
                        location: "us-central1".into(),
                    },
                    api_version: cc_lb_domain::VertexApiVersion::V1Beta1,
                }),
                vertex_credential_type: Some(cc_lb_domain::VertexCredentialType::ServiceAccount),
                vertex_quota_project_id: None,
                api_key_access: None,
            })
            .await?;
        let dump: String = sqlx::query_scalar(
            "SELECT COALESCE(string_agg(encode(api_key_ciphertext, 'escape'), ','), '')
             FROM upstream_api_key_secret_v1",
        )
        .fetch_one(&pool)
        .await?;
        assert!(!dump.contains(secret));
        pool.close().await;
        Ok(())
    }
    .await;
    let teardown = fixture.drop().await;
    result?;
    teardown
}
