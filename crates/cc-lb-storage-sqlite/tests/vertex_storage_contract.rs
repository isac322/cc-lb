use std::{error::Error, sync::Arc};

use cc_lb_aead::AeadService;
use cc_lb_storage_api::{
    MetaStore, StorageError, UpstreamCreate, UpstreamKind, UpstreamRecord, UpstreamStatusUpdate,
    UpstreamStore, UpstreamUpdate,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use sqlx::migrate::Migrator;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

async fn open(database_url: &str) -> TestResult<SqliteStorage> {
    Ok(open_sqlite(database_url, Arc::new(cc_lb_clock::SystemClock)).await?)
}
async fn create_vertex(storage: &SqliteStorage, name: &str) -> TestResult<UpstreamRecord> {
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
async fn qa_sto_1_sqlite_0129_to_0130_preserves_rows_children_epochs_workspace_and_warmup()
-> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("qa-sto-1.sqlite").display()
    );
    let storage = open(&database_url).await?;
    let current = sqlx::migrate!("./migrations");
    let before_0130: Migrator = Migrator::with_migrations(
        current
            .iter()
            .filter(|migration| migration.version <= 129)
            .cloned()
            .collect(),
    );
    before_0130.run(storage.pool()).await?;
    let id = Uuid::from_u128(0x1281);
    sqlx::query(
        "INSERT INTO upstream_spec_v1
         (id,name,kind,base_url,enabled,warmup_enabled,warmup_dialect_plugin,
          spec_revision,created_at,updated_at,deleted_at,workspace_id,owner_epoch)
         VALUES (?, ?, 'gemini_api_key', ?, 1, 1, NULL, 7, 1000, 1001, NULL, ?, 9)",
    )
    .bind(id.to_string())
    .bind("qa-sto-1-source")
    .bind("https://gemini.example.test")
    .bind("workspace-qa")
    .execute(storage.pool())
    .await?;
    sqlx::query(
        "INSERT INTO upstream_api_key_secret_v1
         (upstream_id,api_key_ciphertext,created_at,updated_at)
         VALUES (?, ?, 1000, 1001)",
    )
    .bind(id.to_string())
    .bind(vec![4_u8, 5, 6])
    .execute(storage.pool())
    .await?;
    sqlx::query(
        "INSERT INTO upstream_status_v1
         (upstream_id,last_apply_error,last_apply_at,last_warmup_at,updated_at)
         VALUES (?, 'historical-status', 1002, 1003, 1004)",
    )
    .bind(id.to_string())
    .execute(storage.pool())
    .await?;
    let warmup_id = Uuid::from_u128(0x1282);
    sqlx::query(
        "INSERT INTO warmup_attempts_v1
         (id,upstream_id,attempted_at_unix_secs,scheduled_for_unix_secs,trigger,
          outcome,reason,upstream_spec_revision)
         VALUES (?, ?, 1000, 1000, 'manual', 'success', 'cycle_advanced', 7)",
    )
    .bind(warmup_id.to_string())
    .bind(id.to_string())
    .execute(storage.pool())
    .await?;

    storage.initialize().await?;
    let spec: (i64, String, String, i64, i64) = sqlx::query_as(
        "SELECT spec_revision,workspace_id,kind,owner_epoch,enabled
         FROM upstream_spec_v1 WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(
        spec,
        (
            7,
            "workspace-qa".to_owned(),
            "gemini_api_key".to_owned(),
            9,
            1
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM upstream_api_key_secret_v1 WHERE upstream_id = ?"
        )
        .bind(id.to_string())
        .fetch_one(storage.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM upstream_status_v1 WHERE upstream_id = ?"
        )
        .bind(id.to_string())
        .fetch_one(storage.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM warmup_attempts_v1 WHERE id = ?")
            .bind(warmup_id.to_string())
            .fetch_one(storage.pool())
            .await?,
        1
    );
    let index_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'upstream_spec_v1_name_active_uniq'",
    )
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(index_count, 1);
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(storage.pool())
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn qa_sto_3_sqlite_vertex_check_rejects_invalid_rows_and_accepts_positive_locations()
-> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("qa-sto-3.sqlite").display()
    );
    let storage = open(&database_url).await?;
    MetaStore::initialize(&storage).await?;
    let valid = ["global", "us", "eu", "us-central1", "europe-west4"];
    for (index, location) in valid.into_iter().enumerate() {
        let record = storage
            .create(UpstreamCreate {
                id: Uuid::new_v4(),
                name: format!("qa-sto-3-valid-{index}"),
                kind: UpstreamKind::VertexApiKey,
                base_url: None,
                api_key_ciphertext: Some(vec![1]),
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                vertex: Some(cc_lb_domain::VertexUpstreamConfig {
                    target: cc_lb_domain::VertexTarget::Regional {
                        project_id: "project-12345".into(),
                        location: location.into(),
                    },
                    api_version: cc_lb_domain::VertexApiVersion::V1Beta1,
                }),
                vertex_credential_type: None,
                vertex_quota_project_id: None,
                api_key_access: None,
            })
            .await?;
        assert_eq!(record.kind, UpstreamKind::VertexApiKey);
    }
    let invalid_sql = [
        "UPDATE upstream_spec_v1 SET vertex_api_version='v2' WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
        "UPDATE upstream_spec_v1 SET vertex_location='US-central1' WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
        "UPDATE upstream_spec_v1 SET vertex_project_id='Bad.Project' WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
        "UPDATE upstream_spec_v1 SET vertex_express=1, vertex_project_id='project-12345' WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
        "UPDATE upstream_spec_v1 SET vertex_project_id=NULL WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
        "UPDATE upstream_spec_v1 SET vertex_location=NULL WHERE id=(SELECT id FROM upstream_spec_v1 LIMIT 1)",
        "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_project_id,vertex_location) VALUES ('bad-missing-version','bad-missing-version','vertex_api_key',1,0,1,1,1,'project-12345','us-central1')",
        "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_api_version) VALUES ('bad-nonvertex','bad-nonvertex','gemini_api_key',1,0,1,1,1,'v1')",
        "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_project_id,vertex_location,vertex_api_version,vertex_express) VALUES ('bad-null-express','bad-null-express','vertex_api_key',1,0,1,1,1,'project-12345','us-central1','v1',NULL)",
        "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_project_id,vertex_location,vertex_api_version,vertex_express) VALUES ('bad-null-version','bad-null-version','vertex_api_key',1,0,1,1,1,'project-12345','us-central1',NULL,0)",
        "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_project_id,vertex_location,vertex_api_version,vertex_express) VALUES ('bad-credential-type','bad-credential-type','vertex_credential',1,0,1,1,1,'project-12345','us-central1','v1',0)",
        "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_api_version,vertex_express,vertex_credential_type) VALUES ('bad-credential-express','bad-credential-express','vertex_credential',1,0,1,1,1,'v1',1,'service_account')",
        "INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at,vertex_credential_type) VALUES ('bad-nonvertex-credential-type','bad-nonvertex-credential-type','gemini_api_key',1,0,1,1,1,'service_account')",
    ];
    for sql in invalid_sql {
        assert!(
            sqlx::query(sql).execute(storage.pool()).await.is_err(),
            "must reject: {sql}"
        );
    }
    Ok(())
}
#[tokio::test]
async fn qa_sto_4_sqlite_vertex_record_round_trips_without_configuration_loss() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("qa-sto-4.sqlite").display()
    );
    let storage = open(&database_url).await?;
    MetaStore::initialize(&storage).await?;
    let created = create_vertex(&storage, "qa-sto-4-roundtrip").await?;
    let loaded = storage
        .get_by_id(created.id)
        .await?
        .expect("created record");
    assert_eq!(loaded, created);
    let raw: (String, String, String, String, i64, String) = sqlx::query_as(
        "SELECT kind, base_url, vertex_project_id, vertex_location,
                vertex_express, vertex_api_version
         FROM upstream_spec_v1 WHERE id = ?",
    )
    .bind(created.id.to_string())
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(
        raw,
        (
            "vertex_credential".to_owned(),
            "https://vertex.example.test/".to_owned(),
            "project-12345".to_owned(),
            "us-central1".to_owned(),
            0,
            "v1beta1".to_owned(),
        )
    );
    Ok(())
}

#[tokio::test]
async fn qa_sto_5_sqlite_vertex_owner_epoch_matrix_and_stale_binding_fence() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("qa-sto-5.sqlite").display()
    );
    let storage = open(&database_url).await?;
    MetaStore::initialize(&storage).await?;
    let created = create_vertex(&storage, "qa-sto-5-epochs").await?;
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
    let project_changed = storage
        .update_spec(
            version_only.id,
            version_only.revision,
            UpstreamUpdate {
                vertex: Some(Some(cc_lb_domain::VertexUpstreamConfig {
                    target: cc_lb_domain::VertexTarget::Regional {
                        project_id: "project-67890".into(),
                        location: "us-central1".into(),
                    },
                    api_version: cc_lb_domain::VertexApiVersion::V1,
                })),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    assert_eq!(project_changed.owner_epoch, 1);
    let location_changed = storage
        .update_spec(
            project_changed.id,
            project_changed.revision,
            UpstreamUpdate {
                vertex: Some(Some(cc_lb_domain::VertexUpstreamConfig {
                    target: cc_lb_domain::VertexTarget::Regional {
                        project_id: "project-67890".into(),
                        location: "europe-west4".into(),
                    },
                    api_version: cc_lb_domain::VertexApiVersion::V1,
                })),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    assert_eq!(location_changed.owner_epoch, 2);
    let base_url_changed = storage
        .update_spec(
            location_changed.id,
            location_changed.revision,
            UpstreamUpdate {
                base_url: Some(Some("https://vertex.example.test/v2".parse()?)),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    assert_eq!(base_url_changed.owner_epoch, 3);
    let key_changed = storage
        .update(
            base_url_changed.id,
            base_url_changed.revision,
            UpstreamUpdate {
                api_key_ciphertext: Some(vec![2]),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    assert_eq!(key_changed.owner_epoch, 4);
    Ok(())
}

#[tokio::test]
async fn qa_sto_6_sqlite_terminal_latch_conflict_and_credential_replacement() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("qa-sto-6.sqlite").display()
    );
    let storage = open(&database_url).await?;
    MetaStore::initialize(&storage).await?;
    let created = create_vertex(&storage, "qa-sto-6-terminal").await?;
    storage
        .set_status(
            created.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(
                    cc_lb_domain::VERTEX_CREDENTIAL_INVALID_GRANT.to_owned(),
                )),
                expected_spec_revision: Some(created.revision),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await?;
    let error = storage
        .set_status(
            created.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some("stale".to_owned())),
                expected_spec_revision: Some(created.revision - 1),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await
        .expect_err("stale status must fail");
    assert!(
        matches!(error, StorageError::Conflict { .. }),
        "expected Conflict, got {error:?}"
    );
    storage
        .set_status(
            created.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some("transient".to_owned())),
                expected_spec_revision: Some(created.revision),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await?;
    assert_eq!(
        storage
            .get_by_id(created.id)
            .await?
            .unwrap()
            .last_apply_error
            .as_deref(),
        Some(cc_lb_domain::VERTEX_CREDENTIAL_INVALID_GRANT)
    );
    let replaced = storage
        .update(
            created.id,
            created.revision,
            UpstreamUpdate {
                api_key_ciphertext: Some(vec![2]),
                ..UpstreamUpdate::default()
            },
        )
        .await?;
    assert!(replaced.last_apply_error.is_none());
    Ok(())
}

#[tokio::test]
async fn qa_sto_7_sqlite_status_rebuild_preserves_terminal_marker() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("qa-sto-7.sqlite").display()
    );
    let storage = open(&database_url).await?;
    MetaStore::initialize(&storage).await?;
    let created = create_vertex(&storage, "qa-sto-7-rebuild").await?;
    storage
        .set_status(
            created.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(
                    cc_lb_domain::VERTEX_CREDENTIAL_INVALID_CLIENT.to_owned(),
                )),
                expected_spec_revision: Some(created.revision),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await?;
    let rebuilt = storage
        .list(None, 100)
        .await?
        .into_iter()
        .find(|record| record.id == created.id)
        .expect("rebuilt record");
    assert_eq!(
        rebuilt.last_apply_error.as_deref(),
        Some(cc_lb_domain::VERTEX_CREDENTIAL_INVALID_CLIENT)
    );
    Ok(())
}

#[tokio::test]
async fn qa_sto_8_sqlite_peer_terminal_write_changes_material_without_revision_bump() -> TestResult
{
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("qa-sto-8.sqlite").display()
    );
    let storage = open(&database_url).await?;
    MetaStore::initialize(&storage).await?;
    let created = create_vertex(&storage, "qa-sto-8-reconcile").await?;
    let before = serde_json::to_vec(&created)?;
    storage
        .set_status(
            created.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(
                    cc_lb_domain::VERTEX_CREDENTIAL_INVALID_GRANT.to_owned(),
                )),
                expected_spec_revision: Some(created.revision),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await?;
    let after_record = storage.get_by_id(created.id).await?.expect("record");
    assert_eq!(after_record.revision, created.revision);
    assert_ne!(before, serde_json::to_vec(&after_record)?);
    Ok(())
}
#[tokio::test]
async fn qa_sec_5_sqlite_dump_contains_no_vertex_secret_plaintext() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_path = temp_dir.path().join("qa-sec-5.sqlite");
    let database_url = format!("sqlite://{}", database_path.display());
    let storage = open(&database_url).await?;
    MetaStore::initialize(&storage).await?;

    let api_key = "AIza-qa-sec-5-api-key";
    let private_key = "-----BEGIN PRIVATE KEY-----qa-sec-5-----END PRIVATE KEY-----";
    let refresh_token = "qa-sec-5-refresh-token";
    let aead = AeadService::from_master_key([91; 32]);
    let ciphertext = aead.encrypt(
        format!("{api_key}\n{private_key}\n{refresh_token}").as_bytes(),
        b"qa-sec-5",
    )?;
    storage
        .create(UpstreamCreate {
            id: Uuid::new_v4(),
            name: "qa-sec-5-secret-dump".to_owned(),
            kind: UpstreamKind::VertexCredential,
            base_url: Some("https://vertex.example.test".parse()?),
            api_key_ciphertext: Some(ciphertext),
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
            vertex_quota_project_id: Some("quota-project-123".to_owned()),
            api_key_access: None,
        })
        .await?;
    storage.pool().close().await;
    let dump = std::fs::read(database_path)?;
    let dump = String::from_utf8_lossy(&dump);
    for secret in [api_key, private_key, refresh_token] {
        assert!(
            !dump.contains(secret),
            "database dump contains plaintext secret"
        );
    }
    Ok(())
}
