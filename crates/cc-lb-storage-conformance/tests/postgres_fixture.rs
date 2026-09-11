use anyhow::Result;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_conformance::postgres_fixture;

#[tokio::test]
#[allow(non_snake_case)]
async fn t3_postgres__fixture_initializes_roundtrips_and_tears_down_schema() -> Result<()> {
    let fixture = postgres_fixture().await?;
    let schema_name = fixture.schema_name().to_owned();

    let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(fixture.pool())
        .await?;
    assert_eq!(current_schema, schema_name);
    assert_eq!(
        fixture.storage().backend_kind().await?,
        BackendKind::Postgres
    );

    assert!(!fixture.storage().killswitch_enabled().await?);
    fixture.storage().set_killswitch_enabled(true).await?;
    assert!(fixture.storage().killswitch_enabled().await?);

    fixture.teardown().await?;

    let verification_fixture = postgres_fixture().await?;
    let dropped_schema_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
            .bind(&schema_name)
            .fetch_one(verification_fixture.pool())
            .await?;
    assert!(!dropped_schema_exists);
    verification_fixture.teardown().await?;

    Ok(())
}
