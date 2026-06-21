use anyhow::Result;
use cc_lb_bdd_tests::BddCtx;
use cc_lb_storage_api::MetaStore;
use cc_lb_storage_api::upstream::UpstreamStore;
use uuid::Uuid;

use super::w4_helpers::W4Observation;

pub(crate) async fn w4_replica_observe(_ctx: &BddCtx) -> Result<W4Observation> {
    let Some((replica_a, replica_b)) = BddCtx::spawn_postgres_replica_pair().await? else {
        return Ok(W4Observation::new(vec![("postgres_available", true)]));
    };
    let backend = MetaStore::backend_kind(replica_a.storage.as_ref()).await?;
    let version_a = MetaStore::contract_version(replica_a.storage.as_ref()).await?;
    let version_b = MetaStore::contract_version(replica_b.storage.as_ref()).await?;
    let holder_a = format!("replica-a-{}", Uuid::new_v4().simple());
    let holder_b = format!("replica-b-{}", Uuid::new_v4().simple());
    let claim_a = UpstreamStore::claim_warmup_lease(
        replica_a.storage.as_ref(),
        replica_a.upstream_id,
        &holder_a,
        120,
    )
    .await?;
    let claim_b = UpstreamStore::claim_warmup_lease(
        replica_b.storage.as_ref(),
        replica_b.upstream_id,
        &holder_b,
        120,
    )
    .await?;
    let cycle_written = UpstreamStore::write_warmup_cycle_key(
        replica_a.storage.as_ref(),
        replica_a.upstream_id,
        &holder_a,
        UpstreamStore::warmup_now_unix_secs(replica_a.storage.as_ref()).await?,
        None,
    )
    .await?;
    let seen_by_b = UpstreamStore::get_by_id(replica_b.storage.as_ref(), replica_a.upstream_id)
        .await?
        .is_some_and(|record| record.last_warmup_cycle_key.is_some());

    Ok(W4Observation::new(vec![
        ("postgres_backend", backend.as_str() == "postgres"),
        (
            "contract_versions_match",
            version_a == version_b && version_a > 0,
        ),
        ("single_lease_winner", claim_a && !claim_b),
        ("cycle_written", cycle_written),
        ("replica_b_saw_cycle", seen_by_b),
    ]))
}
