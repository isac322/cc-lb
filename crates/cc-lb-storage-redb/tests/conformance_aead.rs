use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_storage_conformance::scenarios::aead;
use cc_lb_storage_redb::RedbStorage;

#[tokio::test]
async fn conformance_aead_scenarios_pass() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("conformance-aead.redb");
    let storage = RedbStorage::open(&path)?;
    let aead = AeadService::from_master_key([0x17; 32]);

    aead::run_all(Arc::new(storage), &aead).await?;

    Ok(())
}
