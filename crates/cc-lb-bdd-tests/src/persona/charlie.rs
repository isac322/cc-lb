//! Charlie — SRE persona. Admin token plus operational tooling for
//! incident response (killswitch), drain, multi-replica, and warmup
//! lease scenarios (W2 plus parts of W4).

use anyhow::Result;
use cc_lb_storage_api::MetaStore;

use crate::backend::StorageHandle;
use crate::results::{HealthSnapshot, KillswitchState};

pub struct Charlie {
    storage: StorageHandle,
}

impl Charlie {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }

    pub async fn enable_killswitch(&self) -> Result<KillswitchState> {
        MetaStore::set_killswitch_enabled(self.storage.as_ref(), true).await?;
        let enabled = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        Ok(KillswitchState { enabled })
    }

    pub async fn disable_killswitch(&self) -> Result<KillswitchState> {
        MetaStore::set_killswitch_enabled(self.storage.as_ref(), false).await?;
        let enabled = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        Ok(KillswitchState { enabled })
    }

    pub async fn killswitch_state(&self) -> Result<KillswitchState> {
        let enabled = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        Ok(KillswitchState { enabled })
    }

    pub async fn check_health(&self) -> Result<HealthSnapshot> {
        let killswitch = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        let version = MetaStore::contract_version(self.storage.as_ref()).await?;
        Ok(HealthSnapshot {
            liveness_ok: true,
            readiness_ok: version > 0,
            killswitch_enabled: killswitch,
        })
    }
}
