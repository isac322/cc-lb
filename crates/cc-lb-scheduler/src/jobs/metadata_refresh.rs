use apalis_core::task::{Task, builder::TaskBuilder};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::middleware::TraceparentCarrier;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataRefreshJob {
    pub upstream_id: Uuid,
    pub credential_generation: u64,
    pub traceparent: Option<String>,
}

impl MetadataRefreshJob {
    pub const fn new(upstream_id: Uuid, credential_generation: u64) -> Self {
        Self {
            upstream_id,
            credential_generation,
            traceparent: None,
        }
    }

    pub fn idempotency_key(&self) -> String {
        format!(
            "entity:metadata_refresh:{}:{}",
            self.upstream_id, self.credential_generation
        )
    }

    pub fn into_apalis_task<Ctx, IdType>(self, run_at_unix_secs: u64) -> Task<Self, Ctx, IdType>
    where
        Ctx: Default,
    {
        let idempotency_key = self.idempotency_key();
        TaskBuilder::<Self, Ctx, IdType>::new(self)
            .run_at_timestamp(run_at_unix_secs)
            .with_idempotency_key(idempotency_key)
            .build()
    }
}

impl TraceparentCarrier for MetadataRefreshJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}
