use std::sync::{Arc, Mutex};

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::{UpstreamRecord, upstream::UpstreamKind};
use uuid::Uuid;

use super::{ReconcileUpstreams, SchedulerReconcileJobResult};
use crate::error::Result;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

const NOW_SECS: u64 = 1_000;

#[derive(Clone)]
struct FakeReconcileUpstreams {
    records: Arc<Mutex<Vec<UpstreamRecord>>>,
}

impl FakeReconcileUpstreams {
    fn new(records: Vec<UpstreamRecord>) -> Self {
        Self {
            records: Arc::new(Mutex::new(records)),
        }
    }

    fn replace(&self, records: Vec<UpstreamRecord>) {
        *self.records.lock().expect("upstream lock") = records;
    }
}

impl ReconcileUpstreams for FakeReconcileUpstreams {
    fn list(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> impl std::future::Future<Output = Result<Vec<UpstreamRecord>>> + Send + '_ {
        async move {
            let mut records = self.records.lock().expect("upstream lock").clone();
            records.sort_by_key(|record| record.id);
            Ok(records
                .into_iter()
                .filter(|record| after.is_none_or(|id| record.id > id))
                .take(limit)
                .collect())
        }
    }
}

fn assert_done(result: &SchedulerReconcileJobResult, ensured: u64, pruned: u64, failures: u64) {
    match result {
        SchedulerReconcileJobResult::Done(stats) => {
            assert_eq!(stats.jobs_ensured, ensured);
            assert_eq!(stats.jobs_pruned, pruned);
            assert_eq!(stats.failures_recorded, failures);
        }
        SchedulerReconcileJobResult::Retry { error, .. } => panic!("unexpected retry: {error}"),
    }
}

fn expected_keys(upstream_id: Uuid) -> Vec<String> {
    let mut keys = compat_keys_only();
    keys.extend([
        format!("entity:oauth_refresh:{upstream_id}"),
        format!("entity:oauth_usage_poll:{upstream_id}"),
        warmup_key(upstream_id),
    ]);
    keys.sort();
    keys
}

fn compat_keys_only() -> Vec<String> {
    vec!["entity:anthropic_compat_refresh:claude_code_stable_version".to_owned()]
}

fn warmup_key(upstream_id: Uuid) -> String {
    format!("entity:warmup:{upstream_id}")
}

fn oauth_record(upstream_id: Uuid, warmup_enabled: bool) -> UpstreamRecord {
    UpstreamRecord {
        id: upstream_id,
        name: "oauth".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        enabled: true,
        oauth_credentials: Some(EncryptedOAuthTokens::from_ciphertext(vec![1])),
        revision: 1,
        created_at_unix_secs: 1,
        updated_at_unix_secs: 1,
        warmup_enabled,
        ..UpstreamRecord::default()
    }
}
