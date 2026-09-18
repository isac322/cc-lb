use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApiKeyUsageBucketKey {
    pub key_id: String,
    pub bucket_width_secs: u64,
    pub bucket_start_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApiKeyUsage {
    pub requests: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cost_usd_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyUsageBucketDelta {
    pub key: ApiKeyUsageBucketKey,
    pub usage: ApiKeyUsage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyUsageFlush {
    pub writer_epoch: Uuid,
    pub flush_id: Uuid,
    pub lease_until_unix_secs: u64,
    pub deltas: Vec<ApiKeyUsageBucketDelta>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyUsageFlushResult {
    Applied,
    AlreadyApplied,
    LeaseLost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyUsageBucketQuery {
    pub key_ids: Vec<String>,
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
    pub exclude_writer_epoch: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyUsageBucket {
    pub key: ApiKeyUsageBucketKey,
    pub usage: ApiKeyUsage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiKeyUsageCompactionRun {
    pub folded_rows: u64,
    pub pruned_rows: u64,
}

/// A cluster-wide concurrent-request hold recorded by a usage writer.
///
/// Postgres-backed deployments insert one row per admitted request so the
/// `Concurrent` limit is enforced across replicas. Holds whose
/// `acquired_at_unix_secs` is older than the writer lease window are treated
/// as crash leftovers and excluded from admission counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyConcurrencyHold {
    pub hold_id: Uuid,
    pub key_id: String,
    pub writer_epoch: Uuid,
    pub acquired_at_unix_secs: u64,
}
