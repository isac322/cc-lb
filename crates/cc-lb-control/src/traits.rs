use cc_lb_domain::SubscriptionQuotaCandidateSnapshot;
use cc_lb_storage_api::SubscriptionQuotaSample;
use uuid::Uuid;

pub trait SubscriptionQuotaCacheLike: Send + Sync {
    fn upsert_observation(&self, record: &SubscriptionQuotaSample);

    fn upsert_durable_if_newer(&self, record: &SubscriptionQuotaSample) {
        self.upsert_observation(record);
    }
    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot>;
}

pub trait PromptCacheObservationSinkLike: Send + Sync {
    fn enqueue(
        &self,
        record: cc_lb_storage_api::PromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError>;

    fn enqueue_provider(
        &self,
        _record: cc_lb_storage_api::ProviderPromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptCacheObservationEnqueueError {
    ChannelFull,
    ChannelClosed,
}

#[derive(Debug, Default)]
pub struct NoopPromptCacheObservationSink;

impl PromptCacheObservationSinkLike for NoopPromptCacheObservationSink {
    fn enqueue(
        &self,
        _record: cc_lb_storage_api::PromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError> {
        Ok(())
    }

    fn enqueue_provider(
        &self,
        _record: cc_lb_storage_api::ProviderPromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NoopSubscriptionQuotaCache;

impl SubscriptionQuotaCacheLike for NoopSubscriptionQuotaCache {
    fn upsert_observation(&self, _record: &SubscriptionQuotaSample) {}

    fn snapshot_for_upstream(
        &self,
        _upstream_id: Uuid,
        _now_unix_millis: u64,
        _max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        Vec::new()
    }
}
