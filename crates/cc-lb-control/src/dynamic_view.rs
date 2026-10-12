use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
#[doc(hidden)]
pub use cc_lb_domain::PlanInfo;
use cc_lb_domain::RateLimitObservation;
use cc_lb_storage_api::{
    PromptCacheObservationStore, UpstreamRateLimitObservationRecord, UpstreamRecord,
};
use cc_lb_upstream::ApiKeyAwareSignerFactory;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::api_keys::principal_view::PrincipalView;
use crate::traits::{
    NoopPromptCacheObservationSink, NoopSubscriptionQuotaCache, PromptCacheObservationSinkLike,
    SubscriptionQuotaCacheLike,
};
#[non_exhaustive]
pub struct DynamicView {
    pub signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    pub principal_view: Arc<PrincipalView>,
    pub upstream_status_snapshot: Arc<UpstreamStatusSnapshot>,
    pub upstream_rate_limit_cache: Arc<RwLock<UpstreamRateLimitCache>>,
    pub subscription_quota_cache: Arc<dyn SubscriptionQuotaCacheLike>,
    pub subscription_quota_routing_max_staleness_secs: u64,
    pub prompt_cache_observation_store: Option<Arc<dyn PromptCacheObservationStore>>,
    pub prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    pub prompt_cache_grace_margin_secs: u64,
    pub plan_info_by_upstream: HashMap<Uuid, PlanInfo>,
    pub generation: u64,
    upstream_records: Vec<UpstreamRecord>,
}

impl DynamicView {
    pub fn upstreams_snapshot(&self) -> &[UpstreamRecord] {
        &self.upstream_records
    }

    pub fn prompt_cache_observation_store_opt(
        &self,
    ) -> Option<&Arc<dyn PromptCacheObservationStore>> {
        self.prompt_cache_observation_store.as_ref()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpstreamRateLimitCache {
    pub snapshots: HashMap<Uuid, Vec<RateLimitObservation>>,
    pub updated_at_unix_secs: u64,
}

impl UpstreamRateLimitCache {
    pub fn upsert_record(&mut self, record: UpstreamRateLimitObservationRecord) {
        let upstream_id = record.upstream_id;
        let observed_at_unix_secs = record.observed_at_unix_secs;
        let observation = rate_limit_observation_from_record(record);
        let snapshots = self.snapshots.entry(upstream_id).or_default();
        match snapshots.iter_mut().find(|snapshot| {
            snapshot.kind == observation.kind && snapshot.window == observation.window
        }) {
            Some(snapshot) => *snapshot = observation,
            None => snapshots.push(observation),
        }
        self.updated_at_unix_secs = observed_at_unix_secs;
    }
}

fn rate_limit_observation_from_record(
    record: UpstreamRateLimitObservationRecord,
) -> RateLimitObservation {
    RateLimitObservation {
        kind: record.kind,
        window: record.window,
        limit: record.limit,
        remaining: record.remaining,
        reset: record.reset,
    }
}

/// Process-local publication point for the serving [`DynamicView`].
///
/// Readers call [`DynamicViewHolder::load`], a lock-free `ArcSwap` read.
///
/// Writers that rebuild a view from storage and publish it MUST follow the
/// rebuild protocol: acquire [`DynamicViewHolder::lock_rebuild`], then read the
/// resident generation, build the next view from that generation, and install
/// it with [`DynamicViewHolder::try_store_if_newer`] before dropping the guard.
/// Holding the guard across the generation read, the build, and the install
/// serializes every live rebuild-and-install path on this holder, so a
/// publisher can neither reuse a generation another publisher is about to
/// install nor have its committed state replaced by an older build.
///
/// The lock is a non-reentrant async mutex: build-only code paths (view
/// builders and rebinders that only return a view) MUST NOT acquire it; the
/// caller that publishes the view owns the guard. The lock does not serialize
/// storage mutations and does not provide a cross-table storage snapshot.
pub struct DynamicViewHolder {
    inner: ArcSwap<DynamicView>,
    rebuild_lock: tokio::sync::Mutex<()>,
}

impl DynamicViewHolder {
    pub fn new(initial: Arc<DynamicView>) -> Self {
        Self {
            inner: ArcSwap::from(initial),
            rebuild_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Acquire this holder's rebuild/install lock. Waiters are queued in FIFO
    /// order; dropping the returned guard (including by dropping or
    /// cancelling the owning future) releases it. See the type-level
    /// documentation for the generation-read/build/install protocol.
    pub async fn lock_rebuild(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.rebuild_lock.lock().await
    }

    pub fn load(&self) -> Arc<DynamicView> {
        self.inner.load_full()
    }

    pub fn store(&self, view: Arc<DynamicView>) {
        self.inner.store(view);
    }

    /// Store `view` only if its `generation` is strictly greater than
    /// the currently-stored view's generation. Concurrent stores are
    /// resolved by `arc_swap::rcu`; this is a defensive publication check,
    /// not a substitute for the rebuild protocol on
    /// [`DynamicViewHolder::lock_rebuild`]. Returns
    /// `true` if the swap succeeded OR if an even-newer view is now
    /// resident (both are "we're up-to-date" outcomes for the caller).
    pub fn try_store_if_newer(&self, view: Arc<DynamicView>) -> bool {
        let new_generation = view.generation;
        if new_generation <= self.inner.load().generation {
            return false;
        }
        self.inner.rcu(|current| {
            if current.generation < new_generation {
                Arc::clone(&view)
            } else {
                Arc::clone(current)
            }
        });
        self.inner.load().generation >= new_generation
    }

    pub fn generation(&self) -> u64 {
        self.inner.load().generation
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpstreamStatusSnapshot {
    pub entries: HashMap<String, UpstreamStatusEntry>,
    pub applied_at_unix_secs: u64,
    pub revision_hash: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamStatusEntry {
    pub status: ApplyStatus,
    pub last_apply_error: Option<String>,
    pub last_apply_at_unix_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyStatus {
    Active,
    Disabled,
    Error,
}

pub struct DynamicViewBuilder {
    previous_generation: u64,
    signer_factory: Option<Arc<dyn ApiKeyAwareSignerFactory>>,
    principal_view: Option<Arc<PrincipalView>>,
    upstream_status_snapshot: Option<Arc<UpstreamStatusSnapshot>>,
    upstream_rate_limit_cache: Option<Arc<RwLock<UpstreamRateLimitCache>>>,
    subscription_quota_cache: Option<Arc<dyn SubscriptionQuotaCacheLike>>,
    subscription_quota_routing_max_staleness_secs: Option<u64>,
    prompt_cache_observation_store: Option<Arc<dyn PromptCacheObservationStore>>,
    prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    prompt_cache_grace_margin_secs: Option<u64>,
    plan_info_by_upstream: HashMap<Uuid, PlanInfo>,
    upstream_records: Vec<UpstreamRecord>,
}

impl DynamicViewBuilder {
    pub fn new(previous_generation: u64) -> Self {
        Self {
            previous_generation,
            signer_factory: None,
            principal_view: None,
            upstream_status_snapshot: None,
            upstream_rate_limit_cache: None,
            subscription_quota_cache: None,
            subscription_quota_routing_max_staleness_secs: None,
            prompt_cache_observation_store: None,
            prompt_cache_observation_sink: None,
            prompt_cache_grace_margin_secs: None,
            plan_info_by_upstream: HashMap::new(),
            upstream_records: Vec::new(),
        }
    }

    pub fn from_view(view: &DynamicView) -> Self {
        Self {
            previous_generation: view.generation,
            signer_factory: Some(Arc::clone(&view.signer_factory)),
            principal_view: Some(Arc::clone(&view.principal_view)),
            upstream_status_snapshot: Some(Arc::clone(&view.upstream_status_snapshot)),
            upstream_rate_limit_cache: Some(Arc::clone(&view.upstream_rate_limit_cache)),
            subscription_quota_cache: Some(Arc::clone(&view.subscription_quota_cache)),
            subscription_quota_routing_max_staleness_secs: Some(
                view.subscription_quota_routing_max_staleness_secs,
            ),
            prompt_cache_observation_store: view.prompt_cache_observation_store.clone(),
            prompt_cache_observation_sink: Some(Arc::clone(&view.prompt_cache_observation_sink)),
            prompt_cache_grace_margin_secs: Some(view.prompt_cache_grace_margin_secs),
            plan_info_by_upstream: view.plan_info_by_upstream.clone(),
            upstream_records: view.upstreams_snapshot().to_vec(),
        }
    }

    pub fn signer_factory(mut self, signer_factory: Arc<dyn ApiKeyAwareSignerFactory>) -> Self {
        self.signer_factory = Some(signer_factory);
        self
    }

    pub fn principal_view(mut self, principal_view: Arc<PrincipalView>) -> Self {
        self.principal_view = Some(principal_view);
        self
    }

    pub fn upstream_status_snapshot(
        mut self,
        upstream_status_snapshot: Arc<UpstreamStatusSnapshot>,
    ) -> Self {
        self.upstream_status_snapshot = Some(upstream_status_snapshot);
        self
    }

    pub fn upstream_rate_limit_cache(mut self, cache: Arc<RwLock<UpstreamRateLimitCache>>) -> Self {
        self.upstream_rate_limit_cache = Some(cache);
        self
    }

    pub fn subscription_quota_cache(mut self, cache: Arc<dyn SubscriptionQuotaCacheLike>) -> Self {
        self.subscription_quota_cache = Some(cache);
        self
    }

    pub fn subscription_quota_routing_max_staleness_secs(mut self, value: u64) -> Self {
        self.subscription_quota_routing_max_staleness_secs = Some(value);
        self
    }

    pub fn prompt_cache_observation_store(
        mut self,
        store: Arc<dyn PromptCacheObservationStore>,
    ) -> Self {
        self.prompt_cache_observation_store = Some(store);
        self
    }

    pub fn prompt_cache_grace_margin_secs(mut self, grace_margin_secs: u64) -> Self {
        self.prompt_cache_grace_margin_secs = Some(grace_margin_secs);
        self
    }

    pub fn prompt_cache_observation_sink(
        mut self,
        sink: Arc<dyn PromptCacheObservationSinkLike>,
    ) -> Self {
        self.prompt_cache_observation_sink = Some(sink);
        self
    }

    pub fn upstream_records(mut self, records: Vec<UpstreamRecord>) -> Self {
        self.upstream_records = records;
        self
    }

    pub fn plan_info_by_upstream(mut self, plan_info: HashMap<Uuid, PlanInfo>) -> Self {
        self.plan_info_by_upstream = plan_info;
        self
    }

    pub fn build(self) -> Arc<DynamicView> {
        Arc::new(DynamicView {
            signer_factory: self
                .signer_factory
                .expect("DynamicViewBuilder requires signer_factory"),
            principal_view: self
                .principal_view
                .expect("DynamicViewBuilder requires principal_view"),
            upstream_status_snapshot: self.upstream_status_snapshot.unwrap_or_default(),
            upstream_rate_limit_cache: self
                .upstream_rate_limit_cache
                .unwrap_or_else(|| Arc::new(RwLock::new(UpstreamRateLimitCache::default()))),
            subscription_quota_cache: self
                .subscription_quota_cache
                .unwrap_or_else(|| Arc::new(NoopSubscriptionQuotaCache)),
            subscription_quota_routing_max_staleness_secs: self
                .subscription_quota_routing_max_staleness_secs
                .unwrap_or(0),
            prompt_cache_observation_store: self.prompt_cache_observation_store,
            prompt_cache_observation_sink: self
                .prompt_cache_observation_sink
                .unwrap_or_else(|| Arc::new(NoopPromptCacheObservationSink)),
            prompt_cache_grace_margin_secs: self.prompt_cache_grace_margin_secs.unwrap_or(0),
            plan_info_by_upstream: self.plan_info_by_upstream,
            generation: self.previous_generation.saturating_add(1),
            upstream_records: self.upstream_records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cc_lb_domain::Upstream;
    use cc_lb_upstream::{
        RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
        SigningCapability, UpstreamError,
    };

    struct TestSignerFactory;

    impl ApiKeyAwareSignerFactory for TestSignerFactory {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(TestSignerFactory)
        }
    }

    #[async_trait]
    impl SignerFactory for TestSignerFactory {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(TestSigner))
        }
    }

    struct TestSigner;

    #[async_trait]
    impl Signer for TestSigner {
        async fn sign(
            &self,
            shaped: ShapedRequest,
            capability: &mut SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            Ok(SignedRequest::from_shaped(shaped, capability))
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }

    fn test_view(previous_generation: u64) -> Arc<DynamicView> {
        let principal_view = Arc::new(PrincipalView::from_db(
            &[],
            std::collections::HashMap::new(),
        ));
        DynamicViewBuilder::new(previous_generation)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build()
    }

    #[test]
    fn generation_monotonic() {
        let holder = DynamicViewHolder::new(test_view(0));
        for expected in 2..=1000 {
            let next = test_view(holder.generation());
            holder.store(next);
            assert_eq!(holder.generation(), expected);
        }
    }

    #[test]
    fn load_after_store_returns_new() {
        let holder = DynamicViewHolder::new(test_view(0));
        let next = test_view(holder.generation());
        holder.store(Arc::clone(&next));
        assert!(Arc::ptr_eq(&holder.load(), &next));
    }

    #[test]
    fn concurrent_load_during_store_no_panic() {
        let holder = Arc::new(DynamicViewHolder::new(test_view(0)));
        let reader_holder = Arc::clone(&holder);
        let reader = std::thread::spawn(move || {
            for _ in 0..100 {
                let generation = reader_holder.load().generation;
                assert!(generation > 0);
            }
        });
        for _ in 0..100 {
            holder.store(test_view(holder.generation()));
        }
        reader.join().expect("reader does not panic");
    }

    #[test]
    fn concurrent_store_serializes_via_arcswap() {
        let holder = Arc::new(DynamicViewHolder::new(test_view(0)));
        let first_holder = Arc::clone(&holder);
        let second_holder = Arc::clone(&holder);
        let first = std::thread::spawn(move || first_holder.store(test_view(1)));
        let second = std::thread::spawn(move || second_holder.store(test_view(2)));
        first.join().expect("first store completes");
        second.join().expect("second store completes");
        assert!(holder.generation() == 2 || holder.generation() == 3);
    }

    #[test]
    fn builder_increments_generation() {
        let view = test_view(41);
        assert_eq!(view.generation, 42);
    }

    #[test]
    fn upstreams_snapshot_defaults_empty() {
        let view = test_view(0);
        assert!(view.upstreams_snapshot().is_empty());
    }

    #[test]
    fn rebuild_lock_second_acquisition_waits_for_first_guard() {
        use std::task::{Context, Poll, Waker};

        let holder = DynamicViewHolder::new(test_view(0));
        let mut cx = Context::from_waker(Waker::noop());

        let mut first = std::pin::pin!(holder.lock_rebuild());
        let first_guard = match first.as_mut().poll(&mut cx) {
            Poll::Ready(guard) => guard,
            Poll::Pending => panic!("uncontended rebuild lock must be acquired immediately"),
        };

        let mut second = std::pin::pin!(holder.lock_rebuild());
        assert!(
            second.as_mut().poll(&mut cx).is_pending(),
            "second acquisition must wait while the first guard is held"
        );
        // Snapshot reads stay lock-free while the guard is held.
        assert_eq!(holder.load().generation, 1);
        assert_eq!(holder.generation(), 1);

        drop(first_guard);
        assert!(
            second.as_mut().poll(&mut cx).is_ready(),
            "second acquisition must complete once the first guard is dropped"
        );
    }

    #[test]
    fn rebuild_lock_is_per_holder() {
        use std::task::{Context, Poll, Waker};

        let first_holder = DynamicViewHolder::new(test_view(0));
        let second_holder = DynamicViewHolder::new(test_view(0));
        let mut cx = Context::from_waker(Waker::noop());

        let mut first = std::pin::pin!(first_holder.lock_rebuild());
        let first_guard = match first.as_mut().poll(&mut cx) {
            Poll::Ready(guard) => guard,
            Poll::Pending => panic!("uncontended rebuild lock must be acquired immediately"),
        };
        let mut second = std::pin::pin!(second_holder.lock_rebuild());
        assert!(
            second.as_mut().poll(&mut cx).is_ready(),
            "independent holders must not share a rebuild lock"
        );
        drop(first_guard);
    }
}
