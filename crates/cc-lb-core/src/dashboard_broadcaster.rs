use cc_lb_storage_redb::RequestEvent;
use tokio::sync::broadcast::{self, Receiver, Sender};

pub const DEFAULT_DASHBOARD_BROADCAST_CAPACITY: usize = 1024;

#[derive(Clone, Debug)]
pub struct DashboardBroadcaster {
    tx: Sender<RequestEvent>,
}

impl DashboardBroadcaster {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_DASHBOARD_BROADCAST_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let bounded_capacity = capacity.max(1);
        let (tx, _rx) = broadcast::channel(bounded_capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> Receiver<RequestEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: RequestEvent) {
        let _result = self.tx.send(event);
    }
}

impl Default for DashboardBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

pub fn record_dashboard_sse_lagged(skipped: u64) {
    cc_lb_observability::increment_dropped_events_by("sse_lagged", skipped);
}
