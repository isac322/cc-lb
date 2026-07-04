use tokio::sync::mpsc;

use super::metrics::record_notify_dropped;
use crate::event_bus::{
    BusError, BusReceiver, EventFanout, InMemoryBus, LifecycleBusReceiver, RequestEventBus,
    RequestEventUpdate,
};
use crate::metrics_labels::NotifyDropReason;

#[derive(Clone)]
pub struct PgNotifyFanout {
    local_bus: InMemoryBus,
    notifier_tx: mpsc::Sender<RequestEventUpdate>,
}

impl PgNotifyFanout {
    pub fn new(local_bus: InMemoryBus, notifier_tx: mpsc::Sender<RequestEventUpdate>) -> Self {
        Self {
            local_bus,
            notifier_tx,
        }
    }
}

#[async_trait::async_trait]
impl EventFanout for PgNotifyFanout {
    async fn publish_partial(&self, update: RequestEventUpdate) -> Result<(), BusError> {
        publish_partial_nonblocking(&self.local_bus, &self.notifier_tx, update);
        Ok(())
    }

    fn subscribe(&self) -> BusReceiver {
        self.local_bus.subscribe()
    }
}

impl RequestEventBus for PgNotifyFanout {
    fn publish(&self, update: RequestEventUpdate) {
        match update {
            partial @ RequestEventUpdate::Partial(_) => {
                publish_partial_nonblocking(&self.local_bus, &self.notifier_tx, partial);
            }
            final_update @ RequestEventUpdate::Final(_) => self.local_bus.publish(final_update),
        }
    }

    fn subscribe(&self) -> BusReceiver {
        self.local_bus.subscribe()
    }

    fn publish_lifecycle(&self, event: cc_lb_contract::LifecycleEvent) {
        self.local_bus.publish_lifecycle(event);
    }

    fn subscribe_lifecycle(&self) -> LifecycleBusReceiver {
        self.local_bus.subscribe_lifecycle()
    }
}

fn publish_partial_nonblocking(
    local_bus: &InMemoryBus,
    notifier_tx: &mpsc::Sender<RequestEventUpdate>,
    update: RequestEventUpdate,
) {
    local_bus.publish(update.clone());
    match notifier_tx.try_send(update) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            record_notify_dropped(NotifyDropReason::QueueFull);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            record_notify_dropped(NotifyDropReason::QueueClosed);
        }
    }
}
