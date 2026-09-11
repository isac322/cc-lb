use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::Serialize;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerState {
    Starting,
    Ready,
    ShuttingDown,
}

pub struct ServerStateHandle {
    state: ArcSwap<ServerState>,
    ready_notify: Arc<Notify>,
}

impl ServerStateHandle {
    pub fn new_starting() -> Self {
        Self {
            state: ArcSwap::from_pointee(ServerState::Starting),
            ready_notify: Arc::new(Notify::new()),
        }
    }

    pub fn current(&self) -> ServerState {
        *self.state.load_full()
    }

    pub fn transition_to_ready(&self) {
        let previous = self.state.swap(Arc::new(ServerState::Ready));
        if *previous != ServerState::Ready {
            self.ready_notify.notify_waiters();
        }
    }

    pub async fn wait_for_ready(&self) {
        loop {
            if self.current() == ServerState::Ready {
                return;
            }
            let notified = self.ready_notify.notified();
            if self.current() == ServerState::Ready {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::{ServerState, ServerStateHandle};

    #[tokio::test]
    async fn transition_idempotent() {
        let state = Arc::new(ServerStateHandle::new_starting());
        assert_eq!(state.current(), ServerState::Starting);

        let first_waiter = tokio::spawn({
            let state = state.clone();
            async move {
                state.wait_for_ready().await;
                state.current()
            }
        });
        let second_waiter = tokio::spawn({
            let state = state.clone();
            async move {
                state.wait_for_ready().await;
                state.current()
            }
        });

        state.transition_to_ready();
        state.transition_to_ready();

        assert_eq!(
            timeout(Duration::from_secs(1), first_waiter)
                .await
                .expect("first waiter timed out")
                .expect("first waiter task failed"),
            ServerState::Ready
        );
        assert_eq!(
            timeout(Duration::from_secs(1), second_waiter)
                .await
                .expect("second waiter timed out")
                .expect("second waiter task failed"),
            ServerState::Ready
        );
        assert_eq!(state.current(), ServerState::Ready);
        timeout(Duration::from_millis(50), state.wait_for_ready())
            .await
            .expect("ready wait should return immediately after transition");
    }

    #[tokio::test]
    async fn waiter_remains_pending_until_ready() {
        let state = Arc::new(ServerStateHandle::new_starting());
        let waiter = tokio::spawn({
            let state = state.clone();
            async move {
                state.wait_for_ready().await;
                state.current()
            }
        });

        tokio::task::yield_now().await;
        assert_eq!(state.current(), ServerState::Starting);
        assert!(
            !waiter.is_finished(),
            "wait_for_ready must remain pending while the server is starting",
        );

        state.transition_to_ready();
        assert_eq!(waiter.await.expect("waiter task joins"), ServerState::Ready,);
    }
}
