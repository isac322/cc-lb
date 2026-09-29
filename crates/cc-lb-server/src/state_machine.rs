use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::Serialize;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerState {
    Starting,
    Ready,
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
    use std::io::ErrorKind;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;
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
    async fn admin_closed_during_startup_rehandshake() {
        let state = Arc::new(ServerStateHandle::new_starting());
        let addr = unused_loopback_addr().await;

        assert_connect_refused(addr).await;

        let (bound_tx, bound_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let bind_task = tokio::spawn({
            let state = state.clone();
            async move {
                state.wait_for_ready().await;
                let listener = TcpListener::bind(addr)
                    .await
                    .expect("listener should bind after ready");
                let bound_addr = listener.local_addr().expect("listener should have addr");
                let _ = bound_tx.send(bound_addr);
                let _ = release_rx.await;
                drop(listener);
            }
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(state.current(), ServerState::Starting);
        assert_connect_refused(addr).await;

        state.transition_to_ready();
        let bound_addr = timeout(Duration::from_secs(1), bound_rx)
            .await
            .expect("listener bind timed out")
            .expect("listener bind task dropped channel");
        timeout(Duration::from_secs(1), TcpStream::connect(bound_addr))
            .await
            .expect("connect after ready timed out")
            .expect("connect after ready should succeed");

        let _ = release_tx.send(());
        bind_task.await.expect("bind task failed");
    }

    async fn unused_loopback_addr() -> SocketAddr {
        let listener = TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("ephemeral listener should bind");
        let addr = listener.local_addr().expect("ephemeral listener has addr");
        drop(listener);
        addr
    }

    async fn assert_connect_refused(addr: SocketAddr) {
        match timeout(Duration::from_secs(1), TcpStream::connect(addr)).await {
            Ok(Err(error)) if error.kind() == ErrorKind::ConnectionRefused => {}
            Ok(Err(error)) => panic!("expected connection refused for {addr}, got {error}"),
            Ok(Ok(_stream)) => panic!("expected closed port for {addr}, but connect succeeded"),
            Err(_) => panic!("expected connection refused for {addr}, but connect timed out"),
        }
    }
}
