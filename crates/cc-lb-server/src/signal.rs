use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

pub type SighupHandler = Arc<dyn Fn() + Send + Sync + 'static>;

use crate::drain::DrainController;

#[derive(Clone)]
pub struct SignalHandle {
    shutdown: watch::Sender<bool>,
    drain_complete: watch::Sender<bool>,
    drain: DrainController,
    drain_timeout: Duration,
    shutdown_started: Arc<AtomicBool>,
    debug_logging: Arc<AtomicBool>,
}

impl SignalHandle {
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub fn subscribe_drain_complete(&self) -> watch::Receiver<bool> {
        self.drain_complete.subscribe()
    }

    pub fn start_shutdown(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }

        let handle = self.clone();
        tokio::spawn(async move {
            handle.run_shutdown().await;
        });
    }

    pub fn debug_logging_enabled(&self) -> bool {
        self.debug_logging.load(Ordering::Relaxed)
    }

    pub fn is_draining(&self) -> bool {
        self.drain.is_draining()
    }

    pub fn set_draining(&self, draining: bool) {
        self.drain.set_draining(draining);
    }

    async fn run_shutdown(self) {
        self.drain.trigger();
        let _ = self.shutdown.send(true);

        let timed_out = self.drain.await_drained(self.drain_timeout).await;
        if timed_out {
            let force_closed = self.drain.mark_force_closed();
            tracing::warn!(
                force_closed,
                "graceful drain deadline elapsed; force closing in-flight requests"
            );
        }

        let _ = self.drain_complete.send(true);
    }
}

pub fn install(
    drain: DrainController,
    drain_timeout: Duration,
    sighup_handler: Option<SighupHandler>,
) -> SignalHandle {
    let (shutdown, _) = watch::channel(false);
    let (drain_complete, _) = watch::channel(false);
    let handle = SignalHandle {
        shutdown,
        drain_complete,
        drain,
        drain_timeout,
        shutdown_started: Arc::new(AtomicBool::new(false)),
        debug_logging: Arc::new(AtomicBool::new(false)),
    };

    install_sigterm(handle.clone());
    install_sighup(sighup_handler);
    install_sigusr1(handle.debug_logging.clone());
    handle
}

pub async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(unix)]
fn install_sigterm(handle: SignalHandle) {
    tokio::spawn(async move {
        let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        else {
            return;
        };
        let _ = term.recv().await;
        handle.start_shutdown();
    });
}

#[cfg(not(unix))]
fn install_sigterm(handle: SignalHandle) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            handle.start_shutdown();
        }
    });
}

#[cfg(unix)]
fn install_sighup(handler: Option<SighupHandler>) {
    tokio::spawn(async move {
        let Ok(mut sighup) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        else {
            return;
        };
        while sighup.recv().await.is_some() {
            tracing::info!("configuration reload signal received");
            if let Some(handler) = &handler {
                handler();
            }
        }
    });
}

#[cfg(not(unix))]
fn install_sighup(_handler: Option<SighupHandler>) {}

#[cfg(unix)]
fn install_sigusr1(debug_logging: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let Ok(mut sigusr1) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
        else {
            return;
        };
        while sigusr1.recv().await.is_some() {
            debug_logging.store(true, Ordering::Relaxed);
            tracing::info!("temporary debug logging enabled by SIGUSR1");
            let debug_logging = debug_logging.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                debug_logging.store(false, Ordering::Relaxed);
                tracing::info!("temporary debug logging disabled");
            });
        }
    });
}

#[cfg(not(unix))]
fn install_sigusr1(_debug_logging: Arc<AtomicBool>) {}
