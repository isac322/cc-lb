use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

pub type SighupHandler = Arc<dyn Fn() + Send + Sync + 'static>;
type ShutdownHookFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type ShutdownHook = Arc<dyn Fn() -> ShutdownHookFuture + Send + Sync + 'static>;

use cc_lb_engine::DrainController;

#[derive(Clone)]
pub struct SignalHandle {
    shutdown: watch::Sender<bool>,
    drain_complete: watch::Sender<bool>,
    drain: DrainController,
    drain_timeout: Duration,
    shutdown_started: Arc<AtomicBool>,
    tasks: SignalTasks,
    shutdown_hooks: ShutdownHooks,
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

        let shutdown = self.shutdown.clone();
        let drain_complete = self.drain_complete.clone();
        let drain = self.drain.clone();
        let drain_timeout = self.drain_timeout;
        let shutdown_hooks = self.shutdown_hooks.clone();
        self.tasks.spawn(async move {
            run_shutdown(
                shutdown,
                drain_complete,
                drain,
                drain_timeout,
                shutdown_hooks,
            )
            .await;
        });
    }

    pub fn is_draining(&self) -> bool {
        self.drain.is_draining()
    }

    pub fn set_draining(&self, draining: bool) {
        self.drain.set_draining(draining);
    }

    pub fn add_shutdown_hook<F, Fut>(&self, hook: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.shutdown_hooks.push(Arc::new(move || Box::pin(hook())));
    }
}

async fn run_shutdown(
    shutdown: watch::Sender<bool>,
    drain_complete: watch::Sender<bool>,
    drain: DrainController,
    drain_timeout: Duration,
    shutdown_hooks: ShutdownHooks,
) {
    drain.trigger();
    let _ = shutdown.send(true);

    // Drain in-flight requests BEFORE running shutdown hooks: the hooks stop
    // background writers (request-event assembler, pricing, logger) that must
    // still be alive to persist the terminal events of draining requests.
    let timed_out = drain.await_drained(drain_timeout).await;
    if timed_out {
        let force_closed = drain.mark_force_closed();
        tracing::warn!(
            force_closed,
            "graceful drain deadline elapsed with proxy request handlers still in flight"
        );
    }

    // Runs even on drain timeout so shutdown cannot hang forever.
    shutdown_hooks.run_all().await;

    let _ = drain_complete.send(true);
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
        tasks: SignalTasks::default(),
        shutdown_hooks: ShutdownHooks::default(),
    };

    install_sigterm(handle.clone());
    install_sighup(sighup_handler, handle.tasks.clone());
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
    let shutdown = handle.shutdown.clone();
    let drain_complete = handle.drain_complete.clone();
    let drain = handle.drain.clone();
    let drain_timeout = handle.drain_timeout;
    let shutdown_started = handle.shutdown_started.clone();
    let shutdown_hooks = handle.shutdown_hooks.clone();
    let tasks = handle.tasks;
    tasks.spawn(async move {
        let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        else {
            return;
        };
        let _ = term.recv().await;
        if shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }
        run_shutdown(
            shutdown,
            drain_complete,
            drain,
            drain_timeout,
            shutdown_hooks,
        )
        .await;
    });
}

#[cfg(not(unix))]
fn install_sigterm(handle: SignalHandle) {
    let shutdown = handle.shutdown.clone();
    let drain_complete = handle.drain_complete.clone();
    let drain = handle.drain.clone();
    let drain_timeout = handle.drain_timeout;
    let shutdown_started = handle.shutdown_started.clone();
    let shutdown_hooks = handle.shutdown_hooks.clone();
    let tasks = handle.tasks;
    tasks.spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            if shutdown_started.swap(true, Ordering::AcqRel) {
                return;
            }
            run_shutdown(
                shutdown,
                drain_complete,
                drain,
                drain_timeout,
                shutdown_hooks,
            )
            .await;
        }
    });
}

#[cfg(unix)]
fn install_sighup(handler: Option<SighupHandler>, tasks: SignalTasks) {
    tasks.spawn(async move {
        let Ok(mut sighup) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        else {
            return;
        };
        while sighup.recv().await.is_some() {
            if let Some(handler) = &handler {
                tracing::info!("SIGHUP received; reloading TLS certificate and key only");
                handler();
            } else {
                tracing::info!(
                    "SIGHUP received; configuration is fixed at startup and no TLS reload handler is installed, restart cc-lb to apply config changes"
                );
            }
        }
    });
}

#[cfg(not(unix))]
fn install_sighup(_handler: Option<SighupHandler>, _tasks: SignalTasks) {}

#[derive(Clone, Default)]
struct SignalTasks {
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

#[derive(Clone, Default)]
struct ShutdownHooks {
    hooks: Arc<Mutex<Vec<ShutdownHook>>>,
}

impl ShutdownHooks {
    fn push(&self, hook: ShutdownHook) {
        if let Ok(mut hooks) = self.hooks.lock() {
            hooks.push(hook);
        }
    }

    async fn run_all(&self) {
        let hooks = match self.hooks.lock() {
            Ok(hooks) => hooks.clone(),
            Err(_) => Vec::new(),
        };
        for hook in hooks {
            hook().await;
        }
    }
}

impl SignalTasks {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        match self.handles.lock() {
            Ok(mut handles) => handles.push(handle),
            Err(_) => handle.abort(),
        }
    }
}

impl Drop for SignalTasks {
    fn drop(&mut self) {
        if Arc::strong_count(&self.handles) != 1 {
            return;
        }
        if let Ok(mut handles) = self.handles.lock() {
            for handle in handles.drain(..) {
                handle.abort();
            }
        }
    }
}
