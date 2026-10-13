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

/// Upper bound on `App::start`'s wait for the shutdown hooks after the proxy
/// listener has stopped. The hooks run inside `ShutdownRunner::run`, which already
/// bounds each of them internally (the lifecycle assembler's 2 s overflow
/// drain plus its final flush, the diagnostics sink's 2 s drain, and the other
/// per-subscriber shutdowns that all sit within the 500 ms
/// `SHUTDOWN_TASK_TIMEOUT` cleanup budget used for the sibling tasks). 15 s
/// exceeds their combined bound by a wide margin while still guaranteeing the
/// process cannot be held forever by a wedged hook. On expiry `start` warns
/// and returns; the hooks keep running detached.
pub const SHUTDOWN_HOOKS_TIMEOUT: Duration = Duration::from_secs(15);

/// Owns everything needed to run the shutdown sequence exactly once.
/// `start` spawns the run detached (not through `SignalTasks`) on purpose:
/// `SignalTasks` aborts its tracked tasks when the last owner drops, and an
/// aborted shutdown run would cut off the hooks (assembler flush, diagnostics
/// drain) mid-flight and lose their writes. The detached task always runs the
/// hooks to completion and sends `drain_complete` (#948).
#[derive(Clone)]
struct ShutdownRunner {
    shutdown: watch::Sender<bool>,
    drain_complete: watch::Sender<bool>,
    drain: DrainController,
    drain_timeout: Duration,
    shutdown_started: Arc<AtomicBool>,
    shutdown_hooks: ShutdownHooks,
}

#[derive(Clone)]
pub struct SignalHandle {
    runner: ShutdownRunner,
    tasks: SignalTasks,
}

impl ShutdownRunner {
    fn start(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }

        tokio::spawn(self.clone().run());
    }

    fn is_started(&self) -> bool {
        self.shutdown_started.load(Ordering::Acquire)
    }

    async fn run(self) {
        self.drain.trigger();
        let _ = self.shutdown.send(true);

        // Drain in-flight requests BEFORE running shutdown hooks: the hooks stop
        // background writers (request-event assembler, pricing, logger) that must
        // still be alive to persist the terminal events of draining requests.
        let timed_out = self.drain.await_drained(self.drain_timeout).await;
        if timed_out {
            let force_closed = self.drain.mark_force_closed();
            tracing::warn!(
                force_closed,
                "graceful drain deadline elapsed with proxy request handlers still in flight"
            );
        }

        // Runs even on drain timeout so shutdown cannot hang forever.
        self.shutdown_hooks.run_all().await;

        let _ = self.drain_complete.send(true);
    }
}

impl SignalHandle {
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.runner.shutdown.subscribe()
    }

    pub fn subscribe_drain_complete(&self) -> watch::Receiver<bool> {
        self.runner.drain_complete.subscribe()
    }

    pub fn start_shutdown(&self) {
        self.runner.start();
    }

    /// Whether a shutdown run has been triggered (signal or `start_shutdown`).
    /// `App::start` uses this to decide whether the proxy task ended as part
    /// of a graceful shutdown — in which case it waits for `drain_complete` —
    /// or on its own, in which case it must not wait.
    pub fn is_shutdown_started(&self) -> bool {
        self.runner.is_started()
    }

    pub fn is_draining(&self) -> bool {
        self.runner.drain.is_draining()
    }

    pub fn set_draining(&self, draining: bool) {
        self.runner.drain.set_draining(draining);
    }

    pub fn add_shutdown_hook<F, Fut>(&self, hook: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.runner
            .shutdown_hooks
            .push(Arc::new(move || Box::pin(hook())));
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
        runner: ShutdownRunner {
            shutdown,
            drain_complete,
            drain,
            drain_timeout,
            shutdown_started: Arc::new(AtomicBool::new(false)),
            shutdown_hooks: ShutdownHooks::default(),
        },
        tasks: SignalTasks::default(),
    };

    install_sigterm(&handle);
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
fn install_sigterm(handle: &SignalHandle) {
    let runner = handle.runner.clone();
    handle.tasks.spawn(async move {
        let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        else {
            return;
        };
        let _ = term.recv().await;
        runner.start();
    });
}

#[cfg(not(unix))]
fn install_sigterm(handle: &SignalHandle) {
    let runner = handle.runner.clone();
    handle.tasks.spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            runner.start();
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
