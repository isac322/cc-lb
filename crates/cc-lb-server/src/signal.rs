use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub type SighupHandler = Arc<dyn Fn() + Send + Sync + 'static>;
type ShutdownHookFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type ShutdownHook = Arc<dyn Fn() -> ShutdownHookFuture + Send + Sync + 'static>;

use cc_lb_engine::DrainController;

/// Upper bound on `App::start`'s wait for the shutdown hooks after the proxy
/// listener and in-flight request drain have stopped (`hooks_started`). The
/// hooks run inside `ShutdownRunner::run`, which cancels and joins scheduler
/// workers under the 10 s `SCHEDULER_SHUTDOWN_TIMEOUT` budget, then bounds each
/// remaining hook internally (the lifecycle assembler's 2 s overflow drain plus
/// its final flush, the diagnostics sink's 2 s drain, and the other
/// per-subscriber shutdowns within the 500 ms `SHUTDOWN_TASK_TIMEOUT` budget).
/// 25 s exceeds their combined bound while still guaranteeing the process cannot
/// be held forever by a wedged hook. On expiry `start` warns and returns; the
/// hooks keep running detached.
pub const SHUTDOWN_HOOKS_TIMEOUT: Duration = Duration::from_secs(25);

/// Owns everything needed to run the shutdown sequence exactly once.
/// `start` spawns the run detached (not through `SignalTasks`) on purpose:
/// `SignalTasks` aborts its tracked tasks when the last owner drops, and an
/// aborted shutdown run would cut off the hooks (assembler flush, diagnostics
/// drain) mid-flight and lose their writes. The detached task always runs the
/// hooks to completion and sends `drain_complete` (#948).
#[derive(Clone)]
struct ShutdownRunner {
    shutdown: watch::Sender<bool>,
    hooks_started: watch::Sender<bool>,
    drain_complete: watch::Sender<bool>,
    drain: DrainController,
    drain_timeout: Duration,
    shutdown_started: Arc<AtomicBool>,
    drain_start_tokens: Arc<Mutex<Vec<CancellationToken>>>,
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
        self.shutdown.send_replace(true);
        let tokens = match self.drain_start_tokens.lock() {
            Ok(tokens) => tokens.clone(),
            Err(_) => Vec::new(),
        };
        for token in tokens {
            token.cancel();
        }

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
            // Allow force-closed WebSocket handlers to emit their terminal
            // `RequestTerminated` events and release their drain guards before
            // background subscribers/assemblers shut down.
            let _ = self.drain.await_drained(Duration::from_millis(500)).await;
        }

        self.hooks_started.send_replace(true);
        // Runs even on drain timeout so shutdown cannot hang forever.
        self.shutdown_hooks.run_all().await;

        self.drain_complete.send_replace(true);
    }
}

impl SignalHandle {
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.runner.shutdown.subscribe()
    }

    pub fn subscribe_hooks_started(&self) -> watch::Receiver<bool> {
        self.runner.hooks_started.subscribe()
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

    /// Cancel `token` synchronously as soon as graceful drain begins (before
    /// awaiting in-flight request drain, so slow async callbacks cannot block
    /// `hooks_started`).
    pub fn add_drain_start_cancel_token(&self, token: CancellationToken) {
        if let Ok(mut tokens) = self.runner.drain_start_tokens.lock() {
            tokens.push(token);
        }
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
    let (hooks_started, _) = watch::channel(false);
    let (drain_complete, _) = watch::channel(false);
    let handle = SignalHandle {
        runner: ShutdownRunner {
            shutdown,
            hooks_started,
            drain_complete,
            drain,
            drain_timeout,
            shutdown_started: Arc::new(AtomicBool::new(false)),
            drain_start_tokens: Arc::new(Mutex::new(Vec::new())),
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
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        let mut int =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
        match (term.as_mut(), int.as_mut()) {
            (Some(term), Some(int)) => {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = int.recv() => {}
                }
            }
            (Some(term), None) => {
                let _ = term.recv().await;
            }
            (None, Some(int)) => {
                let _ = int.recv().await;
            }
            (None, None) => return,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn shutdown_runner_waits_for_drain_timeout_and_force_closed_release_before_hooks() {
        let drain = DrainController::new();
        let guard = drain.try_track().expect("initial guard acquired");
        let handle = install(drain.clone(), Duration::from_millis(80), None);
        let mut drain_complete = handle.subscribe_drain_complete();

        let (hook_started_tx, mut hook_started_rx) = oneshot::channel();
        let hook_tx = Arc::new(Mutex::new(Some(hook_started_tx)));
        handle.add_shutdown_hook(move || {
            let hook_tx = hook_tx.clone();
            async move {
                if let Ok(mut slot) = hook_tx.lock()
                    && let Some(tx) = slot.take()
                {
                    let _ = tx.send(());
                }
            }
        });

        let started_at = tokio::time::Instant::now();
        handle.start_shutdown();
        tokio::time::timeout(Duration::from_secs(2), drain.wait_for_draining())
            .await
            .expect("shutdown triggers drain");
        assert!(!drain.is_force_closed());

        tokio::time::timeout(Duration::from_secs(2), drain.wait_for_force_close())
            .await
            .expect("drain_timeout forces close");
        assert!(
            started_at.elapsed() >= Duration::from_millis(60),
            "force close must wait for drain_timeout, elapsed={:?}",
            started_at.elapsed()
        );
        assert!(
            hook_started_rx.try_recv().is_err(),
            "hooks must not run before force-closed handler releases its drain guard"
        );

        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), hook_started_rx)
            .await
            .expect("hook runs once guard drops")
            .expect("hook channel sends");
        tokio::time::timeout(
            Duration::from_secs(2),
            wait_for_shutdown(drain_complete.clone()),
        )
        .await
        .expect("drain_complete signals");
        assert!(*drain_complete.borrow_and_update());
    }

    #[tokio::test]
    async fn start_shutdown_before_subscribe_preserves_all_milestones() {
        let drain = DrainController::new();
        let handle = install(drain, Duration::from_millis(10), None);
        handle.start_shutdown();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if handle.is_draining() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown runner reaches drain state");

        let shutdown_rx = handle.subscribe();
        let hooks_started_rx = handle.subscribe_hooks_started();
        let drain_complete_rx = handle.subscribe_drain_complete();

        tokio::time::timeout(Duration::from_secs(2), async move {
            wait_for_shutdown(shutdown_rx).await;
            wait_for_shutdown(hooks_started_rx).await;
            wait_for_shutdown(drain_complete_rx).await;
        })
        .await
        .expect("late subscribers must observe retained milestones even without prior receivers");
    }

    #[tokio::test]
    async fn drain_start_tokens_cancel_synchronously_before_drain_completes_and_hooks_started_precedes_shutdown_hooks()
     {
        let drain = DrainController::new();
        let guard = drain.try_track().expect("initial guard acquired");
        let handle = install(drain.clone(), Duration::from_secs(10), None);
        let hooks_started_rx = handle.subscribe_hooks_started();
        let drain_complete_rx = handle.subscribe_drain_complete();

        let drain_cancel = CancellationToken::new();
        handle.add_drain_start_cancel_token(drain_cancel.clone());

        let (shutdown_hook_tx, mut shutdown_hook_rx) = oneshot::channel();
        let saw_hooks_started_in_hook = Arc::new(AtomicBool::new(false));
        let saw_hooks_started_clone = saw_hooks_started_in_hook.clone();
        let hooks_started_probe = hooks_started_rx.clone();
        let shutdown_hook_tx = Arc::new(Mutex::new(Some(shutdown_hook_tx)));
        handle.add_shutdown_hook(move || {
            let saw_hooks_started_clone = saw_hooks_started_clone.clone();
            let hooks_started_probe = hooks_started_probe.clone();
            let shutdown_hook_tx = shutdown_hook_tx.clone();
            async move {
                saw_hooks_started_clone.store(*hooks_started_probe.borrow(), Ordering::Release);
                if let Ok(mut slot) = shutdown_hook_tx.lock()
                    && let Some(tx) = slot.take()
                {
                    let _ = tx.send(());
                }
            }
        });

        handle.start_shutdown();
        tokio::time::timeout(Duration::from_secs(2), drain_cancel.cancelled())
            .await
            .expect("drain_start cancel token fires while request guard is still held");
        assert!(
            !*hooks_started_rx.borrow(),
            "hooks_started must stay false while drain guard is active"
        );
        assert!(
            shutdown_hook_rx.try_recv().is_err(),
            "shutdown hooks must wait until drain finishes"
        );

        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), async {
            wait_for_shutdown(hooks_started_rx).await;
            shutdown_hook_rx.await.expect("shutdown hook completes");
            wait_for_shutdown(drain_complete_rx).await;
        })
        .await
        .expect("hooks_started, shutdown hooks, and drain_complete complete cleanly");
        assert!(saw_hooks_started_in_hook.load(Ordering::Acquire));
    }
}
