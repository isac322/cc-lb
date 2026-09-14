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

use crate::drain::DrainController;

#[derive(Clone)]
pub struct SignalHandle {
    shutdown: watch::Sender<bool>,
    drain_complete: watch::Sender<bool>,
    drain: DrainController,
    drain_timeout: Duration,
    shutdown_started: Arc<AtomicBool>,
    debug_logging: Arc<AtomicBool>,
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

    pub fn debug_logging_enabled(&self) -> bool {
        self.debug_logging.load(Ordering::Relaxed)
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
    shutdown_hooks.run_all().await;

    let timed_out = drain.await_drained(drain_timeout).await;
    if timed_out {
        let force_closed = drain.mark_force_closed();
        tracing::warn!(
            force_closed,
            "graceful drain deadline elapsed with proxy request handlers still in flight"
        );
    }

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
        debug_logging: Arc::new(AtomicBool::new(false)),
        tasks: SignalTasks::default(),
        shutdown_hooks: ShutdownHooks::default(),
    };

    install_sigterm(handle.clone());
    install_sighup(sighup_handler, handle.tasks.clone());
    install_sigusr1(handle.debug_logging.clone(), handle.tasks.clone());
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
            tracing::info!("configuration reload signal received");
            if let Some(handler) = &handler {
                handler();
            }
        }
    });
}

#[cfg(not(unix))]
fn install_sighup(_handler: Option<SighupHandler>, _tasks: SignalTasks) {}

#[cfg(unix)]
fn install_sigusr1(debug_logging: Arc<AtomicBool>, tasks: SignalTasks) {
    tasks.spawn(async move {
        let Ok(mut sigusr1) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
        else {
            return;
        };
        let reset_sleep = tokio::time::sleep(Duration::from_secs(60));
        tokio::pin!(reset_sleep);

        loop {
            tokio::select! {
                signal = sigusr1.recv() => {
                    if signal.is_none() {
                        return;
                    }
                    debug_logging.store(true, Ordering::Relaxed);
                    reset_sleep.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(60));
                    tracing::info!("temporary debug logging enabled by SIGUSR1");
                }
                _ = &mut reset_sleep, if debug_logging.load(Ordering::Relaxed) => {
                debug_logging.store(false, Ordering::Relaxed);
                tracing::info!("temporary debug logging disabled");
                }
            }
        }
    });
}

#[cfg(not(unix))]
fn install_sigusr1(_debug_logging: Arc<AtomicBool>, _tasks: SignalTasks) {}

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
#[allow(non_snake_case)]
mod t2__tests {
    use super::*;

    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::middleware;
    use axum::routing::post;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    #[tokio::test]
    async fn signal_shutdown_pipeline_orchestration() {
        let drain = DrainController::new();
        let request_started = Arc::new(Notify::new());
        let request_release = Arc::new(Notify::new());
        let app = Router::new()
            .route(
                "/v1/messages",
                post({
                    let request_started = Arc::clone(&request_started);
                    let request_release = Arc::clone(&request_release);
                    move || {
                        let request_started = Arc::clone(&request_started);
                        let request_release = Arc::clone(&request_release);
                        async move {
                            request_started.notify_one();
                            request_release.notified().await;
                            "done"
                        }
                    }
                }),
            )
            .route_layer(middleware::from_fn_with_state(
                drain.clone(),
                crate::drain::proxy_drain_middleware,
            ));
        let request_task = tokio::spawn(
            app.oneshot(
                Request::post("/v1/messages")
                    .body(Body::empty())
                    .expect("request builds"),
            ),
        );
        request_started.notified().await;
        assert_eq!(drain.in_flight(), 1);

        let (shutdown, _) = watch::channel(false);
        let (drain_complete, _) = watch::channel(false);
        let handle = SignalHandle {
            shutdown,
            drain_complete,
            drain: drain.clone(),
            drain_timeout: Duration::from_secs(60),
            shutdown_started: Arc::new(AtomicBool::new(false)),
            debug_logging: Arc::new(AtomicBool::new(false)),
            tasks: SignalTasks::default(),
            shutdown_hooks: ShutdownHooks::default(),
        };
        let mut shutdown_rx = handle.subscribe();
        let mut drain_complete_rx = handle.subscribe_drain_complete();
        let hook_ran = Arc::new(Notify::new());
        let hook_observation = Arc::new(Mutex::new(None));
        handle.add_shutdown_hook({
            let drain = drain.clone();
            let shutdown_rx = shutdown_rx.clone();
            let hook_ran = Arc::clone(&hook_ran);
            let hook_observation = Arc::clone(&hook_observation);
            move || {
                let drain = drain.clone();
                let shutdown_rx = shutdown_rx.clone();
                let hook_ran = Arc::clone(&hook_ran);
                let hook_observation = Arc::clone(&hook_observation);
                async move {
                    *hook_observation.lock().expect("hook observation lock") =
                        Some((drain.is_draining(), *shutdown_rx.borrow()));
                    hook_ran.notify_one();
                }
            }
        });

        handle.start_shutdown();
        shutdown_rx
            .changed()
            .await
            .expect("shutdown sender remains alive");
        hook_ran.notified().await;

        assert!(drain.is_draining());
        assert!(*shutdown_rx.borrow());
        assert_eq!(
            *hook_observation.lock().expect("hook observation lock"),
            Some((true, true)),
            "shutdown hook runs after drain trigger and shutdown publication",
        );
        assert!(
            !*drain_complete_rx.borrow(),
            "drain completion waits for the in-flight request",
        );

        request_release.notify_one();
        request_task
            .await
            .expect("request task joins")
            .expect("request completes");
        drain_complete_rx
            .changed()
            .await
            .expect("drain-complete sender remains alive");
        assert!(*drain_complete_rx.borrow());
        assert_eq!(drain.in_flight(), 0);
    }
}
