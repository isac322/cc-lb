//! Issue #948: `App::start` used to return as soon as the proxy listener
//! stopped, while the shutdown task was still executing the shutdown hooks.
//! Dropping the runtime then cut the hooks off mid-write. These tests drive a
//! real `App` with a hook gated on a oneshot release, so the wait is proven by
//! causality: `drain_complete` cannot fire while the gate is closed.
//!
//! The app's own hooks (registered first) perform real SQLite I/O on blocking
//! threads, which a paused clock would race with auto-advance. The tests
//! therefore run on the real clock until the gated hook has started (every
//! earlier hook has finished by then) and only pause it for the 15 s bound.

use std::net::TcpListener as StdTcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_config::Config;
use cc_lb_server::app::{App, build_app_for_testing};
use cc_lb_server::signal::{SHUTDOWN_HOOKS_TIMEOUT, SignalHandle};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};

/// Failure-only ceiling for steps that complete promptly in a correct build.
const FAILURE_LIMIT: Duration = Duration::from_secs(60);
/// Negative observation: a correct `start` can never return while the gate is
/// closed, so this window always elapses; a regressed `start` returns within
/// its sub-second cleanup and is caught here.
const PENDING_WINDOW: Duration = Duration::from_secs(1);
/// Virtual grace for `start` to finish once the hook bound expires.
const RETURN_OBSERVE: Duration = SHUTDOWN_HOOKS_TIMEOUT.saturating_mul(4);

struct GatedHook {
    started: oneshot::Receiver<()>,
    release: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    finished: Arc<AtomicBool>,
    done: oneshot::Receiver<()>,
}

impl GatedHook {
    /// Registers a shutdown hook that reports `started`, blocks on `release`,
    /// then sets `finished` and reports `done`.
    fn install(signal: &SignalHandle) -> Self {
        let (started_tx, started) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (done_tx, done) = oneshot::channel();
        let release = Arc::new(Mutex::new(Some(release_tx)));
        let finished = Arc::new(AtomicBool::new(false));

        let hook_started = Arc::new(Mutex::new(Some(started_tx)));
        let hook_release_rx = Arc::new(Mutex::new(Some(release_rx)));
        let hook_done = Arc::new(Mutex::new(Some(done_tx)));
        let hook_finished = finished.clone();
        signal.add_shutdown_hook(move || {
            let hook_started = hook_started.clone();
            let hook_release_rx = hook_release_rx.clone();
            let hook_done = hook_done.clone();
            let hook_finished = hook_finished.clone();
            async move {
                if let Ok(mut gate) = hook_started.lock()
                    && let Some(tx) = gate.take()
                {
                    let _ = tx.send(());
                }
                let release_rx = hook_release_rx.lock().ok().and_then(|mut gate| gate.take());
                if let Some(rx) = release_rx {
                    let _ = rx.await;
                }
                hook_finished.store(true, Ordering::Release);
                if let Ok(mut gate) = hook_done.lock()
                    && let Some(tx) = gate.take()
                {
                    let _ = tx.send(());
                }
            }
        });

        Self {
            started,
            release,
            finished,
            done,
        }
    }

    fn release(&self) {
        if let Ok(mut gate) = self.release.lock()
            && let Some(tx) = gate.take()
        {
            let _ = tx.send(());
        }
    }
}

/// Builds an `App` on reserved loopback ports. The reservations are handed back
/// and must be dropped right after `tokio::spawn(app.start())` so `start` owns
/// the listeners, keeping the release-to-bind window minimal.
async fn reserved_app() -> (App, [StdTcpListener; 2]) {
    let reservations = [
        StdTcpListener::bind("127.0.0.1:0").expect("reserve proxy port"),
        StdTcpListener::bind("127.0.0.1:0").expect("reserve admin port"),
    ];
    let mut config = Config::default();
    config.listener.proxy_addr = reservations[0].local_addr().expect("proxy addr");
    config.listener.admin_addr = reservations[1].local_addr().expect("admin addr");
    let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let app = build_app_for_testing(config, clock)
        .await
        .expect("test app builds");
    (app, reservations)
}

#[tokio::test(flavor = "current_thread")]
async fn start_waits_for_shutdown_hooks_before_returning() {
    let (app, reservations) = reserved_app().await;
    let signal = app.signal_handle();
    let mut hook = GatedHook::install(&signal);

    let mut start = tokio::spawn(app.start());
    drop(reservations);
    signal.start_shutdown();

    // The hook has begun and is gated; the shutdown task cannot send
    // drain_complete until it is released, so start must still be running.
    timeout(FAILURE_LIMIT, &mut hook.started)
        .await
        .expect("hook started")
        .expect("hook started channel open");
    assert!(
        timeout(PENDING_WINDOW, &mut start).await.is_err(),
        "start returned while a shutdown hook was still pending"
    );
    assert!(!hook.finished.load(Ordering::Acquire));

    hook.release();

    // After the release the hook completes, drain_complete fires, and start
    // returns. start can only observe drain_complete after the hook finished,
    // so `done` has already been sent when start's join resolves.
    timeout(FAILURE_LIMIT, &mut start)
        .await
        .expect("start returned after the hook was released")
        .expect("start task did not panic")
        .expect("start returned Ok");
    assert!(hook.finished.load(Ordering::Acquire));
    assert!(
        hook.done.try_recv().is_ok(),
        "start returned before the shutdown hook completed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn start_returns_at_the_hook_bound_and_the_hook_task_survives() {
    let (app, reservations) = reserved_app().await;
    let signal = app.signal_handle();
    let mut hook = GatedHook::install(&signal);

    let spawned_at = Instant::now();
    let mut start = tokio::spawn(app.start());
    drop(reservations);
    signal.start_shutdown();

    timeout(FAILURE_LIMIT, &mut hook.started)
        .await
        .expect("hook started")
        .expect("hook started channel open");

    // Every hook doing real I/O has finished; the gate stays closed, so the
    // drain_complete wait inside start can only end on SHUTDOWN_HOOKS_TIMEOUT.
    // Pause so that bound expires in virtual time.
    tokio::time::pause();
    timeout(RETURN_OBSERVE, &mut start)
        .await
        .expect("start returned once the hook bound expired")
        .expect("start task did not panic")
        .expect("start returned Ok");
    assert!(
        spawned_at.elapsed() >= SHUTDOWN_HOOKS_TIMEOUT,
        "start returned after {:?}, before the {SHUTDOWN_HOOKS_TIMEOUT:?} hook bound",
        spawned_at.elapsed()
    );
    assert!(
        !hook.finished.load(Ordering::Acquire),
        "the gated hook completed without a release"
    );

    // The shutdown run is detached from SignalTasks: dropping the last
    // SignalHandle aborts the signal watchers but must not abort the hook run.
    // Releasing the gate now still lets the hook finish.
    drop(signal);
    hook.release();
    timeout(FAILURE_LIMIT, &mut hook.done)
        .await
        .expect("hook finished after the last SignalHandle dropped")
        .expect("hook done channel open");
    assert!(hook.finished.load(Ordering::Acquire));
}
