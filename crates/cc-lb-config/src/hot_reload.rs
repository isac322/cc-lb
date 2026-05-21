use std::path::{Path, PathBuf};
use std::time::Duration;

use notify::{Event, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::Config;

const DEBOUNCE: Duration = Duration::from_millis(500);

pub fn watch_for_reload(path: &Path, tx: mpsc::Sender<Config>) -> JoinHandle<()> {
    let path = path.to_path_buf();

    tokio::spawn(async move {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let mut watcher = match notify::recommended_watcher(move |event| {
            let _ = event_tx.send(event);
        }) {
            Ok(watcher) => watcher,
            Err(_) => return,
        };

        let watch_dir = watch_dir_for(&path);
        if watcher
            .watch(&watch_dir, RecursiveMode::NonRecursive)
            .is_err()
        {
            return;
        }

        run_reload_loop(path, tx, &mut event_rx).await;
    })
}

async fn run_reload_loop(
    path: PathBuf,
    tx: mpsc::Sender<Config>,
    event_rx: &mut mpsc::UnboundedReceiver<notify::Result<Event>>,
) {
    #[cfg(unix)]
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok();

    loop {
        #[cfg(unix)]
        {
            tokio::select! {
                event = event_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };
                    if event_matches_config(&path, event) && !debounce_load_and_send(&path, &tx, event_rx).await {
                        return;
                    }
                }
                _ = recv_sighup(&mut sighup) => {
                    if !debounce_load_and_send(&path, &tx, event_rx).await {
                        return;
                    }
                }
            }
        }

        #[cfg(not(unix))]
        {
            let Some(event) = event_rx.recv().await else {
                return;
            };
            if event_matches_config(&path, event)
                && !debounce_load_and_send(&path, &tx, event_rx).await
            {
                return;
            }
        }
    }
}

async fn debounce_load_and_send(
    path: &Path,
    tx: &mpsc::Sender<Config>,
    event_rx: &mut mpsc::UnboundedReceiver<notify::Result<Event>>,
) -> bool {
    tokio::time::sleep(DEBOUNCE).await;

    while let Ok(event) = event_rx.try_recv() {
        if !event_matches_config(path, event) {
            continue;
        }
    }

    let Ok(config) = Config::load(path) else {
        return true;
    };

    tx.send(config).await.is_ok()
}

fn event_matches_config(path: &Path, event: notify::Result<Event>) -> bool {
    let Ok(event) = event else {
        return false;
    };

    if event.paths.is_empty() {
        return true;
    }

    let target_file_name = path.file_name();
    event.paths.iter().any(|event_path| {
        event_path == path
            || event_path
                .file_name()
                .is_some_and(|name| Some(name) == target_file_name)
    })
}

fn watch_dir_for(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(unix)]
async fn recv_sighup(signal: &mut Option<tokio::signal::unix::Signal>) {
    if let Some(signal) = signal {
        let _ = signal.recv().await;
    } else {
        std::future::pending::<()>().await;
    }
}
