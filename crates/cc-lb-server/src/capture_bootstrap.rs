use std::path::Path;
use std::sync::Arc;

use cc_lb_config::Config;
use cc_lb_control::InMemoryBus;
use cc_lb_engine::Lifecycle;
use tokio::sync::Mutex;

use crate::signal;

pub(crate) struct CaptureRuntime {
    sink: cc_lb_capture::sink::CaptureSink,
    writer_handle: cc_lb_capture::sink::CaptureWriterHandle,
    subscriber_handle: cc_lb_capture::response_subscriber::CaptureResponseSubscriberHandle,
}

pub(crate) async fn start_capture_runtime(
    config: &Config,
    data_dir: &Path,
    bus: &InMemoryBus,
) -> Option<CaptureRuntime> {
    if !config.capture.enabled {
        return None;
    }

    let path = data_dir.join(&config.capture.path);
    match cc_lb_capture::store::open_capture_store(&path).await {
        Ok(store) => {
            let (sink, writer_handle) = cc_lb_capture::sink::CaptureSink::new_with_retention(
                store,
                config.capture.channel_capacity,
                config.capture.retention_max_rows,
            );
            let rx = bus.attach_lifecycle_capture(
                cc_lb_control::event_bus::DEFAULT_LIFECYCLE_CAPTURE_CAPACITY,
            );
            let subscriber_handle =
                cc_lb_capture::response_subscriber::spawn_lifecycle_capture_response_subscriber(
                    rx,
                    sink.clone(),
                );
            Some(CaptureRuntime {
                sink,
                writer_handle,
                subscriber_handle,
            })
        }
        Err(error) => {
            tracing::warn!(%error, "failed to open capture store; capture will be disabled");
            None
        }
    }
}

pub(crate) fn attach_capture_handle(
    lifecycle: Lifecycle,
    runtime: Option<&CaptureRuntime>,
) -> Lifecycle {
    match runtime {
        Some(runtime) => lifecycle.with_capture_handle(
            cc_lb_capture::hook::CaptureHandle::from_sink(runtime.sink.clone()),
        ),
        None => lifecycle,
    }
}

pub(crate) fn add_shutdown_hook(signals: &signal::SignalHandle, runtime: Option<CaptureRuntime>) {
    let Some(runtime) = runtime else {
        return;
    };
    let subscriber_slot = Arc::new(Mutex::new(Some(runtime.subscriber_handle)));
    let writer_slot = Arc::new(Mutex::new(Some(runtime.writer_handle)));
    signals.add_shutdown_hook(move || {
        let subscriber_slot = subscriber_slot.clone();
        let writer_slot = writer_slot.clone();
        async move {
            if let Some(handle) = subscriber_slot.lock().await.take() {
                handle.shutdown().await;
            }
            if let Some(handle) = writer_slot.lock().await.take() {
                handle.shutdown().await;
            }
        }
    });
}
