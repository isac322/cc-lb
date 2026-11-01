mod adaptive;
mod cron;

pub use adaptive::AdaptiveWorker;
pub use cron::{CronWorker, build_cron_worker, build_cron_worker_named};

pub(in crate::worker) use adaptive::{
    build_backend_adaptive_worker, build_backend_adaptive_worker_named,
    build_backend_keepalive_worker_named,
};

pub(crate) fn catch_panic_layer() -> apalis::layers::catch_panic::CatchPanicLayer<
    impl FnMut(&Box<dyn std::any::Any + Send>) -> apalis_core::error::AbortError
    + Clone
    + Send
    + Sync
    + 'static,
    apalis_core::error::AbortError,
> {
    apalis::layers::catch_panic::CatchPanicLayer::with_panic_handler(
        |_: &Box<dyn std::any::Any + Send>| {
            apalis_core::error::AbortError::new(apalis::layers::catch_panic::PanicError(
                "worker task panicked".to_owned(),
            ))
        },
    )
}
