use metrics::LocalRecorderGuard;
use metrics_util::debugging::{DebuggingRecorder, Snapshotter};

#[must_use]
pub fn local_recorder() -> (DebuggingRecorder, Snapshotter) {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    (recorder, snapshotter)
}

#[must_use]
pub fn install_local_recorder(recorder: &DebuggingRecorder) -> LocalRecorderGuard<'_> {
    metrics::set_default_local_recorder(recorder)
}

pub fn with_local_recorder<R>(f: impl FnOnce(&Snapshotter) -> R) -> R {
    let (recorder, snapshotter) = local_recorder();
    metrics::with_local_recorder(&recorder, || f(&snapshotter))
}
