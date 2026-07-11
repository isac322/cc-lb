use http::StatusCode;

use crate::terminal_observer::{LifecycleContext, error_codes};

const CLIENT_CLOSED_STATUS: u16 = 499;

pub(crate) struct DownstreamStreamDropGuard {
    observer: Option<LifecycleContext>,
}

impl DownstreamStreamDropGuard {
    pub(crate) fn armed(observer: Option<LifecycleContext>) -> Self {
        Self { observer }
    }

    pub(crate) const fn disarmed() -> Self {
        Self { observer: None }
    }

    pub(crate) fn disarm(&mut self) {
        self.observer = None;
    }
}

impl Drop for DownstreamStreamDropGuard {
    fn drop(&mut self) {
        if let Some(observer) = self.observer.take()
            && let Ok(status) = StatusCode::from_u16(CLIENT_CLOSED_STATUS)
        {
            observer.set_terminal(status, error_codes::CLIENT_CLOSED_REQUEST);
        }
    }
}
