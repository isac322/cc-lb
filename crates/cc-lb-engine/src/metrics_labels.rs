#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetReason {
    BackfillCap,
    BusLagged,
    StorageError,
}

impl ResetReason {
    pub const ALL: [Self; 3] = [Self::BackfillCap, Self::BusLagged, Self::StorageError];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BackfillCap => "backfill_cap",
            Self::BusLagged => "bus_lagged",
            Self::StorageError => "storage_error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialTrigger {
    RequestStarted,
    RouteCompleted,
    UpstreamResponseStarted,
    UsageObserved,
    StreamCompleted,
    RequestTerminated,
}

impl PartialTrigger {
    pub const ALL: [Self; 6] = [
        Self::RequestStarted,
        Self::RouteCompleted,
        Self::UpstreamResponseStarted,
        Self::UsageObserved,
        Self::StreamCompleted,
        Self::RequestTerminated,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestStarted => "request_started",
            Self::RouteCompleted => "route_completed",
            Self::UpstreamResponseStarted => "upstream_response_started",
            Self::UsageObserved => "usage_observed",
            Self::StreamCompleted => "stream_completed",
            Self::RequestTerminated => "request_terminated",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyDropReason {
    QueueFull,
    QueueClosed,
    SerializeError,
    PgError,
}

impl NotifyDropReason {
    pub const ALL: [Self; 4] = [
        Self::QueueFull,
        Self::QueueClosed,
        Self::SerializeError,
        Self::PgError,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
            Self::QueueClosed => "queue_closed",
            Self::SerializeError => "serialize_error",
            Self::PgError => "pg_error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifySentOutcome {
    Sent,
    TruncatedSent,
    Failed,
}

impl NotifySentOutcome {
    pub const ALL: [Self; 3] = [Self::Sent, Self::TruncatedSent, Self::Failed];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sent => "sent",
            Self::TruncatedSent => "truncated_sent",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyHttpOutcome {
    Success,
    NotFound,
    Unauthorized,
    Timeout,
    NetworkError,
}

impl NotifyHttpOutcome {
    pub const ALL: [Self; 5] = [
        Self::Success,
        Self::NotFound,
        Self::Unauthorized,
        Self::Timeout,
        Self::NetworkError,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::NotFound => "not_found",
            Self::Unauthorized => "unauthorized",
            Self::Timeout => "timeout",
            Self::NetworkError => "network_error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_label_vocabulary_matches_live_tail_runbook() {
        assert_eq!(
            ResetReason::ALL
                .iter()
                .map(|reason| reason.as_str())
                .collect::<Vec<_>>(),
            ["backfill_cap", "bus_lagged", "storage_error"]
        );
        assert_eq!(
            PartialTrigger::ALL
                .iter()
                .map(|trigger| trigger.as_str())
                .collect::<Vec<_>>(),
            [
                "request_started",
                "route_completed",
                "upstream_response_started",
                "usage_observed",
                "stream_completed",
                "request_terminated",
            ]
        );
        assert_eq!(
            NotifyDropReason::ALL
                .iter()
                .map(|reason| reason.as_str())
                .collect::<Vec<_>>(),
            ["queue_full", "queue_closed", "serialize_error", "pg_error"]
        );
        assert_eq!(
            NotifySentOutcome::ALL
                .iter()
                .map(|outcome| outcome.as_str())
                .collect::<Vec<_>>(),
            ["sent", "truncated_sent", "failed"]
        );
        assert_eq!(
            NotifyHttpOutcome::ALL
                .iter()
                .map(|outcome| outcome.as_str())
                .collect::<Vec<_>>(),
            [
                "success",
                "not_found",
                "unauthorized",
                "timeout",
                "network_error",
            ]
        );
    }
}
