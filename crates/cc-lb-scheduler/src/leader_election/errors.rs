use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaderState {
    Leader,
    Follower,
    Single,
}

impl LeaderState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Follower => "follower",
            Self::Single => "single",
        }
    }
}

#[derive(Debug, Error)]
pub enum LeaderError {
    #[cfg(feature = "postgres")]
    #[error("leader database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("leader connection is not available")]
    NotConnected,
    #[error("leader lock lost: {reason}")]
    LockLost { reason: String },
}
