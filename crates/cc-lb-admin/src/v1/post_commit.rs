//! Completion of admin mutations after their domain change has committed.
//!
//! A handler future is dropped when the client disconnects. Once the domain
//! change is committed, the remaining work (audit row, dynamic-view rebuild,
//! follow-up writes) must not be abandoned at the next `.await`, so handlers
//! run that continuation through [`complete_after_commit`].

use crate::AdminState;

/// The post-commit continuation task ended without producing a result.
///
/// Tokio only reports this when the runtime is shutting down; a panic inside
/// the continuation is re-raised on the calling task instead.
#[derive(Debug)]
pub(crate) struct PostCommitAborted;

/// Runs `continuation` on its own task and waits for it.
///
/// The spawned task is not tied to the request future, so it keeps running if
/// the caller is dropped. Panics propagate unchanged to the caller.
pub(crate) async fn complete_after_commit<F, T>(continuation: F) -> Result<T, PostCommitAborted>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::spawn(continuation).await {
        Ok(output) => Ok(output),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => {
            tracing::error!(error = %error, "admin post-commit continuation aborted");
            Err(PostCommitAborted)
        }
    }
}

/// Rebuilds the serving replica's dynamic view after a committed change whose
/// audit write failed. The caller still returns its audit-failure response
/// unchanged; this only keeps routing in step with committed storage.
pub(crate) async fn rebuild_after_audit_failure(state: &AdminState) {
    if let Err(error) = super::apply_dynamic_view_after_mutation(state).await {
        tracing::error!(
            error = %error,
            "admin v1 dynamic view rebind deferred after audit write failure"
        );
    }
}
