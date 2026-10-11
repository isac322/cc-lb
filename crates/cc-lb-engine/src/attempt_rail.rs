//! Type-enforced ownership transitions for one upstream proxy attempt.

use crate::upstream_activity::ActivityGuard;
use std::marker::PhantomData;
use std::sync::Arc;

use cc_lb_upstream::{ShapedRequest, SignedRequest, Signer, SignerError, sign_request};
use http::{HeaderMap, Method, Response};
use url::Url;

use crate::lifecycle::{Body, DispatchError, UpstreamDispatch};
use cc_lb_control::api_keys::limit_engine::Reservation;

/// Input to a proxy attempt before its reservation becomes lifecycle-owned.
pub struct AttemptIntent {
    reservation: Option<Reservation>,
}

impl AttemptIntent {
    /// Creates an intent carrying a required reservation.
    pub fn from_reservation(reservation: Reservation) -> Self {
        Self {
            reservation: Some(reservation),
        }
    }

    pub(crate) fn observe_only() -> Self {
        Self { reservation: None }
    }

    /// Moves the reservation into the state that survives all retry attempts.
    pub fn into_reserved(self) -> Reserved {
        Reserved {
            reservation: self.reservation,
        }
    }
}

/// Lifecycle-owned reservation state that can begin multiple attempts.
pub struct Reserved {
    reservation: Option<Reservation>,
}

impl Reserved {
    /// Starts one scoped attempt without consuming the reservation owner.
    pub fn begin_attempt(&self) -> Scoped<'_> {
        Scoped {
            activity: None,
            _reserved: PhantomData,
        }
    }

    /// Moves the reservation into response finalization for RAII refund or success handoff.
    pub fn into_response_accounting_guard(self) -> ResponseAccountingGuard {
        ResponseAccountingGuard {
            reservation: self.reservation,
        }
    }
}

/// A shaped-and-to-be-signed attempt borrowing its reservation owner.
pub struct Scoped<'reservation> {
    activity: Option<Arc<ActivityGuard>>,
    _reserved: PhantomData<&'reservation Reserved>,
}

impl<'reservation> Scoped<'reservation> {
    pub(crate) fn with_activity(mut self, activity: Option<Arc<ActivityGuard>>) -> Self {
        self.activity = activity;
        self
    }

    /// Signs the shaped request through the plugin API's sealed signing capability.
    pub async fn sign(
        self,
        signer: &dyn Signer,
        shaped_request: ShapedRequest,
    ) -> Result<Signed<'reservation>, SignerError> {
        if let Some(activity) = &self.activity {
            let ttl = sonic_rs::from_slice::<serde_json::Value>(shaped_request.body())
                .ok()
                .map(|value| shaped_cache_ttl(&value))
                .unwrap_or(3600);
            activity.prepare(ttl);
        }
        let signed_request = sign_request(signer, shaped_request).await?;
        Ok(Signed {
            signed_request,
            activity: self.activity,
            _reserved: PhantomData,
        })
    }
}

impl Scoped<'static> {
    pub(crate) fn unreserved_for_lifecycle() -> Self {
        Self {
            activity: None,
            _reserved: PhantomData,
        }
    }
}

fn shaped_cache_ttl(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Object(object) => {
            let own = object
                .get("cache_control")
                .filter(|control| {
                    control.get("type").and_then(serde_json::Value::as_str) == Some("ephemeral")
                })
                .map_or(0, |control| {
                    if control.get("ttl").and_then(serde_json::Value::as_str) == Some("1h") {
                        3600
                    } else {
                        300
                    }
                });
            own.max(object.values().map(shaped_cache_ttl).max().unwrap_or(0))
        }
        serde_json::Value::Array(array) => array.iter().map(shaped_cache_ttl).max().unwrap_or(0),
        _ => 0,
    }
}

/// A signed request that is eligible for dispatch.
pub struct Signed<'reservation> {
    activity: Option<Arc<ActivityGuard>>,
    signed_request: SignedRequest,
    _reserved: PhantomData<&'reservation Reserved>,
}

impl Signed<'_> {
    pub(crate) fn url(&self) -> &Url {
        self.signed_request.url()
    }

    pub(crate) fn method(&self) -> &Method {
        self.signed_request.method()
    }

    pub(crate) fn headers(&self) -> &HeaderMap {
        self.signed_request.headers()
    }

    pub(crate) fn body_len(&self) -> usize {
        self.signed_request.body().len()
    }

    /// Dispatches the sealed signed request, consuming its dispatch capability.
    pub async fn dispatch(
        self,
        dispatcher: &dyn UpstreamDispatch,
    ) -> Result<Response<Body>, DispatchError> {
        if let Some(activity) = &self.activity {
            activity.dispatched();
        }
        let result = dispatcher.dispatch(self.signed_request).await;
        match result {
            Ok(mut response) => {
                if let Some(activity) = self.activity {
                    if matches!(
                        response.status(),
                        http::StatusCode::UNAUTHORIZED | http::StatusCode::TOO_MANY_REQUESTS
                    ) {
                        activity.reject();
                    } else {
                        response.extensions_mut().insert(activity);
                    }
                }
                Ok(response)
            }
            Err(error) => {
                if matches!(
                    error,
                    DispatchError::InvalidUri { .. }
                        | DispatchError::RequestBuild { .. }
                        | DispatchError::BulkheadFull { .. }
                ) && let Some(activity) = self.activity
                {
                    activity.reject();
                }
                Err(error)
            }
        }
    }
}

/// Owns the reservation until buffered or streaming response finalization decides its outcome.
pub struct ResponseAccountingGuard {
    reservation: Option<Reservation>,
}

impl ResponseAccountingGuard {
    pub fn reservation_id(&self) -> Option<&str> {
        self.reservation
            .as_ref()
            .map(|reservation| reservation.id())
    }

    /// Suppresses the RAII refund after a successful response is handed to reconciliation.
    pub fn forget(self) {
        if let Some(reservation) = self.reservation {
            reservation.forget();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AttemptIntent, Reserved};

    #[test]
    fn reserved_begins_multiple_scoped_attempts_before_finalization() {
        // Given
        let reserved = Reserved { reservation: None };

        // When
        let first_attempt = reserved.begin_attempt();
        let _ = first_attempt;
        let retry_attempt = reserved.begin_attempt();

        // Then
        let _ = retry_attempt;
        let accounting_guard = reserved.into_response_accounting_guard();
        accounting_guard.forget();
    }

    #[test]
    fn response_accounting_guard_forget_consumes_its_owner() {
        // Given
        let accounting_guard = Reserved { reservation: None }.into_response_accounting_guard();

        // When
        accounting_guard.forget();

        // Then the move into forget makes a second handoff impossible.
    }

    #[test]
    fn warmup_intent_contract_observe_only_rail_flow() {
        // Given: an AttemptIntent constructed via observe_only (no reservation)
        let intent = AttemptIntent::observe_only();

        // When: the intent moves through the full rail
        let reserved = intent.into_reserved();
        let _scoped = reserved.begin_attempt();
        let accounting_guard = reserved.into_response_accounting_guard();

        // Then: the accounting guard has no reservation to forget
        assert_eq!(accounting_guard.reservation_id(), None);
        accounting_guard.forget();
    }
}
