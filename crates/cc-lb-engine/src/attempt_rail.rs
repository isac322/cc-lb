//! Type-enforced ownership transitions for one upstream proxy attempt.

use std::marker::PhantomData;

use cc_lb_plugin_api::{ShapedRequest, SignedRequest, Signer, SignerError, sign_request};
use http::{HeaderMap, Method, Response};
use url::Url;

use crate::api_keys::limit_engine::Reservation;
use crate::lifecycle::{Body, DispatchError, UpstreamDispatch};

/// Input to a proxy attempt before its reservation becomes lifecycle-owned.
pub struct AttemptIntent {
    reservation: Reservation,
}

impl AttemptIntent {
    /// Creates an intent carrying a required reservation.
    pub fn from_reservation(reservation: Reservation) -> Self {
        Self { reservation }
    }

    /// Moves the reservation into the state that survives all retry attempts.
    pub fn into_reserved(self) -> Reserved {
        Reserved {
            reservation: Some(self.reservation),
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
    _reserved: PhantomData<&'reservation Reserved>,
}

impl<'reservation> Scoped<'reservation> {
    /// Signs the shaped request through the plugin API's sealed signing capability.
    pub async fn sign(
        self,
        signer: &dyn Signer,
        shaped_request: ShapedRequest,
    ) -> Result<Signed<'reservation>, SignerError> {
        let signed_request = sign_request(signer, shaped_request).await?;
        Ok(Signed {
            signed_request,
            _reserved: PhantomData,
        })
    }
}

impl Scoped<'static> {
    pub(crate) fn unreserved_for_lifecycle() -> Self {
        Self {
            _reserved: PhantomData,
        }
    }
}

/// A signed request that is eligible for dispatch.
pub struct Signed<'reservation> {
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
        dispatcher.dispatch(self.signed_request).await
    }
}

/// Owns the reservation until buffered or streaming response finalization decides its outcome.
pub struct ResponseAccountingGuard {
    reservation: Option<Reservation>,
}

impl ResponseAccountingGuard {
    /// Suppresses the RAII refund after a successful response is handed to reconciliation.
    pub fn forget(self) {
        if let Some(reservation) = self.reservation {
            reservation.forget();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Reserved;

    #[test]
    fn reserved_begins_multiple_scoped_attempts_before_finalization() {
        // Given
        let reserved = Reserved { reservation: None };

        // When
        let first_attempt = reserved.begin_attempt();
        drop(first_attempt);
        let retry_attempt = reserved.begin_attempt();

        // Then
        drop(retry_attempt);
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
}
