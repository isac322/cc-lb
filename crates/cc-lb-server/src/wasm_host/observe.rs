use std::sync::Arc;

use cc_lb_domain::PrincipalKind;
use cc_lb_observability::{ObservabilityError, ObservabilityHook, ObserveEvent};
use cc_lb_plugin_wire::schema::WireVersion;
use cc_lb_runtime_wasmtime::WasmPluginWireDispatch;
use rkyv::rancor::Error as RkyvError;

use super::shape::host_upstream_to_wire;

pub struct WasmtimeObservabilityHookPlugin {
    dispatch: Arc<WasmPluginWireDispatch>,
}

impl WasmtimeObservabilityHookPlugin {
    pub fn new(dispatch: Arc<WasmPluginWireDispatch>) -> Self {
        Self { dispatch }
    }
}

impl ObservabilityHook for WasmtimeObservabilityHookPlugin {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        let wire = host_observe_event_to_wire(event);
        let in_bytes =
            rkyv::to_bytes::<RkyvError>(&wire).map_err(|e| ObservabilityError::Dropped {
                reason: format!("rkyv encode ObserveEvent: {e}"),
            })?;
        match self.dispatch.observe_wire_version() {
            Some(WireVersion::V1) => self.dispatch.call_observe(in_bytes.as_slice()),
            None => {
                return Err(ObservabilityError::Dropped {
                    reason: "plugin metadata missing observe hook".to_owned(),
                });
            }
        }
        .map_err(|e| ObservabilityError::Dropped {
            reason: e.to_string(),
        })?;
        Ok(())
    }
}

fn principal_kind_str(kind: &PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::ApiKey => "api_key",
        PrincipalKind::OAuthSubject => "oauth_subject",
        PrincipalKind::InternalKey => "internal_key",
        PrincipalKind::WorkloadIdentity => "workload_identity",
        PrincipalKind::SubscriptionBearer => "subscription_bearer",
    }
}

fn host_observe_event_to_wire(event: ObserveEvent) -> cc_lb_plugin_wire::ObserveEvent {
    use cc_lb_plugin_wire::ObserveEvent as Wire;
    match event {
        ObserveEvent::RequestStarted {
            request_id,
            downstream_user_agent,
        } => Wire::RequestStarted {
            request_id: request_id.into_boxed_str(),
            downstream_user_agent: downstream_user_agent.map(String::into_boxed_str),
        },
        ObserveEvent::AuthnComplete { principal_id, kind } => Wire::AuthnComplete {
            principal_id: principal_id.into_boxed_str(),
            principal_kind: Box::from(principal_kind_str(&kind)),
        },
        ObserveEvent::UpstreamChosen { upstream } => Wire::UpstreamChosen {
            upstream: host_upstream_to_wire(&upstream),
        },
        ObserveEvent::Chunk {
            batch_index,
            event_count,
            total_bytes,
        } => Wire::Chunk {
            batch_index,
            event_count: event_count as u64,
            total_bytes: total_bytes as u64,
        },
        ObserveEvent::RequestFinished {
            status,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            duration_ms,
        } => Wire::RequestFinished {
            status: status.as_u16(),
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            duration_ms,
        },
        ObserveEvent::Error {
            code,
            message,
            source,
        } => Wire::Error {
            code: code.into_boxed_str(),
            message: message.into_boxed_str(),
            source: source.into_boxed_str(),
        },
    }
}
