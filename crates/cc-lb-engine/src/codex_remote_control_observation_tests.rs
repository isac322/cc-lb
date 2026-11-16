use super::*;
use std::sync::Arc;
use cc_lb_control::{InMemoryBus, RequestEventBus};

#[test]
fn websocket_terminal_outcomes_preserve_101_and_never_invent_generation_usage() {
    use RemoteControlWebSocketTermination as End;
    for (termination, expected_code) in [
        (End::Graceful,None),
        (End::Authentication,Some("authentication_failed")),
        (End::OwnerChanged,Some("upstream_affinity_unavailable")),
        (End::Overloaded,Some("upstream_dispatch_failed")),
        (End::Transport,Some("upstream_dispatch_failed")),
        (End::WriteTimeout,Some("upstream_dispatch_failed")),
        (End::Draining,Some("drain_rejected")),
        (End::PeerErrorClose(1011),Some("upstream_stream_error")),
        (End::DrainTimeout,Some("upstream_dispatch_failed")),
    ] {
        let bus = Arc::new(InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock:ClockHandle = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let context = LifecycleContext::new("synthetic-rc-request".into(),bus,&clock);
        context.mark_authn_reached();
        context.emit_request_started(false);
        // This private fixture tests observation only, never authentication or
        // dispatch authority. Production construction still requires its proof.
        let mut observation = RemoteControlObservation {
            context:Some(context.clone()),clock:Some(clock),started:Some(Instant::now()),
            kind:UpstreamKind::OpenaiOauth,response_status:None,finished:false,cancellation_owned:false,
        };
        observation.response_started(StatusCode::SWITCHING_PROTOCOLS,&HeaderMap::new());
        observation.own_websocket().complete_websocket(termination);
        drop(context);
        let mut terminal_count = 0;
        let mut stream_count = 0;
        while let Ok(event) = events.try_recv() {
            match event {
                LifecycleEvent::RequestTerminated { client_status,reason,internal_errors,.. } => {
                    terminal_count += 1;
                    assert_eq!(client_status,101);
                    match expected_code {
                        Some(expected) => {
                            assert!(matches!(reason,cc_lb_lifecycle::TerminationReason::ErrorCode(code) if code == expected));
                            assert_eq!(internal_errors.len(),1);
                            if matches!(termination,End::WriteTimeout | End::DrainTimeout) {
                                assert_eq!(internal_errors[0].kind,cc_lb_domain::InternalErrorKind::Timeout);
                            }
                        }
                        None => assert!(internal_errors.is_empty()),
                    }
                }
                LifecycleEvent::StreamCompleted { result,.. } => {
                    stream_count += 1;
                    if termination == End::Graceful { assert!(!result.unwrap().usage_present); }
                    else { assert!(result.is_err()); }
                }
                LifecycleEvent::UsageObserved {..} | LifecycleEvent::Priced {..} =>
                    panic!("remote control traffic must not fabricate usage or cost"),
                _ => {}
            }
        }
        assert_eq!(terminal_count,1);
        assert_eq!(stream_count,1);
    }
}
