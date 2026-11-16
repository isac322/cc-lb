use super::*;

// QA contract:
// - Only the seven approved unprefixed routes/methods are registered.
// - Anonymous and wrong-purpose requests never poll a body or attempt an upgrade.
// - Enrollment/refresh identity and pair/status payloads are checked after auth.
// - Provider requests stay pinned to the creating owner and workspace.
// Real-account/manual QA, not established by the deterministic fixtures:
// 1. Run the managed cc-lb CLI profile's enroll/refresh/list/revoke with disposable IDs.
// 2. Check only a local relay token reaches CLI; exercise expiry and family revoke.
// 3. Connect a real WS, verify bidirectional data order/ack/cursor and one pong.
// 4. Block each writer in turn, then disconnect/close/drain; observe bounded exit.
// 5. Change owner/workspace or disable principal; the next client frame must fail
//    reauthentication/fencing without any send or pooled-owner failover.
// 6. Reject the provider handshake with 401/429; preserve status/body before 101.

#[tokio::test]
async fn remote_control_routes_reject_before_body_or_upgrade() {
    let router = websocket_test_router(Vec::new(), TerminalStrategy::FirstPick).await;
    for (method, path) in remote_control_cases() {
        for credential in [None, Some(TEST_KEY.plaintext.as_str())] {
            let mut request = Request::builder().method(method.clone()).uri(path);
            if let Some(credential) = credential {
                request = request.header("authorization", format!("Bearer {credential}"));
            }
            if path == codex_remote_control::SERVER {
                request = request
                    .header("connection", "upgrade")
                    .header("upgrade", "websocket")
                    .header("sec-websocket-version", "13")
                    .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==");
            }
            let response = router.clone().oneshot(request.body(
                Body::from_stream(stream::once(panic_when_polled())),
            ).unwrap()).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{method} {path}");
            assert!(!response.headers().contains_key("upgrade"));
            assert_eq!(response.headers()["cc-lb-error-origin"], if path == codex_remote_control::PAIR
                || path == codex_remote_control::PAIR_STATUS || path == codex_remote_control::SERVER {
                "relay"
            } else { "principal" });
        }
    }
}

#[tokio::test]
async fn remote_control_has_no_prefix_trailing_or_wildcard_aliases() {
    let router = websocket_test_router(Vec::new(), TerminalStrategy::FirstPick).await;
    for path in [
        "/backend-api/wham/remote/control/server/enroll/",
        "/backend-api/wham/remote/control/server/unknown",
        "/backend-api/wham/remote/control/environments/env/clients/client/extra",
        "/openai/backend-api/wham/remote/control/server/enroll",
    ] {
        let response = router.clone().oneshot(Request::builder()
            .method("POST").uri(path).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
    for (method, path) in remote_control_cases() {
        let wrong_method = if method == http::Method::POST { "PUT" } else { "POST" };
        let response = router.clone().oneshot(Request::builder()
            .method(wrong_method).uri(path).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{wrong_method} {path}");
    }
}

fn remote_control_cases() -> [(http::Method, &'static str); 7] {
    [
        (http::Method::POST, codex_remote_control::ENROLL),
        (http::Method::POST, codex_remote_control::REFRESH),
        (http::Method::GET, "/backend-api/wham/remote/control/environments/env/clients"),
        (http::Method::DELETE, "/backend-api/wham/remote/control/environments/env/clients/client"),
        (http::Method::POST, codex_remote_control::PAIR),
        (http::Method::POST, codex_remote_control::PAIR_STATUS),
        (http::Method::GET, codex_remote_control::SERVER),
    ]
}

#[test]
fn remote_control_trace_templates_do_not_include_environment_or_client_ids() {
    assert_eq!(proxy_route_template("/backend-api/wham/remote/control/environments/opaque-environment/clients"),
        Some(codex_remote_control::CLIENTS));
    assert_eq!(proxy_route_template("/backend-api/wham/remote/control/environments/opaque-environment/clients/opaque-client"),
        Some(codex_remote_control::CLIENT));
    for path in ["/backend-api/wham/remote/control/environments/env/clients/",
        "/backend-api/wham/remote/control/environments/env/clients/client/extra"] {
        assert_eq!(codex_remote_control::route_template(path),None);
    }
}
