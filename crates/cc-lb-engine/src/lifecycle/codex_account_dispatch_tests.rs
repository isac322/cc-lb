use super::*;
use cc_lb_control::RequestEventBus;
use cc_lb_control::event_bus::InMemoryBus;

fn shaped(
    method: Method,
    path: &str,
    query: Option<&str>,
    body: Bytes,
) -> Result<ShapedRequest, DialectError> {
    shape_request(
        &AccountDialect,
        &DialectShapeContext {
            request_id: "account-test".to_owned(),
            downstream_headers: HeaderMap::new(),
            method,
            path: path.to_owned(),
            query: query.map(str::to_owned),
            body_bytes: body,
        },
        &Upstream::OpenaiDirect {
            base_url: Some(Url::parse("https://model.invalid/v1").unwrap()),
        },
        &Principal {
            id: "principal".to_owned(),
            kind: PrincipalKind::InternalKey,
        },
    )
}

#[test]
fn account_target_preserves_raw_query_and_opaque_control_body() {
    let body = Bytes::from_static(b"not JSON\0\xff");
    let path = "/backend-api/wham/rate-limit-reset-credits/consume";
    let query = "key=keep%2Fraw&key=&cursor=a+b%20c&x=%252F";
    let request = shaped(Method::POST, path, Some(query), body.clone()).unwrap();
    assert_eq!(
        request.url().as_str(),
        format!("https://chatgpt.com{path}?{query}")
    );
    assert_eq!(request.body(), &body);
    assert_eq!(request.method(), Method::POST);
}

#[test]
fn account_allowlist_excludes_switch_models_cloud_and_encoded_aliases() {
    for (method, path) in [
        (Method::POST, "/backend-api/accounts/switch-workspace-token"),
        (Method::POST, "/v1/responses"),
        (Method::GET, "/backend-api/wham/tasks/list"),
        (Method::GET, "/backend-api/wham/accounts/%63heck"),
        (Method::POST, ACCOUNTS_CHECK),
        (Method::GET, "/backend-api/wham/usage/../accounts/check"),
    ] {
        assert!(shaped(method, path, None, Bytes::new()).is_err());
    }
    assert!(shaped(Method::HEAD, ACCOUNTS_CHECK, None, Bytes::new()).is_ok());
}

#[tokio::test]
async fn usage_shaped_control_body_is_observed_without_usage_or_pricing() {
    let bus = Arc::new(InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
    let observer = LifecycleContext::new("account-usage-test".to_owned(), bus, &clock);
    let body = Body::new(AccountResponseBody {
        inner: Body::from(r#"{"usage":{"input_tokens":100,"output_tokens":200}}"#),
        observer: Some(observer),
        status: StatusCode::OK,
    });
    let bytes = axum::body::to_bytes(body, 1024).await.unwrap();
    assert_eq!(
        bytes,
        r#"{"usage":{"input_tokens":100,"output_tokens":200}}"#
    );
    let mut terminated = 0;
    while let Ok(event) = events.try_recv() {
        match event {
            LifecycleEvent::UsageObserved { .. } | LifecycleEvent::Priced { .. } => {
                panic!("control traffic cannot be charged")
            }
            LifecycleEvent::RequestTerminated { .. } => terminated += 1,
            _ => {}
        }
    }
    assert_eq!(terminated, 1);
}
