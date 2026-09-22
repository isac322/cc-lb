use cc_lb_engine::Lifecycle;

async fn valid_flow(
    lifecycle: &Lifecycle,
    headers: &http::HeaderMap,
    req: http::Request<bytes::Bytes>,
) {
    let auth = match lifecycle.authenticate(headers).await {
        Ok(auth) => auth,
        Err(_) => return,
    };
    let _response = lifecycle.handle(req, &auth).await;
}

fn main() {}
