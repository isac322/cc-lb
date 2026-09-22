use cc_lb_engine::Lifecycle;

async fn invalid_flow(lifecycle: &Lifecycle, req: http::Request<bytes::Bytes>) {
    let _response = lifecycle.handle(req).await;
}

fn main() {}
