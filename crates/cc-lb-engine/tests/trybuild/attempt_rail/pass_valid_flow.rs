use cc_lb_engine::UpstreamDispatch;
use cc_lb_engine::attempt_rail::AttemptIntent;
use cc_lb_plugin_api::{ShapedRequest, Signer};

async fn valid_flow(
    intent: AttemptIntent,
    shaped_request: ShapedRequest,
    signer: &dyn Signer,
    dispatcher: &dyn UpstreamDispatch,
) {
    let reserved = intent.into_reserved();
    let scoped = reserved.begin_attempt();
    let signed = match scoped.sign(signer, shaped_request).await {
        Ok(signed) => signed,
        Err(_) => return,
    };
    let _response = signed.dispatch(dispatcher).await;
    let _accounting_guard = reserved.into_response_accounting_guard();
}

fn main() {}
