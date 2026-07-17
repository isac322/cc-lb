use cc_lb_engine::UpstreamDispatch;
use cc_lb_engine::attempt_rail::Scoped;

fn invalid_flow(scoped: Scoped<'_>, dispatcher: &dyn UpstreamDispatch) {
    let _dispatch = scoped.dispatch(dispatcher);
}

fn main() {}
