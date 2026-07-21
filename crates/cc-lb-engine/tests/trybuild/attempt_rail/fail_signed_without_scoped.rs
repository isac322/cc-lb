use cc_lb_engine::attempt_rail::Signed;
use cc_lb_upstream::SignedRequest;

fn invalid_flow(signed_request: SignedRequest) {
    let _signed = Signed { signed_request };
}

fn main() {}
