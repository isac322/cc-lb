use cc_lb_control::api_keys::builtin_authn::AuthnSuccess;
use cc_lb_engine::Authenticated;

fn invalid_flow(success: AuthnSuccess) {
    let _proof = Authenticated {
        success,
        auth_ms: 0,
    };
}

fn main() {}
