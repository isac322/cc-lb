use cc_lb_engine::Authenticated;
use cc_lb_engine::api_keys::builtin_authn::AuthnSuccess;

fn invalid_flow(success: AuthnSuccess) {
    let _proof = Authenticated {
        success,
        auth_ms: 0,
    };
}

fn main() {}
