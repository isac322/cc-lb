#![allow(non_snake_case)]

#[path = "debug_redacts_key.rs"]
mod debug_redacts_key;
#[path = "factory_wrong_strategy.rs"]
mod factory_wrong_strategy;
#[path = "fake_anthropic_accepts_signed_request.rs"]
mod fake_anthropic_accepts_signed_request;
#[path = "on_unauthorized_returns_fail.rs"]
mod on_unauthorized_returns_fail;
#[path = "sign_does_not_modify_body.rs"]
mod sign_does_not_modify_body;
#[path = "sign_inserts_header.rs"]
mod sign_inserts_header;
