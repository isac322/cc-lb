use cc_lb_plugin_api::{AuthStrategy, SignerError, SignerFactory, Upstream};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;

#[tokio::test]
async fn factory_wrong_strategy_returns_error() {
    let factory = AnthropicKeySignerFactory::with_strategy(AuthStrategy::OAuth, "sk-ant-test-key");
    match factory.build(&Upstream::AnthropicDirect).await {
        Err(SignerError::WrongStrategy {
            strategy: AuthStrategy::OAuth,
        }) => {}
        _ => panic!("unexpected result"),
    }
}
