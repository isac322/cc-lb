use cc_lb_plugin_api::{CredentialStrategy, SignerError, SignerFactory, Upstream};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;

#[tokio::test]
async fn factory_wrong_strategy_returns_error() {
    let factory =
        AnthropicKeySignerFactory::with_strategy(CredentialStrategy::OAuth, "sk-ant-test-key");
    match factory.build(&Upstream::AnthropicDirect { base_url: None }).await {
        Err(SignerError::WrongStrategy {
            strategy: CredentialStrategy::OAuth,
        }) => {}
        _ => panic!("unexpected result"),
    }
}
