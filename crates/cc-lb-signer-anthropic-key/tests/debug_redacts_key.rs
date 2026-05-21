use cc_lb_signer_anthropic_key::AnthropicKeySigner;
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;

#[test]
fn debug_redacts_key() {
    let signer = AnthropicKeySigner::new("sk-ant-secret-value");
    let factory = AnthropicKeySignerFactory::new("sk-ant-secret-value");
    let debug = format!("{:?}", signer);
    let factory_debug = format!("{:?}", factory);

    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("sk-ant-secret-value"));
    assert!(factory_debug.contains("[REDACTED]"));
    assert!(!factory_debug.contains("sk-ant-secret-value"));
}
