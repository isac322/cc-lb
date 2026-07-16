use std::process::Command;

use cc_lb_config::Config;

const ENV_OVERRIDE_CHILD: &str = "CC_LB_CONFIG_ENV_OVERRIDE_CHILD";

#[test]
fn double_underscore_env_names_map_to_nested_config_fields() {
    if std::env::var_os(ENV_OVERRIDE_CHILD).is_some() {
        assert_env_override_applies();
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("load_env_override::double_underscore_env_names_map_to_nested_config_fields")
        .env(ENV_OVERRIDE_CHILD, "1")
        .env("CC_LB_LISTENER__PROXY_ADDR", "[::]:9999")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_env_override_applies() {
    let (_dir, path) = crate::common::temp_config(
        r#"[listener]
proxy_addr = "[::]:7777"
"#,
    );

    let config = Config::load(&path).unwrap();

    assert_eq!(config.listener.proxy_addr, "[::]:9999".parse().unwrap());
}
