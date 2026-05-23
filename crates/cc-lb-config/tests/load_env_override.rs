mod common;

use std::env;
use std::ffi::OsString;

use cc_lb_config::Config;

struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = env::var_os(key);
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { env::set_var(self.key, previous) };
        } else {
            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { env::remove_var(self.key) };
        }
    }
}

#[test]
fn double_underscore_env_names_map_to_nested_config_fields() {
    let _guard = EnvGuard::set("CC_LB_LISTENER__PROXY_ADDR", "[::]:9999");
    let (_dir, path) = common::temp_config(
        r#"[listener]
proxy_addr = "[::]:7777"
"#,
    );

    let config = Config::load(&path).unwrap();

    assert_eq!(config.listener.proxy_addr, "[::]:9999".parse().unwrap());
}
