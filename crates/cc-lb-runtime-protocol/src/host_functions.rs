use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cc_lb_core::clock::{ClockHandle, unix_millis};
use extism::{Function, PTR, UserData, ValType, host_fn};
use ring::rand::{SecureRandom, SystemRandom};

const RANDOM_BYTES_MAX: u64 = 65_536;

#[non_exhaustive]
#[derive(Debug, Default)]
pub struct HostState {
    storage: Mutex<HashMap<String, Vec<u8>>>,
}

impl HostState {
    pub fn new() -> Self {
        Self::default()
    }

    fn get(&self, plugin_name: &str, key: &str) -> Result<Vec<u8>, extism::Error> {
        let scoped = scoped_key(plugin_name, key);
        let storage = self
            .storage
            .lock()
            .map_err(|_| extism::Error::msg("storage lock poisoned"))?;
        Ok(storage.get(&scoped).cloned().unwrap_or_default())
    }

    fn put(
        &self,
        plugin_name: &str,
        key: &str,
        value: Vec<u8>,
        quota_bytes: usize,
    ) -> Result<(), extism::Error> {
        let scoped = scoped_key(plugin_name, key);
        let mut storage = self
            .storage
            .lock()
            .map_err(|_| extism::Error::msg("storage lock poisoned"))?;
        let existing_entry = storage
            .get(&scoped)
            .map_or(0, |stored_value| scoped.len() + stored_value.len());
        let used = storage
            .iter()
            .filter(|(stored_key, _)| {
                stored_key.starts_with(plugin_name)
                    && stored_key.as_bytes().get(plugin_name.len()) == Some(&b':')
            })
            .map(|(stored_key, stored_value)| stored_key.len() + stored_value.len())
            .sum::<usize>();
        let next = used
            .saturating_sub(existing_entry)
            .saturating_add(scoped.len().saturating_add(value.len()));
        if next > quota_bytes {
            return Err(extism::Error::msg("scoped storage quota exceeded"));
        }
        storage.insert(scoped, value);
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone)]
pub struct HostFunctionContext {
    plugin_name: String,
    storage_quota_bytes: usize,
    state: Arc<HostState>,
    clock: ClockHandle,
}

impl HostFunctionContext {
    pub fn new(
        plugin_name: String,
        storage_quota_bytes: usize,
        state: Arc<HostState>,
        clock: ClockHandle,
    ) -> Self {
        Self {
            plugin_name,
            storage_quota_bytes,
            state,
            clock,
        }
    }
}

pub fn functions(context: HostFunctionContext) -> Vec<Function> {
    let user_data = UserData::new(context);
    vec![
        Function::new("cc_lb_log", [PTR, PTR], [], user_data.clone(), cc_lb_log),
        Function::new(
            "cc_lb_metric_incr",
            [PTR, PTR],
            [],
            user_data.clone(),
            cc_lb_metric_incr,
        ),
        Function::new(
            "cc_lb_storage_get",
            [PTR],
            [PTR],
            user_data.clone(),
            cc_lb_storage_get,
        ),
        Function::new(
            "cc_lb_storage_put",
            [PTR, PTR],
            [],
            user_data.clone(),
            cc_lb_storage_put,
        ),
        Function::new(
            "cc_lb_now_unix_ms",
            [],
            [ValType::I64],
            user_data.clone(),
            cc_lb_now_unix_ms,
        ),
        Function::new(
            "cc_lb_random_bytes",
            [PTR],
            [PTR],
            user_data,
            cc_lb_random_bytes,
        ),
    ]
}

host_fn!(pub cc_lb_log(data: HostFunctionContext; level: String, msg: String) {
    let context = context_from_user_data(&data)?;
    match level.as_str() {
        "trace" => tracing::trace!(plugin = %context.plugin_name, message = %msg),
        "debug" => tracing::debug!(plugin = %context.plugin_name, message = %msg),
        "warn" => tracing::warn!(plugin = %context.plugin_name, message = %msg),
        "error" => tracing::error!(plugin = %context.plugin_name, message = %msg),
        _ => tracing::info!(plugin = %context.plugin_name, message = %msg),
    }
    Ok(())
});

host_fn!(pub cc_lb_metric_incr(data: HostFunctionContext; name: String, value: u64) {
    let context = context_from_user_data(&data)?;
    metrics::counter!(name.clone(), "plugin" => context.plugin_name.clone()).increment(value);
    Ok(())
});

host_fn!(pub cc_lb_storage_get(data: HostFunctionContext; key: String) -> Vec<u8> {
    let context = context_from_user_data(&data)?;
    context.state.get(&context.plugin_name, &key)
});

host_fn!(pub cc_lb_storage_put(data: HostFunctionContext; key: String, value: Vec<u8>) {
    let context = context_from_user_data(&data)?;
    context
        .state
        .put(&context.plugin_name, &key, value, context.storage_quota_bytes)?;
    Ok(())
});

host_fn!(pub cc_lb_now_unix_ms(data: HostFunctionContext; ) -> i64 {
    let context = context_from_user_data(&data)?;
    let now = unix_millis(context.clock.now())
        .try_into()
        .unwrap_or(i64::MAX);
    Ok(now)
});

host_fn!(pub cc_lb_random_bytes(_data: HostFunctionContext; n: u64) -> Vec<u8> {
    if n > RANDOM_BYTES_MAX {
        return Err(extism::Error::msg("random byte request exceeds host cap"));
    }
    let mut bytes = vec![0_u8; n as usize];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| extism::Error::msg("secure random generation failed"))?;
    Ok(bytes)
});

fn context_from_user_data(
    data: &UserData<HostFunctionContext>,
) -> Result<HostFunctionContext, extism::Error> {
    let data = data.get()?;
    data.lock()
        .map(|context| context.clone())
        .map_err(|_| extism::Error::msg("host function context lock poisoned"))
}

fn scoped_key(plugin_name: &str, key: &str) -> String {
    format!("{plugin_name}:{key}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_core::TestClock;
    use extism::{Manifest, PluginBuilder, Wasm};

    use super::*;

    #[test]
    fn now_host_function_reads_context_clock() {
        let wasm = wat::parse_str(
            r#"
(module
  (import "extism:host/user" "cc_lb_now_unix_ms" (func $now (result i64)))
  (func (export "check_now") (result i32)
    (drop (call $now))
    (i32.const 0))
)
"#,
        )
        .expect("wat parses");
        let context = HostFunctionContext::new(
            "clock-test".to_owned(),
            1024,
            Arc::new(HostState::new()),
            Arc::new(TestClock::new_at_secs(1_800_000_000)),
        );
        let manifest = Manifest::new([Wasm::data(wasm)]).disallow_all_hosts();
        let mut plugin = PluginBuilder::new(&manifest)
            .with_functions(functions(context))
            .with_wasi(false)
            .with_cache_disabled()
            .build()
            .expect("plugin builds");

        plugin
            .call::<(), ()>("check_now", ())
            .expect("clock-backed now check succeeds");
    }
}
