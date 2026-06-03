extern crate alloc;

use alloc::string::{String, ToString};

pub fn run_string_export<F>(handler: F) -> i32
where
    F: FnOnce(String) -> Result<String, String>,
{
    match input_string().and_then(handler) {
        Ok(output) => {
            imp::set_output(output.as_bytes());
            0
        }
        Err(error) => {
            imp::set_error(error.as_bytes());
            -1
        }
    }
}

fn input_string() -> Result<String, String> {
    String::from_utf8(imp::input_bytes()).map_err(|error| error.to_string())
}

#[cfg(target_arch = "wasm32")]
mod imp {
    extern crate alloc;

    use alloc::vec::Vec;

    #[link(wasm_import_module = "extism:host/env")]
    unsafe extern "C" {
        fn input_length() -> u64;
        fn input_load_u8(offset: u64) -> u8;
        fn alloc(length: u64) -> u64;
        fn output_set(offset: u64, length: u64);
        fn error_set(offset: u64);
        fn store_u8(offset: u64, data: u8);
    }

    pub(super) fn input_bytes() -> Vec<u8> {
        let len = unsafe { input_length() } as usize;
        let mut data = Vec::with_capacity(len);
        for offset in 0..len {
            data.push(unsafe { input_load_u8(offset as u64) });
        }
        data
    }

    pub(super) fn set_output(bytes: &[u8]) {
        let offset = store_bytes(bytes);
        unsafe { output_set(offset, bytes.len() as u64) };
    }

    pub(super) fn set_error(bytes: &[u8]) {
        let offset = store_bytes(bytes);
        unsafe { error_set(offset) };
    }

    fn store_bytes(bytes: &[u8]) -> u64 {
        let offset = unsafe { alloc(bytes.len() as u64) };
        for (index, byte) in bytes.iter().enumerate() {
            unsafe { store_u8(offset + index as u64, *byte) };
        }
        offset
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    extern crate alloc;

    use alloc::{string::String, vec::Vec};

    pub(super) fn input_bytes() -> Vec<u8> {
        String::from("Extism guest input is only available on wasm32").into_bytes()
    }

    pub(super) fn set_output(_bytes: &[u8]) {}

    pub(super) fn set_error(_bytes: &[u8]) {}
}
