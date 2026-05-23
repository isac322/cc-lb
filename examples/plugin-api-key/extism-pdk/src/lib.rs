use std::fmt;

pub use extism_pdk_macros::plugin_fn;

pub type FnResult<T> = Result<T, WithReturnCode>;

#[derive(Clone, Debug)]
pub struct Json<T>(pub T);

#[derive(Clone, Debug)]
pub struct Error {
    message: String,
}

impl Error {
    pub fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Self::msg(source.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct WithReturnCode {
    error: Error,
    code: i32,
}

impl WithReturnCode {
    pub fn new(error: Error, code: i32) -> Self {
        Self { error, code }
    }

    fn code(&self) -> i32 {
        if self.code == 0 { 1 } else { self.code }
    }
}

impl fmt::Display for WithReturnCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for WithReturnCode {}

impl From<Error> for WithReturnCode {
    fn from(error: Error) -> Self {
        Self::new(error, 1)
    }
}

impl From<serde_json::Error> for WithReturnCode {
    fn from(source: serde_json::Error) -> Self {
        Self::new(Error::from(source), 1)
    }
}

pub mod config {
    pub fn get(key: &str) -> Option<String> {
        crate::raw::config_get_string(key).ok().flatten()
    }
}

#[doc(hidden)]
pub mod __private {
    use super::{FnResult, Json, WithReturnCode};
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    pub fn run<I, O>(function: fn(Json<I>) -> FnResult<Json<O>>) -> i32
    where
        I: DeserializeOwned,
        O: Serialize,
    {
        match run_inner(function) {
            Ok(()) => 0,
            Err(error) => {
                super::raw::set_error(&error.to_string());
                error.code()
            }
        }
    }

    fn run_inner<I, O>(function: fn(Json<I>) -> FnResult<Json<O>>) -> FnResult<()>
    where
        I: DeserializeOwned,
        O: Serialize,
    {
        let input = super::raw::input().map_err(WithReturnCode::from)?;
        let input = serde_json::from_slice::<I>(&input)?;
        let Json(output) = function(Json(input))?;
        let output = serde_json::to_vec(&output)?;
        super::raw::output(&output).map_err(WithReturnCode::from)?;
        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
mod raw {
    use super::Error;

    #[link(wasm_import_module = "extism:host/env")]
    unsafe extern "C" {
        fn alloc(length: u64) -> u64;
        fn config_get(offset: u64) -> u64;
        fn error_set(offset: u64);
        fn input_offset() -> u64;
        fn length(offset: u64) -> u64;
        fn load_u8(offset: u64) -> i32;
        fn output_set(offset: u64, length: u64);
        fn store_u8(offset: u64, value: i32);
    }

    pub fn input() -> Result<Vec<u8>, Error> {
        let offset = unsafe { input_offset() };
        read_handle(offset)
    }

    pub fn output(bytes: &[u8]) -> Result<(), Error> {
        let offset = write_bytes(bytes)?;
        unsafe { output_set(offset, bytes.len() as u64) };
        Ok(())
    }

    pub fn set_error(message: &str) {
        if let Ok(offset) = write_bytes(message.as_bytes()) {
            unsafe { error_set(offset) };
        }
    }

    pub fn config_get_string(key: &str) -> Result<Option<String>, Error> {
        let key_offset = write_bytes(key.as_bytes())?;
        let value_offset = unsafe { config_get(key_offset) };
        if value_offset == 0 {
            return Ok(None);
        }
        let bytes = read_handle(value_offset)?;
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|source| Error::msg(source.to_string()))
    }

    fn read_handle(offset: u64) -> Result<Vec<u8>, Error> {
        let len = unsafe { length(offset) };
        let len = usize::try_from(len).map_err(|_| Error::msg("extism handle too large"))?;
        let mut out = Vec::with_capacity(len);
        for index in 0..len {
            let byte = unsafe { load_u8(offset.saturating_add(index as u64)) };
            out.push(byte as u8);
        }
        Ok(out)
    }

    fn write_bytes(bytes: &[u8]) -> Result<u64, Error> {
        let len = u64::try_from(bytes.len()).map_err(|_| Error::msg("buffer too large"))?;
        let offset = unsafe { alloc(len) };
        if len > 0 && offset == 0 {
            return Err(Error::msg("extism allocation failed"));
        }
        for (index, byte) in bytes.iter().enumerate() {
            unsafe { store_u8(offset.saturating_add(index as u64), i32::from(*byte)) };
        }
        Ok(offset)
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod raw {
    use super::Error;

    pub fn input() -> Result<Vec<u8>, Error> {
        Err(Error::msg("extism input is only available in wasm"))
    }

    pub fn output(_bytes: &[u8]) -> Result<(), Error> {
        Ok(())
    }

    pub fn set_error(_message: &str) {}

    pub fn config_get_string(_key: &str) -> Result<Option<String>, Error> {
        Ok(None)
    }
}
