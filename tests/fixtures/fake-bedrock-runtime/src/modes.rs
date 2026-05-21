use http::HeaderMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FakeMode {
    Ok,
    ModelStreamErrorException,
    ValidationException,
    ClockSkew,
}

impl FakeMode {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        headers
            .get("x-fake-mode")
            .and_then(|value| value.to_str().ok())
            .map(Self::from_str)
            .unwrap_or(Self::Ok)
    }

    fn from_str(value: &str) -> Self {
        match value {
            "ModelStreamErrorException" => Self::ModelStreamErrorException,
            "ValidationException" => Self::ValidationException,
            "clock-skew" => Self::ClockSkew,
            _ => Self::Ok,
        }
    }
}
