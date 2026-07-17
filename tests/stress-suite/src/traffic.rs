use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedLabel {
    Healthy,
    ProviderError,
    ClientCancel,
    Malformed,
    Unsupported,
}

impl ExpectedLabel {
    pub const fn status_class(self) -> ExpectedStatusClass {
        match self {
            Self::Healthy => ExpectedStatusClass::Success2xx,
            Self::ProviderError => ExpectedStatusClass::Provider529,
            Self::ClientCancel => ExpectedStatusClass::ClientCancelled,
            Self::Malformed => ExpectedStatusClass::Malformed400,
            Self::Unsupported => ExpectedStatusClass::Unsupported4xx,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedStatusClass {
    Success2xx,
    Provider529,
    ClientCancelled,
    Malformed400,
    Unsupported4xx,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestBodyShape {
    MessageText,
    ContentBlocks,
    ToolUse,
    MalformedJson,
    UnsupportedModel,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SlowReaderPolicy {
    Eager,
    Paced { delay_ms: u64 },
    CancelAfterChunks { chunks: u16 },
}
