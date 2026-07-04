use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct TruncatedPartialNotify<'a> {
    pub event_id: String,
    pub phase: &'a str,
    pub producer_url: &'a str,
    pub truncated: bool,
}

#[derive(Deserialize)]
pub struct TruncatedPartialNotifyOwned {
    pub event_id: String,
    pub phase: String,
    pub producer_url: String,
    #[allow(dead_code)]
    pub truncated: bool,
}
