use crate::event_bus::RequestEventUpdate;

use serde::{Deserialize, Serialize};
#[derive(Serialize)]
pub struct InlinePartialNotify<'a> {
    #[serde(flatten)]
    pub update: &'a RequestEventUpdate,
    pub producer_url: &'a str,
}

#[derive(Deserialize)]
pub struct NotifyOrigin {
    pub producer_url: Option<String>,
}

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
