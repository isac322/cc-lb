use cc_lb_control::event_bus::RequestEventUpdate;

use serde::{Deserialize, Serialize};
#[derive(Serialize)]
pub struct InlinePartialNotify<'a> {
    #[serde(flatten)]
    pub update: &'a RequestEventUpdate,
    pub producer_id: &'a str,
}

#[derive(Deserialize)]
pub struct NotifyOrigin {
    pub producer_id: String,
}
