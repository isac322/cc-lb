use crate::RequestEvent;

#[derive(Debug, Clone)]
pub struct StorageTailUpdate {
    pub cursor: u64,
    pub event: RequestEvent,
}
