use uuid::Uuid;

#[must_use]
pub const fn fixed_uuid(n: u32) -> Uuid {
    Uuid::from_u128(n as u128)
}

#[must_use]
pub fn fixed_request_id(n: u32) -> String {
    format!("req_{n}")
}
