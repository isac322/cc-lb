use super::{AdminAuthError, AdminIdentity};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdminAction {
    Read,
    Write,
    SensitiveRead,
}

pub fn authorize(_identity: &AdminIdentity, _action: AdminAction) -> Result<(), AdminAuthError> {
    Ok(())
}
