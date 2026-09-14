use cc_lb_storage_api::AuditActorFields;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AdminIdentity {
    pub authority: String,
    pub subject: String,
    pub kind: AdminActorKind,
    pub provider_id: String,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub groups: Vec<String>,
    pub expires_at_unix_secs: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminActorKind {
    Human,
    Service,
    BreakGlass,
}

impl AdminIdentity {
    pub fn display_actor(&self) -> String {
        self.email
            .clone()
            .unwrap_or_else(|| format!("{}/{}", self.authority, self.subject))
    }

    pub fn audit_fields(&self) -> AuditActorFields {
        AuditActorFields {
            actor: self.display_actor(),
            authority: self.authority.clone(),
            subject: self.subject.clone(),
            kind: match self.kind {
                AdminActorKind::Human => "human",
                AdminActorKind::Service => "service",
                AdminActorKind::BreakGlass => "break_glass",
            }
            .to_owned(),
            email: self.email.clone(),
        }
    }
}
