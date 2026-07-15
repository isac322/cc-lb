use crate::manifest::{Persona, Principal, Session};

pub fn personas() -> Vec<Persona> {
    [
        ("steady-user", "stress-primary", "session-primary-1"),
        ("burst-user", "stress-secondary", "session-secondary-1"),
        ("edge-user", "stress-tertiary", "session-tertiary-1"),
    ]
    .into_iter()
    .map(|(persona_id, principal_id, session_id)| Persona {
        persona_id: persona_id.to_owned(),
        principal_id: principal_id.to_owned(),
        session_id: session_id.to_owned(),
    })
    .collect()
}

pub fn principals() -> Vec<Principal> {
    [
        ("stress-primary", "primary"),
        ("stress-secondary", "secondary"),
        ("stress-tertiary", "tertiary"),
    ]
    .into_iter()
    .map(|(principal_id, credential)| Principal {
        principal_id: principal_id.to_owned(),
        credential_reference: format!("stress-suite/principals/{credential}"),
    })
    .collect()
}

pub fn sessions() -> Vec<Session> {
    [
        ("session-primary-1", "steady-user", "stress-primary"),
        ("session-primary-2", "steady-user", "stress-primary"),
        ("session-secondary-1", "burst-user", "stress-secondary"),
        ("session-tertiary-1", "edge-user", "stress-tertiary"),
    ]
    .into_iter()
    .map(|(session_id, persona_id, principal_id)| Session {
        session_id: session_id.to_owned(),
        persona_id: persona_id.to_owned(),
        principal_id: principal_id.to_owned(),
    })
    .collect()
}
