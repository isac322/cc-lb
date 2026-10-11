//! Resolution of a caller-visible principal id.
//!
//! A keep-alive session records the principal id of the request that created
//! it, and that id is whatever `PrincipalView` was asked for: a principal name
//! or a principal UUID. `PrincipalView::from_db` keys its specs by *name* and
//! only keeps UUIDs as aliases of live rows, so `PrincipalView::get` resolves a
//! name first — a UUID-shaped principal name resolves as a name — and drops
//! deleted principals entirely.
//!
//! Storage boundaries that must agree with the dynamic view — session
//! admission and mode-update retirement — use this module instead of guessing
//! from the id's shape.

use uuid::Uuid;

/// One identity a caller-visible principal id may resolve to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrincipalIdentityRef {
    /// The id is the principal's name: match `principals_v1.name`.
    Name(String),
    /// The id is the principal's immutable UUID: match `principals_v1.id`.
    Id(Uuid),
}

/// The identities `principal_id` can resolve to, in the order `PrincipalView`
/// tries them: as a principal name first, then as a UUID alias.
///
/// A lookup must exclude deleted rows for both identities, because
/// `PrincipalView` publishes neither the name nor the UUID of a deleted
/// principal.
pub fn principal_identity_refs(principal_id: &str) -> Vec<PrincipalIdentityRef> {
    let mut refs = vec![PrincipalIdentityRef::Name(principal_id.to_owned())];
    if let Ok(id) = Uuid::parse_str(principal_id) {
        refs.push(PrincipalIdentityRef::Id(id));
    }
    refs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_resolves_first_and_a_uuid_second() {
        assert_eq!(
            principal_identity_refs("team-a"),
            vec![PrincipalIdentityRef::Name("team-a".to_owned())]
        );
        let uuid = Uuid::from_u128(7);
        assert_eq!(
            principal_identity_refs(&uuid.to_string()),
            vec![
                PrincipalIdentityRef::Name(uuid.to_string()),
                PrincipalIdentityRef::Id(uuid)
            ]
        );
    }

    #[test]
    fn a_uuid_shaped_name_keeps_the_name_lookup_first() {
        // A principal named with a UUID string is legal, and `PrincipalView`
        // resolves that name before any alias, so the name lookup must stay
        // first for a UUID-shaped id too.
        let uuid = Uuid::from_u128(11);
        let refs = principal_identity_refs(&uuid.to_string());
        assert_eq!(refs[0], PrincipalIdentityRef::Name(uuid.to_string()));
    }
}
