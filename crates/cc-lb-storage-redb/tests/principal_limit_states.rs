use cc_lb_storage_redb::{
    PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState, Storage,
    principal_limit_state_key,
};

#[test]
fn principal_limit_states_persist_across_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("limits.redb");

    let storage = Storage::open(&path, [31; 32])?;
    storage.put_principal_limit_state(&state(
        "principal-a",
        PrincipalLimitIdentityKind::Account,
        Some("org-a"),
        true,
        "5h",
        PrincipalLimitKind::Requests,
        Some(5000),
        Some(4999),
        Some("2026-05-20T00:00:01Z"),
    ))?;
    drop(storage);

    let storage = Storage::open(&path, [31; 32])?;
    let reopened = storage
        .get_principal_limit_state(
            "principal-a",
            PrincipalLimitIdentityKind::Account,
            Some("org-a"),
            "5h",
            PrincipalLimitKind::Requests,
        )?
        .expect("principal limit state exists after reopen");

    assert_eq!(reopened.principal_id, "principal-a");
    assert_eq!(reopened.identity_kind, PrincipalLimitIdentityKind::Account);
    assert_eq!(reopened.identity_value.as_deref(), Some("org-a"));
    assert!(reopened.account_observed);
    assert_eq!(reopened.window, "5h");
    assert_eq!(reopened.kind, PrincipalLimitKind::Requests);
    assert_eq!(reopened.limit, Some(5000));
    assert_eq!(reopened.remaining, Some(4999));
    assert_eq!(reopened.reset.as_deref(), Some("2026-05-20T00:00:01Z"));
    Ok(())
}

#[test]
fn principal_limit_state_upsert_keeps_latest_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("limits.redb");
    let storage = Storage::open(&path, [31; 32])?;

    storage.put_principal_limit_state(&state(
        "principal-a",
        PrincipalLimitIdentityKind::Credential,
        Some("credential-a"),
        false,
        "weekly",
        PrincipalLimitKind::Tokens,
        Some(100000),
        Some(99990),
        Some("2026-05-20T00:00:02Z"),
    ))?;
    storage.put_principal_limit_state(&state(
        "principal-a",
        PrincipalLimitIdentityKind::Credential,
        Some("credential-a"),
        false,
        "weekly",
        PrincipalLimitKind::Tokens,
        Some(100000),
        Some(99980),
        Some("2026-05-20T00:00:03Z"),
    ))?;

    let latest = storage
        .get_principal_limit_state(
            "principal-a",
            PrincipalLimitIdentityKind::Credential,
            Some("credential-a"),
            "weekly",
            PrincipalLimitKind::Tokens,
        )?
        .expect("latest snapshot exists");

    assert_eq!(latest.remaining, Some(99980));
    assert_eq!(latest.reset.as_deref(), Some("2026-05-20T00:00:03Z"));
    Ok(())
}

#[test]
fn account_grouping_does_not_collapse_unobserved_identity() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("limits.redb");
    let storage = Storage::open(&path, [31; 32])?;

    storage.put_principal_limit_state(&state(
        "principal-a",
        PrincipalLimitIdentityKind::Account,
        Some("org-a"),
        true,
        "default",
        PrincipalLimitKind::Requests,
        Some(1000),
        Some(900),
        None,
    ))?;
    storage.put_principal_limit_state(&state(
        "principal-a",
        PrincipalLimitIdentityKind::Unobserved,
        None,
        false,
        "default",
        PrincipalLimitKind::Requests,
        Some(1000),
        Some(800),
        None,
    ))?;

    let observed_key = principal_limit_state_key(
        "principal-a",
        PrincipalLimitIdentityKind::Account,
        Some("org-a"),
        "default",
        PrincipalLimitKind::Requests,
    );
    let unobserved_key = principal_limit_state_key(
        "principal-a",
        PrincipalLimitIdentityKind::Unobserved,
        None,
        "default",
        PrincipalLimitKind::Requests,
    );
    assert_ne!(observed_key, unobserved_key);

    let observed = storage
        .get_principal_limit_state(
            "principal-a",
            PrincipalLimitIdentityKind::Account,
            Some("org-a"),
            "default",
            PrincipalLimitKind::Requests,
        )?
        .expect("observed account snapshot exists");
    let unobserved = storage
        .get_principal_limit_state(
            "principal-a",
            PrincipalLimitIdentityKind::Unobserved,
            None,
            "default",
            PrincipalLimitKind::Requests,
        )?
        .expect("unobserved account snapshot exists");

    assert!(observed.account_observed);
    assert_eq!(observed.remaining, Some(900));
    assert!(!unobserved.account_observed);
    assert_eq!(unobserved.identity_value, None);
    assert_eq!(unobserved.remaining, Some(800));
    Ok(())
}

#[test]
fn list_principal_limit_states_filters_by_value_principal_id()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("limits.redb");
    let storage = Storage::open(&path, [31; 32])?;

    storage.put_principal_limit_state(&state(
        "principal-a",
        PrincipalLimitIdentityKind::Account,
        Some("org-a"),
        true,
        "5h",
        PrincipalLimitKind::Requests,
        Some(1000),
        Some(900),
        None,
    ))?;
    storage.put_principal_limit_state(&state(
        "principal-b",
        PrincipalLimitIdentityKind::Account,
        Some("org-b"),
        true,
        "5h",
        PrincipalLimitKind::Requests,
        Some(2000),
        Some(1900),
        None,
    ))?;
    storage.put_principal_limit_state(&state(
        "principal-a",
        PrincipalLimitIdentityKind::Credential,
        Some("credential-a"),
        false,
        "weekly",
        PrincipalLimitKind::Tokens,
        Some(3000),
        Some(2900),
        None,
    ))?;

    let mut listed = storage.list_principal_limit_states("principal-a")?;
    listed.sort_by(|left, right| {
        left.identity_kind
            .as_str()
            .cmp(right.identity_kind.as_str())
    });

    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .all(|state| state.principal_id == "principal-a")
    );
    assert!(
        listed
            .iter()
            .any(|state| state.identity_kind == PrincipalLimitIdentityKind::Account)
    );
    assert!(
        listed
            .iter()
            .any(|state| state.identity_kind == PrincipalLimitIdentityKind::Credential)
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn state(
    principal_id: &str,
    identity_kind: PrincipalLimitIdentityKind,
    identity_value: Option<&str>,
    account_observed: bool,
    window: &str,
    kind: PrincipalLimitKind,
    limit: Option<u64>,
    remaining: Option<u64>,
    reset: Option<&str>,
) -> PrincipalLimitState {
    PrincipalLimitState {
        principal_id: principal_id.to_owned(),
        identity_kind,
        identity_value: identity_value.map(ToOwned::to_owned),
        account_observed,
        window: window.to_owned(),
        kind,
        limit,
        remaining,
        reset: reset.map(ToOwned::to_owned),
        observed_at_unix_secs: 1_764_000_000,
        stored_at_unix_secs: 1_764_000_001,
    }
}
