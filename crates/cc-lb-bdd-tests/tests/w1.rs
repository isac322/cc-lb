//! Writer stream W1 — day-to-day operator traffic.
//!
//! Hosts the W1 feature scenarios (F1, F2, F3, F4, F6, F19, F26;
//! 98 scenarios total in the v5.2 corpus). Each feature lives in its
//! own `mod w1_f<n>` submodule; scenarios inside are emitted by the
//! `bdd_scenario!` macro.
//!
//! See `docs/cc-lb-bdd-test-conversion-map.md` for the full list of
//! pending W1 scenarios and `docs/cc-lb-bdd-fast-subset.md` for the
//! `fast_` prefix budget.

use cc_lb_bdd_tests::{
    AuditEntrySummary, HealthSnapshot, PrincipalCreateResult, PrincipalDisableResult,
    PrincipalModelAclResult, PrincipalSoftDeleteResult, bdd_scenario,
};

mod w1_f1 {
    use super::*;

    bdd_scenario! {
        id: "F1.1a",
        fn_name: fast_f1_1a,
        persona: Alice,
        title: "Alice registers a new principal and sees it become active",
        description:
            "Alice creates a machine principal named 'team-x'. The store \
             returns a record whose enabled flag is true (the user-facing \
             'active' signal). Name and revision must round-trip unchanged.",
        given: |ctx| {
            ctx.alice().await
        },
        when: |alice| {
            alice.create_principal("team-x").await?
        },
        then: |result, ctx| {
            let result: PrincipalCreateResult = result;
            ctx.assert(
                result.is_active,
                "active flag missing on freshly created principal: \
                 expected=true, actual=false",
            );
            ctx.assert(
                result.name == "team-x",
                format!(
                    "principal name mismatch: expected='team-x', actual='{}'",
                    result.name
                ),
            );
            ctx.assert(
                result.revision == 0 || result.revision == 1,
                format!(
                    "freshly created principal revision out of range: \
                     expected=0 or 1, actual={}",
                    result.revision
                ),
            );
        },
    }

    bdd_scenario! {
        id: "F1.1b",
        fn_name: fast_f1_1b,
        persona: Alice,
        title: "Alice registering a principal leaves an audit trail",
        description:
            "Alice creates a machine principal named 'team-y'. \
             A PrincipalCreate audit row is written with actor='admin' \
             and the new principal's id, so 'who created what, when' \
             can be answered from the audit log alone.",
        given: |ctx| {
            ctx.alice().await
        },
        when: |alice| {
            let created = alice.create_principal("team-y").await?;
            let entries = alice.query_audit_for_principal(created.id).await?;
            (created, entries)
        },
        then: |pair, ctx| {
            let (created, entries): (PrincipalCreateResult, Vec<AuditEntrySummary>) = pair;
            ctx.assert(
                !entries.is_empty(),
                format!(
                    "expected at least one audit row for principal {}, \
                     found none",
                    created.id
                ),
            );
            let kinds: Vec<&str> = entries.iter().map(|e| e.kind.as_str()).collect();
            ctx.assert(
                entries.iter().any(|e| e.kind == "PrincipalCreate"),
                format!(
                    "expected a PrincipalCreate audit row, found kinds {:?}",
                    kinds
                ),
            );
            ctx.assert(
                entries.iter().any(|e| e.actor == "admin"),
                format!(
                    "expected an audit row with actor='admin', found actors {:?}",
                    entries.iter().map(|e| e.actor.as_str()).collect::<Vec<_>>()
                ),
            );
        },
    }

    bdd_scenario! {
        id: "F1.4",
        fn_name: fast_f1_4,
        persona: Alice,
        title: "Disabling a principal flips its active flag off",
        description:
            "Alice creates a principal, then disables it. The persisted \
             record carries enabled=false (the user-facing 'inactive' \
             signal) and the audit log gains a PrincipalDisable row. \
             The downstream contract — subsequent calls are refused — \
             is enforced by the request pipeline and lives in W1 F3 / \
             F1.4 integration scenarios; the storage-side invariant is \
             that the flag actually flips.",
        given: |ctx| {
            ctx.alice().await
        },
        when: |alice| {
            let created = alice.create_principal("team-disable").await?;
            let disabled = alice
                .disable_principal(created.id, created.revision)
                .await?;
            let audit = alice.query_audit_for_principal(created.id).await?;
            (disabled, audit)
        },
        then: |pair, ctx| {
            let (disabled, audit): (PrincipalDisableResult, Vec<AuditEntrySummary>) = pair;
            ctx.assert(
                !disabled.is_active,
                format!(
                    "disable did not flip enabled flag: actual={}",
                    disabled.is_active
                ),
            );
            ctx.assert(
                audit.iter().any(|e| e.kind == "PrincipalDisable"),
                format!(
                    "PrincipalDisable audit row missing: kinds = {:?}",
                    audit.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>()
                ),
            );
        },
    }

    bdd_scenario! {
        id: "F1.5",
        fn_name: fast_f1_5,
        persona: Alice,
        title: "Setting allowed models pins the principal's model ACL",
        description:
            "Alice creates a principal then restricts it to a single \
             explicit allow-list, e.g. ['claude-opus-4']. The store \
             returns the same list verbatim — the ACL is the storage \
             ground truth that the downstream proxy uses to reject \
             out-of-list models in F3.x. Storage-side invariant: the \
             list round-trips and the principal revision moves forward.",
        given: |ctx| {
            ctx.alice().await
        },
        when: |alice| {
            let created = alice.create_principal("team-models").await?;
            let acl = alice
                .set_allowed_models(
                    created.id,
                    created.revision,
                    vec!["claude-opus-4".to_owned()],
                )
                .await?;
            (created, acl)
        },
        then: |pair, ctx| {
            let (created, acl): (PrincipalCreateResult, PrincipalModelAclResult) = pair;
            ctx.assert(
                acl.allowed_models == vec!["claude-opus-4".to_owned()],
                format!(
                    "allowed_models did not round-trip: expected=[claude-opus-4], \
                     actual={:?}",
                    acl.allowed_models
                ),
            );
            ctx.assert(
                acl.revision > created.revision,
                format!(
                    "revision did not advance after update: created={}, after_update={}",
                    created.revision, acl.revision
                ),
            );
        },
    }

    bdd_scenario! {
        id: "F1.7",
        fn_name: fast_f1_7,
        persona: Alice,
        title: "Soft-deleting a principal preserves its prior audit trail",
        description:
            "Alice creates a principal 'team-z', then soft-deletes it. \
             The audit log MUST still show the original PrincipalCreate \
             row plus a new PrincipalSoftDelete row, so the history of \
             what the principal did before the soft delete is never \
             erased — the v5.2 invariant for F1.7.",
        given: |ctx| {
            ctx.alice().await
        },
        when: |alice| {
            let created = alice.create_principal("team-z").await?;
            let deleted = alice
                .soft_delete_principal(created.id, created.revision)
                .await?;
            let entries = alice.query_audit_for_principal(created.id).await?;
            (created, deleted, entries)
        },
        then: |triple, ctx| {
            let (created, deleted, entries): (
                PrincipalCreateResult,
                PrincipalSoftDeleteResult,
                Vec<AuditEntrySummary>,
            ) = triple;
            ctx.assert(
                deleted.id == created.id,
                format!(
                    "soft-delete returned wrong id: expected={}, actual={}",
                    created.id, deleted.id
                ),
            );
            ctx.assert(
                deleted.deleted_at_unix_secs.is_some(),
                "soft-delete did not stamp deleted_at",
            );
            let kinds: Vec<&str> = entries.iter().map(|e| e.kind.as_str()).collect();
            ctx.assert(
                entries.iter().any(|e| e.kind == "PrincipalCreate"),
                format!(
                    "PrincipalCreate audit row vanished after soft delete: \
                     remaining kinds = {:?}",
                    kinds
                ),
            );
            ctx.assert(
                entries.iter().any(|e| e.kind == "PrincipalSoftDelete"),
                format!(
                    "PrincipalSoftDelete audit row missing: kinds = {:?}",
                    kinds
                ),
            );
        },
    }
}

mod w1_f26 {
    use super::*;

    bdd_scenario! {
        id: "F26.1",
        fn_name: fast_f26_1,
        persona: Charlie,
        title: "Liveness and readiness are reported as two separate signals",
        description:
            "Charlie checks the health snapshot. The 'liveness' signal \
             only reports the process is up, while the 'readiness' \
             signal reports the storage contract is initialised. They \
             MUST be distinct: a process is alive long before it is \
             ready to serve traffic, and v5.2 F26.1 requires the two \
             to never collapse into a single flag.",
        given: |ctx| {
            ctx.charlie().await
        },
        when: |charlie| {
            charlie.check_health().await?
        },
        then: |health, ctx| {
            let health: HealthSnapshot = health;
            ctx.assert(
                health.liveness_ok,
                "liveness signal must be true after harness boot",
            );
            ctx.assert(
                health.readiness_ok,
                "readiness signal must be true after storage initialise",
            );
            ctx.assert(
                !health.killswitch_enabled,
                "killswitch must default to disabled on fresh init",
            );
        },
    }
}
