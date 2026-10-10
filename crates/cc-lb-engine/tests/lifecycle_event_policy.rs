use std::collections::BTreeSet;

use syn::{Expr, ExprMatch, Item, ItemEnum, ItemFn, Pat, PatOr, PatStruct, PatTupleStruct};

const EVENT_SOURCE: &str = include_str!("../../cc-lb-lifecycle/src/event.rs");
const ASSEMBLER_SOURCE: &str = include_str!("../src/lifecycle_event_assembler.rs");

fn enum_variants(source: &str) -> BTreeSet<String> {
    let file = syn::parse_file(source).expect("lifecycle event source parses");
    file.items
        .into_iter()
        .find_map(|item| match item {
            Item::Enum(ItemEnum {
                ident, variants, ..
            }) if ident == "LifecycleEvent" => {
                Some(variants.into_iter().map(|v| v.ident.to_string()).collect())
            }
            _ => None,
        })
        .expect("LifecycleEvent enum exists")
}

fn function(source: &str, name: &str) -> ItemFn {
    let file = syn::parse_file(source).expect("assembler source parses");
    file.items
        .into_iter()
        .find_map(|item| match item {
            Item::Fn(item) if item.sig.ident == name => Some(item),
            _ => None,
        })
        .expect("target function exists")
}

fn docs(item: &ItemFn) -> String {
    item.attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .filter_map(|attr| attr.meta.require_name_value().ok())
        .filter_map(|meta| match &meta.value {
            Expr::Lit(expr) => match &expr.lit {
                syn::Lit::Str(value) => Some(value.value()),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn match_expr(item: &ItemFn) -> &ExprMatch {
    item.block
        .stmts
        .iter()
        .find_map(|stmt| match stmt {
            syn::Stmt::Expr(Expr::Match(expr), _) => Some(expr),
            syn::Stmt::Local(local) => match local.init.as_ref().map(|init| init.expr.as_ref()) {
                Some(Expr::Match(expr)) => Some(expr),
                _ => None,
            },
            _ => None,
        })
        .expect("target function contains a top-level match")
}

fn pattern_variants(pattern: &Pat) -> Vec<String> {
    match pattern {
        Pat::Path(path) => path
            .path
            .segments
            .last()
            .map(|s| vec![s.ident.to_string()])
            .unwrap_or_default(),
        Pat::Struct(PatStruct { path, .. }) | Pat::TupleStruct(PatTupleStruct { path, .. }) => path
            .segments
            .last()
            .map(|s| vec![s.ident.to_string()])
            .unwrap_or_default(),
        Pat::Or(PatOr { cases, .. }) => cases.iter().flat_map(pattern_variants).collect(),
        _ => Vec::new(),
    }
}

fn is_noop(expr: &Expr) -> bool {
    match expr {
        Expr::Path(path) => path.path.is_ident("None"),
        Expr::Block(block) => {
            block.block.stmts.is_empty()
                || (block.block.stmts.len() == 1
                    && matches!(
                        &block.block.stmts[0],
                        syn::Stmt::Expr(inner, None) if is_noop(inner)
                    ))
        }
        Expr::Tuple(tuple) => tuple.elems.is_empty(),
        _ => false,
    }
}

fn assert_policy(
    source: &str,
    function_name: &str,
    variants: &BTreeSet<String>,
    allowed_noops: &[&str],
) {
    let item = function(source, function_name);
    let expression = match_expr(&item);
    let allowed: BTreeSet<String> = allowed_noops
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let mut covered = BTreeSet::new();
    let mut noops = BTreeSet::new();
    let mut handled = BTreeSet::new();

    for arm in &expression.arms {
        assert!(
            !matches!(arm.pat, Pat::Wild(_) | Pat::Ident(_)),
            "{function_name} must not use a catch-all arm"
        );
        let names = pattern_variants(&arm.pat);
        assert!(
            !names.is_empty(),
            "{function_name} arm must name lifecycle event variants"
        );
        let noop = is_noop(&arm.body);
        for name in names {
            covered.insert(name.clone());
            if noop {
                noops.insert(name);
            } else {
                handled.insert(name);
            }
        }
    }

    assert_eq!(
        &covered, variants,
        "{function_name} must explicitly cover every LifecycleEvent variant"
    );
    assert!(
        noops.is_subset(&allowed),
        "{function_name} contains an undocumented no-op variant: {:?}",
        noops.difference(&allowed).collect::<Vec<_>>()
    );
    let required_handled: &[&str] = match function_name {
        "partial_emit_trigger" => &["ParseCompleted", "AuthCompleted"],
        "merge" => &["ParseCompleted", "AuthCompleted", "LimitDecision"],
        _ => &[],
    };
    for name in required_handled {
        if variants.contains(*name) {
            assert!(
                handled.contains(*name),
                "{function_name} must retain a handled arm for mixed-policy variant {name}"
            );
        }
    }
    let rationale = docs(&item);
    for name in &noops {
        assert!(
            rationale.contains(name),
            "{function_name} no-op {name} must have a documented rationale"
        );
    }
}

#[test]
fn lifecycle_event_assembler_matches_are_explicit_and_documented() {
    let variants = enum_variants(EVENT_SOURCE);
    assert_policy(
        ASSEMBLER_SOURCE,
        "partial_emit_trigger",
        &variants,
        &[
            "ParseCompleted",
            "AuthCompleted",
            "AuthenticationCompleted",
            "LimitDecision",
            "UpstreamAttempt",
            "RequestLogUpstreamErrorObserved",
            "UpstreamStreamDiagnosticsObserved",
            "Priced",
            "CacheObserved",
        ],
    );
    assert_policy(
        ASSEMBLER_SOURCE,
        "merge",
        &variants,
        &[
            "ParseCompleted",
            "AuthCompleted",
            "LimitDecision",
            "RequestTerminated",
            "AuthenticationCompleted",
        ],
    );
}

#[test]
fn policy_rejects_wildcards_and_new_or_handled_noops() {
    let variants = BTreeSet::from(["Known".to_owned(), "Added".to_owned()]);
    let mixed_handled_became_noop =
        "/// ParseCompleted\nfn partial_emit_trigger(event: E) { match event {
            E::ParseCompleted { ok: true } => None,
            E::ParseCompleted { ok: false } => {},
        } }";
    let mixed_variants = BTreeSet::from(["ParseCompleted".to_owned()]);
    assert!(
        std::panic::catch_unwind(|| {
            assert_policy(
                mixed_handled_became_noop,
                "partial_emit_trigger",
                &mixed_variants,
                &["ParseCompleted"],
            )
        })
        .is_err()
    );
    let unit_noop =
        "/// Added\nfn target(event: E) { match event { E::Known => (), E::Added => { () } } }";
    assert!(
        std::panic::catch_unwind(|| { assert_policy(unit_noop, "target", &variants, &["Added"]) })
            .is_err()
    );
    let none_noop =
        "/// Added\nfn target(event: E) { match event { E::Known => None, E::Added => { None } } }";
    assert!(
        std::panic::catch_unwind(|| assert_policy(none_noop, "target", &variants, &["Added"]))
            .is_err()
    );
    let wildcard = "fn target(event: E) { match event { E::Known => {}, _ => {} } }";
    assert!(
        std::panic::catch_unwind(|| assert_policy(wildcard, "target", &variants, &[])).is_err()
    );

    let missing = "fn target(event: E) { match event { E::Known => {} } }";
    assert!(std::panic::catch_unwind(|| assert_policy(missing, "target", &variants, &[])).is_err());

    let handled_became_noop =
        "/// Known\nfn target(event: E) { match event { E::Known => {}, E::Added => {} } }";
    assert!(
        std::panic::catch_unwind(|| assert_policy(
            handled_became_noop,
            "target",
            &variants,
            &["Added"]
        ))
        .is_err()
    );
}
