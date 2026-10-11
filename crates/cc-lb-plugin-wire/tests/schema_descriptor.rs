#[path = "../schema_descriptor.rs"]
mod generator;

#[test]
fn nested_layout_changes_change_descriptors_but_formatting_does_not() {
    let source = "#[derive(Archive)] pub struct Root { pub items: Box<[Child]>, } #[derive(Archive)] pub struct Child { pub used: f64, pub reset: u64, }";
    let baseline = generator::describe(&[source], "Root");
    assert_eq!(
        baseline,
        generator::describe(&[&source.replace(",", ",\n")], "Root")
    );
    for changed in [
        source.replace("pub used: f64,", "pub used: f32,"),
        source.replace(
            "pub used: f64, pub reset: u64,",
            "pub reset: u64, pub used: f64,",
        ),
        source.replace("pub reset: u64,", "pub reset: u64, pub blocked: bool,"),
        source.replace("pub items:", "#[rkyv(with = InlineAsBox)] pub items:"),
    ] {
        assert_ne!(baseline, generator::describe(&[&changed], "Root"));
    }
}

#[test]
fn unsupported_reachable_layouts_fail_closed() {
    for source in [
        "struct Root { value: Alias } type Alias = u64;",
        "struct Root { value: Choice } enum Choice { One, Two }",
        "#[repr(C)] struct Root { value: u64 }",
        "#[derive(custom::Archive)] struct Root { value: u64 }",
        "#![cfg(feature = \"other\")] struct Root { value: u64 }",
        "struct Root { #[cfg(feature = \"other\")] value: u64 }",
        "struct Root { value: Unknown }",
        "use custom::Box; struct Root { value: Box<str> }",
        "use custom::Mapper as InlineAsBox; struct Root { #[rkyv(with = InlineAsBox)] value: &str }",
        "struct Root { #[rkyv(with = CustomAdapter)] value: u64 }",
        "type InlineAsBox = Other; struct Root { #[rkyv(with = InlineAsBox)] value: &str }",
        "#[cfg_attr(feature = \"packed\", repr(packed))] struct Root { value: u64 }",
    ] {
        let source = source.replace("struct ", "#[derive(Archive)] struct ");
        assert!(std::panic::catch_unwind(|| generator::describe(&[&source], "Root")).is_err());
    }
}

#[test]
fn real_v2_quota_graph_changes_are_fingerprinted() {
    let v1 = include_str!("../src/v1/mod.rs");
    let v2 = include_str!("../src/v2.rs");
    for root in ["FilterRequest", "FilterRequestRef"] {
        let baseline = generator::describe(&[v1, v2], root);
        assert_ne!(
            baseline,
            generator::describe(
                &[
                    v1,
                    &v2.replace("max_staleness_secs: u64", "max_staleness_secs: u32")
                ],
                root
            )
        );
        assert_eq!(
            baseline,
            generator::describe(&[v1, &v2.replace("///", "/// More documentation:")], root)
        );
    }
}

#[test]
fn manual_archive_cannot_replace_the_canonical_derive() {
    let source = "#[derive(Archive, Serialize)] struct Root { value: u64 }";
    generator::describe(&[source], "Root");
    let manual = format!(
        "{} impl Archive for Root {{}}",
        source.replace("Archive, ", "")
    );
    assert!(std::panic::catch_unwind(|| generator::describe(&[&manual], "Root")).is_err());
}
