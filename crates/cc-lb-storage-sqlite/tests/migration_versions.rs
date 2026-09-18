//! Two migration files may never share a version prefix.
//!
//! sqlx derives the `_sqlx_migrations` primary key from the filename prefix, so a
//! duplicate prefix aborts every migration run against a fresh database with
//! `duplicate key value violates unique constraint "_sqlx_migrations_pkey"`.
//! Stacked branches that renumber migrations are the usual way this happens.

use std::collections::BTreeMap;
use std::path::Path;

#[test]
fn migration_versions_are_unique() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut by_version: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for entry in std::fs::read_dir(&dir).expect("read migrations directory") {
        let name = entry.expect("read migration entry").file_name();
        let name = name.to_string_lossy().to_string();
        let Some(stem) = name.strip_suffix(".sql") else {
            continue;
        };
        let Some((version, _)) = stem.split_once('_') else {
            panic!("migration {name} has no version separator");
        };
        assert!(
            version.chars().all(|c| c.is_ascii_digit()),
            "migration {name} has a non-numeric version prefix"
        );
        by_version.entry(version.to_owned()).or_default().push(name);
    }

    assert!(!by_version.is_empty(), "no migrations found in {dir:?}");

    let collisions: Vec<_> = by_version
        .iter()
        .filter(|(_, files)| files.len() > 1)
        .collect();
    assert!(
        collisions.is_empty(),
        "migration versions must be unique, found {collisions:?}"
    );
}
