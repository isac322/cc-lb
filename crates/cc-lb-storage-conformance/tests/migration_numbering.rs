use std::{fs, path::Path};

fn migration_versions(directory: &Path) -> Vec<u32> {
    let mut versions = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter_map(|name| name.split_once('_').map(|(version, _)| version.to_owned()))
        .map(|version| version.parse::<u32>().unwrap())
        .collect::<Vec<_>>();
    versions.sort_unstable();
    versions
}

#[test]
fn storage_migrations_keep_the_expected_numbering() {
    let crates_directory = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let sqlite = migration_versions(&crates_directory.join("cc-lb-storage-sqlite/migrations"));
    let postgres = migration_versions(&crates_directory.join("cc-lb-storage-postgres/migrations"));

    assert_eq!(sqlite, (1..=53).collect::<Vec<_>>());
    assert_eq!(
        postgres,
        (1..=83)
            .filter(|version| *version != 43)
            .collect::<Vec<_>>()
    );
    assert!(
        include_str!("../../cc-lb-storage-sqlite/migrations/0052_request_events_source_kind.sql")
            .contains("source_kind")
    );
    assert!(
        include_str!("../../cc-lb-storage-sqlite/migrations/0052_request_events_source_kind.sql")
            .contains("source_ref_id")
    );
    assert!(
        include_str!("../../cc-lb-storage-postgres/migrations/0082_request_events_source_kind.sql")
            .contains("source_kind")
    );
    assert!(
        include_str!("../../cc-lb-storage-postgres/migrations/0082_request_events_source_kind.sql")
            .contains("source_ref_id")
    );
    assert!(
        include_str!(
            "../../cc-lb-storage-sqlite/migrations/0053_cache_keepalive_running_lease.sql"
        )
        .contains("accounting_key_id")
    );
    assert!(
        include_str!(
            "../../cc-lb-storage-sqlite/migrations/0053_cache_keepalive_running_lease.sql"
        )
        .contains("running_since_unix_secs")
    );
    assert!(
        include_str!(
            "../../cc-lb-storage-postgres/migrations/0083_cache_keepalive_running_lease.sql"
        )
        .contains("accounting_key_id")
    );
    assert!(
        include_str!(
            "../../cc-lb-storage-postgres/migrations/0083_cache_keepalive_running_lease.sql"
        )
        .contains("running_since_unix_secs")
    );
}
