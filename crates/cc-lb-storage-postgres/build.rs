use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is required"));
    emit_rerun_tree(&manifest_dir, Path::new("migrations"));
    write_fast_init_ddl(&manifest_dir);
}

fn write_fast_init_ddl(manifest_dir: &Path) {
    let migrations_dir = manifest_dir.join("migrations");
    let mut entries = fs::read_dir(&migrations_dir)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read Postgres migration directory {}: {error}",
                migrations_dir.display()
            )
        })
        .map(|entry| {
            entry.unwrap_or_else(|error| {
                panic!(
                    "failed to read an entry in Postgres migration directory {}: {error}",
                    migrations_dir.display()
                )
            })
        })
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                == Some("sql")
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    let mut ddl = String::new();
    for entry in entries {
        let filename = entry.file_name();
        let path = entry.path();
        let mut migration = fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!(
                "failed to read Postgres migration {}: {error}",
                path.display()
            )
        });

        match filename.to_str() {
            Some("0041_upstream_spec_split.sql") => {
                migration = migration.replace("SELECT pg_advisory_xact_lock(410041);", "");
            }
            Some("0055_add_warmup_attempts_v1.sql") => {
                migration = migration.replace("CREATE EXTENSION IF NOT EXISTS pgcrypto;", "");
            }
            Some("0107_upstream_name_active_unique.sql") => {
                let original = "WHERE t.relname = 'upstream_spec_v1'";
                let replacement = "WHERE t.relname = 'upstream_spec_v1' AND t.relnamespace = current_schema()::regnamespace";
                assert!(
                    migration.contains(original),
                    "Postgres migration 0107 no longer contains the expected schema predicate"
                );
                migration = migration.replace(original, replacement);
            }
            Some("0139_request_events_relational_columns.sql") => {
                migration = migration.replace("COMMIT;\n", "");
            }
            _ => {}
        }

        ddl.push_str(&migration);
        ddl.push_str("\n;\n");
    }

    let output_path = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is required"))
        .join("postgres_fast_init_ddl.sql");
    fs::write(&output_path, ddl).unwrap_or_else(|error| {
        panic!(
            "failed to write generated Postgres fast-init DDL {}: {error}",
            output_path.display()
        )
    });
}

fn emit_rerun_tree(manifest_dir: &Path, relative_path: &Path) {
    println!("cargo:rerun-if-changed={}", relative_path.display());

    let path = manifest_dir.join(relative_path);
    if path.is_file() {
        return;
    }
    if !path.is_dir() {
        panic!("Postgres migration input is missing: {}", path.display());
    }

    let mut entries = fs::read_dir(&path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read Postgres migration directory {}: {error}",
                path.display()
            )
        })
        .map(|entry| {
            entry.unwrap_or_else(|error| {
                panic!(
                    "failed to read an entry in Postgres migration directory {}: {error}",
                    path.display()
                )
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        emit_rerun_tree(manifest_dir, &relative_path.join(entry.file_name()));
    }
}
