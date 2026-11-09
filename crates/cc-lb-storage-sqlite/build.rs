use std::{env, fs, path::Path, path::PathBuf, process::Command};

fn emit_migration_rerun_paths(path: &Path) {
    if !path.is_dir() {
        return;
    }

    for entry in fs::read_dir(path).expect("read migrations directory") {
        let entry = entry.expect("read migrations directory entry");
        let entry_path = entry.path();
        if entry_path.is_dir() {
            emit_migration_rerun_paths(&entry_path);
        } else if entry_path
            .extension()
            .is_some_and(|extension| extension == "sql")
        {
            println!("cargo:rerun-if-changed={}", entry_path.display());
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=migrations");
    let migrations_dir = PathBuf::from("migrations");
    emit_migration_rerun_paths(&migrations_dir);
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is set"));
    let snapshot = out_dir.join("fast_init.sql");
    let status = Command::new("python3")
        .arg("-c")
        .arg(PYTHON_GENERATOR)
        .arg(&migrations_dir)
        .arg(&snapshot)
        .status();

    match status {
        Ok(status) if status.success() => {}
        Ok(_) | Err(_) => {
            fs::write(&snapshot, "").expect("write empty SQLite fast-init snapshot");
        }
    }
}

const PYTHON_GENERATOR: &str = r#"
import hashlib
import pathlib
import sqlite3
import sys

migrations_dir = pathlib.Path(sys.argv[1])
out_path = pathlib.Path(sys.argv[2])

connection = sqlite3.connect(":memory:", isolation_level=None)
connection.execute(
    """CREATE TABLE _sqlx_migrations (
        version BIGINT PRIMARY KEY,
        description TEXT NOT NULL,
        installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        success BOOLEAN NOT NULL,
        checksum BLOB NOT NULL,
        execution_time BIGINT NOT NULL
    )"""
)

for migration in sorted(migrations_dir.glob("*.sql")):
    stem = migration.stem
    version = int(stem.split("_", 1)[0])
    description = stem.split("_", 1)[1].replace("_", " ")
    raw = migration.read_bytes()
    checksum = hashlib.sha384(raw).digest()
    connection.executescript(raw.decode("utf-8"))
    connection.execute(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (?, ?, ?, ?, ?)",
        (version, description, 1, checksum, 1),
    )

connection.execute(
    "INSERT INTO meta_v1 (key, value) VALUES ('backend_kind', 'sqlite') ON CONFLICT(key) DO NOTHING;"
)

def identifier(value):
    return '"' + value.replace('"', '""') + '"'

def without_trailing_semicolon(sql):
    return sql.rstrip().rstrip(";")

lines = []
for table_name, ddl in connection.execute(
    "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
):
    lines.append(without_trailing_semicolon(ddl) + ";")
    columns = [row[1] for row in connection.execute(f"PRAGMA table_info({identifier(table_name)})")]
    if not columns:
        continue
    quoted_columns = ", ".join(identifier(column) for column in columns)
    quoted_values = " || ', ' || ".join(f"quote({identifier(column)})" for column in columns)
    for (values,) in connection.execute(
        f"SELECT {quoted_values} FROM {identifier(table_name)}"
    ):
        lines.append(
            f"INSERT INTO {identifier(table_name)} ({quoted_columns}) VALUES ({values});"
        )

for (ddl,) in connection.execute(
    "SELECT sql FROM sqlite_master WHERE type != 'table' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
):
    lines.append(without_trailing_semicolon(ddl) + ";")

out_path.write_text("\n".join(lines) + ("\n" if lines else ""))
"#;
