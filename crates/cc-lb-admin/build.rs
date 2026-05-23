use std::{env, fs, path::PathBuf, process::Command};

fn main() {
    emit_rerun_directives();

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is required"));
    let web_dir = manifest_dir.join("web");
    let dist_dir = web_dir.join("dist");

    if env::var("CC_LB_ADMIN_SKIP_SPA").as_deref() == Ok("1") {
        write_skip_placeholder(&dist_dir);
        return;
    }

    let bun = match find_in_path("bun") {
        Some(path) => path,
        None if is_doc_build() => {
            println!(
                "cargo:warning=bun was not found while documenting cc-lb-admin; writing skipped SPA placeholder"
            );
            write_skip_placeholder(&dist_dir);
            return;
        }
        None => panic!(
            "bun is required to build cc-lb-admin: install Bun >= 1.3.14 or set CC_LB_ADMIN_SKIP_SPA=1"
        ),
    };

    let install_status = Command::new(&bun)
        .arg("install")
        .arg("--frozen-lockfile")
        .current_dir(&web_dir)
        .status()
        .unwrap_or_else(|error| {
            panic!("failed to invoke bun install for cc-lb-admin SPA: {error}")
        });

    if !install_status.success() {
        panic!(
            "bun install --frozen-lockfile failed for cc-lb-admin SPA with status {install_status}"
        );
    }

    let status = Command::new(&bun)
        .arg("run")
        .arg("build")
        .current_dir(&web_dir)
        .status()
        .unwrap_or_else(|error| panic!("failed to invoke bun for cc-lb-admin SPA build: {error}"));

    if !status.success() {
        panic!("bun run build failed for cc-lb-admin SPA with status {status}");
    }
}

fn emit_rerun_directives() {
    println!("cargo:rerun-if-changed=web/src");
    println!("cargo:rerun-if-changed=web/public");
    println!("cargo:rerun-if-changed=web/package.json");
    println!("cargo:rerun-if-changed=web/bun.lock");
    println!("cargo:rerun-if-changed=web/index.html");
    println!("cargo:rerun-if-changed=web/vite.config.ts");
    println!("cargo:rerun-if-env-changed=CC_LB_ADMIN_SKIP_SPA");
}

fn find_in_path(binary: &str) -> Option<PathBuf> {
    let paths = env::var_os("PATH")?;
    for dir in env::split_paths(&paths) {
        let candidate = dir.join(binary);
        if candidate.is_file() {
            return Some(candidate);
        }

        #[cfg(windows)]
        {
            for extension in ["exe", "cmd", "bat"] {
                let candidate = dir.join(format!("{binary}.{extension}"));
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
    }
    None
}

fn is_doc_build() -> bool {
    env::var_os("DOCS_RS").is_some()
        || env::var_os("CARGO_DOC").is_some()
        || env::var_os("CARGO_CFG_DOC").is_some()
}

fn write_skip_placeholder(dist_dir: &PathBuf) {
    fs::create_dir_all(dist_dir).expect("failed to create cc-lb-admin SPA dist directory");
    fs::write(
        dist_dir.join("index.html"),
        r#"<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>cc-lb Admin</title>
  </head>
  <body>
    <div id="root">SPA build skipped (CC_LB_ADMIN_SKIP_SPA=1)</div>
  </body>
</html>
"#,
    )
    .expect("failed to write cc-lb-admin SPA skip placeholder");
}
