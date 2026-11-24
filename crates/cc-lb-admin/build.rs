use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    if env::var_os("CARGO_FEATURE_EMBED_SPA").is_none() {
        return;
    }

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is required"));
    let web_dir = manifest_dir.join("../..").join("web");
    let dist_dir = web_dir.join("dist");

    emit_spa_env_rerun_directives();

    let skip_spa = env::var("CC_LB_ADMIN_SKIP_SPA").as_deref() == Ok("1");
    let prebuilt_spa = env::var("CC_LB_ADMIN_PREBUILT_SPA").as_deref() == Ok("1");
    if skip_spa && prebuilt_spa {
        panic!("CC_LB_ADMIN_SKIP_SPA=1 and CC_LB_ADMIN_PREBUILT_SPA=1 are mutually exclusive");
    }

    if skip_spa {
        write_skip_placeholder(&dist_dir);
        return;
    }

    if prebuilt_spa {
        validate_dist(&dist_dir, "CC_LB_ADMIN_PREBUILT_SPA=1");
        return;
    }
    emit_spa_rerun_directives(&web_dir);

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
            "bun is required to build cc-lb-admin: install Bun >= 1.4.3, set \
             CC_LB_ADMIN_PREBUILT_SPA=1 with a built web/dist, or set \
             CC_LB_ADMIN_SKIP_SPA=1"
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
        .args(["run", "--shell=bun", "build"])
        .env("VITE_CC_LB_VERSION", env!("CARGO_PKG_VERSION"))
        .current_dir(&web_dir)
        .status()
        .unwrap_or_else(|error| panic!("failed to invoke bun for cc-lb-admin SPA build: {error}"));

    if !status.success() {
        panic!("bun run --shell=bun build failed for cc-lb-admin SPA with status {status}");
    }

    validate_dist(&dist_dir, "bun run --shell=bun build");
}

fn emit_spa_env_rerun_directives() {
    println!("cargo:rerun-if-env-changed=CC_LB_ADMIN_SKIP_SPA");
    println!("cargo:rerun-if-env-changed=CC_LB_ADMIN_PREBUILT_SPA");
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    println!("cargo:rerun-if-env-changed=CARGO_DOC");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_DOC");
}

fn emit_spa_rerun_directives(web_dir: &Path) {
    for path in ["src", "public"] {
        emit_rerun_tree(web_dir, Path::new(path));
    }
    for path in [
        "package.json",
        "bun.lock",
        "index.html",
        "vite.config.ts",
        "tsconfig.json",
        "tsconfig.app.json",
        "tsconfig.node.json",
    ] {
        emit_rerun_tree(web_dir, Path::new(path));
    }
}

fn emit_rerun_tree(web_dir: &Path, relative_path: &Path) {
    let path = web_dir.join(relative_path);
    println!(
        "cargo:rerun-if-changed={}",
        Path::new("../../web").join(relative_path).display()
    );

    if path.is_file() {
        return;
    }
    if !path.is_dir() {
        panic!("cc-lb-admin SPA build input is missing: {}", path.display());
    }

    let mut entries = fs::read_dir(&path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read cc-lb-admin SPA build input directory {}: {error}",
                path.display()
            )
        })
        .map(|entry| {
            entry.unwrap_or_else(|error| {
                panic!(
                    "failed to read an entry in cc-lb-admin SPA build input directory {}: {error}",
                    path.display()
                )
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        emit_rerun_tree(web_dir, &relative_path.join(entry.file_name()));
    }
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

fn validate_dist(dist_dir: &Path, source: &str) {
    if !dist_dir.is_dir() {
        panic!(
            "{source} requires an existing cc-lb-admin SPA dist directory at {}",
            dist_dir.display()
        );
    }

    let index_path = dist_dir.join("index.html");
    let index = fs::read_to_string(&index_path).unwrap_or_else(|error| {
        panic!(
            "{source} requires a readable {}: {error}",
            index_path.display()
        )
    });
    if index.trim().is_empty() {
        panic!("{source} requires a non-empty {}", index_path.display());
    }
    if index.contains("CC_LB_ADMIN_SKIP_SPA=1") {
        panic!(
            "{source} rejected the skip placeholder at {}",
            index_path.display()
        );
    }

    let assets_dir = dist_dir.join("assets");
    if !assets_dir.is_dir() {
        panic!(
            "{source} requires an assets directory at {}",
            assets_dir.display()
        );
    }
    let has_asset = fs::read_dir(&assets_dir)
        .unwrap_or_else(|error| {
            panic!(
                "{source} failed to read cc-lb-admin SPA assets directory {}: {error}",
                assets_dir.display()
            )
        })
        .any(|entry| entry.is_ok_and(|entry| entry.path().is_file()));
    if !has_asset {
        panic!(
            "{source} requires at least one asset file in {}",
            assets_dir.display()
        );
    }
}

fn write_skip_placeholder(dist_dir: &Path) {
    println!("cargo:rerun-if-changed=../../web/dist/index.html");
    const INDEX_HTML: &[u8] = br#"<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <link rel="stylesheet" href="/assets/index-skip.css" />
    <title>cc-lb Admin</title>
  </head>
  <body>
    <div id="root">SPA build skipped (CC_LB_ADMIN_SKIP_SPA=1)</div>
    <script type="module" src="/assets/index-skip.js"></script>
  </body>
</html>
"#;
    const INDEX_CSS: &[u8] = b"#root{font-family:sans-serif;}\n";
    const INDEX_JS: &[u8] = b"export {};\n";

    let assets_dir = dist_dir.join("assets");
    let expected_top_only = fs::read_dir(dist_dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .all(|e| matches!(e.file_name().to_str(), Some("index.html" | "assets")))
        })
        .unwrap_or(false);
    let expected_assets_only = fs::read_dir(&assets_dir)
        .map(|entries| {
            entries.filter_map(Result::ok).all(|e| {
                matches!(
                    e.file_name().to_str(),
                    Some("index-skip.css" | "index-skip.js")
                )
            })
        })
        .unwrap_or(false);
    let already_matches = expected_top_only
        && expected_assets_only
        && fs::read(dist_dir.join("index.html")).ok().as_deref() == Some(INDEX_HTML)
        && fs::read(assets_dir.join("index-skip.css")).ok().as_deref() == Some(INDEX_CSS)
        && fs::read(assets_dir.join("index-skip.js")).ok().as_deref() == Some(INDEX_JS);
    if already_matches {
        return;
    }

    if dist_dir.exists() {
        fs::remove_dir_all(dist_dir).expect("failed to clear cc-lb-admin SPA dist directory");
    }
    fs::create_dir_all(dist_dir).expect("failed to create cc-lb-admin SPA dist directory");
    fs::create_dir_all(&assets_dir).expect("failed to create cc-lb-admin SPA assets directory");
    fs::write(dist_dir.join("index.html"), INDEX_HTML)
        .expect("failed to write cc-lb-admin SPA skip placeholder");
    fs::write(assets_dir.join("index-skip.css"), INDEX_CSS)
        .expect("failed to write cc-lb-admin SPA skip stylesheet");
    fs::write(assets_dir.join("index-skip.js"), INDEX_JS)
        .expect("failed to write cc-lb-admin SPA skip script");

    // Backdate newly created placeholder files so `cargo:rerun-if-changed=../../web/dist/index.html`
    // does not compare newer than the current build script's `run/stdout` mtime.
    if let Some(past) = std::time::SystemTime::now().checked_sub(std::time::Duration::from_secs(2))
    {
        for path in [
            dist_dir.join("index.html"),
            assets_dir.join("index-skip.css"),
            assets_dir.join("index-skip.js"),
        ] {
            if let Ok(file) = fs::File::options().write(true).open(path) {
                let _ = file.set_modified(past);
            }
        }
    }
}
