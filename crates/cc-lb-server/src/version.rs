use crate::build_meta::BuildMeta;

pub fn format_version() -> String {
    let meta = BuildMeta::current();
    format!(
        "{}\nCommit: {}\nBuilt: {} (rustc {})\nFeatures: {}\nTarget: {}",
        meta.version, meta.git_sha, meta.build_time, meta.rustc, meta.features, meta.target
    )
}
