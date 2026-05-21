use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BuildMeta {
    pub version: String,
    pub git_sha: String,
    pub build_time: String,
    pub rustc: String,
    pub features: String,
    pub target: String,
}

impl BuildMeta {
    pub fn current() -> Self {
        Self {
            version: option_env!("CC_LB_VERSION")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_owned(),
            git_sha: option_env!("CC_LB_GIT_SHA").unwrap_or("unknown").to_owned(),
            build_time: option_env!("CC_LB_BUILD_TIME")
                .unwrap_or("unknown")
                .to_owned(),
            rustc: option_env!("CC_LB_RUSTC").unwrap_or("unknown").to_owned(),
            features: option_env!("CC_LB_FEATURES")
                .unwrap_or("unknown")
                .to_owned(),
            target: option_env!("CC_LB_TARGET").unwrap_or("unknown").to_owned(),
        }
    }
}
