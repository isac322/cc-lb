use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BuildMeta {
    pub version: String,
    pub git_sha: &'static str,
    pub build_time: String,
    pub rustc: &'static str,
    pub features: String,
    pub target: &'static str,
    pub profile: &'static str,
}

impl BuildMeta {
    pub fn current() -> Self {
        Self {
            version: option_env!("CC_LB_VERSION")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_owned(),
            git_sha: option_env!("CC_LB_GIT_SHA").unwrap_or("unknown"),
            build_time: option_env!("CC_LB_BUILD_TIME")
                .unwrap_or("unknown")
                .to_owned(),
            rustc: option_env!("CC_LB_RUSTC").unwrap_or("unknown"),
            features: option_env!("CC_LB_FEATURES")
                .unwrap_or("unknown")
                .to_owned(),
            target: option_env!("CC_LB_TARGET").unwrap_or("unknown"),
            profile: option_env!("CC_LB_PROFILE").unwrap_or("unknown"),
        }
    }

    pub fn admin_build_info(&self) -> cc_lb_admin::AdminBuildInfo {
        cc_lb_admin::AdminBuildInfo {
            git_sha: self.git_sha,
            rustc: self.rustc,
            profile: self.profile,
            target: self.target,
        }
    }
}
