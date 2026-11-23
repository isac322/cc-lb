use std::ops::Deref;

use serde::Serialize;

pub use cc_lb_runtime_sync::build_meta::init;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct BuildMeta(pub cc_lb_runtime_sync::build_meta::BuildMeta);

impl Deref for BuildMeta {
    type Target = cc_lb_runtime_sync::build_meta::BuildMeta;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl BuildMeta {
    pub fn current() -> Self {
        Self(cc_lb_runtime_sync::build_meta::BuildMeta::current())
    }

    pub fn admin_build_info(&self) -> cc_lb_admin::AdminBuildInfo {
        cc_lb_admin::AdminBuildInfo {
            git_sha: self.0.git_sha,
            rustc: self.0.rustc,
            profile: self.0.profile,
            target: self.0.target,
        }
    }
}
