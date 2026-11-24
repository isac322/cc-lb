pub use cc_lb_runtime_sync::build_meta::format_version;

pub fn compact_version() -> String {
    cc_lb_runtime_sync::build_meta::compact_version("cc-lb-admin")
}
