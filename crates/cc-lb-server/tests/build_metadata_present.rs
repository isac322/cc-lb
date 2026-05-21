use cc_lb_server::build_meta::BuildMeta;

#[test]
fn build_metadata_present() {
    let meta = BuildMeta::current();
    assert!(!meta.version.is_empty());
    assert!(!meta.git_sha.is_empty());
    assert!(!meta.build_time.is_empty());
    assert!(!meta.rustc.is_empty());
    assert!(!meta.features.is_empty());
    assert!(!meta.target.is_empty());
}
