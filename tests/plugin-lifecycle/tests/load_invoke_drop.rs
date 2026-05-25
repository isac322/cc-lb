use std::sync::Arc;

use cc_lb_runtime_extism::ExtismRuntime;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_new_clone_and_drop_releases_references() {
    let runtime = Arc::new(ExtismRuntime::new());
    let weak = Arc::downgrade(&runtime);

    let clones = (0..64).map(|_| runtime.clone()).collect::<Vec<_>>();
    for clone in clones {
        let handle = tokio::spawn(async move {
            drop(clone);
        });
        handle.await.expect("runtime drop task joins");
    }

    drop(runtime);
    assert!(weak.upgrade().is_none(), "runtime still has references");
}
