use std::sync::Arc;

use cc_lb_storage_api::Storage;

fn _accepts_arc_dyn_storage(_: Arc<dyn Storage>) {}
fn _accepts_box_dyn_storage(_: Box<dyn Storage>) {}
